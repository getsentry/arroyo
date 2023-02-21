import logging
import time
from typing import Callable, MutableMapping, Optional, Union, cast

from arroyo.commit import CommitPolicy, CommitPolicyState
from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.types import (
    FILTERED_PAYLOAD,
    FilteredPayload,
    Message,
    Partition,
    TPayload,
    Value,
)

logger = logging.getLogger(__name__)


class FilterStep(ProcessingStep[Union[FilteredPayload, TPayload]]):
    """
    Determines if a message should be submitted to the next processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], bool],
        next_step: ProcessingStep[Union[FilteredPayload, TPayload]],
        commit_policy: Optional[CommitPolicy] = None,
    ):
        self.__test_function = function
        self.__next_step = next_step

        if commit_policy is not None:
            self.__commit_policy_state: Optional[
                CommitPolicyState
            ] = commit_policy.get_state_machine()
        else:
            self.__commit_policy_state = None

        self.__uncommitted_offsets: MutableMapping[Partition, int] = {}
        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        assert not self.__closed

        now = time.time()

        if not isinstance(message.payload, FilteredPayload) and self.__test_function(
            cast(Message[TPayload], message)
        ):
            if self.__commit_policy_state is not None:
                self.__flush_uncommitted_offsets(now)
            self.__next_step.submit(message)
        elif self.__commit_policy_state is not None:
            self.__uncommitted_offsets.update(message.committable)

            if self.__commit_policy_state.should_commit(now, message.committable):
                self.__flush_uncommitted_offsets(now)

    def __flush_uncommitted_offsets(self, now: float) -> None:
        if not self.__uncommitted_offsets:
            return

        new_message: Message[Union[FilteredPayload, TPayload]] = Message(
            Value(FILTERED_PAYLOAD, self.__uncommitted_offsets)
        )
        self.__next_step.submit(new_message)

        if self.__commit_policy_state is not None:
            self.__commit_policy_state.did_commit(now, self.__uncommitted_offsets)
        self.__uncommitted_offsets = {}

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__flush_uncommitted_offsets(time.time())
        self.__next_step.close()
        self.__next_step.join(timeout)
