import logging
import time
from typing import Callable, MutableMapping, Optional, Union, cast

from arroyo.commit import CommitPolicy, CommitPolicyState
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import (
    FILTERED_PAYLOAD,
    FilteredPayload,
    Message,
    Partition,
    TStrategyPayload,
    Value,
)

logger = logging.getLogger(__name__)


class FilterStep(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    """
    Determines if a message should be submitted to the next processing step.

    `FilterStep` takes a callback, `function`, and if that callback returns
    `False`, the message is dropped.

    Sometimes that behavior is not actually desirable because streams of
    messages is what makes the consumer commit in regular intervals. If you
    filter 100% of messages for a period of time, your consumer may not commit
    its offsets as a result.

    For that scenario, you can pass your `CommitPolicy` to `FilterStep`. That
    will cause `FilterStep` to emit "sentinel messages" that contain no
    payload, but only carry forward partition offsets for later strategies to
    commit. Those messages have a payload of type `FilteredPayload`.

    For that reason, basically every strategy needs to be able to handle
    `Message[Union[FilteredPayload, T]]` instead of `Message[T]`, i.e. it needs
    to subtype `ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]`.
    If it doesn't, and rather just handles the regular `Message[T]`, it cannot
    be composed with this step, and many other default strategies of arroyo.

    If no `CommitPolicy` is passed, no "sentinel messages" are emitted and
    downstream steps do not have to deal with such messages (despite the type
    system telling them so).
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], bool],
        next_step: ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
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

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed

        now = time.time()

        if not isinstance(message.payload, FilteredPayload) and self.__test_function(
            cast(Message[TStrategyPayload], message)
        ):
            for partition in message.committable:
                self.__uncommitted_offsets.pop(partition, None)
            self.__next_step.submit(message)
        elif self.__commit_policy_state is not None:
            self.__uncommitted_offsets.update(message.committable)

        policy = self.__commit_policy_state

        if policy is not None and policy.should_commit(now, message.committable):
            self.__flush_uncommitted_offsets(now)

    def __flush_uncommitted_offsets(self, now: float) -> None:
        if not self.__uncommitted_offsets:
            return

        new_message: Message[Union[FilteredPayload, TStrategyPayload]] = Message(
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
