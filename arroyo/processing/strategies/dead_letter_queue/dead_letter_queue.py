import logging
from typing import Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
)
from arroyo.types import Message, TPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)

RECEIVED_MESSAGE_METRIC = "dlq.received_message"


class DeadLetterQueue(ProcessingStep[TPayload]):
    """
    DLQ Processing Step.

    Attempts to submit a given message to the next processing step,
    handling `InvalidMessages` according to the given Policy.
    """

    def __init__(
        self,
        next_step: ProcessingStep[TPayload],
        policy: DeadLetterQueuePolicy,
    ) -> None:
        self.__next_step = next_step
        self.__policy = policy
        self.__closed = False
        self.__metrics = get_metrics()

    def poll(self) -> None:
        try:
            self.__next_step.poll()
        except InvalidMessages as e:
            self._handle_invalid_messages(e)

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed
        try:
            self.__next_step.submit(message)
        except InvalidMessages as e:
            self._handle_invalid_messages(e)

    def _handle_invalid_messages(self, e: InvalidMessages) -> None:
        self.__metrics.increment(RECEIVED_MESSAGE_METRIC, len(e.messages))
        self.__policy.handle_invalid_messages(e)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()
        logger.debug("Terminating %r...", self.__policy)
        self.__policy.terminate()
        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__policy.join(timeout)
        self.__next_step.close()
        self.__next_step.join(timeout)
