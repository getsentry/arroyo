import logging
from typing import Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import Message, TPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)


class DeadLetterQueue(ProcessingStep[TPayload]):
    """
    DLQ Processing Step.

    Attempts to submit a given message to the next processing step,
    handling an `InvalidMessage` according to the given Policy.
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
        except InvalidMessage as e:
            self.__metrics.increment("dlq.received_message")
            self.__policy.handle_invalid_message(e)

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed
        try:
            self.__next_step.submit(message)
        except InvalidMessage as e:
            self.__metrics.increment("dlq.received_message")
            self.__policy.handle_invalid_message(e)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()
        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
