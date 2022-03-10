from arroyo.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import Message, TPayload
from arroyo.utils.metrics import get_metrics


class IgnoreInvalidMessagePolicy(DeadLetterQueuePolicy[TPayload]):
    def __init__(self) -> None:
        self.__metrics = get_metrics()

    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        self.__metrics.increment("dlq.dropped_messages")
