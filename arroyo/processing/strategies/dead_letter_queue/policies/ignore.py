from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.utils.metrics import get_metrics


class IgnoreInvalidMessagePolicy(DeadLetterQueuePolicy):
    def __init__(self) -> None:
        self.__metrics = get_metrics()

    def handle_invalid_message(self, e: InvalidMessage) -> None:
        self.__metrics.increment("dlq.dropped_message")
