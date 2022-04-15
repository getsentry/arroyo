from typing import Optional

from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
)
from arroyo.utils.metrics import get_metrics


class IgnoreInvalidMessagePolicy(DeadLetterQueuePolicy):
    def __init__(self) -> None:
        self.__metrics = get_metrics()

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        self.__metrics.increment("dlq.dropped_messages", len(e.messages))

    def join(self, timeout: Optional[float]) -> None:
        return
