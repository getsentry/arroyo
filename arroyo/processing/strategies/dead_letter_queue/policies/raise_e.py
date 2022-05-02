from typing import Optional

from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
)


class RaiseInvalidMessagePolicy(DeadLetterQueuePolicy):
    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        raise e

    def join(self, timeout: Optional[float]) -> None:
        return
