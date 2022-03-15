from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import TPayload


class RaiseInvalidMessagePolicy(DeadLetterQueuePolicy[TPayload]):
    def handle_invalid_message(self, e: InvalidMessage) -> None:
        raise e
