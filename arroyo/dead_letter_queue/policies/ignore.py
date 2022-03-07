from arroyo.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import Message, TPayload


class IgnoreInvalidMessagePolicy(DeadLetterQueuePolicy[TPayload]):
    def __init__(self) -> None:
        pass

    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        return
