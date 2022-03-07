from arroyo.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import Message, TPayload


class CountInvalidMessagePolicy(DeadLetterQueuePolicy[TPayload]):
    def __init__(self, limit: int) -> None:
        self.__limit = limit
        self.__count = 0

    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        self.__count += 1
        if self.__count > self.__limit:
            raise e
