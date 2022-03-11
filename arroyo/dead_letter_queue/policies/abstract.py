from abc import ABC, abstractmethod
from typing import Generic

from arroyo.types import Message, TPayload


class InvalidMessage(Exception):
    pass


class DeadLetterQueuePolicy(ABC, Generic[TPayload]):
    """
    A DLQ Policy defines how to handle an invalid message.
    """

    @abstractmethod
    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        """
        Decide what to do with an invalid message.
        """
        pass