from abc import ABC, abstractmethod

from arroyo.types import Message, TPayload


class InvalidMessage(Exception):
    def __init__(self, message: Message[TPayload]):
        self.message = message

    def __str__(self) -> str:
        return f"Invalid Message: {self.message}"


class DeadLetterQueuePolicy(ABC):
    """
    A DLQ Policy defines how to handle an invalid message.
    """

    @abstractmethod
    def handle_invalid_message(self, e: InvalidMessage) -> None:
        """
        Decide what to do with an invalid message.
        """
        pass
