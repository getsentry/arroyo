from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from arroyo.types import Message, TPayload


class InvalidMessage(Exception):
    def __init__(self, message: Message[TPayload], topic: Optional[str] = None):
        self.message = message
        self.topic = topic or "unknown"
        self.timestamp = str(datetime.now())

    def __str__(self) -> str:
        return (
            f"Invalid Message originally produced to: {self.topic}\n"
            f"Exception thrown at: {self.timestamp}\n"
            f"Message: {self.message}"
        )


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
