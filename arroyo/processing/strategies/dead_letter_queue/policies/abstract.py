from abc import ABC, abstractmethod
from typing import Any, Sequence


class InvalidMessages(Exception):
    """
    An exception to be thrown to pass bad messages to the DLQ
    so they are handled correctly.
    """

    def __init__(self, messages: Sequence[Any]):
        self.messages = messages

    def __str__(self) -> str:
        return f"Invalid Message(s): {self.messages}"


class DeadLetterQueuePolicy(ABC):
    """
    A DLQ Policy defines how to handle invalid messages.
    """

    @abstractmethod
    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        """
        Decide what to do with invalid messages.
        """
        pass
