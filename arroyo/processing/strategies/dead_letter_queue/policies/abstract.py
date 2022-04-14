from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional, Sequence


class InvalidMessages(Exception):
    """
    An exception to be thrown to pass bad messages to the DLQ
    so they are handled correctly.

    If the original topic the message(s) were produced to is known,
    that should be passed along via this exception.
    """

    def __init__(self, messages: Sequence[Any], original_topic: Optional[str] = None):
        self.messages = messages
        self.topic = original_topic or "unknown"
        self.timestamp = str(datetime.now())

    def __str__(self) -> str:
        return (
            f"Invalid Message originally produced to: {self.topic}\n"
            f"Exception thrown at: {self.timestamp}\n"
            f"Message(s): {self.messages}"
        )


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
