from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Sequence, Union

Serializable = Union[str, bytes]


class InvalidMessages(Exception):
    """
    An exception to be thrown to pass bad messages to the DLQ
    so they are handled correctly.

    If the original topic the message(s) were produced to is known,
    that should be passed along via this exception.
    """

    def __init__(
        self,
        messages: Sequence[Serializable],
        reason: Optional[str] = None,
        original_topic: Optional[str] = None,
    ):
        self.messages = messages
        self.reason = reason or "unknown"
        self.topic = original_topic or "unknown"
        self.timestamp = str(datetime.now())

    def __str__(self) -> str:
        return (
            f"Invalid Message originally produced to: {self.topic}\n"
            f"Reason: {self.reason}\n"
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
        raise NotImplementedError()

    @abstractmethod
    def join(self, timeout: Optional[float]) -> None:
        """
        Cleanup any asynchronous tasks that may be running.
        """
        raise NotImplementedError()
