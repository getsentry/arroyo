from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Sequence, Union

Serializable = Union[str, bytes]


class InvalidMessage(Exception):
    """
    An exception to be thrown to pass a bad message to the DLQ.

    A reason can be optionally passed describing why the message
    is bad.

    If the original topic the message was produced to is known,
    that should be passed along via this exception.
    """

    def __init__(
        self,
        message: Serializable,
        reason: Optional[str] = None,
        original_topic: Optional[str] = None,
    ):
        self.message = message
        self.reason = reason or "unknown"
        self.topic = original_topic or "unknown"
        self.timestamp = str(datetime.now())

    def __str__(self) -> str:
        return (
            f"Invalid Message originally produced to: {self.topic}\n"
            f"Reason: {self.reason}\n"
            f"Exception thrown at: {self.timestamp}\n"
            f"Message: {str(self.message)}"
        )


class InvalidBatchedMessages(Exception):
    """
    An exception to be thrown to pass batched invalid messages.
    to the DLQ. Generally should only be used by batching
    strategies which collect multiple `InvalidMessage` exceptions.
    """

    def __init__(self, exceptions: Sequence[InvalidMessage]):
        self.exceptions = exceptions


class DeadLetterQueuePolicy(ABC):
    """
    A DLQ Policy defines how to handle invalid messages.
    """

    @abstractmethod
    def handle_invalid_messages(self, e: InvalidBatchedMessages) -> None:
        """
        Decide what to do with a batch of invalid messages.
        """
        pass
