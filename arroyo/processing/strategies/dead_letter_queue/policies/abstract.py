import base64
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Sequence, Union

Serializable = Union[str, bytes]


@dataclass(frozen=True)
class InvalidMessage:
    payload: Serializable
    timestamp: datetime
    reason: Optional[str] = None
    original_topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    def to_dict(self) -> Mapping[str, str]:
        if isinstance(self.payload, bytes):
            try:
                decoded = self.payload.decode("utf-8")
            except UnicodeDecodeError:
                decoded = base64.b64encode(self.payload).decode("utf-8")
        else:
            decoded = self.payload
        return {
            "payload": decoded,
            "timestamp": str(self.timestamp),
            "reason": str(self.reason),
            "original_topic": str(self.original_topic),
            "partition": str(self.partition),
            "offset": str(self.offset),
        }


class InvalidMessages(Exception):
    """
    An exception to be thrown to pass bad messages to the DLQ
    so they are handled correctly.
    """

    def __init__(
        self,
        messages: Sequence[InvalidMessage],
    ):
        self.messages = messages


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
