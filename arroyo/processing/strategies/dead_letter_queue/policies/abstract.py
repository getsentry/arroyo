from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence, Union

Serializable = Union[str, bytes]


@dataclass(frozen=True)
class InvalidMessage:
    payload: Serializable
    timestamp: datetime
    reason: str = "unknown"
    original_topic: str = "unknown"
    partition: int = -1
    offset: int = -1


class InvalidMessages(Exception):
    """
    An exception to be thrown to pass bad messages to the DLQ
    so they are handled correctly.
    """

    def __init__(self, messages: Sequence[InvalidMessage]):
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
        pass
