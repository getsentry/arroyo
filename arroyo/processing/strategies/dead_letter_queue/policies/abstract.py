import base64
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Sequence, Union

Serializable = Union[str, bytes]


@dataclass(frozen=True)
class InvalidMessage:
    """
    A dataclass to generally represent a bad Kafka message.
    The Kafka specific fields (offset etc) are all optional
    since this can be used to pass almost anything to the
    DLQ.

    A `reason` can be provided for debugging purposes. If a
    produce policy is configured on the relevant DLQ, a message
    in the form returned by `to_dict()` will be produced via
    the policy.

    An `InvalidMessages` exception should be raised containing
    one or more objects of this class in order to actually
    pass data to the DLQ.
    """

    payload: Serializable
    timestamp: datetime
    reason: Optional[str] = None
    consumer_group: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    def to_dict(self) -> Mapping[str, Optional[Union[str, int, datetime]]]:
        if isinstance(self.payload, bytes):
            try:
                decoded = self.payload.decode("utf-8")
            except UnicodeDecodeError:
                decoded = "(base64) " + base64.b64encode(self.payload).decode("utf-8")
        else:
            decoded = self.payload
        return {
            "payload": decoded,
            "timestamp": self.timestamp,
            "reason": self.reason,
            "consumer_group": self.consumer_group,
            "partition": self.partition,
            "offset": self.offset,
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
