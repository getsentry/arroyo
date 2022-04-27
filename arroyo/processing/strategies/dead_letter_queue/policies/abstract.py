import base64
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Sequence, Union

SerializedPayload = Union[str, bytes]
JSONSerializable = Optional[Union[str, int, datetime]]


class InvalidMessage(ABC):
    """
    A class representing a bad message to be passed to the DLQ.

    An `InvalidMessages` exception should be raised containing
    one or more `InvalidMessage` objects in order to actually
    pass data to the DLQ.

    If a produce policy is configured on the relevant DLQ, a
    message in the form returned by `to_dict()` will be produced
    via the policy.
    """

    @abstractmethod
    def to_dict(self) -> Mapping[str, JSONSerializable]:
        raise NotImplementedError

    def deserialize_payload(self, payload: SerializedPayload) -> str:
        if isinstance(payload, bytes):
            try:
                decoded = payload.decode("utf-8")
            except UnicodeDecodeError:
                decoded = "(base64) " + base64.b64encode(payload).decode("utf-8")
        else:
            decoded = payload
        return decoded


@dataclass(frozen=True)
class InvalidRawMessage(InvalidMessage):
    """
    A dataclass to generally represent any kind of bad message.
    A `reason` can be provided for debugging purposes.
    """

    payload: SerializedPayload
    reason: Optional[str] = None

    def to_dict(self) -> Mapping[str, JSONSerializable]:
        return {"payload": self.deserialize_payload(self.payload)}


@dataclass(frozen=True)
class InvalidKafkaMessage(InvalidMessage):
    """
    A dataclass to generally represent a bad Kafka message.
    A `reason` can be provided for debugging purposes.
    """

    payload: SerializedPayload
    timestamp: datetime
    reason: Optional[str] = None
    consumer_group: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    def to_dict(self) -> Mapping[str, JSONSerializable]:
        return {
            "payload": self.deserialize_payload(self.payload),
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
        messages: Sequence[InvalidKafkaMessage],
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
        pass
