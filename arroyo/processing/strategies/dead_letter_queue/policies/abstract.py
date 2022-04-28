import base64
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Sequence, Tuple, Union

from arroyo.backends.kafka.consumer import Headers

SerializedPayload = Union[str, bytes]
DeserializedHeaders = Sequence[Tuple[str, str]]
JSONSerializable = Optional[Union[str, int, datetime, DeserializedHeaders]]


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

    def _deserialize_payload(self, payload: SerializedPayload) -> str:
        if isinstance(payload, bytes):
            return self._deserialize_bytes(payload)
        return payload

    def _deserialize_bytes(self, value: bytes) -> str:
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            decoded = "(base64) " + base64.b64encode(value).decode("utf-8")
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
        return {"payload": super()._deserialize_payload(self.payload)}


@dataclass(frozen=True)
class InvalidKafkaMessage(InvalidMessage):
    """
    A dataclass to generally represent a bad Kafka message.
    A `reason` can be provided for debugging purposes.
    """

    payload: SerializedPayload
    timestamp: datetime
    topic: str
    consumer_group: str
    partition: int
    offset: int
    headers: Headers
    key: Optional[bytes] = None
    reason: Optional[str] = None

    def to_dict(self) -> Mapping[str, JSONSerializable]:
        decoded_key: Optional[str] = None
        if self.key is not None:
            decoded_key = super()._deserialize_bytes(self.key)
        return {
            "payload": super()._deserialize_payload(self.payload),
            "timestamp": self.timestamp,
            "topic": self.topic,
            "consumer_group": self.consumer_group,
            "partition": self.partition,
            "offset": self.offset,
            "headers": self.__deserialize_headers(),
            "key": decoded_key,
            "reason": self.reason,
        }

    def __deserialize_headers(self) -> DeserializedHeaders:
        return [(key, self._deserialize_bytes(value)) for (key, value) in self.headers]


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
