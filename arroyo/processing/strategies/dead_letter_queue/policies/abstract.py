import base64
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Union

from arroyo.backends.kafka.consumer import Headers
from arroyo.utils.codecs import Encoder

SerializedPayload = Union[str, bytes]

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class JSONMessageEncoder(Encoder[bytes, Mapping[str, Any]]):
    """
    JSON message encoder to support `bytes` and `datetime` objects.
    """

    def __default(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime(DATE_TIME_FORMAT)
        elif isinstance(value, bytes):
            return self.__deserialize_bytes(value)
        else:
            raise TypeError

    def __deserialize_bytes(self, value: bytes) -> str:
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            decoded = "(base64) " + base64.b64encode(value).decode("utf-8")
        return decoded

    def encode(self, value: Mapping[str, Any]) -> bytes:
        return json.dumps(value, default=self.__default).encode("utf-8")


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

    def to_bytes(self) -> bytes:
        """
        JSON encoded representation of this Invalid Message.
        """
        return JSONMessageEncoder().encode(self.__dict__)


@dataclass(frozen=True)
class InvalidRawMessage(InvalidMessage):
    """
    A dataclass to generally represent any kind of bad message.
    A `reason` can be provided for debugging purposes.
    """

    payload: SerializedPayload
    reason: Optional[str] = None


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
