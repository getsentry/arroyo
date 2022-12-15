from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Mapping, Protocol, TypeVar

TReplaced = TypeVar("TReplaced")


@dataclass(order=True, unsafe_hash=True)
class Topic:
    __slots__ = ["name"]

    name: str

    def __contains__(self, partition: Partition) -> bool:
        return partition.topic == self


@dataclass(order=True, unsafe_hash=True)
class Partition:
    __slots__ = ["topic", "index"]

    topic: Topic
    index: int


TPayload = TypeVar("TPayload")


@dataclass(unsafe_hash=True)
class Message(Generic[TPayload]):
    """
    Contains a payload and partitions to be committed after processing.
    Can either represent a single message from a Kafka broker (BrokerValue)
    or something else, such as a number of messages grouped together for a
    batch processing step (Payload).
    """

    __slots__ = ["value"]

    value: BaseValue[TPayload]

    def __init__(
        self,
        value: BaseValue[TPayload],
    ) -> None:
        self.value = value

    def __repr__(self) -> str:
        # XXX: Field values can't be excluded from ``__repr__`` with
        # ``dataclasses.field(repr=False)`` as this class is defined with
        # ``__slots__`` for performance reasons. The class variable names
        # would conflict with the instance slot names, causing an error.
        return f"{type(self).__name__}({self.committable!r})"

    @property
    def payload(self) -> TPayload:
        return self.value.payload

    @property
    def committable(self) -> Mapping[Partition, int]:
        return self.value.committable

    def replace(self, payload: TReplaced) -> Message[TReplaced]:
        return Message(self.value.replace(payload))


class BaseValue(Generic[TPayload]):
    @property
    def payload(self) -> TPayload:
        raise NotImplementedError()

    @property
    def committable(self) -> Mapping[Partition, int]:
        raise NotImplementedError()

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        raise NotImplementedError


@dataclass(unsafe_hash=True)
class Value(BaseValue[TPayload]):
    """
    Any other payload that may not map 1:1 to a single message from a
    consumer. May represent a batch spanning many partitions.
    """

    __slots__ = ["__payload", "__committable"]
    __payload: TPayload
    __committable: Mapping[Partition, int]

    def __init__(self, payload: TPayload, committable: Mapping[Partition, int]) -> None:
        self.__payload = payload
        self.__committable = committable

    @property
    def payload(self) -> TPayload:
        return self.__payload

    @property
    def committable(self) -> Mapping[Partition, int]:
        return self.__committable

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        return Value(value, self.__committable)


@dataclass(unsafe_hash=True)
class BrokerValue(BaseValue[TPayload]):
    """
    A payload received from the consumer or producer after it is done producing.
    Partition, offset, and timestamp values are present.
    """

    __slots__ = ["__payload", "partition", "offset", "timestamp"]
    __payload: TPayload
    partition: Partition
    offset: int
    timestamp: datetime

    def __init__(
        self, payload: TPayload, partition: Partition, offset: int, timestamp: datetime
    ):
        self.__payload = payload
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        return BrokerValue(value, self.partition, self.offset, self.timestamp)

    @property
    def payload(self) -> TPayload:
        return self.__payload

    @property
    def committable(self) -> Mapping[Partition, int]:
        return {self.partition: self.next_offset}

    @property
    def next_offset(self) -> int:
        return self.offset + 1


class Commit(Protocol):
    def __call__(self, offsets: Mapping[Partition, int], force: bool = False) -> None:
        pass
