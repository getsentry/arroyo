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
    Can either represent a single message from a Kafka broker (BrokerPayload)
    or something else, such as a number of messages grouped together for a
    batch processing step (Payload).
    """

    __slots__ = ["data"]

    data: BasePayload[TPayload]

    def __init__(
        self,
        data: BasePayload[TPayload],
    ) -> None:
        self.data = data

    def __repr__(self) -> str:
        # XXX: Field values can't be excluded from ``__repr__`` with
        # ``dataclasses.field(repr=False)`` as this class is defined with
        # ``__slots__`` for performance reasons. The class variable names
        # would conflict with the instance slot names, causing an error.
        return f"{type(self).__name__}({self.committable!r})"

    @property
    def payload(self) -> TPayload:
        return self.data.payload

    @property
    def committable(self) -> Mapping[Partition, Position]:
        return self.data.committable


@dataclass(frozen=True)
class Position:
    __slots__ = ["offset", "timestamp"]
    offset: int
    timestamp: datetime

    def __getstate__(self) -> dict[str, int | datetime]:
        return dict(
            (slot, getattr(self, slot))
            for slot in self.__slots__
            if hasattr(self, slot)
        )

    def __setstate__(self, state: dict[str, int | datetime]) -> None:
        for slot, value in state.items():
            object.__setattr__(self, slot, value)


class BasePayload(Generic[TPayload]):
    @property
    def payload(self) -> TPayload:
        raise NotImplementedError()

    @property
    def committable(self) -> Mapping[Partition, Position]:
        raise NotImplementedError()

    def replace(self, value: TReplaced) -> BasePayload[TReplaced]:
        raise NotImplementedError


@dataclass(unsafe_hash=True)
class Payload(BasePayload[TPayload]):
    """
    Any other payload that may not map 1:1 to a single message from a
    consumer. May represent a batch spanning many partitions.
    """

    __slots__ = ["__payload", "__committable"]
    __payload: TPayload
    __committable: Mapping[Partition, Position]

    def __init__(
        self, payload: TPayload, committable: Mapping[Partition, Position]
    ) -> None:
        self.__payload = payload
        self.__committable = committable

    @property
    def payload(self) -> TPayload:
        return self.__payload

    @property
    def committable(self) -> Mapping[Partition, Position]:
        return self.__committable

    def replace(self, value: TReplaced) -> BasePayload[TReplaced]:
        return Payload(value, self.__committable)


@dataclass(unsafe_hash=True)
class BrokerPayload(BasePayload[TPayload]):
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

    def replace(self, value: TReplaced) -> BasePayload[TReplaced]:
        return BrokerPayload(value, self.partition, self.offset, self.timestamp)

    @property
    def payload(self) -> TPayload:
        return self.__payload

    @property
    def committable(self) -> Mapping[Partition, Position]:
        return {self.partition: Position(self.next_offset, self.timestamp)}

    @property
    def next_offset(self) -> int:
        return self.offset + 1


class Commit(Protocol):
    def __call__(
        self, positions: Mapping[Partition, Position], force: bool = False
    ) -> None:
        pass
