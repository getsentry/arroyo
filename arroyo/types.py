from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Mapping, Protocol, TypeVar


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
    Represents a single message within a partition.
    """

    __slots__ = ["partition", "offset", "payload", "timestamp"]

    partition: Partition
    offset: int
    payload: TPayload
    timestamp: datetime

    def __init__(
        self,
        partition: Partition,
        offset: int,
        payload: TPayload,
        timestamp: datetime,
    ) -> None:
        self.partition = partition
        self.offset = offset
        self.payload = payload
        self.timestamp = timestamp

    def __repr__(self) -> str:
        # XXX: Field values can't be excluded from ``__repr__`` with
        # ``dataclasses.field(repr=False)`` as this class is defined with
        # ``__slots__`` for performance reasons. The class variable names
        # would conflict with the instance slot names, causing an error.
        return f"{type(self).__name__}(partition={self.partition!r}, offset={self.offset!r})"

    @property
    def next_offset(self) -> int:
        return self.offset + 1

    @property
    def position_to_commit(self) -> Position:
        return Position(self.next_offset, self.timestamp)


@dataclass(frozen=True)
class Position:
    __slots__ = ["offset", "timestamp"]
    offset: int
    timestamp: datetime


class Commit(Protocol):
    def __call__(
        self, positions: Mapping[Partition, Position], force: bool = False
    ) -> None:
        pass
