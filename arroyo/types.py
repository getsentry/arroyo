from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Mapping, Optional, Protocol, TypeVar

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


TMessagePayload = TypeVar("TMessagePayload", covariant=True)
TStrategyPayload = TypeVar("TStrategyPayload", contravariant=True)


class FilteredPayload:
    __slots__ = ()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, FilteredPayload)

    def __repr__(self) -> str:
        return "<FilteredPayload>"


FILTERED_PAYLOAD = FilteredPayload()


@dataclass(unsafe_hash=True)
class Message(Generic[TMessagePayload]):
    """
    Contains a payload and partitions to be committed after processing.
    Can either represent a single message from a Kafka broker (BrokerValue)
    or something else, such as a number of messages grouped together for a
    batch processing step (Payload).
    """

    __slots__ = ["value"]

    value: BaseValue[TMessagePayload]

    def __init__(
        self,
        value: BaseValue[TMessagePayload],
    ) -> None:
        self.value = value

    def __repr__(self) -> str:
        # XXX: Field values can't be excluded from ``__repr__`` with
        # ``dataclasses.field(repr=False)`` as this class is defined with
        # ``__slots__`` for performance reasons. The class variable names
        # would conflict with the instance slot names, causing an error.

        if type(self.payload) in (float, int, bool, FilteredPayload):
            # For the case where value is a float or int, the repr is small and
            # therefore safe. This is very useful in tests.
            #
            # To prevent any nasty surprises with custom integer subtypes that
            # may have modified reprs we do not use isinstance.
            return f"{type(self).__name__}({self.payload}, {self.committable!r})"

        # For any other type we cannot be sure the repr is performant.
        return f"{type(self).__name__}({self.committable!r})"

    @property
    def payload(self) -> TMessagePayload:
        return self.value.payload

    @property
    def payload_unfiltered(self) -> TMessagePayload:
        payload = self.payload
        assert not isinstance(payload, FilteredPayload)
        return payload

    @property
    def committable(self) -> Mapping[Partition, int]:
        return self.value.committable

    @property
    def timestamp(self) -> Optional[datetime]:
        return self.value.timestamp

    def replace(self, payload: TReplaced) -> Message[TReplaced]:
        return Message(self.value.replace(payload))


class BaseValue(Generic[TMessagePayload]):
    @property
    def payload(self) -> TMessagePayload:
        raise NotImplementedError()

    @property
    def committable(self) -> Mapping[Partition, int]:
        raise NotImplementedError()

    @property
    def timestamp(self) -> Optional[datetime]:
        raise NotImplementedError()

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        raise NotImplementedError


@dataclass(unsafe_hash=True)
class Value(BaseValue[TMessagePayload]):
    """
    Any other payload that may not map 1:1 to a single message from a
    consumer. May represent a batch spanning many partitions.
    """

    __slots__ = ["__payload", "__committable"]
    __payload: TMessagePayload
    __committable: Mapping[Partition, int]
    __timestamp: Optional[datetime]

    def __init__(
        self,
        payload: TMessagePayload,
        committable: Mapping[Partition, int],
        timestamp: Optional[datetime] = None,
    ) -> None:
        self.__payload = payload
        self.__committable = committable
        self.__timestamp = timestamp

    @property
    def payload(self) -> TMessagePayload:
        return self.__payload

    @property
    def committable(self) -> Mapping[Partition, int]:
        return self.__committable

    @property
    def timestamp(self) -> Optional[datetime]:
        return self.__timestamp

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        return Value(value, self.__committable, self.timestamp)


@dataclass(unsafe_hash=True)
class BrokerValue(BaseValue[TMessagePayload]):
    """
    A payload received from the consumer or producer after it is done producing.
    Partition, offset, and timestamp values are present.
    """

    __slots__ = ["__payload", "partition", "offset", "timestamp"]
    __payload: TMessagePayload
    partition: Partition
    offset: int
    timestamp: datetime

    def __init__(
        self,
        payload: TMessagePayload,
        partition: Partition,
        offset: int,
        timestamp: datetime,
    ):
        self.__payload = payload
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp

    def replace(self, value: TReplaced) -> BaseValue[TReplaced]:
        return BrokerValue(value, self.partition, self.offset, self.timestamp)

    @property
    def payload(self) -> TMessagePayload:
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
