from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import (
    Any,
    Deque,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
)

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import (
    FILTERED_PAYLOAD,
    BrokerValue,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    TStrategyPayload,
    Value,
)


class InvalidMessage(Exception):
    """
    InvalidMessage should be raised if a message is not valid for processing and
    should not be retried. It will be placed a DLQ if one is configured.

    It can be raised from the submit, poll or join methods of any processing strategy.

    Once a filtered message is forwarded to the next step, `needs_commit` should be set to False,
    in order to prevent multiple filtered messages from being forwarded for a single invalid message.
    """

    def __init__(self, partition: Partition, offset: int) -> None:
        self.partition = partition
        self.offset = offset
        self.needs_commit = True


class InvalidMessageOutOfOrder(Exception):
    """
    Fatal exception. Strategies are not permitted to raise invalid messages out of order.
    """

    pass


@dataclass(frozen=True)
class DlqLimit:
    """
    Defines any limits that should be placed on the number of messages that are
    forwarded to the DLQ. This exists to prevent 100% of messages from going into
    the DLQ if something is misconfigured or bad code is deployed. In this scenario,
    it may be preferable to stop processing messages altogether and deploy a fix
    rather than rerouting every message to the DLQ.

    The ratio and max_consecutive_count are counted on a per-partition basis.
    """

    max_invalid_ratio: Optional[float] = None
    max_consecutive_count: Optional[int] = None


class DlqLimitState:
    """
    Keeps track of the current state of the DLQ limit. This is used to determine
    when messages should be rejected.
    """

    def __init__(
        self,
        limit: DlqLimit,
        valid_messages: Optional[Mapping[Partition, int]] = None,
        invalid_messages: Optional[Mapping[Partition, int]] = None,
        invalid_consecutive_messages: Optional[Mapping[Partition, int]] = None,
    ) -> None:
        self.__limit = limit
        self.__valid_messages = valid_messages or {}
        self.__invalid_messages = invalid_messages or {}
        self.__invalid_consecutive_messages = invalid_consecutive_messages or {}

    def should_accept(self, value: BrokerValue[TStrategyPayload]) -> bool:
        if self.__limit.max_invalid_ratio is not None:
            invalid = self.__invalid_messages.get(value.partition, 0)
            valid = self.__valid_messages.get(value.partition, 0)

            ratio = invalid / valid
            if ratio > self.__limit.max_invalid_ratio:
                return False

        if self.__limit.max_consecutive_count is not None:
            invalid_consecutive_messages = self.__invalid_consecutive_messages.get(
                value.partition, 0
            )

            if invalid_consecutive_messages > self.__limit.max_consecutive_count:
                return False

        return True


class DlqProducer(ABC, Generic[TStrategyPayload]):
    @abstractmethod
    def produce(
        self, value: BrokerValue[TStrategyPayload]
    ) -> Future[BrokerValue[TStrategyPayload]]:
        """
        Produce a message to DLQ.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def build_initial_state(cls, limit: DlqLimit) -> DlqLimitState:
        """
        Called on consumer start to build the current DLQ state
        """
        raise NotImplementedError


class NoopDlqProducer(DlqProducer[Any]):
    """
    Drops all invalid messages
    """

    def produce(
        self, value: BrokerValue[KafkaPayload]
    ) -> Future[BrokerValue[KafkaPayload]]:
        future: Future[BrokerValue[KafkaPayload]] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(value)
        return future

    @classmethod
    def build_initial_state(cls, limit: DlqLimit) -> DlqLimitState:
        return DlqLimitState(limit)


class KafkaDlqProducer(DlqProducer[KafkaPayload]):
    """
    KafkaDLQProducer forwards invalid messages to a Kafka topic

    Two additional fields are added to the headers of the Kafka message
    "original_partition": The partition of the original message
    "original_offset": The offset of the original message
    """

    def __init__(self, producer: Producer[KafkaPayload], topic: Topic) -> None:
        self.__producer = producer
        self.__topic = topic

    def produce(
        self, value: BrokerValue[KafkaPayload]
    ) -> Future[BrokerValue[KafkaPayload]]:
        value.payload.headers.append(
            ("original_partition", f"{value.partition.index}".encode("utf-8"))
        )
        value.payload.headers.append(
            ("original_offset", f"{value.offset}".encode("utf-8"))
        )

        return self.__producer.produce(self.__topic, value.payload)

    @classmethod
    def build_initial_state(cls, limit: DlqLimit) -> DlqLimitState:
        # TODO: Build the current state by reading the DLQ topic in Kafka
        return DlqLimitState(limit)


@dataclass(frozen=True)
class DlqPolicy(Generic[TStrategyPayload]):
    """
    DLQ policy defines the DLQ configuration, and is passed to the stream processor
    upon creation of the consumer. It consists of the DLQ producer implementation and
    any limits that should be applied.
    """

    producer: DlqProducer[TStrategyPayload]
    limit: DlqLimit


class BufferedMessages(Generic[TStrategyPayload]):
    """
    Manages a buffer of messages that are pending commit. This is used to retreive raw messages
    in case they need to be placed in the DLQ.
    """

    def __init__(self, dlq_policy: Optional[DlqPolicy[TStrategyPayload]]) -> None:
        self.__dlq_policy = dlq_policy
        self.__buffered_messages: MutableMapping[
            Partition, Deque[BrokerValue[TStrategyPayload]]
        ] = defaultdict(deque)

    def append(self, message: BrokerValue[TStrategyPayload]) -> None:
        """
        Append a message to DLQ buffer
        """
        if self.__dlq_policy is not None:
            self.__buffered_messages[message.partition].append(message)

    def pop(
        self, partition: Partition, offset: int
    ) -> Optional[BrokerValue[TStrategyPayload]]:
        """
        Return the message at the given offset or None if it is not found in the buffer.
        Messages up to the offset for the given partition are removed.
        """
        if self.__dlq_policy is not None:
            buffered = self.__buffered_messages[partition]

            while buffered:
                if buffered[0].offset == offset:
                    return buffered.popleft()
                if buffered[0].offset > offset:
                    break
                self.__buffered_messages[partition].popleft()

            return None

        return None

    def reset(self) -> None:
        """
        Reset the buffer.
        """
        self.__buffered_messages = defaultdict(deque)


class InvalidMessageState:
    """
    This class is designed to be used internally by processing strategies to
    store invalid messages pending commit.

    If strict_ordering is True, an exception is raised if any invalid messages
    are not in order.
    """

    def __init__(self, strict_ordering: bool = True) -> None:
        self.__strict_ordering = strict_ordering
        self.__invalid_messages: MutableSequence[InvalidMessage] = []

    def __len__(self) -> int:
        return len(self.__invalid_messages)

    def append(self, invalid_message: InvalidMessage) -> None:
        """
        Mark the invalid message as committed so other strategies in the pipeline
        don't try to commit the same offset when the exception is reraised.
        """
        invalid_message.needs_commit = False
        self.__invalid_messages.append(invalid_message)

    def build(self) -> Optional[Message[FilteredPayload]]:
        """
        Returns a filtered message to be committed down the line. If there is
        nothing to commit, return None.
        """
        committable: MutableMapping[Partition, int] = {}
        for m in self.__invalid_messages:
            next_offset = m.offset + 1
            if self.__strict_ordering and m.partition in committable:
                if next_offset < committable[m.partition]:
                    raise InvalidMessageOutOfOrder

            if m.needs_commit:
                committable[m.partition] = next_offset

        if committable:
            return Message(Value(FILTERED_PAYLOAD, committable))

        return None

    def reset(self) -> None:
        self.__invalid_messages = []
