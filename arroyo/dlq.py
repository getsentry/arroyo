from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Generic, Mapping, Optional

from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.types import BrokerValue, Partition, Topic, TStrategyPayload


class InvalidMessage(Exception):
    """
    InvalidMessage should be raised if a message is not valid for processing and
    should not be retried. It will be placed a DLQ if one is configured.

    It can be raised from the submit, poll or join methods of any processing strategy.
    """

    def __init__(self, partition: Partition, offset: int) -> None:
        self.__partition = partition
        self.__offset = offset


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
    """

    def __init__(self, producer: KafkaProducer, topic: Topic) -> None:
        self.__producer = producer
        self.__topic = topic

    def produce(
        self, value: BrokerValue[KafkaPayload]
    ) -> Future[BrokerValue[KafkaPayload]]:
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
