from __future__ import annotations

from datetime import datetime
import logging

from concurrent.futures import Future


from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import (
    BrokerValue,
)

from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Topic
from concurrent.futures import Future
from arroyo.dlq import DlqProducer, DlqLimitState

logger = logging.getLogger(__name__)


class StaleMessage(Exception):
    """
    StaleMessage is raised for messages that are considered backlogged or delayed
    based on the routing logic (e.g., timestamps older than a threshold).
    These messages are valid but not suitable for real-time processing.
    """

    def __init__(self, partition, offset, needs_commit=True):
        self.partition = partition
        self.offset = offset
        self.needs_commit = needs_commit

    @classmethod
    def from_value(cls, value):
        if not isinstance(value, BrokerValue):
            raise ValueError("StaleMessage is only supported before batching.")

        return cls(value.partition, value.offset)

    def __eq__(self, other):
        return (
            isinstance(other, StaleMessage)
            and self.partition == other.partition
            and self.offset == other.offset
            and self.needs_commit == other.needs_commit
        )

    def __reduce__(self):
        return self.__class__, (self.partition, self.offset, self.needs_commit)


class KafkaBacklogProducer(DlqProducer[KafkaPayload]):
    """
    A custom DLQ producer that routes stale messages (backlogged) to a backlog topic, InvalidMessages to the DLQ,
    and incoming real time messages to the primary topic.
    """

    def __init__(
        self,
        producer: Producer[KafkaPayload],
        primary_topic: Topic,
        backlog_topic: Topic,
        dlq_topic: Topic,
        backlog_threshold_ms: int,
    ):
        self.__producer = producer
        self.__primary_topic = primary_topic
        self.__backlog_topic = backlog_topic
        self.__dlq_topic = dlq_topic
        self.__backlog_threshold_ms = backlog_threshold_ms

    def produce(
        self, value: BrokerValue[KafkaPayload]
    ) -> Future[BrokerValue[KafkaPayload]]:
        """
        Custom routing logic for handling messages:
        - Stale messages are routed to the backlog topic.
        - Invalid messages are routed to the DLQ topic.
        - Real-time messages are routed to the real-time topic.
        """
        try:
            if self.is_stale_message(value):
                target_topic = self.__backlog_topic
                logger.info(f"Routing stale message to backlog topic: {value.offset}")
            else:
                target_topic = self.__primary_topic
                logger.info(
                    f"Routing real-time message to real-time topic: {value.offset}"
                )
        except Exception as e:
            target_topic = self.__dlq_topic
            logger.warning(
                f"Routing invalid message to DLQ topic: {value.offset} - Error: {e}"
            )

        return self.__producer.produce(target_topic, value.payload)

    def is_stale_message(self, value: BrokerValue[KafkaPayload]) -> bool:
        """
        Determine if the message is stale based on its timestamp.
        """
        if not value.timestamp:
            raise ValueError("Message is missing a timestamp")

        current_time = datetime.now()
        message_time = value.timestamp

        # Check if the message is older than the threshold
        return (
            current_time - message_time
        ).total_seconds() * 1000 > self.__backlog_threshold_ms

    @classmethod
    def build_initial_state(cls, limit, assignment):
        """
        Build the initial state for DLQ limits.
        """
        return DlqLimitState(limit)
