import json
import time
from collections import deque
from concurrent.futures import Future
from datetime import datetime
from typing import Any, Deque, Mapping, Optional

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
    InvalidMessages,
    JSONSerializable,
)
from arroyo.types import Message, Topic
from arroyo.utils.codecs import Encoder
from arroyo.utils.metrics import get_metrics

MAX_QUEUE_SIZE = 5000
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class JSONMessageEncoder(Encoder[bytes, Mapping[str, JSONSerializable]]):
    def __default(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime(DATE_TIME_FORMAT)
        else:
            raise TypeError

    def encode(self, value: Mapping[str, JSONSerializable]) -> bytes:
        return json.dumps(value, default=self.__default).encode("utf-8")


class ProduceInvalidMessagePolicy(DeadLetterQueuePolicy):
    """
    Produces given InvalidMessages to a dead letter topic.
    """

    def __init__(
        self, producer: Producer[KafkaPayload], dead_letter_topic: Topic
    ) -> None:
        self.__metrics = get_metrics()
        self.__dead_letter_topic = dead_letter_topic
        self.__producer = producer
        self.__futures: Deque[Future[Message[KafkaPayload]]] = deque()

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        """
        Produces a message to the given dead letter topic for each
        invalid message. Produced message is in the form provided
        by `InvalidMessage.to_dict()`
        """
        for message in e.messages:
            payload = self._build_payload(message)
            self._produce(payload)
        self.__metrics.increment("dlq.produced_messages", len(e.messages))

    def _build_payload(self, message: InvalidMessage) -> KafkaPayload:
        data = JSONMessageEncoder().encode(message.to_dict())
        return KafkaPayload(key=None, value=data, headers=[])

    def _produce(self, payload: KafkaPayload) -> None:
        """
        Prune done futures and asynchronously produce
        """
        while self.__futures and self.__futures[0].done():
            self.__futures.popleft()
        if len(self.__futures) >= MAX_QUEUE_SIZE:
            self.__futures[0].result(timeout=1.0)
            self.__futures.popleft()

        self.__futures.append(
            self.__producer.produce(
                destination=self.__dead_letter_topic, payload=payload
            )
        )

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.perf_counter()
        while self.__futures:
            if self.__futures[0].done():
                self.__futures.popleft()
            if timeout is not None and time.perf_counter() - start > timeout:
                break
