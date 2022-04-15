import json
import time
from collections import deque
from concurrent.futures import Future
from typing import Any, Deque, Optional

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
)
from arroyo.types import Message, Topic
from arroyo.utils.metrics import get_metrics


class ProduceInvalidMessagePolicy(DeadLetterQueuePolicy):
    """
    Produces given InvalidMessages to a dead letter topic.
    """

    def __init__(self, producer: KafkaProducer, dead_letter_topic: Topic) -> None:
        self.__metrics = get_metrics()
        self.__dead_letter_topic = dead_letter_topic
        self.__producer = producer
        self.__futures: Deque[Future[Message[KafkaPayload]]] = deque()

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        """
        Produces a message to the given dead letter topic for each
        invalid message in the form:

        {
            "topic": <original topic the bad message was produced to>,
            "reason": <why the message(s) are bad>
            "timestamp": <time at which exception was thrown>,
            "message": <original bad message>
        }
        """
        for message in e.messages:
            payload = self._build_payload(e, message)
            self._produce(payload)
        self.__metrics.increment("dlq.produced_messages", len(e.messages))

    def _build_payload(self, e: InvalidMessages, message: Any) -> KafkaPayload:
        data = json.dumps(
            {
                "topic": e.topic,
                "reason": e.reason,
                "timestamp": e.timestamp,
                "message": message,
            }
        ).encode("utf-8")
        return KafkaPayload(key=None, value=data, headers=[])

    def _produce(self, payload: KafkaPayload) -> None:
        """
        Wait for queue to clear if filled, then asynchronously produce
        the message, adding the process to the queue.
        """
        if len(self.__futures) >= 10:
            self.join()
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
