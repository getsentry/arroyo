import base64
import json
import time
from collections import deque
from concurrent.futures import Future
from typing import Deque, Optional

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
    Serializable,
)
from arroyo.types import Message, Topic
from arroyo.utils.metrics import get_metrics

MAX_QUEUE_SIZE = 5000


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

    def _build_payload(self, e: InvalidMessages, message: Serializable) -> KafkaPayload:
        if isinstance(message, bytes):
            message = base64.b64encode(message).decode("utf-8")
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
        Prune done futures from queue if filled, then asynchronously
        produce the message, adding the process (future) to the queue.
        """
        self._prune_done_futures()
        self.__futures.append(
            self.__producer.produce(
                destination=self.__dead_letter_topic, payload=payload
            )
        )

    def _prune_done_futures(self) -> None:
        """
        Filter futures deque, should iterate only once except
        for rare edge case all processes are still running.
        """
        while len(self.__futures) >= MAX_QUEUE_SIZE:
            self.__futures = deque(
                [future for future in self.__futures if not future.done()]
            )

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.perf_counter()
        while self.__futures:
            if self.__futures[0].done():
                self.__futures.popleft()
            if timeout is not None and time.perf_counter() - start > timeout:
                break
