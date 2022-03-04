from collections import deque
from concurrent.futures import Future

import logging
from typing import (
    Callable,
    Deque,
    Mapping,
    NamedTuple,
    Optional,
)
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep

from arroyo.types import Message, Partition, Position, Topic

logger = logging.getLogger(__name__)


class MessageToFuture(NamedTuple):
    message: Message[KafkaPayload]
    future: Future  # [Message[KafkaPayload]]


class ProduceStep(ProcessingStep[KafkaPayload]):
    """
    Simple strategy that applies given transform function and produces to a topic.
    """

    def __init__(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        producer: KafkaProducer,
        topic: Topic,
    ):
        self.__commit = commit
        self.__producer = producer
        self.__topic = topic
        self.__closed = False
        self.__futures: Deque[MessageToFuture] = deque()

    def poll(self) -> None:
        self._commit()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed
        future = self.__producer.produce(
            destination=self.__topic, payload=message.payload
        )
        self.__futures.append(MessageToFuture(message, future))

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()

    def join(self, timeout: Optional[float] = None) -> None:
        self._commit()

    def _commit(self) -> None:

        commitable: Optional[Message[KafkaPayload]] = None

        while self.__futures and self.__futures[0].future.done():
            commitable, _ = self.__futures.popleft()
        if commitable is not None:
            self.__commit(
                {
                    commitable.partition: Position(
                        commitable.offset, commitable.timestamp
                    )
                }
            )
