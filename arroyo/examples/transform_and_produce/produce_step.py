from __future__ import annotations
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
    """
    Map a submitted message to a Future returned by the Producer.

    This is useful for being able to commit the latest offset back
    to the original consumer.
    """

    message: Message[KafkaPayload]
    future: Future[Message[KafkaPayload]]


class ProduceStep(ProcessingStep[KafkaPayload]):
    """
    Simple step that produces a given message onto a topic.

    Meant to be used as the last step in a series of processing steps.

    Requires the commit function of the original consumer that got this message
    to let it know that the message has been produced so it can commit the
    original offset.
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
        """
        Check status of any async tasks, in this case check status of
        messages produced by producer and commit releveant offset.
        """
        self._commit()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed
        # Produce the message
        future = self.__producer.produce(
            destination=self.__topic, payload=message.payload
        )
        # KafkaProducer asynchronously produces a message so it returns a "Future"
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

        # Commit the latest offset that has its corresponding produce finished
        if commitable is not None:
            self.__commit(
                {
                    commitable.partition: Position(
                        commitable.offset, commitable.timestamp
                    )
                }
            )
