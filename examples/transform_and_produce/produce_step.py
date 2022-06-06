from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future
from typing import Callable, Deque, Mapping, MutableMapping, NamedTuple, Optional

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import ProcessingStrategy
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


class ProduceStrategy(ProcessingStrategy[KafkaPayload]):
    """
    Produces a message onto a topic asynchronously and commits
    the offset of the original message once producing is complete.

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
        messages produced by producer and commit relevant offset.
        """
        self._commit_and_prune_futures()

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
        self._commit_and_prune_futures(timeout)

    def _commit_and_prune_futures(self, timeout: Optional[float] = None) -> None:
        """
        Commit the latest offset of any completed message from the original
        consumer.
        """
        start = time.perf_counter()

        committable: MutableMapping[Partition, Message[KafkaPayload]] = {}

        while self.__futures and self.__futures[0].future.done():
            message, _ = self.__futures.popleft()
            # overwrite any existing message as we assume the deque is in order
            # committing offset x means all offsets up to and including x are processed
            committable[message.partition] = message

            if timeout is not None and time.perf_counter() - start > timeout:
                break

        # Commit the latest offset that has its corresponding produce finished, per partition

        if committable:
            self.__commit(
                {
                    partition: Position(message.next_offset, message.timestamp)
                    for partition, message in committable.items()
                }
            )
