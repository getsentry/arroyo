import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from typing import Deque, MutableMapping, MutableSequence, NamedTuple, Optional, Tuple

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.types import Commit, Message, Partition, Position, Topic

logger = logging.getLogger(__name__)


class MessageRoute(NamedTuple):
    """
    A MessageRoute is a tuple of (producer, topic) that a message should be
    routed to.
    """

    producer: Producer[KafkaPayload]
    topic: Topic


class MessageRouter(ABC):
    """
    Router that determines how to route messages to the correct destination.
    """

    @abstractmethod
    def populate_routes(self) -> MutableSequence[KafkaProducer]:
        """
        This method must return a sequence of producers on which data can be
        sent to. This is needed so that the strategy can perform maintenance
        on all the producers like calling poll() periodically, flushing all
        of them on shutdown, etc.
        """
        raise NotImplementedError

    @abstractmethod
    def get_route_for_message(self, message: Message[KafkaPayload]) -> MessageRoute:
        """
        This method must return the MessageRoute on which the
        message should be produced. Implementations of this method can vary
        based on the specific use case. For example, if there is no message
        routing needed, this method can simply return the single producer and
        topic tuple. If there are multiple producers and topics,
        the implementation can use the message payload to determine which
        producer and topic to use. Use case like Slicing, can use org_id passed
        in via message headers to determine the (producer, topic) tuple to send
        the message to.
        """
        raise NotImplementedError


class RoutingProducerStep(ProcessingStrategy[KafkaPayload]):
    """
    This strategy is used to route messages to different producers/topics
    based on the message payload. It relies on the implementation of the
    message router to determine the producer and topic to route the message to.
    """

    def __init__(
        self,
        commit_function: Commit,
        commit_max_batch_size: int,
        commit_max_batch_time: float,
        message_router: MessageRouter,
    ) -> None:
        self.__commit_function = commit_function
        self.__commit_max_batch_size = commit_max_batch_size
        self.__commit_max_batch_time = commit_max_batch_time
        self.__message_router = message_router
        self.__all_producers = self.__message_router.populate_routes()
        self.__closed = False
        self.__offsets_to_be_committed: MutableMapping[Partition, Position] = {}
        self.__messages_processed_since_last_commit = 0
        self.__started = time.time()
        self.__queue: Deque[
            Tuple[Message[KafkaPayload], Future[Message[KafkaPayload]]]
        ] = deque()

    def _should_commit(self) -> bool:
        now = time.time()
        duration = now - self.__started
        if self.__messages_processed_since_last_commit >= self.__commit_max_batch_size:
            logger.info(
                f"Max size reached: total of {self.__messages_processed_since_last_commit} messages after {duration:.{2}f} seconds"
            )
            return True
        if now >= (self.__started + self.__commit_max_batch_time):
            logger.info(
                f"Max time reached: total of {self.__messages_processed_since_last_commit} messages after {duration:.{2}f} seconds"
            )
            return True

        return False

    def poll(self) -> None:
        """
        KafkaProducer's call poll() periodically in a thread. So we don't
        need to explicitly call poll() here.
        """
        while self.__queue:
            message, future = self.__queue[0]

            if not future.done():
                break

            exc = future.exception()
            if exc is not None:
                raise exc

            self.__queue.popleft()
            self.__messages_processed_since_last_commit += 1
            self.__offsets_to_be_committed[message.partition] = Position(
                message.next_offset, message.timestamp
            )

        if self._should_commit():
            self.__commit_function(self.__offsets_to_be_committed)
            self.__messages_processed_since_last_commit = 0
            self.__offsets_to_be_committed = {}
            self.__started = time.time()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed
        producer, topic = self.__message_router.get_route_for_message(message)
        self.__queue.append(
            (message, producer.produce(destination=topic, payload=message.payload))
        )

    def terminate(self) -> None:
        self.__closed = True

    def close(self) -> None:
        self.__closed = True
        for producer in self.__all_producers:
            producer.close()

    def join(self, timeout: Optional[float] = None) -> None:
        """
        When close() is called on the strategy, all the producers are closed.
        The producers also have logic to flush when close is called. Hence,
        we don't need explicit call to producer flush here.
        """
        if self.__messages_processed_since_last_commit:
            logger.info(
                f"Committing {self.__messages_processed_since_last_commit} messages..."
            )
            self.__commit_function(self.__offsets_to_be_committed)
            self.__messages_processed_since_last_commit = 0
            self.__offsets_to_be_committed = {}
            self.__started = time.time()
