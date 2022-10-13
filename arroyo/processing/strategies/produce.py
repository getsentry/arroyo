import logging
import time
from collections import deque
from concurrent.futures import Future
from typing import Deque, Optional, Tuple

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidKafkaMessage,
    InvalidMessages,
)
from arroyo.types import Commit, Message, Position, Topic

logger = logging.getLogger(__name__)


class ProduceAndCommit(ProcessingStrategy[KafkaPayload]):
    """
    This strategy can be used to produce Kafka messages to a destination topic. A typical use
    case could be to consume messages from one topic, apply some transformations and then output
    to another topic.

    For each message received in the submit method, it attempts to produce a single Kafka message
    in a thread. If there are too many pending futures, we MessageRejected will be raised to notify
    stream processor to slow down.

    On poll we check for completion of the produced messages. If the message has been successfully
    produced then the offset is committed. If an error occured the InvalidMessages exception will
    be raised.

    Important: The destination topic is always the `topic` passed into the constructor and not the
    topic being referenced in the message itself (which typically refers to the original topic from
    where the message was consumed from).

    Caution: MessageRejected is not properly handled by the ParallelTransform step. Exercise
    caution if chaining this step anywhere after a parallel transform.
    """

    def __init__(
        self,
        producer: Producer[KafkaPayload],
        topic: Topic,
        commit: Commit,
        max_buffer_size: int = 10000,
    ):
        self.__producer = producer
        self.__topic = topic
        self.__commit = commit
        self.__max_buffer_size = max_buffer_size

        self.__queue: Deque[
            Tuple[Message[KafkaPayload], Future[Message[KafkaPayload]]]
        ] = deque()

        self.__closed = False

    def poll(self) -> None:
        while self.__queue:
            message, future = self.__queue[0]

            if not future.done():
                break

            exc = future.exception()

            if exc is not None:
                raise InvalidMessages(
                    [
                        InvalidKafkaMessage(
                            payload=message.payload.value,
                            timestamp=message.timestamp,
                            topic=message.partition.topic.name,
                            consumer_group="",
                            partition=message.partition.index,
                            offset=message.offset,
                            headers=message.payload.headers,
                            key=message.payload.key,
                        )
                    ]
                )

            self.__queue.popleft()

            self.__commit(
                {message.partition: Position(message.next_offset, message.timestamp)}
            )

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed

        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        self.__queue.append(
            (message, self.__producer.produce(self.__topic, message.payload))
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        # Commit all previously staged offsets
        self.__commit({}, force=True)

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, future = self.__queue.popleft()

            future.result(remaining)

            offset = {message.partition: Position(message.offset, message.timestamp)}

            logger.info("Committing offset: %r", offset)
            self.__commit(offset)
