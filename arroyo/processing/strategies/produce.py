import logging
import time
from collections import deque
from concurrent.futures import Future
from typing import Deque, Mapping, Optional, Tuple, Union

from arroyo.backends.abstract import Producer
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.types import (
    FILTERED_PAYLOAD,
    BrokerValue,
    Commit,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    TPayload,
    Value,
)

logger = logging.getLogger(__name__)


class Produce(ProcessingStrategy[TPayload]):
    """
    This strategy can be used to produce Kafka messages to a destination topic. A typical use
    case could be to consume messages from one topic, apply some transformations and then output
    to another topic.

    For each message received in the submit method, it attempts to produce a single Kafka message
    in a thread. If there are too many pending futures, we MessageRejected will be raised to notify
    stream processor to slow down.

    On poll we check for completion of the produced messages. If the message has been successfully
    produced then the message is submitted to the next step. If an error occured the exception will
    be raised.

    Important: The destination topic is always the `topic` passed into the constructor and not the
    topic being referenced in the message itself (which typically refers to the original topic from
    where the message was consumed from).
    """

    def __init__(
        self,
        producer: Producer[TPayload],
        topic: Topic,
        next_step: ProcessingStrategy[TPayload],
        max_buffer_size: int = 10000,
    ):
        self.__producer = producer
        self.__topic = topic
        self.__next_step = next_step
        self.__max_buffer_size = max_buffer_size

        self.__queue: Deque[
            Tuple[Mapping[Partition, int], Optional[Future[BrokerValue[TPayload]]]]
        ] = deque()

        self.__closed = False

    def poll(self) -> None:
        while self.__queue:
            committable, future = self.__queue[0]

            if future is not None and not future.done():
                break

            payload: Union[TPayload, FilteredPayload]
            if future is None:
                payload = FILTERED_PAYLOAD
            else:
                payload = future.result().payload

            message = Message(Value(payload, committable))

            self.__queue.popleft()
            self.__next_step.poll()
            self.__next_step.submit(message)

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        if isinstance(message.payload, FilteredPayload):
            producer_future = None
        else:
            producer_future = self.__producer.produce(self.__topic, message.payload)

        self.__queue.append((message.committable, producer_future))

    def close(self) -> None:
        self.__closed = True
        self.__next_step.close()

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        remaining = timeout

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            committable, future = self.__queue.popleft()

            payload: Union[TPayload, FilteredPayload]
            if future is None:
                payload = FILTERED_PAYLOAD
            else:
                payload = future.result().payload

            message = Message(Value(payload, committable))

            self.__next_step.poll()
            self.__next_step.submit(message)

        self.__next_step.join(remaining)


class ProduceAndCommit(ProcessingStrategy[TPayload]):
    """
    This strategy produces then commits offsets. It doesn't do much on
    on it's own since it is simply the Produce and CommitOffsets strategies
    chained together.

    This is provided for convenience and backwards compatibility. Will be
    removed in a future version.
    """

    def __init__(
        self,
        producer: Producer[TPayload],
        topic: Topic,
        commit: Commit,
        max_buffer_size: int = 10000,
    ):
        self.__strategy: Produce[TPayload] = Produce(
            producer, topic, CommitOffsets(commit), max_buffer_size
        )

    def poll(self) -> None:
        self.__strategy.poll()

    def submit(self, message: Message[TPayload]) -> None:
        self.__strategy.submit(message)

    def close(self) -> None:
        self.__strategy.close()

    def terminate(self) -> None:
        self.__strategy.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__strategy.join(timeout)
