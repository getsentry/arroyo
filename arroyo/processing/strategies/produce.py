import logging
import time
from collections import deque
from typing import Deque, Optional, Tuple, Union

from arroyo.backends.abstract import Producer, ProducerFuture
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import (
    BrokerValue,
    FilteredPayload,
    Message,
    Topic,
    TStrategyPayload,
    Value,
)

logger = logging.getLogger(__name__)


class Produce(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
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
        producer: Producer[TStrategyPayload],
        topic: Topic,
        next_step: ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
        max_buffer_size: int = 10000,
    ):
        self.__producer = producer
        self.__topic = topic
        self.__next_step = next_step
        self.__max_buffer_size = max_buffer_size

        self.__queue: Deque[
            Tuple[
                Message[Union[FilteredPayload, TStrategyPayload]],
                Optional[ProducerFuture[BrokerValue[TStrategyPayload]]],
            ]
        ] = deque()

        self.__closed = False

    def poll(self) -> None:
        if not self.__queue:
            self.__next_step.poll()
            return

        while self.__queue:
            original_message, future = self.__queue[0]

            if future is None:
                message = original_message
            else:
                if not future.done():
                    break

                message = Message(
                    Value(
                        future.result().payload,
                        original_message.committable,
                        original_message.timestamp,
                    )
                )

            self.__queue.popleft()
            self.__next_step.poll()
            self.__next_step.submit(message)

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed

        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        future: Optional[ProducerFuture[BrokerValue[TStrategyPayload]]] = None

        if not isinstance(message.payload, FilteredPayload):
            try:
                future = self.__producer.produce(self.__topic, message.payload)
            except BufferError as exc:
                logger.exception(exc)
                raise MessageRejected from exc

        self.__queue.append((message, future))

    def close(self) -> None:
        self.__closed = True

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

            original_message, future = self.__queue.popleft()

            if future is None:
                message = original_message
            else:
                if not future.done():
                    break

                message = Message(
                    Value(
                        future.result().payload,
                        original_message.committable,
                        original_message.timestamp,
                    )
                )

            self.__next_step.poll()
            self.__next_step.submit(message)

        self.__next_step.close()
        self.__next_step.join(remaining)
