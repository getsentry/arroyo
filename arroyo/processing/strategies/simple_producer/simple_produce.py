from collections import namedtuple
from concurrent.futures import Future
from typing import (
    Callable,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
)
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Message, Partition, Position, Topic


MessageMetadata = namedtuple("MessageMetadata", ["partition", "offset", "timestamp"])


class SimpleProduceStrategy(ProcessingStrategy[KafkaPayload]):
    """
    Simple strategy that applies given transform function and produces to a topic.
    """

    def __init__(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        transformation: Callable[[Message[KafkaPayload]], Message[KafkaPayload]],
        producer: KafkaProducer,
        topic: Topic,
    ):
        self.__commit = commit
        self.__transformation = transformation
        self.__producer = producer
        self.__topic = topic
        self.__closed = False
        self.__message_to_future: MutableMapping[
            MessageMetadata, Future[Message[KafkaPayload]]
        ] = {}

    def poll(self) -> None:
        self._commit()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed
        transformed = self.__transformation(message)
        message_future = self.__producer.produce(
            destination=self.__topic, payload=transformed.payload
        )
        self.__message_to_future[
            MessageMetadata(message.partition, message.offset, message.timestamp)
        ] = message_future

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()

    def join(self, timeout: Optional[float] = None) -> None:
        self._commit()

    def _commit(self) -> None:

        done: MutableSet[Future[Message[KafkaPayload]]] = set()
        commitable: MutableSet[MessageMetadata] = set()
        cancelled = set()

        for message, future in self.__message_to_future.items():
            if future.done() and not future.cancelled():
                done.add(future)
                commitable.add(message)
            elif future.cancelled():
                cancelled.add(future)

        if len(commitable) > 0:
            print(f"Committing Offsets: {[m.offset for m in commitable]}")

            self.__commit(
                {
                    m.partition: Position(
                        m.offset,
                        m.timestamp,
                    )
                    for m in commitable
                }
            )

            self.__message_to_future = {
                m: f
                for m, f in self.__message_to_future.items()
                if (m not in commitable)
            }


class SimpleStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        transformation: Callable[[Message[KafkaPayload]], Message[KafkaPayload]],
        producer: KafkaProducer,
        topic: Topic,
    ) -> None:
        self.__transformation = transformation
        self.__producer = producer
        self.__topic = topic

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        print("Creating Strategy")
        return SimpleProduceStrategy(
            commit, self.__transformation, self.__producer, self.__topic
        )
