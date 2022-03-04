import logging
from typing import (
    Callable,
    Mapping,
)
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Partition, Position, Topic
from arroyo.examples.transform_and_produce.hash_password_strategy import (
    HashPasswordStrategy,
)
from arroyo.examples.transform_and_produce.produce_step import ProduceStep

logger = logging.getLogger(__name__)


class HashPasswordAndProduceStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    def __init__(
        self,
        producer: KafkaProducer,
        topic: Topic,
    ) -> None:
        self.__producer = producer
        self.__topic = topic

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        print("Creating Strategy")
        return HashPasswordStrategy(
            next_step=ProduceStep(commit, self.__producer, self.__topic)
        )
