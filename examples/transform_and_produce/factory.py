import logging
from typing import Callable, Mapping

from examples.transform_and_produce.hash_password_strategy import HashPasswordStrategy
from examples.transform_and_produce.produce_step import ProduceStrategy

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Partition, Position, Topic

logger = logging.getLogger(__name__)


class HashPasswordAndProduceStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    A factory which builds the strategy.

    Since this strategy is supposed to simply hash a password and then produce a new message,
    all it needs is the producer + topic to produce to.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        topic: Topic,
    ) -> None:
        self.__producer = producer
        self.__topic = topic

    def create_with_partitions(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        return HashPasswordStrategy(
            next_step=ProduceStrategy(commit, self.__producer, self.__topic)
        )
