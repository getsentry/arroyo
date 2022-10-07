import logging
from typing import Mapping

from examples.transform_and_produce.hash_password_strategy import HashPasswordStrategy

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.produce import ProduceAndCommit
from arroyo.types import Commit, Partition, Topic

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
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        return HashPasswordStrategy(
            next_step=ProduceAndCommit(self.__producer, self.__topic, commit)
        )
