import hashlib
import json
import logging
from typing import Callable, Mapping

from examples.transform_and_produce.produce_step import ProduceStrategy

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.transform import TransformStep
from arroyo.types import Message, Partition, Position, Topic

logger = logging.getLogger(__name__)


def hash_password(message: Message[KafkaPayload]) -> KafkaPayload:
    # Expected format of the message is {"username": "<username>", "password": "<password>"}
    auth = json.loads(message.payload.value)
    hashed = hashlib.sha256(auth["password"].encode("utf-8")).hexdigest()
    data = json.dumps({"username": auth["username"], "password": hashed}).encode(
        "utf-8"
    )
    return KafkaPayload(key=None, value=data, headers=[])


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

        return TransformStep(
            function=hash_password,
            next_step=ProduceStrategy(commit, self.__producer, self.__topic),
        )
