import hashlib
import json
import logging
from typing import Mapping

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import CommitOffsets, Produce, TransformStep
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Commit, Message, Partition, Topic

logger = logging.getLogger(__name__)


def hash_password(value: Message[KafkaPayload]) -> KafkaPayload:
    # Expected format of the message is {"username": "<username>", "password": "<password>"}
    auth = json.loads(value.payload.value)
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
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:

        return TransformStep(
            function=hash_password,
            next_step=Produce(self.__producer, self.__topic, CommitOffsets(commit)),
        )
