import json
import logging
from typing import Any, Mapping, MutableSequence, Sequence

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import (
    BatchStep,
    CommitOffsets,
    Produce,
    TransformStep,
    UnbatchStep,
)
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.batching import ValuesBatch
from arroyo.types import BaseValue, Commit, Message, Partition, Topic, Value

logger = logging.getLogger(__name__)


def resolve_index(msgs: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
    # do something
    return msgs


def index_data(
    batch: Message[ValuesBatch[KafkaPayload]],
) -> ValuesBatch[KafkaPayload]:
    logger.info("Indexing %d messages", len(batch.payload))
    parsed_msgs = [json.loads(s.payload.value.decode("utf-8")) for s in batch.payload]
    indexed_messages = resolve_index(parsed_msgs)
    ret: MutableSequence[BaseValue[KafkaPayload]] = []
    for i in range(0, len(batch.payload)):
        ret.append(
            Value(
                payload=KafkaPayload(
                    key=None,
                    headers=[],
                    value=json.dumps(indexed_messages[i]).encode(),
                ),
                committable=batch.payload[i].committable,
            )
        )
    return ret


class BatchedIndexerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    This strategy factory provides an example of the use of BatchStep
    and UnbatchStep.

    It consumes from Kafka, batches messages, passes the batch to a transform
    step. The transform step is suppsoed to index the messages in batches
    and produce a transformed version.
    At this point Unbatchstep breaks up the batch and sends the messages to
    a Produce step.
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

        return BatchStep(
            max_batch_size=10,
            max_batch_time=2.0,
            next_step=TransformStep(
                function=index_data,
                next_step=UnbatchStep(
                    next_step=Produce(
                        self.__producer, self.__topic, CommitOffsets(commit)
                    )
                ),
            ),
        )
