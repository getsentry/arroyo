import json
import logging
from typing import Any, Mapping, Sequence

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import (
    BatchStep,
    MessageBatch,
    ProduceAndCommit,
    TransformStep,
    UnbatchStep,
)
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Commit, Message, Partition, Topic

logger = logging.getLogger(__name__)


def resolve_index(msgs: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
    # do something
    return msgs


def index_data(
    batch: Message[MessageBatch[KafkaPayload]],
) -> MessageBatch[KafkaPayload]:
    logger.info("Indexing %d messages", len(batch.payload.messages))
    parsed_msgs = [
        json.loads(s.payload.value.decode("utf-8")) for s in batch.payload.messages
    ]
    indexed_messages = resolve_index(parsed_msgs)
    ret = []
    for i in range(0, len(batch.payload.messages)):
        ret.append(
            Message(
                partition=batch.payload.messages[i].partition,
                offset=batch.payload.messages[i].offset,
                timestamp=batch.payload.messages[i].timestamp,
                payload=KafkaPayload(
                    key=None,
                    headers=[],
                    value=json.dumps(indexed_messages[i]).encode(),
                ),
            )
        )
    return MessageBatch(
        messages=ret,
        offsets=batch.payload.offsets,
    )


class BatchedIndexerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    This strategy factory rpovides an example of the use of BatchStep
    and Unbatchstep.

    It consumes from Kafka, batches messages, passes the batch to a transform
    step. The transform step is suppsoed to index the messages in batches
    and produce a transformed version.
    At this point Unbatchstep breaks up the batch and sends the messages to
    a ProduceAndCommit step.
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
            max_batch_time_sec=2,
            next_step=TransformStep(
                function=index_data,
                next_step=UnbatchStep(
                    next_step=ProduceAndCommit(self.__producer, self.__topic, commit)
                ),
            ),
        )
