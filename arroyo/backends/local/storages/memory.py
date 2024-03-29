from datetime import datetime
from typing import Iterator, MutableMapping, MutableSequence, Optional, Sequence, Tuple

from arroyo.backends.local.storages.abstract import (
    MessageStorage,
    PartitionDoesNotExist,
    TopicDoesNotExist,
    TopicExists,
)
from arroyo.errors import OffsetOutOfRange
from arroyo.types import BrokerValue, Partition, Topic, TStrategyPayload


class MemoryMessageStorage(MessageStorage[TStrategyPayload]):
    def __init__(self) -> None:
        self.__topics: MutableMapping[
            Topic, Sequence[MutableSequence[Tuple[TStrategyPayload, datetime]]]
        ] = {}

    def create_topic(self, topic: Topic, partitions: int) -> None:
        if topic in self.__topics:
            raise TopicExists(topic)

        self.__topics[topic] = [[] for i in range(partitions)]

    def list_topics(self) -> Iterator[Topic]:
        return iter(self.__topics.keys())

    def delete_topic(self, topic: Topic) -> None:
        try:
            del self.__topics[topic]
        except KeyError as e:
            raise TopicDoesNotExist(topic) from e

    def get_partition_count(self, topic: Topic) -> int:
        try:
            return len(self.__topics[topic])
        except KeyError as e:
            raise TopicDoesNotExist(topic) from e

    def __get_messages(
        self, partition: Partition
    ) -> MutableSequence[Tuple[TStrategyPayload, datetime]]:
        # TODO: Maybe this should be enforced in the ``Partition`` constructor?
        if not partition.index >= 0:
            raise PartitionDoesNotExist(partition)

        try:
            return self.__topics[partition.topic][partition.index]
        except KeyError as e:
            raise TopicDoesNotExist(partition.topic) from e
        except IndexError as e:
            raise PartitionDoesNotExist(partition) from e

    def consume(
        self, partition: Partition, offset: int
    ) -> Optional[BrokerValue[TStrategyPayload]]:
        messages = self.__get_messages(partition)

        try:
            payload, timestamp = messages[offset]
        except IndexError:
            if offset == len(messages):
                return None
            else:
                raise OffsetOutOfRange()
        return BrokerValue(payload, partition, offset, timestamp)

    def produce(
        self, partition: Partition, payload: TStrategyPayload, timestamp: datetime
    ) -> BrokerValue[TStrategyPayload]:
        messages = self.__get_messages(partition)

        offset = len(messages)
        messages.append((payload, timestamp))

        return BrokerValue(payload, partition, offset, timestamp)
