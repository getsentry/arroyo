import contextlib
import itertools
import uuid
from abc import abstractmethod
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Iterator, Optional
from unittest import TestCase

import pytest

from arroyo.backends.abstract import Consumer, Producer
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.abstract import (
    MessageStorage,
    PartitionDoesNotExist,
    TopicDoesNotExist,
    TopicExists,
)
from arroyo.backends.local.storages.file import FileMessageStorage, InvalidChecksum
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import KafkaPayload, Partition, Topic
from arroyo.utils.clock import TestingClock
from tests.backends.mixins import StreamsTestMixin


class LocalStreamsTestMixin(StreamsTestMixin):
    def setUp(self) -> None:
        self.storage = self.get_message_storage()
        self.broker = LocalBroker(self.storage, TestingClock())

    @abstractmethod
    def get_message_storage(self) -> MessageStorage:
        raise NotImplementedError

    @contextlib.contextmanager
    def get_topic(self, partitions: int = 1) -> Iterator[Topic]:
        topic = Topic(uuid.uuid1().hex)
        self.broker.create_topic(topic, partitions)
        yield topic

    def get_consumer(
        self, group: Optional[str] = None, enable_end_of_partition: bool = True
    ) -> Consumer:
        return self.broker.get_consumer(
            group if group is not None else uuid.uuid1().hex,
            enable_end_of_partition=enable_end_of_partition,
        )

    def get_producer(self) -> Producer:
        return self.broker.get_producer()

    def get_payloads(self) -> Iterator[KafkaPayload]:
        for i in itertools.count():
            yield KafkaPayload(key=None, value=str(i).encode("utf-8"), headers=[])

    @pytest.mark.xfail(strict=True, reason="rebalancing not implemented")
    def test_pause_resume_rebalancing(self) -> None:
        return super().test_pause_resume_rebalancing()

    def test_storage(self) -> None:
        topic = Topic(uuid.uuid1().hex)
        partitions = 3

        self.storage.create_topic(topic, partitions)

        assert [*self.storage.list_topics()] == [topic]

        assert self.storage.get_partition_count(topic) == partitions

        with pytest.raises(TopicExists):
            self.storage.create_topic(topic, partitions)

        with pytest.raises(TopicDoesNotExist):
            self.storage.get_partition_count(Topic("invalid"))

        with pytest.raises(TopicDoesNotExist):
            self.storage.consume(Partition(Topic("invalid"), 0), 0)

        with pytest.raises(TopicDoesNotExist):
            self.storage.produce(
                Partition(Topic("invalid"), 0),
                KafkaPayload(key=None, value=b"0", headers=[]),
                datetime.now(),
            )

        with pytest.raises(PartitionDoesNotExist):
            self.storage.consume(Partition(topic, -1), 0)

        with pytest.raises(PartitionDoesNotExist):
            self.storage.consume(Partition(topic, partitions + 1), 0)

        with pytest.raises(PartitionDoesNotExist):
            self.storage.produce(
                Partition(topic, -1),
                KafkaPayload(key=None, value=b"0", headers=[]),
                datetime.now(),
            )

        with pytest.raises(PartitionDoesNotExist):
            self.storage.produce(
                Partition(topic, partitions + 1),
                KafkaPayload(key=None, value=b"0", headers=[]),
                datetime.now(),
            )

        self.storage.delete_topic(topic)

        with pytest.raises(TopicDoesNotExist):
            self.storage.delete_topic(topic)


class LocalStreamsMemoryStorageTestCase(LocalStreamsTestMixin, TestCase):
    def get_message_storage(self) -> MessageStorage:
        return MemoryMessageStorage()


class LocalStreamsFileStorageTestCase(LocalStreamsTestMixin, TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        super().setUp()

    def get_message_storage(self) -> MessageStorage:
        return FileMessageStorage(self.directory.name)

    def test_unaligned_offset(self) -> None:
        topic = Topic(uuid.uuid1().hex)
        partition = Partition(topic, 0)
        self.storage.create_topic(topic, 1)

        message = self.storage.produce(
            partition, KafkaPayload(key=None, value=b"1", headers=[]), datetime.now()
        )

        invalid_offset = message.offset + 4
        assert message.next_offset > invalid_offset > message.offset

        with pytest.raises(InvalidChecksum):
            self.storage.consume(partition, invalid_offset)
