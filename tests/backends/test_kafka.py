import contextlib
import itertools
import os
import pickle
import uuid
from contextlib import closing
from datetime import datetime
from pickle import PickleBuffer
from typing import Any, Iterator, Mapping, MutableSequence, Optional
from unittest import TestCase

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.kafka.configuration import build_kafka_configuration
from arroyo.backends.kafka.consumer import as_kafka_configuration_bool
from arroyo.errors import ConsumerError, EndOfPartition
from arroyo.synchronized import Commit, commit_codec
from arroyo.types import Message, Partition, Position, Topic
from tests.backends.mixins import StreamsTestMixin


def test_payload_equality() -> None:
    assert KafkaPayload(None, b"", []) == KafkaPayload(None, b"", [])
    assert KafkaPayload(b"key", b"value", []) == KafkaPayload(b"key", b"value", [])
    assert KafkaPayload(None, b"", [("key", b"value")]) == KafkaPayload(
        None, b"", [("key", b"value")]
    )
    assert not KafkaPayload(None, b"a", []) == KafkaPayload(None, b"b", [])
    assert not KafkaPayload(b"this", b"", []) == KafkaPayload(b"that", b"", [])
    assert not KafkaPayload(None, b"", [("key", b"this")]) == KafkaPayload(
        None, b"", [("key", b"that")]
    )


def test_payload_pickle_simple() -> None:
    payload = KafkaPayload(b"key", b"value", [])
    assert pickle.loads(pickle.dumps(payload)) == payload


def test_payload_pickle_out_of_band() -> None:
    payload = KafkaPayload(b"key", b"value", [])
    buffers: MutableSequence[PickleBuffer] = []
    data = pickle.dumps(payload, protocol=5, buffer_callback=buffers.append)
    assert pickle.loads(data, buffers=[b.raw() for b in buffers]) == payload


@contextlib.contextmanager
def get_topic(
    configuration: Mapping[str, Any], partitions_count: int
) -> Iterator[Topic]:
    name = f"test-{uuid.uuid1().hex}"
    client = AdminClient(configuration)
    [[key, future]] = client.create_topics(
        [NewTopic(name, num_partitions=partitions_count, replication_factor=1)]
    ).items()
    assert key == name
    assert future.result() is None
    try:
        yield Topic(name)
    finally:
        [[key, future]] = client.delete_topics([name]).items()
        assert key == name
        assert future.result() is None


class KafkaStreamsTestCase(StreamsTestMixin[KafkaPayload], TestCase):

    configuration = build_kafka_configuration(
        {"bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092")}
    )

    @contextlib.contextmanager
    def get_topic(self, partitions: int = 1) -> Iterator[Topic]:
        with get_topic(self.configuration, partitions) as topic:
            try:
                yield topic
            finally:
                pass

    def get_consumer(
        self,
        group: Optional[str] = None,
        enable_end_of_partition: bool = True,
        auto_offset_reset: str = "earliest",
        strict_offset_reset: Optional[bool] = None,
    ) -> KafkaConsumer:
        return KafkaConsumer(
            {
                **self.configuration,
                "auto.offset.reset": auto_offset_reset,
                "arroyo.strict.offset.reset": strict_offset_reset,
                "enable.auto.commit": "false",
                "enable.auto.offset.store": "false",
                "enable.partition.eof": enable_end_of_partition,
                "group.id": group if group is not None else uuid.uuid1().hex,
                "session.timeout.ms": 10000,
            },
        )

    def get_producer(self) -> KafkaProducer:
        return KafkaProducer(self.configuration)

    def get_payloads(self) -> Iterator[KafkaPayload]:
        for i in itertools.count():
            yield KafkaPayload(None, f"{i}".encode("utf8"), [])

    def test_auto_offset_reset_earliest(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="earliest")) as consumer:
                consumer.subscribe([topic])

                message = consumer.poll(10.0)
                assert isinstance(message, Message)
                assert message.offset == 0

    def test_auto_offset_reset_latest(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="latest")) as consumer:
                consumer.subscribe([topic])

                try:
                    consumer.poll(10.0)  # XXX: getting the subcription is slow
                except EndOfPartition as error:
                    assert error.partition == Partition(topic, 0)
                    assert error.offset == 1
                else:
                    raise AssertionError("expected EndOfPartition error")

    def test_lenient_offset_reset_latest(self) -> None:
        payload = KafkaPayload(b"a", b"0", [])
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                message = producer.produce(topic, payload).result(5.0)

            # write a nonsense offset
            with closing(self.get_consumer(strict_offset_reset=False)) as consumer:
                consumer.subscribe([topic])
                consumer.poll(10.0)  # Wait for assignment
                consumer.stage_positions(
                    {
                        message.partition: Position(
                            offset=message.offset + 1000, timestamp=message.timestamp
                        )
                    },
                )
                consumer.commit_positions()

            with closing(self.get_consumer(strict_offset_reset=False)) as consumer:
                consumer.subscribe([topic])
                result_message = consumer.poll(10.0)
                assert result_message is not None
                assert result_message.payload.key == b"a"
                assert result_message.payload.value == b"0"

                # make sure we reset our offset now
                consumer.commit_positions()

    def test_auto_offset_reset_error(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="error")) as consumer:
                consumer.subscribe([topic])

                with pytest.raises(ConsumerError):
                    consumer.poll(10.0)  # XXX: getting the subcription is slow


def test_cooperative_rebalancing() -> None:
    configuration = build_kafka_configuration(
        {"bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092")}
    )

    partitions_count = 2

    group_id = uuid.uuid4().hex
    producer = KafkaProducer(configuration)

    consumer_a = KafkaConsumer(
        {
            **configuration,
            "partition.assignment.strategy": "cooperative-sticky",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "group.id": group_id,
            "session.timeout.ms": 10000,
        },
    )
    consumer_b = KafkaConsumer(
        {
            **configuration,
            "partition.assignment.strategy": "cooperative-sticky",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "group.id": group_id,
            "session.timeout.ms": 10000,
        },
    )

    with get_topic(configuration, partitions_count) as topic, closing(
        producer
    ), closing(consumer_a), closing(consumer_b):
        for i in range(10):
            for j in range(partitions_count):
                producer.produce(
                    Partition(topic, j),
                    KafkaPayload(None, f"{j}-{i}".encode("utf8"), []),
                )

        consumer_a.subscribe([topic])

        assert consumer_a.poll(10.0) is not None

        # Consumer A has 2 partitions assigned, B has none
        assert len(consumer_a.tell()) == 2
        assert len(consumer_b.tell()) == 0

        consumer_b.subscribe([topic])

        # At some point, 1 partition will move to consumer B
        consumer_a.pause([p for p in consumer_a.tell()])
        for i in range(10):
            assert consumer_a.poll(0) is None  # attempt to force session timeout
            if consumer_b.poll(1.0) is not None:
                break

        assert len(consumer_a.tell()) == 1
        assert len(consumer_b.tell()) == 1

        # Resume A and assert that both consumer_a and consumer_b are getting messages
        consumer_a.resume([p for p in consumer_a.tell()])
        assert consumer_a.poll(1.0) is not None
        assert consumer_b.poll(1.0) is not None


def test_commit_codec() -> None:
    commit = Commit("group", Partition(Topic("topic"), 0), 0, datetime.now())
    assert commit_codec.decode(commit_codec.encode(commit)) == commit


def test_as_kafka_configuration_bool() -> None:
    assert as_kafka_configuration_bool(False) is False
    assert as_kafka_configuration_bool("false") is False
    assert as_kafka_configuration_bool("FALSE") is False
    assert as_kafka_configuration_bool("0") is False
    assert as_kafka_configuration_bool("f") is False
    assert as_kafka_configuration_bool(0) is False

    assert as_kafka_configuration_bool(True) == True
    assert as_kafka_configuration_bool("true") == True
    assert as_kafka_configuration_bool("TRUE") == True
    assert as_kafka_configuration_bool("1") == True
    assert as_kafka_configuration_bool("t") == True
    assert as_kafka_configuration_bool(1) == True

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(None)

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("tru")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("flase")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool(2)

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(0.0)
