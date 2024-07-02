import contextlib
import itertools
import os
import pickle
import subprocess
import time
import uuid
from contextlib import closing
from pickle import PickleBuffer
from typing import (
    Any,
    Generator,
    Iterator,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
)
from unittest import mock

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.backends.kafka.configuration import (
    KafkaBrokerConfig,
    build_kafka_configuration,
)
from arroyo.backends.kafka.consumer import as_kafka_configuration_bool
from arroyo.commit import IMMEDIATE, Commit
from arroyo.errors import ConsumerError, EndOfPartition
from arroyo.processing.processor import StreamProcessor
from arroyo.types import BrokerValue, Partition, Topic
from tests.backends.mixins import StreamsTestMixin

commit_codec = CommitCodec()


class KafkaDistributed:
    soiled = False

    def kill(self, brokers: Sequence[int]) -> None:
        subprocess.check_call(
            ["docker", "kill"] + [f"arroyo-kafka-{i}-1" for i in brokers]
        )
        self.soiled = True


@pytest.fixture
def kafka_distributed() -> Generator[KafkaDistributed, None, None]:
    subprocess.check_call(["docker-compose", "up", "-d"])

    handle = KafkaDistributed()

    yield handle

    if handle.soiled:
        subprocess.check_call(["docker-compose", "down"])
        subprocess.check_call(["docker-compose", "rm"])


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
    configuration: Mapping[str, Any],
    partitions_count: int,
    skip_teardown: bool = False,
    replication_factor: int = 1,
) -> Iterator[Topic]:
    name = f"test-{uuid.uuid1().hex}"
    client = AdminClient(configuration)
    [[key, future]] = client.create_topics(
        [
            NewTopic(
                name,
                num_partitions=partitions_count,
                replication_factor=replication_factor,
                config={"min.insync.replicas": 2},
            )
        ]
    ).items()
    assert key == name
    assert future.result() is None
    try:
        yield Topic(name)
    finally:
        if not skip_teardown:
            [[key, future]] = client.delete_topics([name]).items()
            assert key == name
            assert future.result() is None


class TestKafkaStreams(StreamsTestMixin[KafkaPayload]):
    distributed = False

    @property
    def configuration(self) -> KafkaBrokerConfig:
        if self.distributed:
            return build_kafka_configuration(
                {
                    "bootstrap.servers": os.environ.get(
                        "DEFAULT_DISTRIBUTED_BROKERS",
                        "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
                    ),
                    "acks": "all",
                }
            )
        else:
            return build_kafka_configuration(
                {
                    "bootstrap.servers": os.environ.get(
                        "DEFAULT_BROKERS", "127.0.0.1:9092"
                    )
                }
            )

    @contextlib.contextmanager
    def get_topic(
        self,
        partitions: int = 1,
        skip_teardown: bool = False,
        replication_factor: int = 1,
    ) -> Iterator[Topic]:
        with get_topic(
            self.configuration,
            partitions,
            skip_teardown=skip_teardown,
            replication_factor=replication_factor,
        ) as topic:
            yield topic

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

                value = consumer.poll(10.0)
                assert isinstance(value, BrokerValue)
                assert value.payload.value == b"0"

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

                partition = Partition(topic, 0)

                consumer.stage_offsets(
                    {partition: message.offset},
                )
                consumer.commit_offsets()

            with closing(self.get_consumer(strict_offset_reset=False)) as consumer:
                consumer.subscribe([topic])
                result_message = consumer.poll(10.0)
                assert result_message is not None
                assert result_message.payload.key == b"a"
                assert result_message.payload.value == b"0"

                # make sure we reset our offset now
                consumer.commit_offsets()

    def test_auto_offset_reset_error(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="error")) as consumer:
                consumer.subscribe([topic])

                with pytest.raises(ConsumerError):
                    consumer.poll(10.0)  # XXX: getting the subcription is slow

    def test_consumer_stream_processor_shutdown(self) -> None:
        strategy = mock.Mock()
        strategy.poll.side_effect = RuntimeError("goodbye")
        factory = mock.Mock()
        factory.create_with_partitions.return_value = strategy

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer()) as consumer:
                processor = StreamProcessor(consumer, topic, factory, IMMEDIATE)

                with pytest.raises(RuntimeError):
                    processor.run()

    def test_kafka_broker_died(self, kafka_distributed: KafkaDistributed) -> None:
        self.distributed = True

        NUM_PAYLOADS = 100
        recovered_payloads = set()

        with self.get_topic(
            partitions=32, skip_teardown=True, replication_factor=3
        ) as topic:
            with closing(self.get_producer()) as producer:
                payloads = self.get_payloads()
                for _ in range(NUM_PAYLOADS):
                    producer.produce(topic, next(payloads)).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="earliest")) as consumer:
                consumer.subscribe([topic])

                value = consumer.poll(10.0)
                assert isinstance(value, BrokerValue)
                recovered_payloads.add(int(value.payload.value))
                consumer.stage_offsets(value.committable)

                kafka_distributed.kill([1])

                try:
                    for _ in range(NUM_PAYLOADS - len(recovered_payloads)):
                        value = consumer.poll(10.0)
                        assert isinstance(value, BrokerValue)
                        recovered_payloads.add(int(value.payload.value))
                        consumer.stage_offsets(value.committable)
                        consumer.commit_offsets()
                except Exception as e:
                    print(f"First consumer crashed with: {e}")
                else:
                    print("First consumer did not crash")

            # we should not be able to recover from this scenario without restarting the consumer
            assert len(recovered_payloads) < NUM_PAYLOADS

            with closing(self.get_consumer(auto_offset_reset="earliest")) as consumer:
                consumer.subscribe([topic])

                with pytest.raises(EndOfPartition):
                    while True:
                        value = consumer.poll(10.0)
                        assert isinstance(value, BrokerValue)
                        recovered_payloads.add(int(value.payload.value))
                        consumer.stage_offsets(value.committable)
                        consumer.commit_offsets()

            # after restart, we should have recovered all payloads
            assert len(recovered_payloads) == NUM_PAYLOADS


def test_commit_codec() -> None:
    commit = Commit(
        "group", Partition(Topic("topic"), 0), 0, time.time(), time.time() - 5
    )
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
