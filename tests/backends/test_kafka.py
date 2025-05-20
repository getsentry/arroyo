import contextlib
import itertools
import os
import pickle
import time
import uuid
from contextlib import closing
from pickle import PickleBuffer
from typing import Any, Iterator, Mapping, MutableSequence, Optional
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
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.types import BrokerValue, Partition, Topic
from tests.backends.mixins import StreamsTestMixin

commit_codec = CommitCodec()


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
    configuration = dict(configuration)
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


class TestKafkaStreams(StreamsTestMixin[KafkaPayload]):
    kip_848 = False

    @property
    def configuration(self) -> KafkaBrokerConfig:
        config = {
            "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092"),
        }

        return build_kafka_configuration(config)

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
        max_poll_interval_ms: Optional[int] = None,
    ) -> KafkaConsumer:
        configuration = {
            **self.configuration,
            "auto.offset.reset": auto_offset_reset,
            "arroyo.strict.offset.reset": strict_offset_reset,
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "enable.partition.eof": enable_end_of_partition,
            "group.id": group if group is not None else uuid.uuid1().hex,
            "session.timeout.ms": 10000,
        }

        if max_poll_interval_ms:
            configuration["max.poll.interval.ms"] = max_poll_interval_ms

            # session timeout cannot be higher than max poll interval
            if max_poll_interval_ms < 45000:
                configuration["session.timeout.ms"] = max_poll_interval_ms

        if self.cooperative_sticky:
            configuration["partition.assignment.strategy"] = "cooperative-sticky"

        if self.kip_848:
            configuration["group.protocol"] = "consumer"
            configuration.pop("session.timeout.ms")
            configuration.pop("max.poll.interval.ms", None)
            assert "group.protocol.type" not in configuration
            assert "heartbeat.interval.ms" not in configuration

        return KafkaConsumer(configuration)

    def get_producer(self, use_simple_futures: bool = False) -> KafkaProducer:
        return KafkaProducer(self.configuration, use_simple_futures=use_simple_futures)

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

    @pytest.mark.parametrize("use_simple_futures", [True, False])
    def test_auto_offset_reset_latest(self, use_simple_futures: bool) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer(use_simple_futures)) as producer:
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

    @pytest.mark.parametrize("use_simple_futures", [True, False])
    def test_producer_future_behavior(self, use_simple_futures: bool) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer(use_simple_futures)) as producer:
                future = producer.produce(topic, next(self.get_payloads()))
                assert not future.done()
                assert future.result(5.0)
                assert future.done()

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

    def test_consumer_polls_when_paused(self) -> None:
        strategy = mock.Mock()
        factory = mock.Mock()
        factory.create_with_partitions.return_value = strategy

        poll_interval = 6000

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer, closing(
                self.get_consumer(max_poll_interval_ms=poll_interval)
            ) as consumer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

                processor = StreamProcessor(consumer, topic, factory, IMMEDIATE)

                # Wait for the consumer to subscribe and first message to be processed
                for _ in range(1000):
                    processor._run_once()
                    if strategy.submit.call_count > 0:
                        break
                    time.sleep(0.1)

                assert strategy.submit.call_count == 1

                # Now we start raising message rejected. the next produced message doesn't get processed
                strategy.submit.side_effect = MessageRejected()

                producer.produce(topic, next(self.get_payloads())).result(5.0)
                processor._run_once()
                assert consumer.paused() == []
                # After ~5 seconds the consumer should be paused. On the next two calls to run_once it
                # will pause itself, then poll the consumer.
                time.sleep(5.0)
                processor._run_once()
                processor._run_once()
                assert len(consumer.paused()) == 1

                # Now we exceed the poll interval. After that we stop raising MessageRejected and
                # the consumer unpauses itself.
                time.sleep(2.0)
                strategy.submit.side_effect = None
                processor._run_once()
                assert consumer.paused() == []


class TestKafkaStreamsIncrementalRebalancing(TestKafkaStreams):
    # re-test the kafka consumer with cooperative-sticky rebalancing
    cooperative_sticky = True


class TestKafkaStreamsKip848(TestKafkaStreams):
    kip_848 = True

    @pytest.mark.xfail(reason="To be fixed")
    def test_pause_resume_rebalancing(self) -> None:
        super().test_pause_resume_rebalancing()


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
