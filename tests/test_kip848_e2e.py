from typing import Any

import time
import contextlib
from contextlib import closing
import os
import threading
import logging
from typing import Iterator, Mapping

from confluent_kafka.admin import AdminClient, NewTopic
from arroyo.types import Commit, Message, Partition, Topic
from arroyo.backends.kafka.configuration import build_kafka_consumer_configuration
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload
from arroyo.processing.strategies import RunTask, CommitOffsets, ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.processing.processor import StreamProcessor
from arroyo.backends.kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

TOPIC = "test-kip848"


@contextlib.contextmanager
def get_topic(
    configuration: Mapping[str, Any], partitions_count: int
) -> Iterator[Topic]:
    name = TOPIC
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


def test_kip848_e2e() -> None:
    counter = 0

    def print_msg(message: Message[Any]) -> Message[Any]:
        nonlocal counter
        ((partition, offset),) = message.committable.items()
        print(f"message: {partition.index}-{offset}")
        counter += 1
        return message

    class Strat(RunTask[Any, Any]):
        def join(self, *args: Any, **kwargs: Any) -> None:
            print("joining strategy, sleeping 5 seconds")
            time.sleep(5)
            print("joining strategy, sleeping 5 seconds -- DONE")
            return super().join(*args, **kwargs)

    class Factory(ProcessingStrategyFactory[KafkaPayload]):
        def create_with_partitions(
            self, commit: Commit, partitions: Mapping[Partition, int]
        ) -> ProcessingStrategy[KafkaPayload]:
            print("assign: ", [p.index for p in partitions])
            return Strat(print_msg, CommitOffsets(commit))

    default_config = {
        "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092")
    }

    with get_topic(default_config, 2) as topic:
        producer = KafkaProducer(default_config)

        with closing(producer):
            for i in range(30):
                message = KafkaPayload(None, i.to_bytes(1, "big"), [])
                producer.produce(topic, message).result()

        consumer_config = build_kafka_consumer_configuration(
            default_config,
            group_id="kip848",
        )

        consumer_config["group.protocol"] = "consumer"
        consumer_config.pop("session.timeout.ms", None)
        consumer_config.pop("max.poll.interval.ms", None)
        consumer_config.pop("partition.assignment.strategy", None)
        consumer_config.pop("group.protocol.type", None)
        consumer_config.pop("heartbeat.interval.ms", None)

        consumer = KafkaConsumer(consumer_config)

        processor = StreamProcessor(
            consumer=consumer, topic=Topic(TOPIC), processor_factory=Factory()
        )

        def shutdown() -> None:
            for i in range(100):
                time.sleep(0.1)
                if counter == 30:
                    break
            print("shutting down")
            processor.signal_shutdown()

        t = threading.Thread(target=shutdown)
        t.start()

        processor.run()

        assert counter == 30

        t.join()
