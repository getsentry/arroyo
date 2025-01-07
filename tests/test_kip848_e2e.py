from typing import Any

import time
import contextlib
from contextlib import closing
import os
import signal
import threading
# import random
import logging
from typing import Iterator, Mapping

from confluent_kafka.admin import AdminClient, NewTopic
from arroyo.types import Message, Topic
from arroyo.backends.kafka.configuration import build_kafka_consumer_configuration
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload
from arroyo.processing.strategies import RunTask, CommitOffsets
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


def print_msg(message: Message[Any]) -> Message[Any]:
    ((partition, offset),) = message.committable.items()
    print(f"message: {partition.index}-{offset}")
    return message


class Factory(ProcessingStrategyFactory):
    def create_with_partitions(self, commit, partitions):
        print("assign: ", [p.index for p in partitions])
        return Strat(print_msg, CommitOffsets(commit))


class Strat(RunTask):
    def join(self, *args, **kwargs):
        print("joining strategy, sleeping 5 seconds")
        time.sleep(5)
        print("joining strategy, sleeping 5 seconds -- DONE")
        return super().join(*args, **kwargs)


def test_kip848_e2e() -> None:

    default_config = {
        "bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092")
    }

    # Produce 100 of messages for our consumer
    with get_topic(default_config, 2) as topic:
        producer = KafkaProducer(default_config)

        with closing(producer):
            for i in range(100):
                message = KafkaPayload(None, f"{i}", [])
                producer.produce(topic, message).result()

        consumer_config = build_kafka_consumer_configuration(
            default_config,
            group_id="kip848",
        )

        consumer_config["group.protocol"] = "consumer"
        consumer_config["partition.assignment.strategy"] = "cooperative-sticky"
        consumer_config.pop("session.timeout.ms", None)
        consumer_config.pop("max.poll.interval.ms", None)
        consumer_config.pop("partition.assignment.strategy", None)
        consumer_config.pop("group.protocol.type", None)
        consumer_config.pop("heartbeat.interval.ms", None)

        consumer = KafkaConsumer(consumer_config)

        processor = StreamProcessor(
            consumer=consumer, topic=Topic(TOPIC), processor_factory=Factory()
        )

        def handler(signum: int, frame: Any) -> None:
            processor.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        def shutdown_randomly():
            # s = random.randint(30, 120)
            s = 5  # tmp: shutdown after 5 seconds
            time.sleep(s)
            print(f"shutting down after {s} seconds")
            processor.signal_shutdown()

        t = threading.Thread(target=shutdown_randomly, daemon=True)
        t.start()

        processor.run()
