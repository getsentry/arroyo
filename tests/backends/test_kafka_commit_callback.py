from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from arroyo.backends.kafka import KafkaConsumer
from arroyo.backends.kafka.configuration import build_kafka_configuration
from tests.metrics import Increment, TestingMetricsBackend


def test_commit_callback_success_metric() -> None:
    configuration = build_kafka_configuration(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "arroyo.enable.auto.commit": True,
        }
    )

    consumer = KafkaConsumer(configuration)

    TestingMetricsBackend.calls.clear()

    partitions = [
        ConfluentTopicPartition("test-topic", 0, 10),
        ConfluentTopicPartition("test-topic", 1, 20),
    ]

    consumer._KafkaConsumer__on_commit_callback(None, partitions)  # type: ignore[attr-defined]

    assert (
        Increment(
            "arroyo.consumer.commit_status",
            1,
            {"group_id": "test-group", "status": "success"},
        )
        in TestingMetricsBackend.calls
    )


def test_commit_callback_error_metric() -> None:
    """Test that failed commits emit a metric with status=error"""
    configuration = build_kafka_configuration(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "arroyo.enable.auto.commit": True,
        }
    )

    consumer = KafkaConsumer(configuration)

    TestingMetricsBackend.calls.clear()

    kafka_error = KafkaError(1, "Test commit error")
    error = KafkaException(kafka_error)

    partitions = [
        ConfluentTopicPartition("test-topic", 0, 10),
    ]

    consumer._KafkaConsumer__on_commit_callback(error, partitions)  # type: ignore[attr-defined]

    assert (
        Increment(
            "arroyo.consumer.commit_status",
            1,
            {"group_id": "test-group", "status": "error"},
        )
        in TestingMetricsBackend.calls
    )
