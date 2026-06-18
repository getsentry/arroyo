import json
from concurrent.futures import Future
from unittest import mock

from confluent_kafka import TIMESTAMP_CREATE_TIME, KafkaError
from confluent_kafka import Message as ConfluentMessage

from arroyo.backends.kafka.configuration import producer_stats_callback
from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.types import BrokerValue
from tests.metrics import Increment, TestingMetricsBackend


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_with_both_latencies(
    mock_get_metrics: mock.Mock,
) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps(
        {
            "brokers": {
                "1": {
                    "int_latency": {"p99": 2000, "avg": 1000},
                    "outbuf_latency": {"p99": 4000, "avg": 2000},
                }
            }
        }
    )

    producer_stats_callback(stats_json, None)

    assert mock_metrics.gauge.call_count == 2
    mock_metrics.gauge.assert_any_call(
        "arroyo.producer.librdkafka.p99_int_latency",
        2.0,
        tags={"broker_id": "1", "producer_name": "unknown"},
    )
    mock_metrics.gauge.assert_any_call(
        "arroyo.producer.librdkafka.p99_outbuf_latency",
        4.0,
        tags={"broker_id": "1", "producer_name": "unknown"},
    )


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_no_brokers(mock_get_metrics: mock.Mock) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps({})

    producer_stats_callback(stats_json, None)

    mock_metrics.gauge.assert_not_called()


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_empty_broker_stats(
    mock_get_metrics: mock.Mock,
) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps({"brokers": {"1": {}}})

    producer_stats_callback(stats_json, None)

    mock_metrics.gauge.assert_not_called()


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_with_all_metrics(mock_get_metrics: mock.Mock) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps(
        {
            "brokers": {
                "1": {
                    "int_latency": {"p99": 2000, "avg": 1000},  # 2/1 milliseconds
                    "outbuf_latency": {"p99": 4000, "avg": 2000},  # 4/2 milliseconds
                    "rtt": {"p99": 1500, "avg": 750},  # 1.5/0.75 milliseconds
                }
            }
        }
    )

    producer_stats_callback(stats_json, None)

    assert mock_metrics.gauge.call_count == 3
    mock_metrics.gauge.assert_any_call(
        "arroyo.producer.librdkafka.p99_int_latency",
        2.0,
        tags={"broker_id": "1", "producer_name": "unknown"},
    )
    mock_metrics.gauge.assert_any_call(
        "arroyo.producer.librdkafka.p99_outbuf_latency",
        4.0,
        tags={"broker_id": "1", "producer_name": "unknown"},
    )
    mock_metrics.gauge.assert_any_call(
        "arroyo.producer.librdkafka.p99_rtt",
        1.5,
        tags={"broker_id": "1", "producer_name": "unknown"},
    )


class TestKafkaProducerMetrics:
    """
    Tests for the produce status metrics recorded by KafkaProducer's delivery
    callback.
    """

    def test_delivery_callback_records_success(self) -> None:
        """The delivery callback records a success metric and resolves the future"""
        producer = KafkaProducer(
            {"bootstrap.servers": "fake:9092", "client.id": "test-producer-name"}
        )
        payload = KafkaPayload(None, b"value", [])
        future: Future[BrokerValue[KafkaPayload]] = Future()

        mock_message = mock.Mock(spec=ConfluentMessage)
        mock_message.timestamp.return_value = (TIMESTAMP_CREATE_TIME, 1234567890000)
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 0

        producer._KafkaProducer__delivery_callback(future, payload, None, mock_message)  # type: ignore[attr-defined]
        producer._KafkaProducer__flush_metrics()  # type: ignore[attr-defined]

        assert future.result().payload == payload
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {"status": "success", "producer_name": "test-producer-name"},
            )
            in TestingMetricsBackend.calls
        )

    def test_delivery_callback_records_error(self) -> None:
        """The delivery callback records an error metric and sets the exception"""
        producer = KafkaProducer({"bootstrap.servers": "fake:9092"})
        payload = KafkaPayload(None, b"value", [])
        future: Future[BrokerValue[KafkaPayload]] = Future()

        mock_error = mock.Mock(spec=KafkaError)
        mock_message = mock.Mock(spec=ConfluentMessage)

        producer._KafkaProducer__delivery_callback(  # type: ignore[attr-defined]
            future, payload, mock_error, mock_message
        )
        producer._KafkaProducer__flush_metrics()  # type: ignore[attr-defined]

        assert future.exception() is not None
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "error"})
            in TestingMetricsBackend.calls
        )
