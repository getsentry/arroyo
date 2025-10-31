from typing import Optional
from unittest import mock

from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentKafkaProducer

from arroyo.backends.kafka.consumer import ConfluentProducer
from tests.metrics import Increment, TestingMetricsBackend


class TestConfluentProducer:
    """
    Tests for ConfluentProducer wrapper around confluent_kafka.Producer.
    """

    def test_init(self) -> None:
        """Test that ConfluentProducer can be instantiated"""
        config = {"bootstrap.servers": "fake:9092"}
        producer = ConfluentProducer(config)

        assert isinstance(producer, ConfluentProducer)
        assert isinstance(producer, ConfluentKafkaProducer)

    def test_metrics_callback_records_success(self) -> None:
        """Test that the metrics callback records success metric"""
        producer = ConfluentProducer(
            {"bootstrap.servers": "fake:9092", "client.id": "test-producer-name"}
        )
        mock_message = mock.Mock(spec=ConfluentMessage)
        producer._ConfluentProducer__metrics_delivery_callback(None, mock_message)
        producer.flush()  # Flush buffered metrics
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {"status": "success", "producer_name": "test-producer-name"},
            )
            in TestingMetricsBackend.calls
        )

    def test_metrics_callback_records_error(self) -> None:
        """Test that the metrics callback records error metric"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        mock_error = mock.Mock(spec=KafkaError)
        mock_message = mock.Mock(spec=ConfluentMessage)
        producer._ConfluentProducer__metrics_delivery_callback(mock_error, mock_message)
        producer.flush()  # Flush buffered metrics
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "error"})
            in TestingMetricsBackend.calls
        )

    def test_delivery_callback_wraps_user_callback(self) -> None:
        """Test that the delivery callback wrapper calls both metrics and user callbacks"""
        producer = ConfluentProducer(
            {"bootstrap.servers": "fake:9092", "client.id": "test-producer-name"}
        )
        user_callback_invoked = []

        def user_callback(
            error: Optional[KafkaError], message: ConfluentMessage
        ) -> None:
            user_callback_invoked.append((error, message))

        wrapped = producer._ConfluentProducer__delivery_callback(user_callback)
        mock_message = mock.Mock(spec=ConfluentMessage)
        wrapped(None, mock_message)
        producer.flush()  # Flush buffered metrics
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {"status": "success", "producer_name": "test-producer-name"},
            )
            in TestingMetricsBackend.calls
        )
        assert len(user_callback_invoked) == 1
        assert user_callback_invoked[0] == (None, mock_message)
