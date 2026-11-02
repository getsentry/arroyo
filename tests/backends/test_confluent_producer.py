from typing import Optional
from unittest import mock

from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentKafkaProducer

from arroyo.backends.kafka.consumer import ConfluentProducer
from tests.metrics import Increment, TestingMetricsBackend, Timing


def make_message(
    key: Optional[bytes] = None,
    value: Optional[bytes] = b"payload",
    headers: Optional[list[tuple[str, Optional[bytes]]]] = None,
) -> mock.Mock:
    message = mock.Mock(spec=ConfluentMessage)
    message.topic.return_value = "test-topic"
    message.key.return_value = key
    message.value.return_value = value
    message.headers.return_value = headers
    return message


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
        mock_message = make_message()
        producer._ConfluentProducer__metrics_delivery_callback(None, mock_message, 1000.0)  # type: ignore[attr-defined]
        producer.flush()  # Flush buffered metrics
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {
                    "status": "success",
                    "topic": "test-topic",
                    "producer_name": "test-producer-name",
                },
            )
            in TestingMetricsBackend.calls
        )

    def test_metrics_callback_records_error(self) -> None:
        """Test that the metrics callback records error metric"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        mock_error = mock.Mock(spec=KafkaError)
        mock_message = make_message()
        producer._ConfluentProducer__metrics_delivery_callback(mock_error, mock_message, 1000.0)  # type: ignore[attr-defined]
        producer.flush()  # Flush buffered metrics
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {"status": "error", "topic": "test-topic"},
            )
            in TestingMetricsBackend.calls
        )

    @mock.patch("arroyo.backends.kafka.consumer.time.time")
    def test_metrics_callback_records_callback_latency(
        self, mock_time: mock.Mock
    ) -> None:
        """Test that the metrics callback records a callback_latency timing metric"""
        mock_time.return_value = 1000.5

        producer = ConfluentProducer(
            {"bootstrap.servers": "fake:9092", "client.id": "test-producer-name"}
        )
        mock_message = make_message()
        producer._ConfluentProducer__metrics_delivery_callback(  # type: ignore[attr-defined]
            None, mock_message, time_of_produce=1000.0
        )
        producer.flush()

        assert (
            Timing(
                "arroyo.producer.callback_latency",
                0.5,
                {"producer_name": "test-producer-name"},
            )
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

        wrapped = producer._ConfluentProducer__delivery_callback(user_callback, 1000.0)  # type: ignore[attr-defined]
        mock_message = make_message()
        wrapped(None, mock_message)
        producer.flush()  # Flush buffered metrics
        assert (
            Increment(
                "arroyo.producer.produce_status",
                1,
                {
                    "status": "success",
                    "topic": "test-topic",
                    "producer_name": "test-producer-name",
                },
            )
            in TestingMetricsBackend.calls
        )
        assert len(user_callback_invoked) == 1
        assert user_callback_invoked[0] == (None, mock_message)

    def test_metrics_callback_records_produced_bytes(self) -> None:
        """Successful deliveries record key, value and header bytes per topic"""
        producer = ConfluentProducer(
            {"bootstrap.servers": "fake:9092", "client.id": "test-producer-name"}
        )
        callback = producer._ConfluentProducer__metrics_delivery_callback  # type: ignore[attr-defined]
        # key 3 + value 7 + headers (7 + 1) + (5 + 0)
        callback(
            None,
            make_message(key=b"key", headers=[("version", b"1"), ("empty", None)]),
            1000.0,
        )
        # value 7
        callback(None, make_message(), 1000.0)
        # Failed deliveries are not counted
        callback(mock.Mock(spec=KafkaError), make_message(), 1000.0)
        producer.flush()

        assert (
            Increment(
                "arroyo.producer.produced_bytes",
                30,
                {"topic": "test-topic", "producer_name": "test-producer-name"},
            )
            in TestingMetricsBackend.calls
        )
