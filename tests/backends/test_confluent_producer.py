from typing import Optional
from unittest import mock

import pytest
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage

from arroyo.backends.kafka.consumer import ConfluentProducer
from tests.metrics import Increment, TestingMetricsBackend


class TestConfluentProducer:
    """
    Tests for ConfluentProducer wrapper around confluent_kafka.Producer.

    Note: confluent_kafka.Producer is an immutable C extension that cannot be mocked.
    Tests verify callback wrapping, metrics recording, and produce() method behavior.
    """

    def test_metrics_callback_records_success(self) -> None:
        """Test that the metrics callback records success metric"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        mock_message = mock.Mock(spec=ConfluentMessage)
        producer._ConfluentProducer__metrics_delivery_callback(None, mock_message)
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "success"})
            in TestingMetricsBackend.calls
        )

    def test_metrics_callback_records_error(self) -> None:
        """Test that the metrics callback records error metric"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        mock_error = mock.Mock(spec=KafkaError)
        mock_message = mock.Mock(spec=ConfluentMessage)
        producer._ConfluentProducer__metrics_delivery_callback(mock_error, mock_message)
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "error"})
            in TestingMetricsBackend.calls
        )

    def test_delivery_callback_wraps_user_callback(self) -> None:
        """Test that the delivery callback wrapper calls both metrics and user callbacks"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        user_callback_invoked = []

        def user_callback(
            error: Optional[KafkaError], message: ConfluentMessage
        ) -> None:
            user_callback_invoked.append((error, message))

        wrapped = producer._ConfluentProducer__delivery_callback(user_callback)
        mock_message = mock.Mock(spec=ConfluentMessage)
        wrapped(None, mock_message)
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "success"})
            in TestingMetricsBackend.calls
        )
        assert len(user_callback_invoked) == 1
        assert user_callback_invoked[0] == (None, mock_message)

    def test_delivery_callback_without_user_callback(self) -> None:
        """Test that the delivery callback works when user callback is None"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        wrapped = producer._ConfluentProducer__delivery_callback(None)
        mock_message = mock.Mock(spec=ConfluentMessage)
        wrapped(None, mock_message)
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "success"})
            in TestingMetricsBackend.calls
        )

    def test_delivery_callback_calls_user_callback_with_error(self) -> None:
        """Test that user callback receives error status"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        user_callback_invoked = []

        def user_callback(
            error: Optional[KafkaError], message: ConfluentMessage
        ) -> None:
            user_callback_invoked.append((error, message))

        wrapped = producer._ConfluentProducer__delivery_callback(user_callback)
        mock_error = mock.Mock(spec=KafkaError)
        mock_message = mock.Mock(spec=ConfluentMessage)
        wrapped(mock_error, mock_message)
        assert (
            Increment("arroyo.producer.produce_status", 1, {"status": "error"})
            in TestingMetricsBackend.calls
        )
        assert len(user_callback_invoked) == 1
        assert user_callback_invoked[0] == (mock_error, mock_message)

    def test_multiple_callbacks_record_multiple_metrics(self) -> None:
        """Test that multiple callback invocations record multiple metrics"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        TestingMetricsBackend.calls.clear()
        mock_message = mock.Mock(spec=ConfluentMessage)
        mock_error = mock.Mock(spec=KafkaError)

        producer._ConfluentProducer__metrics_delivery_callback(None, mock_message)
        producer._ConfluentProducer__metrics_delivery_callback(mock_error, mock_message)
        producer._ConfluentProducer__metrics_delivery_callback(None, mock_message)
        success_metrics = [
            call
            for call in TestingMetricsBackend.calls
            if isinstance(call, Increment)
            and call.name == "arroyo.producer.produce_status"
            and call.tags == {"status": "success"}
        ]
        error_metrics = [
            call
            for call in TestingMetricsBackend.calls
            if isinstance(call, Increment)
            and call.name == "arroyo.producer.produce_status"
            and call.tags == {"status": "error"}
        ]

        assert len(success_metrics) == 2
        assert len(error_metrics) == 1

    def test_produce_wraps_callback_parameter(self) -> None:
        """Test that produce() properly wraps the callback parameter"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        user_callback = mock.Mock()
        with mock.patch.object(
            producer,
            "_ConfluentProducer__delivery_callback",
            wraps=producer._ConfluentProducer__delivery_callback,
        ) as mock_wrapper:
            try:
                producer.produce(topic="test", value=b"data", callback=user_callback)
            except Exception:
                pass
            mock_wrapper.assert_called_once_with(user_callback)

    def test_produce_wraps_on_delivery_parameter(self) -> None:
        """Test that produce() properly wraps the on_delivery parameter"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        user_callback = mock.Mock()

        with mock.patch.object(
            producer,
            "_ConfluentProducer__delivery_callback",
            wraps=producer._ConfluentProducer__delivery_callback,
        ) as mock_wrapper:
            try:
                producer.produce(topic="test", value=b"data", on_delivery=user_callback)
            except Exception:
                pass
            mock_wrapper.assert_called_once_with(user_callback)

    def test_produce_callback_takes_precedence_over_on_delivery(self) -> None:
        """Test that callback parameter takes precedence over on_delivery"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})
        callback1 = mock.Mock()
        callback2 = mock.Mock()

        with mock.patch.object(
            producer,
            "_ConfluentProducer__delivery_callback",
            wraps=producer._ConfluentProducer__delivery_callback,
        ) as mock_wrapper:
            try:
                producer.produce(
                    topic="test",
                    value=b"data",
                    callback=callback1,
                    on_delivery=callback2,
                )
            except Exception:
                pass
            mock_wrapper.assert_called_once_with(callback1)

    def test_produce_without_callback_still_adds_metrics(self) -> None:
        """Test that produce() adds metrics callback even when user provides none"""
        producer = ConfluentProducer({"bootstrap.servers": "fake:9092"})

        with mock.patch.object(
            producer,
            "_ConfluentProducer__delivery_callback",
            wraps=producer._ConfluentProducer__delivery_callback,
        ) as mock_wrapper:
            try:
                producer.produce(topic="test", value=b"data")
            except Exception:
                pass
            mock_wrapper.assert_called_once_with(None)

    def test_instantiation(self) -> None:
        """Test that ConfluentProducer can be instantiated"""
        config = {"bootstrap.servers": "fake:9092"}
        producer = ConfluentProducer(config)

        assert isinstance(producer, ConfluentProducer)
