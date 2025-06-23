import json
from unittest import mock

from arroyo.backends.kafka.configuration import producer_stats_callback


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

    producer_stats_callback(stats_json)

    assert mock_metrics.timing.call_count == 4
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.p99_int_latency",
        2.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.avg_int_latency",
        1.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.p99_outbuf_latency",
        4.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.avg_outbuf_latency",
        2.0,
        tags={"broker_id": "1"},
    )


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_no_brokers(mock_get_metrics: mock.Mock) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps({})

    producer_stats_callback(stats_json)

    mock_metrics.timing.assert_not_called()


@mock.patch("arroyo.backends.kafka.configuration.get_metrics")
def test_producer_stats_callback_empty_broker_stats(
    mock_get_metrics: mock.Mock,
) -> None:
    mock_metrics = mock.Mock()
    mock_get_metrics.return_value = mock_metrics

    stats_json = json.dumps({"brokers": {"1": {}}})

    producer_stats_callback(stats_json)

    mock_metrics.timing.assert_not_called()


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

    producer_stats_callback(stats_json)

    assert mock_metrics.timing.call_count == 6
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.p99_int_latency",
        2.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.avg_int_latency",
        1.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.p99_outbuf_latency",
        4.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.avg_outbuf_latency",
        2.0,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.p99_rtt",
        1.5,
        tags={"broker_id": "1"},
    )
    mock_metrics.timing.assert_any_call(
        "arroyo.producer.librdkafka.avg_rtt",
        0.75,
        tags={"broker_id": "1"},
    )
