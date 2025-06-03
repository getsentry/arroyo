import pytest

from arroyo.utils.metrics import Gauge, MetricName, configure_metrics, get_metrics, get_consumer_metrics
from tests.metrics import Gauge as GaugeCall
from tests.metrics import Increment, Timing, TestingMetricsBackend, _TestingMetricsBackend


def test_gauge_simple() -> None:
    backend = TestingMetricsBackend

    name: MetricName = "name"  # type: ignore
    tags = {"tag": "value"}
    gauge = Gauge(backend, name, tags)

    with gauge:
        pass

    assert backend.calls == [
        GaugeCall(name, 0.0, tags),
        GaugeCall(name, 1.0, tags),
        GaugeCall(name, 0.0, tags),
    ]


def test_configure_metrics() -> None:
    assert get_metrics() == TestingMetricsBackend

    with pytest.raises(AssertionError):
        configure_metrics(_TestingMetricsBackend())

    # Can be reset to something else with force
    configure_metrics(_TestingMetricsBackend(), force=True)
    assert get_metrics() != TestingMetricsBackend


def test_consumer_metrics_wrapper() -> None:
    """Test that ConsumerMetricsWrapper automatically adds consumer_member_id to all metrics."""
    # Reset to a fresh backend
    backend = _TestingMetricsBackend()
    configure_metrics(backend, force=True)

    consumer_member_id = "test-consumer-123"
    consumer_metrics = get_consumer_metrics(consumer_member_id)

    # Test increment
    consumer_metrics.increment("test.counter", 5, tags={"extra": "tag"})

    # Test gauge
    consumer_metrics.gauge("test.gauge", 10.5)

    # Test timing
    consumer_metrics.timing("test.timer", 100, tags={"another": "tag"})

    expected_calls = [
        Increment("test.counter", 5, {"consumer_member_id": consumer_member_id, "extra": "tag"}),
        GaugeCall("test.gauge", 10.5, {"consumer_member_id": consumer_member_id}),
        Timing("test.timer", 100, {"consumer_member_id": consumer_member_id, "another": "tag"}),
    ]

    assert backend.calls == expected_calls
