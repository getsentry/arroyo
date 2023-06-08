import pytest

from arroyo.utils.metrics import Gauge, MetricName, configure_metrics, get_metrics
from tests.metrics import Gauge as GaugeCall
from tests.metrics import TestingMetricsBackend, _TestingMetricsBackend


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
