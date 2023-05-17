from arroyo.utils.metrics_buffer import MetricsBuffer
from tests.metrics import Increment, TestingMetricsBackend


def test_basic() -> None:
    buffer = MetricsBuffer(TestingMetricsBackend)
    buffer.increment("test", 1)
    buffer.increment("test", 1)
    buffer.increment("test", 1)

    buffer.flush()

    assert TestingMetricsBackend.calls == [Increment("test", 3, None)]
