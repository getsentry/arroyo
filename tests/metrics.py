from typing import MutableSequence, NamedTuple, Optional, Union

from arroyo.utils.metrics import MetricName, Metrics, Tags


class Increment(NamedTuple):
    name: MetricName
    value: Union[int, float]
    tags: Optional[Tags]


class Gauge(NamedTuple):
    name: MetricName
    value: Union[int, float]
    tags: Optional[Tags]


class Timing(NamedTuple):
    name: MetricName
    value: Union[int, float]
    tags: Optional[Tags]


class _TestingMetricsBackend(Metrics):
    """
    A metrics backend that logs all metrics recorded. Intended for testing
    the behavior of the metrics implementations themselves. Not for general
    use (it will cause unbounded memory consumption.)
    """

    def __init__(self) -> None:
        self.calls: MutableSequence[Union[Increment, Gauge, Timing]] = []

    def increment(
        self,
        name: MetricName,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        self.calls.append(Increment(name, value, tags))

    def gauge(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.calls.append(Gauge(name, value, tags))

    def timing(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.calls.append(Timing(name, value, tags))


TestingMetricsBackend = _TestingMetricsBackend()
