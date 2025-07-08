from __future__ import annotations

from abc import abstractmethod
from typing import Any, Mapping, Optional, Protocol, Union, runtime_checkable

from arroyo.utils.metric_defs import MetricName

Tags = Mapping[str, str]


@runtime_checkable
class Metrics(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self,
        name: MetricName,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Sets a gauge metric to the given value.
        """
        raise NotImplementedError

    @abstractmethod
    def timing(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Records a timing metric.
        """
        raise NotImplementedError


class DummyMetricsBackend(Metrics):
    """
    Default metrics backend that does not record anything.
    """

    def increment(
        self,
        name: MetricName,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
    ) -> None:
        pass

    def gauge(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        pass

    def timing(
        self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        pass


class Gauge:
    def __init__(
        self,
        metrics: Metrics,
        name: MetricName,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__metrics = metrics
        self.__name = name
        self.__tags = tags

        self.__value = 0.0

        self.__report()

    def __enter__(self) -> None:
        self.increment()

    def __exit__(
        self,
        type: Optional[Any] = None,
        value: Optional[Any] = None,
        traceback: Optional[Any] = None,
    ) -> None:
        self.decrement()

    def __report(self) -> None:
        self.__metrics.gauge(self.__name, self.__value, self.__tags)

    def increment(self, value: float = 1.0) -> None:
        self.__value += value
        self.__report()

    def decrement(self, value: float = 1.0) -> None:
        self.__value -= value
        self.__report()


_metrics_backend: Optional[Metrics] = None
_dummy_metrics_backend = DummyMetricsBackend()


def configure_metrics(metrics: Metrics, force: bool = False) -> None:
    """
    Metrics can generally only be configured once, unless force is passed
    on subsequent initializations.
    """
    global _metrics_backend

    if not force:
        assert _metrics_backend is None, "Metrics is already set"

    # Perform a runtime check of metrics instance upon initialization of
    # this class to avoid errors down the line when it is used.
    assert isinstance(metrics, Metrics)
    _metrics_backend = metrics


def get_metrics() -> Metrics:
    if _metrics_backend is None:
        return _dummy_metrics_backend
    return _metrics_backend


__all__ = ["configure_metrics", "Metrics", "MetricName", "Tags"]
