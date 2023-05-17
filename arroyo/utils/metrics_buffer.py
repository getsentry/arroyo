import time
from collections import defaultdict
from typing import MutableMapping, Optional, Union

from arroyo.utils.metrics import Metrics, Tags

METRICS_FREQUENCY_SEC = 1.0  # In seconds


class MetricsBuffer:
    """
    A buffer for metrics, preaggregating them before they are sent to the
    underlying metrics backend.

    Should only be used in performance sensitive code if there's no other
    option. Buffering is lossy.

    Buffer is flushed every second at least.
    """

    def __init__(
        self,
        metrics: Metrics,
        tags: Optional[Tags] = None,
        record_timers_min: bool = False,
        record_timers_max: bool = False,
        record_timers_avg: bool = False,
        record_timers_count: bool = False,
        record_timers_sum: bool = False,
    ) -> None:
        self.__metrics = metrics
        self.__tags = tags

        self.__record_timers_min = record_timers_min
        self.__record_timers_max = record_timers_max
        self.__record_timers_avg = record_timers_avg
        self.__record_timers_count = record_timers_count
        self.__record_timers_sum = record_timers_sum

        self.__timers_max: MutableMapping[str, float] = defaultdict(float)
        self.__timers_min: MutableMapping[str, float] = defaultdict(float)
        self.__timers_sum: MutableMapping[str, float] = defaultdict(float)
        self.__timers_count: MutableMapping[str, float] = defaultdict(float)
        self.__counters: MutableMapping[str, int] = defaultdict(int)
        self.__gauges: MutableMapping[str, Union[float, int]] = defaultdict(int)
        self.__reset()

    def timing(self, metric: str, duration: float, tags: Optional[Tags] = None) -> None:
        self.__timers_max[metric] = max(self.__timers_max[metric], duration)
        self.__timers_min[metric] = min(self.__timers_max[metric], duration)
        self.__timers_sum[metric] += duration
        self.__timers_count[metric] += 1
        self.__throttled_record()

    def increment(self, metric: str, delta: int, tags: Optional[Tags] = None) -> None:
        self.__counters[metric] += delta
        self.__throttled_record()

    def gauge(
        self, metric: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__gauges[metric] = value

    def flush(self) -> None:
        value: Union[float, int]

        if self.__record_timers_sum:
            for metric, value in self.__timers_sum.items():
                self.__metrics.timing(f"{metric}.buffered_sum", value, tags=self.__tags)

        if self.__record_timers_avg:
            for metric, value in self.__timers_sum.items():
                self.__metrics.timing(
                    f"{metric}.buffered_avg",
                    value / self.__timers_count[metric],
                    tags=self.__tags,
                )

        if self.__record_timers_count:
            for metric, value in self.__timers_count.items():
                self.__metrics.timing(
                    f"{metric}.buffered_count", value, tags=self.__tags
                )

        if self.__record_timers_max:
            for metric, value in self.__timers_max.items():
                self.__metrics.timing(f"{metric}.buffered_max", value, tags=self.__tags)

        if self.__record_timers_min:
            for metric, value in self.__timers_min.items():
                self.__metrics.timing(f"{metric}.buffered_min", value, tags=self.__tags)

        for metric, value in self.__counters.items():
            self.__metrics.increment(metric, value, tags=self.__tags)

        for metric, value in self.__gauges.items():
            self.__metrics.gauge(metric, value, tags=self.__tags)

        self.__reset()

    def __reset(self) -> None:
        self.__timers_min.clear()
        self.__timers_max.clear()
        self.__timers_sum.clear()
        self.__timers_count.clear()
        self.__counters.clear()
        self.__gauges.clear()
        self.__last_record_time = time.time()

    def __throttled_record(self) -> None:
        if time.time() - self.__last_record_time > METRICS_FREQUENCY_SEC:
            self.flush()
