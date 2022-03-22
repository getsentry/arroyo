from collections import deque
from time import time
from typing import NamedTuple, Optional, Sequence, Tuple

from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.utils.metrics import get_metrics


class _Bucket(NamedTuple):
    """
    1-second time bucket to count number of hits in this second.
    """

    timestamp: int
    hits: int


class CountInvalidMessagePolicy(DeadLetterQueuePolicy):
    """
    Ignore invalid messages up to a certain limit per time unit window in seconds.
    This window is 1 minute by default. The exception associated with the invalid
    message is raised for all incoming invalid messages which go past this limit.

    A saved state in the form `[(<timestamp: int>, <hits: int>), ...]` can be passed
    on init to load the previously saved state. This state should be aggregated to
    1 second buckets.
    """

    def __init__(
        self,
        limit: int,
        seconds: int = 60,
        load_state: Optional[Sequence[Tuple[int, int]]] = None,
    ) -> None:
        self.__limit = limit
        self.__seconds = seconds
        self.__metrics = get_metrics()
        if load_state is None:
            load_state = []
        self.__hits = deque(
            iterable=[_Bucket(hit[0], hit[1]) for hit in load_state],
            maxlen=self.__seconds,
        )

    def handle_invalid_message(self, e: InvalidMessage) -> None:
        self._add()
        if self._count() > self.__limit:
            raise e
        self.__metrics.increment("dlq.dropped_message")

    def _add(self) -> None:
        now = int(time())
        if len(self.__hits) and self.__hits[-1].timestamp == now:
            bucket = self.__hits[-1]
            self.__hits[-1] = _Bucket(bucket.timestamp, bucket.hits + 1)
        else:
            self.__hits.append(_Bucket(timestamp=now, hits=1))

    def _count(self) -> int:
        start = int(time()) - self.__seconds
        return sum(bucket.hits for bucket in self.__hits if bucket.timestamp >= start)
