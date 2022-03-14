from collections import deque
from time import time
from typing import Callable, NamedTuple, Optional, Sequence, Tuple

from arroyo.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)
from arroyo.types import Message, TPayload
from arroyo.utils.metrics import get_metrics


class _Bucket(NamedTuple):
    """
    1-second time bucket to count number of hits in this second.
    """

    timestamp: int
    hits: int


class CountInvalidMessagePolicy(DeadLetterQueuePolicy[TPayload]):
    """
    Ignore invalid messages up to a certain limit per time unit window in seconds.
    This window is 1 minute by default. The exception associated with the invalid
    message is raised for all incoming invalid messages which go past this limit.

    If the state of counted hits is to be persisted outside of the DLQ, a callback
    function may be passed accepting a timestamp to add a hit to.

    A saved state in the form `[(<timestamp: int>, <hits: int>), ...]` can be passed
    on init to load the previously saved state. This state should be aggregated to
    1 second buckets.
    """

    def __init__(
        self,
        limit: int,
        seconds: int = 60,
        load_state: Sequence[Tuple[int, int]] = [],
        add_hit_callback: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.__limit = limit
        self.__seconds = seconds
        self.__metrics = get_metrics()
        self.__hits = deque(
            iterable=[_Bucket(hit[0], hit[1]) for hit in load_state],
            maxlen=self.__seconds,
        )
        self.__add_hit_callback = add_hit_callback

    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        self._add()
        if self._count() > self.__limit:
            raise e
        self.__metrics.increment("dlq.dropped_messages")

    def _add(self) -> None:
        now = int(time())
        if self.__add_hit_callback is not None:
            self.__add_hit_callback(now)
        if len(self.__hits) and self.__hits[-1].timestamp == now:
            bucket = self.__hits[-1]
            self.__hits[-1] = _Bucket(bucket.timestamp, bucket.hits + 1)
        else:
            self.__hits.append(_Bucket(timestamp=now, hits=1))

    def _count(self) -> int:
        start = int(time()) - self.__seconds
        return sum(bucket.hits for bucket in self.__hits if bucket.timestamp >= start)
