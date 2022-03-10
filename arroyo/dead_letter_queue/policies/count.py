from collections import deque
from time import time
from typing import Deque, NamedTuple

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
    This window is 1 minute by default.
    """

    def __init__(self, limit: int, seconds: int = 60) -> None:
        self.__limit = limit
        self.__size = seconds
        self.__hits: Deque[_Bucket] = deque()
        self.__metrics = get_metrics()

    def handle_invalid_message(
        self, message: Message[TPayload], e: InvalidMessage
    ) -> None:
        self._add()
        if self._count() > self.__limit:
            raise e
        self.__metrics.increment("dlq.dropped_messages")

    def _add(self) -> None:
        now = int(time())
        if len(self.__hits) and self.__hits[-1].timestamp == now:
            bucket = self.__hits[-1]
            self.__hits[-1] = _Bucket(bucket.timestamp, bucket.hits + 1)
        else:
            self.__hits.append(_Bucket(timestamp=now, hits=1))
            if len(self.__hits) > self.__size:
                self.__hits.popleft()

    def _count(self) -> int:
        start = int(time()) - self.__size
        return sum(bucket.hits for bucket in self.__hits if bucket.timestamp >= start)
