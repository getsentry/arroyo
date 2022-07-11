from collections import deque
from time import time
from typing import NamedTuple, Optional, Sequence, Tuple

from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidMessages,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
)


class _Bucket(NamedTuple):
    """
    1-second time bucket to count number of hits in this second.
    """

    timestamp: int
    hits: int


class CountInvalidMessagePolicy(DeadLetterQueuePolicy):
    """
    Does not raise invalid messages up to a certain limit per time unit window
    in seconds. This window is 1 minute by default. The exception associated with
    the invalid messages is raised for all incoming invalid messages which go past
    this limit.

    A `next_policy` (a DLQ Policy) must be passed to handle any exception which
    remains within the per second limit. This gives full control over what happens
    to exceptions within the limit.

    A saved state in the form `[(<timestamp: int>, <hits: int>), ...]` can be passed
    on init to load a previously saved state. This state should be aggregated to
    1 second buckets.
    """

    def __init__(
        self,
        next_policy: DeadLetterQueuePolicy,
        limit: int,
        seconds: int = 60,
        load_state: Optional[Sequence[Tuple[int, int]]] = None,
    ) -> None:
        self.__closed = False
        self.__limit = limit
        self.__seconds = seconds
        self.__next_policy = next_policy
        if load_state is None:
            load_state = []
        self.__hits = deque(
            iterable=[_Bucket(hit[0], hit[1]) for hit in load_state],
            maxlen=self.__seconds,
        )

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        assert not self.__closed
        self._add(e)
        if self._count() > self.__limit:
            raise e
        self.__next_policy.handle_invalid_messages(e)

    def _add(self, e: InvalidMessages) -> None:
        now = int(time())
        num_hits = len(e.messages)
        if len(self.__hits) and self.__hits[-1].timestamp == now:
            bucket = self.__hits[-1]
            self.__hits[-1] = _Bucket(bucket.timestamp, bucket.hits + num_hits)
        else:
            self.__hits.append(_Bucket(timestamp=now, hits=num_hits))

    def _count(self) -> int:
        start = int(time()) - self.__seconds
        return sum(bucket.hits for bucket in self.__hits if bucket.timestamp >= start)

    def join(self, timeout: Optional[float]) -> None:
        self.__next_policy.join(timeout)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()
