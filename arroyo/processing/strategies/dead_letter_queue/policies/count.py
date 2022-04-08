from collections import deque
from time import time
from typing import NamedTuple, Optional, Sequence, Tuple

from arroyo.backends.kafka.consumer import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    InvalidMessages,
)
from arroyo.processing.strategies.dead_letter_queue.policies.produce import (
    ProduceInvalidMessagePolicy,
)
from arroyo.types import Topic
from arroyo.utils.metrics import get_metrics


class _Bucket(NamedTuple):
    """
    1-second time bucket to count number of hits in this second.
    """

    timestamp: int
    hits: int


class CountInvalidMessagePolicy(ProduceInvalidMessagePolicy):
    """
    Ignore invalid messages up to a certain limit per time unit window in seconds.
    This window is 1 minute by default. The exception associated with the invalid
    messages is raised for all incoming invalid messages which go past this limit.

    A saved state in the form `[(<timestamp: int>, <hits: int>), ...]` can be passed
    on init to load a previously saved state. This state should be aggregated to
    1 second buckets.

    If a `KafkaProducer` and a `Topic` are passed to this policy, invalid messages
    will not be ignored but will be produced to the topic using the producer instead.
    """

    def __init__(
        self,
        limit: int,
        seconds: int = 60,
        load_state: Optional[Sequence[Tuple[int, int]]] = None,
        producer: Optional[KafkaProducer] = None,
        dead_letter_topic: Optional[Topic] = None,
    ) -> None:
        self.__produce = producer is not None and dead_letter_topic is not None
        if producer is not None and dead_letter_topic is not None:
            super().__init__(producer, dead_letter_topic)
        self.__limit = limit
        self.__seconds = seconds
        self.__metrics = get_metrics()
        if load_state is None:
            load_state = []
        self.__hits = deque(
            iterable=[_Bucket(hit[0], hit[1]) for hit in load_state],
            maxlen=self.__seconds,
        )

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        self._add(e)
        if self._count() > self.__limit:
            raise e
        self._produce_or_ignore(e)

    def _produce_or_ignore(self, e: InvalidMessages) -> None:
        if self.__produce:
            super().handle_invalid_messages(e)
        else:
            self.__metrics.increment("dlq.dropped_messages", len(e.messages))

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
