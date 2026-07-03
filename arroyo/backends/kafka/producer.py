import atexit
import os
import threading
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, Protocol, Sequence

from arroyo.backends.abstract import ProducerFuture, SimpleProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Partition, Topic

MAX_PENDING_FUTURES = 10_000

_pending_futures: defaultdict[
    str, deque[ProducerFuture[BrokerValue[KafkaPayload]]]
] = defaultdict(lambda: deque(maxlen=MAX_PENDING_FUTURES))
"""
_pending_futures is global as a process may have several `FutureTrackingProducer` instances,
and we want to collect futures from all running instances with a single call.
Keys are the names of each `FutureTrackingProducer` instance in the current process, values are
deques with a maxlen to prevent unbounded queue size in case we're set to track futures,
but `collect_futures()` is never called.
"""


class CloseableProducerProtocol(Protocol):
    """Interface used by FutureTrackingProducer. Represents a producer that has a shutdown method."""

    def produce(
        self, dest: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]:
        ...

    def close(self) -> Future[None]:
        ...


class FutureTrackingProducer:
    """
    FutureTrackingProducer is a producer abstraction that optionally registers futures
    within the `_pending_futures` dict (depending on if the `ARROYO_TRACK_PRODUCER_FUTURES`
    env var is set). Applications can then call the `collect_futures()` static method to
    get all registered futures, await them, etc.

    Args:
        name: Unique identifying name of this FutureTrackingProducer. Used to key `_pending_futures`.
        producer_factory: Callable that returns a producer object.
    """

    def _should_track_futures(self) -> bool:
        return os.environ.get("ARROYO_TRACK_PRODUCER_FUTURES", "").lower() == "true"

    def __init__(
        self,
        name: str,
        producer_factory: Callable[[], CloseableProducerProtocol],
    ) -> None:
        self.name = name
        self._producer_factory = producer_factory
        self._inner_producer: CloseableProducerProtocol | None = None
        self._track_futures = self._should_track_futures()
        # Used to ensure we don't instantiate duplicate producers when calling produce() from different threads.
        self._producer_lock = threading.Lock()

    def _get(self) -> CloseableProducerProtocol:
        # None check first so we don't have any lock contention on produces
        if self._inner_producer is None:
            with self._producer_lock:
                if self._inner_producer is None:
                    self._inner_producer = self._producer_factory()
                    atexit.register(self._shutdown)
        return self._inner_producer

    def track_future(self, future: ProducerFuture[BrokerValue[KafkaPayload]]) -> None:
        _pending_futures[self.name].append(future)

    @staticmethod
    def collect_futures() -> dict[str, set[ProducerFuture[BrokerValue[KafkaPayload]]]]:
        """
        Clears the `_pending_futures` dict, and returns a copy with all values converted to sets.
        This generally should not be called by users, and should only be called if you want to
        do something based on the outcome of a _group_ of futures, and not just the outcomes of individual futures.
        For the latter, use delivery callbacks.

        TODO: Add locking around `_pending_futures`. For now, his operation should also always be called
        in the same thread as the one calling `produce()`.
        """

        pending_copy = _pending_futures.copy()
        _pending_futures.clear()
        return {name: set(val) for name, val in pending_copy.items()}

    def produce(
        self,
        dest: Topic | Partition,
        payload: KafkaPayload,
        callbacks: Sequence[Callable[[Future[BrokerValue[KafkaPayload]]], Any]] = [],
    ) -> None:
        """
        Produces the given payload to the given topic.
        Since FutureTrackingProducer tracks futures internally, it does not return the
        producer future to the user, but the user can still add callbacks
        to the future via the `callbacks` arg.

        Args:
            dest: Topic (or specific partition) to produce to.
            payload: KafkaPayload to produce.
            callbacks: List of Callables to add to the future as done callbacks. The future itself
                       is the only arg passed to the callback.
        """

        future = self._get().produce(dest, payload)
        if self._track_futures:
            self.track_future(future)
        if callbacks:
            # Arroyo producers can return a SimpleProducerFuture,
            # which does not accept callbacks.
            if not isinstance(future, SimpleProducerFuture):
                for c in callbacks:
                    future.add_done_callback(c)
            else:
                raise RuntimeError(
                    (
                        "Cannot add callbacks to SimpleProducerFuture, either remove the callbacks "
                        "or instantiate your producer with `use_simple_futures=False`."
                    )
                )

    def _shutdown(self) -> None:
        if self._inner_producer is not None:
            self._inner_producer.close().result()
