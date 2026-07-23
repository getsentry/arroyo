import atexit
import threading
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, Protocol, Sequence, cast

from arroyo.backends.abstract import ProducerFuture, SimpleProducerFuture
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Partition, Topic

_pending_futures: dict[str, deque[ProducerFuture[BrokerValue[KafkaPayload]]]] = {}
"""
_pending_futures is global as a process may have several `FutureTrackingProducer` instances,
and we want to collect futures from all running instances with a single call.
Keys are the names of each `FutureTrackingProducer` instance in the current process, values are
deques with a maxlen set by the corresponding `FutureTrackingProducer` instance.
"""


class CloseableProducerProtocol(Protocol):
    """Interface used by FutureTrackingProducer. Represents a producer that has a shutdown method."""

    def produce(
        self, dest: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]:
        ...

    def close(self) -> Future[None]:
        ...

    def get_config(self) -> dict[str, Any]:
        ...


class FutureTrackingProducer:
    """
    FutureTrackingProducer is a producer abstraction that optionally:
    - Registers produce futures within the global `_pending_futures` dict, which applications can then
      call the `collect_futures()` static method to get all futures in the dict
    - Registers produce futures within a local backpressure queue, which, once full, will apply backpressure
      by awaiting the result of the oldest future in the backpressure queue
      - The purpose of this is to prevent the internal producer's produce queue from filling up, which raises an error

    Args:
        name: Unique identifying name of this FutureTrackingProducer. Used to key `_pending_futures`.
        producer_factory: Callable that returns a producer object.
        should_track_futures: Whether futures should be tracked in the global `_pending_futures`. Should only be True
                              if you plan on calling `FutureTrackingProducer.collect_futures()` in your application,
                              meaning you care about the results of batches of producer futures.
                              Max size of the queue = ((internal producer queue max size) * `queue_size_scale`).
                              Default False.
        should_backpressure: Whether futures should be tracked in this producer's local `_backpressure_queue`.
                             This will cause the producer to await the result of the oldest future in `_backpressure_queue`
                             after producing when the queue is full.
                             Max size of the queue = ((internal producer queue max size) * `queue_size_scale`).
                             Default True.
        backpressure_queue_size_scale: How large the maxlen of this producer's future queues should be relative to
                                       the internal producer's producer queue max length. Must be a float in the
                                       range (0, 1.0). Default 0.8.
    """

    def __init__(
        self,
        name: str,
        producer_factory: Callable[[], CloseableProducerProtocol],
        should_track_futures: bool = False,
        should_backpressure: bool = True,
        queue_size_scale: float = 0.8,
    ) -> None:
        assert (
            name not in _pending_futures
        ), "All FutureTrackingProducer instances in a single process must have a unique name."
        assert (
            queue_size_scale > 0 and queue_size_scale < 1
        ), "queue_size_scale must be in the range (0, 1.0)."
        self.name = name
        self._producer_factory = producer_factory
        self._inner_producer: CloseableProducerProtocol | None = None
        self._track_futures = should_track_futures
        self._backpressure = should_backpressure
        self._queue_size_scale = queue_size_scale
        # Used to ensure we don't instantiate duplicate producers when calling produce() from different threads.
        self._producer_lock = threading.Lock()
        self._backpressure_queue: deque[
            ProducerFuture[BrokerValue[KafkaPayload]]
        ] = deque(maxlen=0)
        _pending_futures[name] = deque(maxlen=0)

    def _get_queue_max_len(self) -> int:
        """
        Returns ((internal producer queue max size) * `queue_size_scale`).
        Used to calculate the max length of future queues.
        """
        if self._inner_producer:
            # queue.buffering.max.messages has a default value of 100k
            internal_producer_queue_max_size = int(
                self._inner_producer.get_config().get(
                    "queue.buffering.max.messages", 100_000
                )
            )
            return int(internal_producer_queue_max_size * self._queue_size_scale)
        return 0

    def _initialize_producer_and_queues(self) -> None:
        self._inner_producer = self._producer_factory()
        atexit.register(self._shutdown)
        queues_max_len = self._get_queue_max_len()
        if self._backpressure:
            self._backpressure_queue = deque(maxlen=queues_max_len)
        if self._track_futures:
            _pending_futures[self.name] = deque(maxlen=queues_max_len)

    def _get(self) -> CloseableProducerProtocol:
        # None check first so we don't have any lock contention on produces
        if self._inner_producer is None:
            with self._producer_lock:
                if self._inner_producer is None:
                    self._initialize_producer_and_queues()
        return cast(CloseableProducerProtocol, self._inner_producer)

    def track_future(self, future: ProducerFuture[BrokerValue[KafkaPayload]]) -> None:
        if self._track_futures:
            _pending_futures[self.name].append(future)
        if self._backpressure:
            if len(self._backpressure_queue) == self._backpressure_queue.maxlen:
                try:
                    # Backpressure on the result of the oldest future in the backpressure queue
                    self._backpressure_queue[0].result()
                except Exception:
                    pass
            self._backpressure_queue.append(future)

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

        collected = {name: set(val) for name, val in _pending_futures.items()}
        for producer in _pending_futures:
            _pending_futures[producer].clear()
        return collected

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
        _pending_futures.pop(self.name, None)
        if self._inner_producer is not None:
            self._inner_producer.close().result()
