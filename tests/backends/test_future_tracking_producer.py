from collections.abc import Iterator
from concurrent.futures import Future
from datetime import datetime
from functools import partial
from typing import Any, cast

import pytest

from arroyo.backends.abstract import ProducerFuture, SimpleProducerFuture
from arroyo.backends.kafka import FutureTrackingProducer, KafkaPayload
from arroyo.backends.kafka.producer import _pending_futures
from arroyo.types import BrokerValue, Partition, Topic


def make_kafka_payload() -> KafkaPayload:
    """Generates dummy KafkaPayload."""
    return KafkaPayload(None, b"", [])


def make_broker_value() -> BrokerValue[KafkaPayload]:
    """Generates dummy BrokerValue[KafkaPayload]."""
    return BrokerValue(
        make_kafka_payload(), Partition(Topic("test"), 0), 0, datetime(1999, 2, 19)
    )


class ResultTrackingFuture(Future):  # type: ignore
    """A Future that records how many times its result() has been awaited."""

    def __init__(self) -> None:
        super().__init__()
        self.result_call_count = 0

    def result(self, timeout: float | None = None) -> Any:
        self.result_call_count += 1
        return super().result(timeout)


class DummyProducer:
    def __init__(
        self,
        use_simple_futures: bool,
        queue_max_messages: int = 12500,
        produced_futures: list[ProducerFuture[BrokerValue[KafkaPayload]]] | None = None,
        track_results: bool = False,
    ):
        self.use_simple_futures = use_simple_futures
        self.queue_max_messages = queue_max_messages
        self.produced_futures = produced_futures
        self.track_results = track_results

    def produce(
        self, destination: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]:
        future: ProducerFuture[BrokerValue[KafkaPayload]]
        if self.use_simple_futures:
            future = SimpleProducerFuture()
        elif self.track_results:
            future = ResultTrackingFuture()
        else:
            future = Future()
        future.set_result(make_broker_value())
        if self.produced_futures is not None:
            self.produced_futures.append(future)
        return future

    def close(self) -> Future[None]:
        f: Future[None] = Future()
        f.set_result(None)
        return f

    def get_config(self) -> dict[str, Any]:
        return {"queue.buffering.max.messages": self.queue_max_messages}


def get_dummy_producer(
    use_simple_futures: bool,
    queue_max_messages: int = 12500,
    produced_futures: list[ProducerFuture[BrokerValue[KafkaPayload]]] | None = None,
    track_results: bool = False,
) -> DummyProducer:
    return DummyProducer(
        use_simple_futures=use_simple_futures,
        queue_max_messages=queue_max_messages,
        produced_futures=produced_futures,
        track_results=track_results,
    )


@pytest.fixture(autouse=True)
def clear_pending_futures() -> Iterator[None]:
    _pending_futures.clear()
    yield
    _pending_futures.clear()


def test_producer_tracks_futures() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=True),
        should_track_futures=True,
    )
    producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures) == 1
    collected = FutureTrackingProducer.collect_futures()
    future = next(iter(collected["test.producer"]))
    assert future.result() == make_broker_value()
    assert len(_pending_futures["test.producer"]) == 0


def test_producer_executes_callbacks() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=False),
        should_track_futures=True,
    )
    received: list[Future[BrokerValue[KafkaPayload]]] = []

    def callback(future: Future[BrokerValue[KafkaPayload]]) -> None:
        received.append(future)

    producer.produce(Topic("test"), make_kafka_payload(), callbacks=[callback])
    collected = FutureTrackingProducer.collect_futures()
    tracked_future = next(iter(collected["test.producer"]))

    assert len(received) == 1
    assert received[0] is tracked_future
    assert received[0].done()


def test_producer_rejects_callbacks_for_simple_futures() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=True),
        should_track_futures=True,
    )

    def callback(future: Future[BrokerValue[KafkaPayload]]) -> None:
        pass

    with pytest.raises(RuntimeError, match="SimpleProducerFuture"):
        producer.produce(Topic("test"), make_kafka_payload(), callbacks=[callback])


def test_pending_futures_max_len() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=True),
        should_track_futures=True,
    )
    for _ in range(10001):
        producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures["test.producer"]) == 10000


def test_producer_does_not_track_futures_when_disabled() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=True),
        should_track_futures=False,
    )
    producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures["test.producer"]) == 0
    assert FutureTrackingProducer.collect_futures() == {"test.producer": set()}


def test_backpressure_queue_awaits_oldest_future_when_full() -> None:
    produced: list[ProducerFuture[BrokerValue[KafkaPayload]]] = []
    producer = FutureTrackingProducer(
        "test.producer",
        partial(
            get_dummy_producer,
            use_simple_futures=False,
            queue_max_messages=6,
            produced_futures=produced,
            track_results=True,
        ),
        should_backpressure=True,
        queue_size_scale=0.5,
    )

    for _ in range(3):
        producer.produce(Topic("test"), make_kafka_payload())
    assert all(cast(ResultTrackingFuture, f).result_call_count == 0 for f in produced)

    producer.produce(Topic("test"), make_kafka_payload())
    assert cast(ResultTrackingFuture, produced[0]).result_call_count == 1
    assert produced[0] not in producer._backpressure_queue
    assert len(producer._backpressure_queue) == 3
    assert list(producer._backpressure_queue) == produced[1:]


def test_backpressure_queue_disabled() -> None:
    producer = FutureTrackingProducer(
        "test.producer",
        partial(get_dummy_producer, use_simple_futures=True, queue_max_messages=6),
        should_backpressure=False,
        queue_size_scale=0.5,
    )
    for _ in range(10):
        producer.produce(Topic("test"), make_kafka_payload())
    assert producer._backpressure_queue.maxlen == 0
    assert len(producer._backpressure_queue) == 0
