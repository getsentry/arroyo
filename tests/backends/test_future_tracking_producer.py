from collections.abc import Iterator
from concurrent.futures import Future
from datetime import datetime
from functools import partial
from unittest.mock import patch

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


class DummyProducer:
    def __init__(self, use_simple_futures: bool):
        self.use_simple_futures = use_simple_futures

    def produce(
        self, destination: Topic | Partition, payload: KafkaPayload
    ) -> ProducerFuture[BrokerValue[KafkaPayload]]:
        future: ProducerFuture[BrokerValue[KafkaPayload]]
        if self.use_simple_futures:
            future = SimpleProducerFuture()
        else:
            future = Future()
        future.set_result(make_broker_value())
        return future

    def close(self) -> Future[None]:
        f: Future[None] = Future()
        f.set_result(None)
        return f


def get_dummy_producer(use_simple_futures: bool) -> DummyProducer:
    return DummyProducer(use_simple_futures=use_simple_futures)


@pytest.fixture(autouse=True)
def clear_pending_futures() -> Iterator[None]:
    _pending_futures.clear()
    yield
    _pending_futures.clear()


@pytest.fixture
def track_futures() -> Iterator[None]:
    with patch.object(
        FutureTrackingProducer, "_should_track_futures", return_value=True
    ):
        yield


def test_producer_tracks_futures(track_futures: None) -> None:
    producer = FutureTrackingProducer(
        "test.producer", partial(get_dummy_producer, use_simple_futures=True)
    )
    producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures) == 1
    collected = FutureTrackingProducer.collect_futures()
    future = next(iter(collected["test.producer"]))
    assert future.result() == make_broker_value()
    assert len(_pending_futures) == 0


def test_producer_executes_callbacks(track_futures: None) -> None:
    producer = FutureTrackingProducer(
        "test.producer", partial(get_dummy_producer, use_simple_futures=False)
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


def test_producer_rejects_callbacks_for_simple_futures(track_futures: None) -> None:
    producer = FutureTrackingProducer(
        "test.producer", partial(get_dummy_producer, use_simple_futures=True)
    )

    def callback(future: Future[BrokerValue[KafkaPayload]]) -> None:
        pass

    with pytest.raises(RuntimeError, match="SimpleProducerFuture"):
        producer.produce(Topic("test"), make_kafka_payload(), callbacks=[callback])


def test_pending_futures_max_len(track_futures: None) -> None:
    producer = FutureTrackingProducer(
        "test.producer", partial(get_dummy_producer, use_simple_futures=True)
    )
    for _ in range(10001):
        producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures["test.producer"]) == 10000


def test_producer_does_not_track_futures_when_disabled() -> None:
    with patch.object(
        FutureTrackingProducer, "_should_track_futures", return_value=False
    ):
        producer = FutureTrackingProducer(
            "test.producer", partial(get_dummy_producer, use_simple_futures=True)
        )
        producer.produce(Topic("test"), make_kafka_payload())
    assert len(_pending_futures) == 0
    assert FutureTrackingProducer.collect_futures() == {}
