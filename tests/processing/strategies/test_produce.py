from datetime import datetime
from unittest import mock

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.produce import Produce
from arroyo.types import Message, Partition, Topic, Value
from arroyo.utils.clock import MockedClock


def test_produce() -> None:
    orig_topic = Topic("orig-topic")
    result_topic = Topic("result-topic")
    clock = MockedClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: LocalBroker[KafkaPayload] = LocalBroker(broker_storage, clock)
    broker.create_topic(result_topic, partitions=1)

    producer = broker.get_producer()
    next_step = mock.Mock()

    strategy = Produce(producer, result_topic, next_step, 2)

    value = b'{"something": "something"}'
    data = KafkaPayload(None, value, [])
    now = datetime.now()

    message = Message(Value(data, {Partition(orig_topic, 0): 1}, now))

    strategy.submit(message)

    produced_message = broker_storage.consume(Partition(result_topic, 0), 0)
    assert produced_message is not None
    assert produced_message.payload.value == value
    assert broker_storage.consume(Partition(result_topic, 0), 1) is None
    assert next_step.submit.call_count == 0
    assert next_step.poll.call_count == 0
    strategy.poll()
    assert next_step.submit.call_count == 1
    assert next_step.poll.call_count == 1
    strategy.submit(message)
    strategy.poll()
    assert next_step.submit.call_count == 2
    assert next_step.poll.call_count == 2

    # Backpressure if buffer size = 2 exceeded
    with pytest.raises(MessageRejected):
        for _ in range(3):
            strategy.submit(message)

    strategy.join()


def test_produce_forwards_poll() -> None:
    result_topic = Topic("result-topic")

    next_step = mock.Mock()
    producer = mock.Mock()

    strategy: Produce[str] = Produce(producer, result_topic, next_step, 2)

    strategy.poll()

    assert next_step.poll.call_count == 1
