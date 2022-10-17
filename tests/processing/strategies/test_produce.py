from datetime import datetime
from unittest import mock

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies.produce import ProduceAndCommit
from arroyo.types import Message, Partition, Topic
from arroyo.utils.clock import TestingClock


def test_produce_result() -> None:
    epoch = datetime(1970, 1, 1)
    orig_topic = Topic("orig-topic")
    result_topic = Topic("result-topic")
    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: LocalBroker[KafkaPayload] = LocalBroker(broker_storage, clock)
    broker.create_topic(result_topic, partitions=1)

    producer = broker.get_producer()
    commit = mock.Mock()

    strategy = ProduceAndCommit(producer, result_topic, commit)

    value = b'{"something": "something"}'
    data = KafkaPayload(None, value, [])

    message = Message(
        Partition(orig_topic, 0),
        1,
        data,
        epoch,
    )

    strategy.submit(message)

    produced_message = broker_storage.consume(Partition(result_topic, 0), 0)
    assert produced_message is not None
    assert produced_message.payload.value == value
    assert broker_storage.consume(Partition(result_topic, 0), 1) is None
    assert commit.call_count == 0
    strategy.poll()
    assert commit.call_count == 1

    strategy.submit(message)
    strategy.poll()
    assert commit.call_count == 2

    # Commit count immediately increases once we call join()
    strategy.join()
    assert commit.call_count == 3
