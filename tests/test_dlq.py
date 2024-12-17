import pickle
from datetime import datetime
from typing import Generator
from unittest.mock import ANY

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.dlq import (
    BufferedMessages,
    DlqLimit,
    DlqLimitState,
    DlqPolicy,
    DlqPolicyWrapper,
    InvalidMessage,
    InvalidMessageState,
    KafkaDlqProducer,
)
from arroyo.types import BrokerValue, Partition, Topic

topic = Topic("test")
dlq_topic = Topic("test-dlq")
partition = Partition(topic, 0)


def generate_values() -> Generator[BrokerValue[KafkaPayload], None, None]:
    i = 0
    while True:
        yield BrokerValue(
            KafkaPayload(None, str(i).encode("utf-8"), []),
            Partition(topic, 0),
            i,
            datetime.now(),
        )
        i += 1


def test_buffered_messages() -> None:
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage())

    dlq_policy = DlqPolicy(
        KafkaDlqProducer(broker.get_producer(), dlq_topic), DlqLimit(), None
    )

    buffer: BufferedMessages[KafkaPayload] = BufferedMessages(dlq_policy)

    generator = generate_values()

    for _ in range(10):
        buffer.append(next(generator))

    assert buffer.pop(partition, 1) == BrokerValue(
        KafkaPayload(None, b"1", []), partition, 1, ANY
    )


def test_buffered_messages_limit() -> None:
    buffered_message_limit = 2
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage())

    dlq_policy = DlqPolicy(
        KafkaDlqProducer(broker.get_producer(), dlq_topic),
        DlqLimit(),
        buffered_message_limit,
    )

    buffer: BufferedMessages[KafkaPayload] = BufferedMessages(dlq_policy)

    generator = generate_values()

    for _ in range(10):
        buffer.append(next(generator))

    # It's gone
    assert buffer.pop(partition, 1) is None

    assert buffer.pop(partition, 9) == BrokerValue(
        KafkaPayload(None, b"9", []), partition, 9, ANY
    )


def test_no_buffered_messages() -> None:
    buffer: BufferedMessages[KafkaPayload] = BufferedMessages(None)

    generator = generate_values()
    for _ in range(10):
        buffer.append(next(generator))
    assert buffer.pop(partition, 9) is None


def test_invalid_message_state() -> None:
    inv = InvalidMessageState()
    assert len(inv) == 0
    inv.append(InvalidMessage(Partition(Topic("test_topic"), 0), 2))
    assert len(inv) == 1
    inv.reset()
    assert len(inv) == 0


def test_dlq_policy_wrapper() -> None:
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage())
    broker.create_topic(dlq_topic, 1)
    dlq_policy = DlqPolicy(
        KafkaDlqProducer(broker.get_producer(), dlq_topic), DlqLimit(), None
    )
    partition = Partition(topic, 0)
    wrapper = DlqPolicyWrapper(dlq_policy)
    wrapper.reset_dlq_limits({partition: 0})
    wrapper.MAX_PENDING_FUTURES = 1
    for i in range(10):
        message = BrokerValue(KafkaPayload(None, b"", []), partition, i, datetime.now())
        wrapper.produce(message)
    wrapper.flush({partition: 11})


def test_dlq_policy_wrapper_limit_exceeded() -> None:
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage())
    broker.create_topic(dlq_topic, 1)
    dlq_policy = DlqPolicy(
        KafkaDlqProducer(broker.get_producer(), dlq_topic), DlqLimit(0.0, 1), None
    )
    partition = Partition(topic, 0)
    wrapper = DlqPolicyWrapper(dlq_policy)
    wrapper.reset_dlq_limits({partition: 0})
    wrapper.MAX_PENDING_FUTURES = 1

    message = BrokerValue(KafkaPayload(None, b"", []), partition, 1, datetime.now())
    with pytest.raises(RuntimeError):
        wrapper.produce(message)


def test_invalid_message_pickleable() -> None:
    exc = InvalidMessage(Partition(Topic("test_topic"), 0), 2)
    pickled_exc = pickle.dumps(exc)
    unpickled_exc = pickle.loads(pickled_exc)
    assert exc == unpickled_exc


def test_dlq_limit_state() -> None:
    starting_offset = 2
    partition = Partition(Topic("test_topic"), 0)
    last_invalid_offset = {partition: starting_offset}
    limit = DlqLimit(None, 5)
    state = DlqLimitState(limit, last_invalid_offsets=last_invalid_offset)

    # 1 valid message followed by 4 invalid
    for i in range(4, 9):
        assert state.record_invalid_message(
            BrokerValue(i, partition, i, datetime.now())
        )

    # Next message should not be accepted
    assert not state.record_invalid_message(
        BrokerValue(9, partition, 9, datetime.now())
    )
