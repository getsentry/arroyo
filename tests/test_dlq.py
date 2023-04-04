from datetime import datetime
from typing import Generator
from unittest.mock import ANY

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.dlq import BufferedMessages, DlqLimit, DlqPolicy, KafkaDlqProducer
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
