import pickle
from datetime import datetime

from arroyo.types import BrokerValue, Message, Partition, Topic, Value


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_message() -> None:
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    # Broker payload
    broker_message = Message(BrokerValue(b"", partition, 0, now))
    assert pickle.loads(pickle.dumps(broker_message)) == broker_message

    # Generic payload
    message = Message(Value(b"", {partition: 1}, datetime.now()))
    assert pickle.loads(pickle.dumps(message)) == message

    # Replace payload
    new_broker_message = broker_message.replace(b"123")
    assert new_broker_message.payload == b"123"
    assert new_broker_message.committable == {partition: 1}
    new_message = message.replace(b"123")
    assert new_message.payload == b"123"
    assert new_message.committable == message.committable


def test_broker_value() -> None:
    partition = Partition(Topic("topic"), 0)
    offset = 5
    now = datetime.now()

    broker_value = BrokerValue(
        b"asdf",
        partition,
        offset,
        now,
    )

    assert broker_value.committable == {partition: offset + 1}

    assert pickle.loads(pickle.dumps(broker_value)) == broker_value

    # Hashable
    _ = {broker_value}
