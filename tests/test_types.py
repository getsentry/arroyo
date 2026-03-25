import pickle
from datetime import datetime

import pytest

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


def test_broker_value_string_timestamp() -> None:
    """BrokerValue should coerce ISO8601 string timestamps to datetime objects."""
    partition = Partition(Topic("topic"), 0)
    now = datetime(2024, 1, 15, 10, 30, 0)
    iso_string = now.isoformat()

    broker_value = BrokerValue(
        b"asdf",
        partition,
        5,
        iso_string,  # type: ignore[arg-type]
    )

    assert isinstance(broker_value.timestamp, datetime)
    assert broker_value.timestamp == now


def test_value_string_timestamp() -> None:
    """Value should coerce ISO8601 string timestamps to datetime objects."""
    partition = Partition(Topic("topic"), 0)
    now = datetime(2024, 1, 15, 10, 30, 0)
    iso_string = now.isoformat()

    value = Value(
        b"asdf",
        {partition: 1},
        iso_string,  # type: ignore[arg-type]
    )

    assert isinstance(value.timestamp, datetime)
    assert value.timestamp == now


def test_message_string_timestamp() -> None:
    """Message.timestamp should return datetime even when string was passed."""
    partition = Partition(Topic("topic"), 0)
    now = datetime(2024, 1, 15, 10, 30, 0)
    iso_string = now.isoformat()

    message = Message(BrokerValue(b"", partition, 0, iso_string))  # type: ignore[arg-type]

    assert isinstance(message.timestamp, datetime)
    assert message.timestamp == now


def test_timestamp_coercion_with_timezone_offset() -> None:
    """String timestamps with timezone info should be parsed correctly."""
    partition = Partition(Topic("topic"), 0)

    broker_value = BrokerValue(
        b"asdf",
        partition,
        5,
        "2024-01-15T10:30:00+00:00",  # type: ignore[arg-type]
    )

    assert isinstance(broker_value.timestamp, datetime)
    assert broker_value.timestamp.year == 2024
    assert broker_value.timestamp.month == 1
    assert broker_value.timestamp.day == 15


def test_timestamp_coercion_invalid_type() -> None:
    """Non-string, non-datetime timestamps should raise TypeError."""
    partition = Partition(Topic("topic"), 0)

    with pytest.raises(TypeError):
        BrokerValue(
            b"asdf",
            partition,
            5,
            12345,  # type: ignore[arg-type]
        )
