import pickle
from datetime import datetime

from arroyo.types import BrokerPayload, Message, Partition, Payload, Position, Topic


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_message() -> None:
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    # Broker payload
    message = Message(BrokerPayload(b"", partition, 0, now))
    assert pickle.loads(pickle.dumps(message)) == message

    # Generic payload
    message = Message(Payload(b"", {partition: Position(1, now)}))
    assert pickle.loads(pickle.dumps(message)) == message


def test_position() -> None:
    position = Position(1, datetime.now())

    # Pickleable
    assert pickle.loads(pickle.dumps(position)) == position

    # Hashable
    _ = {position}


def test_broker_payload() -> None:
    partition = Partition(Topic("topic"), 0)
    offset = 5
    now = datetime.now()

    broker_payload = BrokerPayload(
        b"asdf",
        partition,
        offset,
        now,
    )

    assert broker_payload.committable == {partition: Position(offset + 1, now)}

    assert pickle.loads(pickle.dumps(broker_payload)) == broker_payload

    # Hashable
    _ = {broker_payload}
