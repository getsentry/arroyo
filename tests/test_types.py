import pickle
from datetime import datetime

from arroyo.types import BrokerPayload, Message, Partition, Position, Topic


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_message_pickling() -> None:
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    message = Message(
        BrokerPayload(partition, 0, now, b""), {partition: Position(0, now)}
    )
    assert pickle.loads(pickle.dumps(message)) == message


def test_broker_payload() -> None:
    partition = Partition(Topic("topic"), 0)
    offset = 5
    now = datetime.now()

    broker_payload = BrokerPayload(
        partition,
        offset,
        now,
        b"asdf",
    )

    assert broker_payload.next_offset == offset + 1
    assert broker_payload.position_to_commit == Position(offset + 1, now)

    assert pickle.loads(pickle.dumps(broker_payload)) == broker_payload

    # Hashable
    _ = {broker_payload}
