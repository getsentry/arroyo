import pickle
from datetime import datetime

from arroyo.types import Message, Partition, Position, Topic


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_message_pickling() -> None:
    message = Message(Partition(Topic("topic"), 0), 0, b"", datetime.now())
    assert pickle.loads(pickle.dumps(message)) == message


def test_position() -> None:
    position = Position(1, datetime.now())

    # Pickleable
    assert pickle.loads(pickle.dumps(position)) == position

    # Hashable
    test = {}
    test[position] = 1
