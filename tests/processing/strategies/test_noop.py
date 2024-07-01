from datetime import datetime

from arroyo.processing.strategies.noop import Noop
from arroyo.types import Message, Partition, Topic, Value


def test_noop() -> None:
    """
    Test that the interface of the noop strategy is correct.
    """
    now = datetime.now()

    strategy = Noop()
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(Value(b"hello", {partition: 1}, now)))
    strategy.poll()
    strategy.submit(Message(Value(b"world", {partition: 2}, now)))
    strategy.poll()
