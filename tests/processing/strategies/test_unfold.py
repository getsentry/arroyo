from datetime import datetime
from typing import Sequence
from unittest.mock import Mock, call

from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.unfold import Unfold
from arroyo.types import Message, Partition, Topic, Value


def generator(num: int) -> Sequence[int]:
    return [i for i in range(num)]


def test_unfold() -> None:
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    message = Message(Value(2, {partition: 1}, now))
    next_step = Mock()

    strategy = Unfold(generator, next_step)
    strategy.submit(message)

    assert next_step.submit.call_args_list == [
        call(Message(Value(0, committable={}, timestamp=now))),
        call(Message(Value(1, committable={partition: 1}, timestamp=now))),
    ]

    strategy.close()
    strategy.join()


def test_message_rejected() -> None:
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = Unfold(generator, next_step)

    message = Message(Value(2, {partition: 1}, now))
    strategy.submit(message)

    assert next_step.submit.call_count == 1

    # Message doesn't actually go through since it was rejected
    assert next_step.submit.call_args_list == [
        call(Message(Value(0, committable={}, timestamp=now))),
    ]

    # clear the side effect, both messages should be submitted now
    next_step.submit.reset_mock(side_effect=True)

    strategy.poll()

    assert next_step.submit.call_args_list == [
        call(Message(Value(0, committable={}, timestamp=now))),
        call(Message(Value(1, committable={partition: 1}, timestamp=now))),
    ]

    strategy.close()
    strategy.join()
