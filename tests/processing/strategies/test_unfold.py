from datetime import datetime
from typing import Sequence
from unittest.mock import Mock, call

from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.unfold import Unfold
from arroyo.types import Message, Partition, Topic, Value

PARTITION = Partition(Topic("topic"), 0)
NOW = datetime.now()


def generator(num: int) -> Sequence[Value[int]]:
    return [Value(i, {}, NOW) for i in range(num)]


def test_unfold() -> None:

    message = Message(Value(2, {PARTITION: 1}, NOW))
    next_step = Mock()

    strategy = Unfold(generator, next_step)
    strategy.submit(message)

    assert next_step.submit.call_args_list == [
        # first message has no committable since the original message has not fully been processed
        call(Message(Value(0, {}, NOW))),
        # second message is last message from batch, so we can say the original msg was fully processed
        call(Message(Value(1, {PARTITION: 1}, NOW))),
    ]

    strategy.close()
    strategy.join()


def test_message_rejected() -> None:
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = Unfold(generator, next_step)

    message = Message(Value(2, {PARTITION: 1}, NOW))
    strategy.submit(message)

    assert next_step.submit.call_count == 1

    # Message doesn't actually go through since it was rejected
    assert next_step.submit.call_args_list == [
        call(Message(Value(0, {}, NOW))),
    ]

    # poll again, to show that it does not block (regression)
    strategy.poll()
    assert next_step.submit.call_args_list == [
        call(Message(Value(0, {}, NOW))),
        call(Message(Value(0, {}, NOW))),
    ]

    # clear the side effect, both messages should be submitted now
    next_step.submit.reset_mock(side_effect=True)

    strategy.poll()

    assert next_step.submit.call_args_list == [
        call(Message(Value(0, {}, NOW))),
        call(Message(Value(1, {PARTITION: 1}, NOW))),
    ]

    strategy.close()
    strategy.join()
