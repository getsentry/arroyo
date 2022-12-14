from unittest.mock import Mock, call

from arroyo.processing.strategies.filter import FilterStep
from arroyo.types import Message, Partition, Topic, Value
from tests.assertions import assert_changes, assert_does_not_change


def test_filter() -> None:
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(test_function, next_step)

    fail_message = Message(Value(False, {Partition(Topic("topic"), 0): 1}))

    with assert_does_not_change(lambda: int(next_step.submit.call_count), 0):
        filter_step.submit(fail_message)

    pass_message = Message(Value(True, {Partition(Topic("topic"), 0): 1}))

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        filter_step.submit(pass_message)

    assert next_step.submit.call_args == call(pass_message)

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        filter_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        filter_step.join()
