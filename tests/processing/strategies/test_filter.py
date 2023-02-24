from typing import Union
from unittest.mock import Mock, call

from arroyo.commit import CommitPolicy
from arroyo.processing.strategies.filter import FilterStep
from arroyo.types import (
    FILTERED_PAYLOAD,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)
from tests.assertions import assert_changes, assert_does_not_change


def test_filter() -> None:
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        assert not isinstance(message.payload, FilteredPayload)
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


def test_commit_policy_basic() -> None:
    topic = Topic("topic")
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(
        test_function, next_step, commit_policy=CommitPolicy(None, 3)
    )

    init_message = Message(Value(False, {Partition(topic, 1): 1}))

    filter_step.submit(init_message)
    assert next_step.submit.call_count == 0

    for i in range(2):
        fail_message = Message(Value(False, {Partition(topic, 0): i}))

        filter_step.submit(fail_message)
        assert next_step.submit.call_count == 0

    fail_message = Message(Value(False, {Partition(topic, 0): 2}))
    filter_step.submit(fail_message)

    # Assert that the filter message kept track of the new offsets across
    # partitions, and is flushing them all out since this is the third message
    # and according to our commit policy we are supposed to commit at this
    # point, roughly.
    expected_filter_message: Message[Union[FilteredPayload, bool]] = Message(
        Value(FILTERED_PAYLOAD, {Partition(topic, 1): 1, Partition(topic, 0): 2})
    )
    assert next_step.submit.mock_calls == [call(expected_filter_message)]

    next_step.submit.reset_mock()
    # Since all offsets have been recently flushed, join()/shutdown should not
    # send an additional filter message
    filter_step.join()
    assert next_step.submit.call_count == 0

    fail_message = Message(Value(False, {Partition(topic, 0): 3}))
    filter_step.submit(fail_message)
    assert next_step.submit.call_count == 0
    assert next_step.join.call_count == 1

    # Since there was a filtered message with no flush inbetween, join() needs
    # to send a filter message to flush out uncommitted offsets. If we do not
    # do that forcibly, __commit(force=True) in downstream strategies will do
    # nothing.
    filter_step.join()

    assert next_step.submit.mock_calls == [
        call(Message(Value(FILTERED_PAYLOAD, {Partition(topic, 0): 3})))
    ]

    assert next_step.join.call_count == 2


def test_commit_policy_filtered_messages_alternating() -> None:
    topic = Topic("topic")
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(
        test_function, next_step, commit_policy=CommitPolicy(None, 3)
    )

    filter_step.submit(Message(Value(True, {Partition(topic, 1): 1})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 2})))
    filter_step.submit(Message(Value(True, {Partition(topic, 1): 3})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 4})))
    filter_step.submit(Message(Value(True, {Partition(topic, 1): 5})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 6})))

    assert next_step.submit.mock_calls == [
        call(Message(Value(True, {Partition(topic, 1): 1}))),
        call(Message(Value(True, {Partition(topic, 1): 3}))),
        call(Message(Value(FILTERED_PAYLOAD, {Partition(topic, 1): 4}))),
        call(Message(Value(True, {Partition(topic, 1): 5}))),
    ]


def test_no_commit_policy_does_not_forward_filtered_messages() -> None:
    topic = Topic("topic")
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(test_function, next_step)

    filter_step.submit(Message(Value(True, {Partition(topic, 1): 1})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 2})))
    filter_step.submit(Message(Value(True, {Partition(topic, 1): 3})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 4})))
    filter_step.submit(Message(Value(True, {Partition(topic, 1): 5})))
    filter_step.submit(Message(Value(False, {Partition(topic, 1): 6})))

    assert next_step.submit.mock_calls == [
        call(Message(Value(True, {Partition(topic, 1): 1}))),
        call(Message(Value(True, {Partition(topic, 1): 3}))),
        call(Message(Value(True, {Partition(topic, 1): 5}))),
    ]
