import time
from datetime import datetime
from unittest.mock import Mock, call

import pytest

from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import BrokerValue, Message, Partition, Topic, Value
from tests.assertions import assert_changes


@pytest.mark.parametrize("better_backpressure", [False, True])
def test_run_task(better_backpressure: bool) -> None:
    mock_func = Mock()
    next_step = Mock()
    now = datetime.now()

    strategy = RunTask(mock_func, next_step, better_backpressure=better_backpressure)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(Value(b"hello", {partition: 1}, now)))
    strategy.poll()
    strategy.submit(Message(Value(b"world", {partition: 2}, now)))
    strategy.poll()

    # Wait for async functions to finish
    retries = 10

    for _i in range(0, retries):
        if mock_func.call_count < 2 or next_step.submit.call_count < 2:
            strategy.poll()
            time.sleep(0.1)
        else:
            break

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2

    strategy.join()
    strategy.close()

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2


@pytest.mark.parametrize("better_backpressure", [False, True])
def test_transform(better_backpressure: bool) -> None:
    next_step = Mock()
    now = datetime.now()

    def transform_function(value: Message[int]) -> int:
        return value.payload * 2

    transform_step = RunTask(
        transform_function, next_step, better_backpressure=better_backpressure
    )

    original_message = Message(Value(1, {Partition(Topic("topic"), 0): 1}, now))

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_message)

    assert next_step.submit.call_args == call(
        Message(
            Value(
                transform_function(original_message),
                original_message.committable,
                original_message.timestamp,
            )
        )
    )

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        transform_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        transform_step.close()
        transform_step.join()

    next_step.reset_mock()
    next_step.submit.reset_mock()

    broker_payload = BrokerValue(1, Partition(Topic("topic"), 0), 0, datetime.now())

    original_broker_message = Message(broker_payload)

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_broker_message)

    assert next_step.submit.call_args == call(
        Message(
            BrokerValue(
                transform_function(original_broker_message),
                broker_payload.partition,
                broker_payload.offset,
                broker_payload.timestamp,
            )
        )
    )


def test_backpressure_function_called_once() -> None:
    """
    With better_backpressure=True, the function should only be called once
    per message even when next_step raises MessageRejected.
    """
    call_count = 0

    def counting_function(msg: Message[bytes]) -> bytes:
        nonlocal call_count
        call_count += 1
        return msg.payload

    next_step = Mock()
    # First call rejects, second accepts
    next_step.submit.side_effect = [MessageRejected(Mock()), None]

    strategy = RunTask(counting_function, next_step, better_backpressure=True)
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    msg = Message(Value(b"hello", {partition: 1}, now))
    strategy.submit(msg)

    assert call_count == 1

    # poll() should retry and succeed
    strategy.poll()

    assert call_count == 1
    assert next_step.submit.call_count == 2


def test_backpressure_join_flushes_message() -> None:
    """
    With better_backpressure=True, join() should flush carried-over messages.
    """

    def identity(msg: Message[bytes]) -> bytes:
        return msg.payload

    next_step = Mock()
    # First submit rejects, then accepts during join
    next_step.submit.side_effect = [MessageRejected(Mock()), None]

    strategy = RunTask(identity, next_step, better_backpressure=True)
    partition = Partition(Topic("topic"), 0)
    now = datetime.now()

    msg = Message(Value(b"hello", {partition: 1}, now))
    strategy.submit(msg)

    assert next_step.submit.call_count == 1

    # join() should flush the carried-over message
    strategy.join(timeout=1.0)

    assert next_step.submit.call_count == 2
    assert next_step.join.call_count == 1
