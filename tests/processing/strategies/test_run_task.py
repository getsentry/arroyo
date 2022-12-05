import time
from datetime import datetime
from unittest import mock

from arroyo.processing.strategies.run_task import RunTask, RunTaskInThreads
from arroyo.types import BrokerValue, Message, Partition, Position, Topic, Value


def test_run_task() -> None:
    mock_func = mock.Mock()
    next_step = mock.Mock()

    strategy = RunTask(mock_func, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(Value(b"hello", {partition: Position(0, datetime.now())})))
    strategy.poll()
    strategy.submit(Message(Value(b"world", {partition: Position(1, datetime.now())})))
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


def test_run_task_in_threads() -> None:
    mock_func = mock.Mock()
    next_step = mock.Mock()

    strategy = RunTaskInThreads(mock_func, 2, 4, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(BrokerValue(b"hello", partition, 0, datetime.now())))
    strategy.poll()
    strategy.submit(Message(BrokerValue(b"world", partition, 1, datetime.now())))
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
