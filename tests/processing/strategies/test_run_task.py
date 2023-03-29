import time
from unittest.mock import Mock

from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import Message, Partition, Topic, Value


def test_run_task() -> None:
    mock_func = Mock()
    next_step = Mock()

    strategy = RunTask(mock_func, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(Value(b"hello", {partition: 1})))
    strategy.poll()
    strategy.submit(Message(Value(b"world", {partition: 2})))
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
