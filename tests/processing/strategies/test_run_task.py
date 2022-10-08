from unittest import mock
from datetime import datetime
from arroyo.processing.run_task import RunTask
from arroyo.types import Message, Partition, Topic


def test_run_task() -> None:
    mock_func = mock.Mock()
    commit_func = mock.Mock()

    strategy = RunTask(mock_func, 2, commit_func)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(partition, 0, b"hello", datetime.now()))
    strategy.poll()
    strategy.submit(Message(partition, 1, b"world", datetime.now()))
    strategy.poll()

    assert mock_func.call_count == 2
    assert commit_func.call_count == 2

    strategy.join()
    strategy.close()

    assert commit_func.call_count == 3
