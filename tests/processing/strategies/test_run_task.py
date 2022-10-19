import time
from datetime import datetime
from unittest import mock

from arroyo.processing.strategies.run_task import RunTaskInThreads
from arroyo.types import Message, Partition, Topic


def test_run_task() -> None:
    mock_func = mock.Mock()
    commit_func = mock.Mock()

    strategy = RunTaskInThreads(mock_func, 2, 4, commit_func)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(partition, 0, b"hello", datetime.now()))
    strategy.poll()
    strategy.submit(Message(partition, 1, b"world", datetime.now()))
    strategy.poll()

    # Wait for async functions to finish
    retries = 10

    for _i in range(0, retries):
        if mock_func.call_count < 2 or commit_func.call_count < 2:
            strategy.poll()
            time.sleep(0.1)
        else:
            break

    assert mock_func.call_count == 2
    assert commit_func.call_count == 2

    strategy.join()
    strategy.close()

    assert mock_func.call_count == 2
    assert commit_func.call_count == 3
