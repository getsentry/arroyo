from datetime import datetime
import time
from unittest.mock import Mock
import pytest

from arroyo.processing.strategies.run_task_in_threads import RunTaskInThreads
from arroyo.types import BrokerValue, Message, Topic, Partition


@pytest.mark.parametrize("poll_after_msg", (True, False))
@pytest.mark.parametrize("poll_before_join", (True, False))
def test_run_task_in_threads(poll_after_msg: bool, poll_before_join: bool) -> None:
    mock_func = Mock()
    next_step = Mock()

    strategy = RunTaskInThreads(mock_func, 2, 4, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(BrokerValue(b"hello", partition, 0, datetime.now())))
    if poll_after_msg:
        strategy.poll()
    strategy.submit(Message(BrokerValue(b"world", partition, 1, datetime.now())))
    if poll_after_msg:
        strategy.poll()

    if poll_before_join:
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
