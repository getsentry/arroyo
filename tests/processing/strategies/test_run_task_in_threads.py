from datetime import datetime
import time
from unittest.mock import Mock
import pytest

from arroyo.processing.strategies.run_task_in_threads import RunTaskInThreads
from arroyo.types import BrokerValue, Message, Topic, Partition


@pytest.mark.parametrize("poll_after_msg", (True, False))
@pytest.mark.parametrize("poll_before_join", (True, False))
@pytest.mark.parametrize("abandon_messages_on_shutdown", (True, False))
def test_run_task_in_threads(poll_after_msg: bool, poll_before_join: bool, abandon_messages_on_shutdown: bool) -> None:
    mock_func = Mock()
    next_step = Mock()

    strategy = RunTaskInThreads(mock_func, 2, 4, next_step, abandon_messsages_on_shutdown=abandon_messages_on_shutdown)
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

    if abandon_messages_on_shutdown is True:
        # Nothing is ever submitted since poll never gets called
        if not poll_before_join and not poll_after_msg:
            assert next_step.poll.call_count == 0
            assert next_step.submit.call_count == 0
    else:
        assert mock_func.call_count == 2
        assert next_step.poll.call_count == 2
        assert next_step.submit.call_count == 2
