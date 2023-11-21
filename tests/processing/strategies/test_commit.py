from datetime import datetime
from unittest.mock import ANY, Mock

from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.types import Message, Partition, Topic, Value
from tests.metrics import Increment, TestingMetricsBackend


def test_commit() -> None:
    commit_func = Mock()
    strategy = CommitOffsets(commit_func)

    strategy.submit(
        Message(Value(b"", {Partition(Topic("topic"), 1): 5}, datetime.now()))
    )

    assert commit_func.call_count == 1


def test_commit_poll() -> None:
    # This is currently necessary to ensure that offsets are committed (still
    # debounced) even when the topic is empty.
    commit_func = Mock()
    strategy = CommitOffsets(commit_func)

    strategy.poll()

    assert commit_func.call_count == 1


def test_record_metric() -> None:
    commit_func = Mock()
    strategy = CommitOffsets(commit_func)
    now = datetime.now()

    strategy.submit(Message(Value(b"", {Partition(Topic("topic"), 1): 5}, now)))
    strategy.poll()

    metrics = TestingMetricsBackend
    assert metrics.calls == [
        Increment(name="arroyo.consumer.latency", value=ANY, tags=None)
    ]
