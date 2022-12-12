from datetime import datetime
from unittest.mock import Mock

from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.types import Message, Partition, Position, Topic, Value


def test_commit() -> None:
    commit_func = Mock()
    strategy = CommitOffsets(commit_func)

    strategy.submit(
        Message(Value(b"", {Partition(Topic("topic"), 1): Position(5, datetime.now())}))
    )

    assert commit_func.call_count == 1
