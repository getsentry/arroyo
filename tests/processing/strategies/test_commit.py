from datetime import datetime
from typing import Any
from unittest.mock import Mock

from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.types import Message, Partition, Topic


def test_commit() -> None:
    commit_func = Mock()
    strategy: CommitOffsets[Any] = CommitOffsets(commit_func)
    strategy.submit(Message(Partition(Topic("topic"), 1), 5, b"", datetime.now()))
    assert commit_func.call_count == 1
