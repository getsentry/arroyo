from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from arroyo.types import Partition


@dataclass(frozen=True)
class CommitPolicy:
    min_commit_frequency_sec: Optional[float]
    min_commit_messages: Optional[int]

    def __post_init__(self) -> None:
        assert (
            self.min_commit_frequency_sec is not None
            or self.min_commit_messages is not None
        ), "Must provide either min_commit_frequency_sec or min_commit_messages"

    def should_commit(self, elapsed_time: float, elapsed_messages: int) -> bool:
        if (
            self.min_commit_frequency_sec is not None
            and elapsed_time >= self.min_commit_frequency_sec
        ):
            return True

        if (
            self.min_commit_messages is not None
            and elapsed_messages >= self.min_commit_messages
        ):
            return True

        return False


IMMEDIATE = CommitPolicy(None, 1)
ONCE_PER_SECOND = CommitPolicy(1, None)


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset", "orig_message_ts"]

    group: str
    partition: Partition
    offset: int
    orig_message_ts: Optional[datetime]
