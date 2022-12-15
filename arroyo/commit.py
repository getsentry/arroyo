import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, MutableMapping, Optional

from arroyo.types import Partition, Position


@dataclass
class CommitPolicy:
    min_commit_frequency_sec: Optional[float]
    min_commit_messages: Optional[int]

    __committed_offsets: MutableMapping[Partition, int] = field(default_factory=dict)
    __last_committed_time: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        assert (
            self.min_commit_frequency_sec is not None
            or self.min_commit_messages is not None
        ), "Must provide either min_commit_frequency_sec or min_commit_messages"

    def should_commit(
        self, now: float, positions: Mapping[Partition, Position]
    ) -> bool:
        if self.min_commit_frequency_sec is not None:
            elapsed = now - self.__last_committed_time
            if elapsed >= self.min_commit_frequency_sec:
                return True

        if self.min_commit_messages is not None:
            messages_since_last_commit = 0
            for partition, pos in positions.items():
                prev_offset = self.__committed_offsets.setdefault(
                    partition, pos.offset - 1
                )
                messages_since_last_commit += pos.offset - prev_offset

            # XXX: is it faster to do this check in the loop and
            # potentially early-return, or do it outside and keep
            # the loop small?
            if messages_since_last_commit >= self.min_commit_messages:
                return True

        return False

    def did_commit(self, now: float, positions: Mapping[Partition, Position]) -> None:
        self.__last_committed_time = now
        for partition, pos in positions.items():
            self.__committed_offsets[partition] = pos.offset


IMMEDIATE = CommitPolicy(None, 1)
ONCE_PER_SECOND = CommitPolicy(1, None)


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset", "orig_message_ts"]

    group: str
    partition: Partition
    offset: int
    orig_message_ts: Optional[datetime]
