from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, MutableMapping, Optional

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

    def get_state_machine(self) -> CommitPolicyState:
        return CommitPolicyState(self)


@dataclass
class CommitPolicyState:
    policy: CommitPolicy

    __committed_offsets: MutableMapping[Partition, int] = field(default_factory=dict)
    __last_committed_time: float = field(default_factory=time.time)

    def should_commit(self, now: float, offsets: Mapping[Partition, int]) -> bool:
        if self.policy.min_commit_frequency_sec is not None:
            elapsed = now - self.__last_committed_time
            if elapsed >= self.policy.min_commit_frequency_sec:
                return True

        if self.policy.min_commit_messages is not None:
            messages_since_last_commit = 0
            for partition, pos in offsets.items():
                prev_offset = self.__committed_offsets.setdefault(partition, pos - 1)
                messages_since_last_commit += pos - prev_offset

            # XXX: is it faster to do this check in the loop and
            # potentially early-return, or do it outside and keep
            # the loop small?
            if messages_since_last_commit >= self.policy.min_commit_messages:
                return True

        return False

    def did_commit(self, now: float, offsets: Mapping[Partition, int]) -> None:
        self.__last_committed_time = now
        self.__committed_offsets.update(offsets)


IMMEDIATE = CommitPolicy(None, 1)
ONCE_PER_SECOND = CommitPolicy(1, None)


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset", "orig_message_ts"]

    group: str
    partition: Partition
    offset: int
    orig_message_ts: Optional[datetime]
