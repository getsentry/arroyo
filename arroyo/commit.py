from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from arroyo.types import Partition


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset", "orig_message_ts", "received_p99"]

    group: str
    partition: Partition
    offset: int
    orig_message_ts: float
    received_p99: Optional[float]
