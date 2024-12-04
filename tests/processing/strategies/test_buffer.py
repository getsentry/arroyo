from datetime import datetime
from unittest.mock import Mock, call
from typing import List

from arroyo.processing.strategies.buffer import Buffer
from arroyo.types import BaseValue, Message, Partition, Topic, Value


class BufferTest:
    def __init__(self) -> None:
        self._buffer: List[int] = []

    @property
    def buffer(self) -> List[int]:
        return self._buffer

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    @property
    def is_ready(self) -> bool:
        return len(self._buffer) >= 3

    def append(self, message: BaseValue[int]) -> None:
        self._buffer.append(message.payload)

    def new(self) -> "BufferTest":
        return BufferTest()


def test_buffer() -> None:
    now = datetime.now()
    next_step = Mock()
    strategy = Buffer(BufferTest(), next_step)
    partition = Partition(Topic("topic"), 0)

    for i in range(6):
        strategy.submit(
            Message(
                Value(
                    i,
                    {
                        partition: i + 1,
                    },
                    now,
                )
            )
        )
        strategy.poll()

    next_step.submit.assert_has_calls(
        [
            call(Message(Value([0, 1, 2], {partition: 3}, now))),
            call(Message(Value([3, 4, 5], {partition: 6}, now))),
        ]
    )
