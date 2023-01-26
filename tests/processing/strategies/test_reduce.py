from typing import Callable, Set
from unittest.mock import Mock, call

from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, Message, Partition, Topic, Value


def test_reduce() -> None:
    def accumulator(result: Set[int], value: BaseValue[int]) -> Set[int]:
        result.add(value.payload)
        return result

    initial_value: Callable[[], Set[int]] = lambda: set()

    next_step = Mock()

    strategy = Reduce(3, 5.0, accumulator, initial_value, next_step)

    partition = Partition(Topic("topic"), 0)

    for i in range(6):
        strategy.submit(
            Message(
                Value(
                    i,
                    {
                        partition: i + 1,
                    },
                )
            )
        )
        strategy.poll()

    next_step.submit.assert_has_calls(
        [
            call(Message(Value({0, 1, 2}, {partition: 3}))),
            call(Message(Value({3, 4, 5}, {partition: 6}))),
        ]
    )
