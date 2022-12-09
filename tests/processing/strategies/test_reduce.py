from datetime import datetime
from typing import Set
from unittest.mock import ANY, Mock, call

from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, Message, Partition, Position, Topic, Value


def test_reduce() -> None:
    def accumulator(result: Set[int], value: BaseValue[int]) -> Set[int]:
        result.add(value.payload)
        return result

    initial_value: Set[int] = set()

    next_step = Mock()

    strategy = Reduce(3, 5.0, accumulator, initial_value, next_step)

    partition = Partition(Topic("topic"), 0)

    for i in range(6):
        strategy.submit(Message(Value(i, {partition: Position(i + 1, datetime.now())})))
        strategy.poll()

    next_step.submit.assert_has_calls(
        [
            call(Message(Value({0, 1, 2}, {partition: Position(3, ANY)}))),
            call(Message(Value({3, 4, 5}, {partition: Position(6, ANY)}))),
        ]
    )
