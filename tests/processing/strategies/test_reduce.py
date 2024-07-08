from datetime import datetime
from typing import Callable, Set
from unittest.mock import Mock, call

from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, Message, Partition, Topic, Value
import pytest


@pytest.mark.parametrize("abandon_messages_on_shutdown", (True, False))
def test_reduce(abandon_messages_on_shutdown: bool) -> None:
    now = datetime.now()

    def accumulator(result: Set[int], value: BaseValue[int]) -> Set[int]:
        result.add(value.payload)
        return result

    initial_value: Callable[[], Set[int]] = lambda: set()

    next_step = Mock()

    strategy = Reduce(3, 5.0, accumulator, initial_value, next_step, abandon_messages_on_shutdown=abandon_messages_on_shutdown)

    partition = Partition(Topic("topic"), 0)

    for i in range(7):
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
            call(Message(Value({0, 1, 2}, {partition: 3}, now))),
            call(Message(Value({3, 4, 5}, {partition: 6}, now))),
        ]
    )

    strategy.close()
    strategy.join()

    if abandon_messages_on_shutdown:
        assert next_step.submit.call_count == 2
    else:
        assert next_step.submit.call_count == 3
