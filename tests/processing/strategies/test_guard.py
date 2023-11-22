from datetime import datetime
from typing import Any
from unittest.mock import Mock, call

import pytest

from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.guard import StrategyGuard
from arroyo.types import (
    FILTERED_PAYLOAD,
    BrokerValue,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)


def test_guard() -> None:
    partition = Partition(Topic("topic"), 1)
    now = datetime.now()
    message = Message(BrokerValue(b"", partition, 5, now))

    # Reject all messages that aren't the filtered one
    def inner_strategy_submit(msg: Message[bytes]) -> None:
        if not isinstance(msg.payload, FilteredPayload):
            value = msg.value
            assert isinstance(value, BrokerValue)
            raise InvalidMessage(value.partition, value.offset)

    inner_strategy = Mock()
    inner_strategy.submit = Mock(side_effect=inner_strategy_submit)
    next_step = Mock()

    strategy: StrategyGuard[Any] = StrategyGuard(lambda _: inner_strategy, next_step)

    with pytest.raises(InvalidMessage):
        strategy.submit(message)
    strategy.poll()

    assert inner_strategy.submit.call_count == 1
    # Close and join do not raise
    strategy.close()
    assert next_step.submit.call_args_list == [
        call(Message(Value(FILTERED_PAYLOAD, {partition: 6})))
    ]
    strategy.join()
