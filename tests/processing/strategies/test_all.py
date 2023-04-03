"""
A general-purpose testsuite that asserts certain behavior is implemented by all strategies.
"""

from functools import partial
from typing import Protocol, Sequence, Union
from unittest.mock import Mock, call

import pytest

from arroyo.commit import IMMEDIATE
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.filter import FilterStep
from arroyo.processing.strategies.reduce import Reduce
from arroyo.processing.strategies.run_task import RunTask
from arroyo.processing.strategies.run_task_in_threads import RunTaskInThreads
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    RunTaskWithMultiprocessing,
)
from arroyo.types import (
    FILTERED_PAYLOAD,
    BaseValue,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)

DummyStrategy = ProcessingStrategy[Union[FilteredPayload, bool]]
DummyMessage = Message[Union[FilteredPayload, bool]]


class StrategyFactory(Protocol):
    def __call__(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        ...


def run_task_function(raises_invalid_message: bool, x: Message[bool]) -> bool:
    if raises_invalid_message and not x.payload:
        (partition,) = x.committable
        raise InvalidMessage(partition, x.committable[partition])
    assert isinstance(x.payload, bool)
    return x.payload


def run_task_factory(
    next_step: DummyStrategy, raises_invalid_message: bool = False
) -> RunTask[bool, bool]:
    return RunTask(partial(run_task_function, raises_invalid_message), next_step)


def run_task_with_multiprocessing_factory(
    next_step: DummyStrategy, raises_invalid_message: bool = False
) -> RunTaskWithMultiprocessing[bool, bool]:
    return RunTaskWithMultiprocessing(
        partial(run_task_function, raises_invalid_message),
        next_step=next_step,
        num_processes=2,
        max_batch_size=5,
        max_batch_time=60,
        input_block_size=16384,
        output_block_size=16384,
    )


def run_task_in_threads_factory(
    next_step: DummyStrategy, raises_invalid_message: bool = False
) -> RunTaskInThreads[bool, bool]:
    return RunTaskInThreads(
        partial(run_task_function, raises_invalid_message),
        next_step=next_step,
        concurrency=1,
        max_pending_futures=10,
    )


def filter_step_factory(
    next_step: DummyStrategy, raises_invalid_message: bool = False
) -> FilterStep[bool]:
    if raises_invalid_message:
        pytest.skip("does not support invalid message")
    return FilterStep(
        lambda message: message.payload, next_step, commit_policy=IMMEDIATE
    )


def reduce_step_factory(
    next_step: DummyStrategy, raises_invalid_message: bool = False
) -> Reduce[bool, bool]:
    if raises_invalid_message:
        pytest.skip("does not support invalid message")

    def accumulator(result: bool, value: BaseValue[bool]) -> bool:
        assert isinstance(result, bool)
        assert isinstance(value.payload, bool)
        return value.payload

    return Reduce(
        max_batch_size=1,
        max_batch_time=1,
        accumulator=accumulator,
        initial_value=bool,
        next_step=next_step,
    )


FACTORIES: Sequence[StrategyFactory] = [
    run_task_factory,
    run_task_with_multiprocessing_factory,
    run_task_in_threads_factory,
    filter_step_factory,
    reduce_step_factory,
]


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_filters(strategy_factory: StrategyFactory) -> None:
    next_step = Mock()
    step = strategy_factory(next_step)

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    messages: Sequence[DummyMessage] = [
        Message(Value(True, {Partition(Topic("topic"), 0): 1})),
        Message(Value(FILTERED_PAYLOAD, {Partition(Topic("topic"), 0): 2})),
        Message(Value(True, {Partition(Topic("topic"), 0): 3})),
    ]

    for message in messages:
        step.poll()
        step.submit(message)

    step.close()
    step.join()

    assert next_step.submit.call_args_list == list(map(call, messages))


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_dlq(strategy_factory: StrategyFactory) -> None:
    next_step = Mock()
    step = strategy_factory(next_step, raises_invalid_message=True)

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    messages = [
        Message(Value(True, {Partition(Topic("topic"), 0): 1})),
        Message(Value(False, {Partition(Topic("topic"), 0): 2})),
        Message(Value(True, {Partition(Topic("topic"), 0): 3})),
    ]

    step.poll()
    step.submit(messages[0])

    invalid_messages = []

    step.poll()
    try:
        step.submit(messages[1])
    except InvalidMessage as e:
        invalid_messages.append(e)

    try:
        step.poll()
    except InvalidMessage as e:
        invalid_messages.append(e)

    step.submit(messages[2])

    step.close()

    try:
        step.join()
    except InvalidMessage as e:
        invalid_messages.append(e)

    assert len(invalid_messages) == 1

    assert next_step.submit.call_args_list == [
        call(messages[0]),
        call(Message(Value(FILTERED_PAYLOAD, {Partition(Topic("topic"), 0): 2}))),
        call(messages[2]),
    ]
