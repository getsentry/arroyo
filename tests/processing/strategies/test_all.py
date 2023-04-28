"""
A general-purpose testsuite that asserts certain behavior is implemented by all strategies.
"""

from datetime import datetime
from functools import partial
from typing import (
    Any,
    Literal,
    MutableSequence,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)
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
    BrokerValue,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)

DummyStrategy = ProcessingStrategy[Union[FilteredPayload, bool]]
DummyMessage = Message[Union[FilteredPayload, bool]]

NOW = datetime.now()


class StrategyFactory(Protocol):
    def __call__(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        ...


def run_task_function(raises_invalid_message: bool, x: Message[bool]) -> bool:
    if raises_invalid_message and not x.payload:
        assert isinstance(x.value, BrokerValue)
        raise InvalidMessage.from_value(x.value)
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
        num_processes=4,
        max_batch_size=10,
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


PartitionIdx = int
Offset = int
MessagePattern = Sequence[Tuple[PartitionIdx, Offset, Literal["invalid", "valid"]]]


class MockProcessingStep(ProcessingStrategy[Any]):
    """
    Rejects all messages. Asserts poll not called after close.
    """

    def __init__(self) -> None:
        self.closed = False

    def poll(self) -> None:
        assert self.closed is not True, "Cannot call poll after close"

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def terminate(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def submit(self, message: Message[Any]) -> None:
        """
        Reject only all invalid messages
        """
        if message.payload == False:
            assert isinstance(message.value, BrokerValue)
            raise InvalidMessage(message.value.partition, message.value.offset)


@pytest.mark.parametrize("strategy_factory", FACTORIES)
@pytest.mark.parametrize(
    "message_pattern,expected_output",
    [
        (
            [(0, 0, "valid"), (0, 1, "invalid"), (0, 2, "valid")],
            [(0, 0, "valid"), (0, 1, "invalid"), (0, 2, "valid")],
        ),
        (
            [(0, 0, "valid"), (0, 1, "invalid"), (0, 2, "valid"), (0, 3, "invalid")],
            [(0, 0, "valid"), (0, 1, "invalid"), (0, 2, "valid"), (0, 3, "invalid")],
        ),
        (
            [(0, 0, "valid"), (1, 0, "valid"), (0, 1, "invalid"), (1, 1, "invalid")],
            [(0, 0, "valid"), (1, 0, "valid"), (0, 1, "invalid"), (1, 1, "invalid")],
        ),
    ],
)
def test_dlq(
    strategy_factory: StrategyFactory,
    message_pattern: MessagePattern,
    expected_output: MessagePattern,
) -> None:
    next_step = Mock(wraps=MockProcessingStep())
    step = strategy_factory(next_step, raises_invalid_message=True)

    topic = Topic("topic")

    messages = [
        Message(BrokerValue(type_ == "valid", Partition(topic, partition), offset, NOW))
        for partition, offset, type_ in message_pattern
    ]

    # The way that submit(), poll(), join() etc are called and how their errors
    # are handled is made to resemble how the StreamProcessor does it.

    def protected_call(method_name: str, *args: Any) -> bool:
        try:
            getattr(step, method_name)(*args)
            return True
        except InvalidMessage as e:
            invalid_messages.append(e)
            return False

    invalid_messages: MutableSequence[InvalidMessage] = []

    for message in messages:
        protected_call("poll")
        protected_call("submit", message)

    step.close()

    join_count = 0
    while not protected_call("join"):
        protected_call("poll")
        join_count += 1

        if join_count > len(messages):
            raise RuntimeError("needed to call join() too often")

    assert invalid_messages == [
        InvalidMessage(Partition(topic, partition), offset, needs_commit=False)
        for partition, offset, type_ in message_pattern
        if type_ == "invalid"
    ]

    # we poll so aggressively that essentially no batching of filtered payloads
    # should happen in most strategies.
    assert next_step.submit.call_args_list == [
        call(
            Message(BrokerValue(True, Partition(topic, partition), offset, NOW))
            if type_ == "valid"
            else Message(
                Value(FILTERED_PAYLOAD, {Partition(topic, partition): offset + 1})
            )
        )
        for partition, offset, type_ in message_pattern
    ]
