"""
A general-purpose testsuite that asserts certain behavior is implemented by all strategies.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from typing import Any, Literal, MutableSequence, Optional, Sequence, Tuple, Type, Union
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
    MultiprocessingPool,
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


class StrategyFactory(ABC):
    @abstractmethod
    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError


def run_task_function(raises_invalid_message: bool, x: Message[bool]) -> bool:
    if raises_invalid_message and not x.payload:
        assert isinstance(x.value, BrokerValue)
        raise InvalidMessage.from_value(x.value)
    assert isinstance(x.payload, bool)
    return x.payload


class RunTaskFactory(StrategyFactory):
    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        return RunTask(partial(run_task_function, raises_invalid_message), next_step)

    def shutdown(self) -> None:
        pass


class RunTaskWithMultiprocessingFactory(StrategyFactory):
    def __init__(self) -> None:
        self.__pool = MultiprocessingPool(num_processes=4)

    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        return RunTaskWithMultiprocessing(
            partial(run_task_function, raises_invalid_message),
            next_step=next_step,
            max_batch_size=10,
            max_batch_time=60,
            pool=self.__pool,
            input_block_size=16384,
            output_block_size=16384,
        )

    def shutdown(self) -> None:
        self.__pool.close()


class RunTaskInThreadsFactory(StrategyFactory):
    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        return RunTaskInThreads(
            partial(run_task_function, raises_invalid_message),
            next_step=next_step,
            concurrency=1,
            max_pending_futures=10,
        )

    def shutdown(self) -> None:
        pass


class FilterFactory(StrategyFactory):
    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        if raises_invalid_message:
            pytest.skip("does not support invalid message")

        return FilterStep(
            lambda message: message.payload, next_step, commit_policy=IMMEDIATE
        )

    def shutdown(self) -> None:
        pass


class ReduceFactory(StrategyFactory):
    def strategy(
        self, next_step: DummyStrategy, raises_invalid_message: bool = False
    ) -> DummyStrategy:
        if raises_invalid_message:
            pytest.skip("does not support invalid message")

        def accumulator(
            result: Union[FilteredPayload, bool], value: BaseValue[bool]
        ) -> bool:
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

    def shutdown(self) -> None:
        pass


FACTORIES: Sequence[Type[StrategyFactory]] = [
    RunTaskFactory,
    RunTaskWithMultiprocessingFactory,
    RunTaskInThreadsFactory,
    FilterFactory,
    ReduceFactory,
]


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_filters(strategy_factory: Type[StrategyFactory]) -> None:
    next_step = Mock()
    factory = strategy_factory()
    step = factory.strategy(next_step)

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    messages: Sequence[DummyMessage] = [
        Message(Value(True, {Partition(Topic("topic"), 0): 1}, NOW)),
        Message(Value(FILTERED_PAYLOAD, {Partition(Topic("topic"), 0): 2})),
        Message(Value(True, {Partition(Topic("topic"), 0): 3}, NOW)),
    ]

    for message in messages:
        step.poll()
        step.submit(message)

    step.close()
    step.join()
    factory.shutdown()

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
    strategy_factory: Type[StrategyFactory],
    message_pattern: MessagePattern,
    expected_output: MessagePattern,
) -> None:
    next_step = Mock(wraps=MockProcessingStep())
    factory = strategy_factory()
    step = factory.strategy(next_step, raises_invalid_message=True)

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

    factory.shutdown()

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


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_terminate(strategy_factory: Type[StrategyFactory]) -> None:
    next_step = Mock()
    factory = strategy_factory()
    step = factory.strategy(next_step)
    step.terminate()

    assert next_step.terminate.call_args_list == [call()]
    factory.shutdown()


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_join(strategy_factory: Type[StrategyFactory]) -> None:
    next_step = Mock()

    factory = strategy_factory()
    step = factory.strategy(next_step)
    step.close()
    step.join()
    factory.shutdown()

    assert next_step.close.call_args_list == [call()]

    assert next_step.join.call_args_list == [call(timeout=None)]


@pytest.mark.parametrize("strategy_factory", FACTORIES)
def test_poll_next_step(strategy_factory: Type[StrategyFactory]) -> None:
    next_step = Mock()
    factory = strategy_factory()
    step = factory.strategy(next_step)

    # Ensure that polling a strategy forwards the poll unconditionally even if
    # there are no messages to process, or no progress at all. Otherwise there
    # are weird effects where all messages on a test topic (such as in
    # benchmarking/QE) have been processed but the last batch of offsets never
    # gets committed.
    step.poll()
    step.terminate()
    factory.shutdown()

    assert next_step.poll.call_args_list == [call()]
