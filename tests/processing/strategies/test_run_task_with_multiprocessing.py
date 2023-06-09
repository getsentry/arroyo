import multiprocessing
import time
from multiprocessing.managers import SharedMemoryManager
from typing import Any
from unittest.mock import Mock, call

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MessageBatch,
    RunTaskWithMultiprocessing,
    ValueTooLarge,
    parallel_run_task_worker_apply,
)
from arroyo.types import Message, Partition, Topic, Value
from tests.assertions import assert_changes, assert_does_not_change
from tests.metrics import Gauge as GaugeCall
from tests.metrics import Increment as IncrementCall
from tests.metrics import TestingMetricsBackend
from tests.metrics import Timing as TimingCall


def test_message_batch() -> None:
    partition = Partition(Topic("test"), 0)

    smm = SharedMemoryManager()
    smm.start()

    block = smm.SharedMemory(16384)
    assert block.size == 16384

    message = Message(
        Value(
            KafkaPayload(None, b"\x00" * 16000, []),
            {partition: 1},
        )
    )

    batch: MessageBatch[Message[KafkaPayload]] = MessageBatch(block)
    with assert_changes(lambda: len(batch), 0, 1):
        batch.append(message)

    assert batch[0] == message
    assert list(batch) == [(0, message)]

    with assert_does_not_change(lambda: len(batch), 1), pytest.raises(ValueTooLarge):
        batch.append(message)

    smm.shutdown()


def transform_payload_expand(value: Message[KafkaPayload]) -> KafkaPayload:
    return KafkaPayload(
        value.payload.key,
        value.payload.value * 2,
        value.payload.headers,
    )


def test_parallel_run_task_worker_apply() -> None:
    messages = [
        Message(
            Value(
                KafkaPayload(None, b"\x00" * size, []),
                {Partition(Topic("test"), 0): i + 1},
            )
        )
        for i, size in enumerate([4000, 4000, 8000, 12000])
    ]

    smm = SharedMemoryManager()
    smm.start()
    input_block = smm.SharedMemory(32768)
    assert input_block.size == 32768

    input_batch = MessageBatch[Message[Any]](input_block)
    for message in messages:
        input_batch.append(message)

    assert len(input_batch) == 4

    output_block = smm.SharedMemory(16384)
    assert output_block.size == 16384

    result = parallel_run_task_worker_apply(
        transform_payload_expand,
        input_batch,
        output_block,
    )

    # The first batch should be able to fit 2 messages.
    assert result.next_index_to_process == 2
    assert len(result.valid_messages_transformed) == 2

    result = parallel_run_task_worker_apply(
        transform_payload_expand,
        input_batch,
        output_block,
        result.next_index_to_process,
    )

    # The second batch should be able to fit one message.
    assert result.next_index_to_process == 3
    assert len(result.valid_messages_transformed) == 1

    # The last message is too large to fit in the batch.
    with pytest.raises(ValueTooLarge):
        parallel_run_task_worker_apply(
            transform_payload_expand,
            input_batch,
            output_block,
            result.next_index_to_process,
        )
    smm.shutdown()


NO_KEY = "No Key"


def get_subprocess_count() -> int:
    return len(multiprocessing.active_children())


def test_parallel_transform_step() -> None:
    next_step = Mock()

    messages = [
        Message(
            Value(
                KafkaPayload(None, b"\x00" * size, []),
                {Partition(Topic("test"), 0): i + 1},
            )
        )
        for i, size in enumerate([4000, 4000, 8000, 2000])
    ]

    starting_processes = get_subprocess_count()
    worker_processes = 2
    manager_processes = 1
    metrics = TestingMetricsBackend

    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ), assert_changes(
        lambda: metrics.calls,
        [],
        [
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                0.0,
                tags=None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.processes",
                2.0,
                tags=None,
            ),
            IncrementCall(
                name="arroyo.strategies.run_task_with_multiprocessing.batch.input.overflow",
                value=1,
                tags=None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                1.0,
                tags=None,
            ),
            TimingCall(
                "arroyo.strategies.run_task_with_multiprocessing.batch.size.msg",
                3,
                None,
            ),
            TimingCall(
                "arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
                16000,
                None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                2.0,
                tags=None,
            ),
            TimingCall(
                "arroyo.strategies.run_task_with_multiprocessing.batch.size.msg",
                1,
                None,
            ),
            TimingCall(
                "arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
                2000,
                None,
            ),
        ],
    ):
        transform_step = RunTaskWithMultiprocessing(
            transform_payload_expand,
            next_step,
            num_processes=worker_processes,
            max_batch_size=5,
            max_batch_time=60,
            input_block_size=16384,
            output_block_size=16384,
        )

        for message in messages:
            transform_step.poll()
            transform_step.submit(message)

        transform_step.close()

    metrics.calls.clear()

    with assert_changes(
        get_subprocess_count,
        starting_processes + worker_processes + manager_processes,
        starting_processes,
    ), assert_changes(
        lambda: metrics.calls,
        [],
        [
            IncrementCall(
                name="arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow",
                value=1,
                tags=None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                1.0,
                tags=None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                0.0,
                tags=None,
            ),
        ],
    ):
        transform_step.join()

    assert next_step.submit.call_count == len(messages)


def test_parallel_run_task_terminate_workers() -> None:
    next_step = Mock()

    starting_processes = get_subprocess_count()
    worker_processes = 2
    manager_processes = 1

    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ):
        transform_step = RunTaskWithMultiprocessing(
            transform_payload_expand,  # doesn't matter
            next_step,
            num_processes=worker_processes,
            max_batch_size=5,
            max_batch_time=60,
            input_block_size=4096,
            output_block_size=4096,
        )

    with assert_changes(
        get_subprocess_count,
        starting_processes + worker_processes + manager_processes,
        starting_processes,
    ), assert_changes(lambda: int(next_step.terminate.call_count), 0, 1):
        transform_step.terminate()


_COUNT_CALLS = 0


def count_calls(value: Message[int]) -> int:
    global _COUNT_CALLS
    _COUNT_CALLS += 1
    return value.payload + _COUNT_CALLS


def test_message_rejected_multiple() -> None:
    """
    Regression test.

    When submitting multiple messages into the strategy, the first message
    should be retried until next_step stops raising MessageRejected.
    Only after the first message has been successfully submitted, should the
    second message be sent.

    We had a bug where RunTaskWithMultiprocessing would attempt submitting the
    second message without successfully delivering the first message.
    """
    # Handles MessageRejected from subsequent steps
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = RunTaskWithMultiprocessing(
        count_calls,
        next_step,
        num_processes=1,
        max_batch_size=1,
        max_batch_time=60,
        input_block_size=4096,
        output_block_size=4096,
    )

    strategy.submit(Message(Value(1, {})))
    strategy.submit(Message(Value(-100, {})))

    start_time = time.time()

    while next_step.submit.call_count < 5:
        time.sleep(0.1)
        strategy.poll()

        if time.time() - start_time > 5:
            raise AssertionError("took too long to poll")

    # The strategy keeps trying to submit the same message
    # since it's continually rejected
    assert next_step.submit.call_args_list == [
        call(Message(Value(2, {}))),
        call(Message(Value(2, {}))),
        call(Message(Value(2, {}))),
        call(Message(Value(2, {}))),
        call(Message(Value(2, {}))),
    ]

    # clear the side effect, let the message through now
    next_step.submit.reset_mock(side_effect=True)

    start_time = time.time()
    while next_step.submit.call_count < 2:
        time.sleep(0.1)
        strategy.poll()

        if time.time() - start_time > 5:
            raise AssertionError("took too long to poll")

    # The messages should have been submitted successfully now
    assert next_step.submit.call_args_list == [
        call(Message(Value(2, {}))),
        call(Message(Value(-98, {}))),
    ]

    strategy.close()
    strategy.join()
    assert next_step.submit.call_args_list == [
        call(Message(Value(2, {}))),
        call(Message(Value(-98, {}))),
    ]

    assert TestingMetricsBackend.calls == [
        GaugeCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
            value=0.0,
            tags=None,
        ),
        GaugeCall(
            name="arroyo.strategies.run_task_with_multiprocessing.processes",
            value=1,
            tags=None,
        ),
        GaugeCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
            value=1.0,
            tags=None,
        ),
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.size.msg",
            value=2,
            tags=None,
        ),
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
            value=0,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
        GaugeCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
            value=0.0,
            tags=None,
        ),
    ]


def run_sleep(value: Message[float]) -> float:
    time.sleep(value.payload)
    return value.payload


def test_regression_join_timeout_one_message() -> None:
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = RunTaskWithMultiprocessing(
        run_sleep,
        next_step,
        num_processes=1,
        max_batch_size=1,
        max_batch_time=60,
        input_block_size=4096,
        output_block_size=4096,
    )

    strategy.poll()
    strategy.submit(Message(Value(10, {})))

    start = time.time()

    strategy.close()
    strategy.join(timeout=3)

    time_taken = time.time() - start

    assert time_taken < 4

    assert next_step.submit.call_count == 0


def test_regression_join_timeout_many_messages() -> None:
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = RunTaskWithMultiprocessing(
        run_sleep,
        next_step,
        num_processes=1,
        max_batch_size=1,
        max_batch_time=60,
        input_block_size=4096,
        output_block_size=4096,
    )

    for _ in range(10):
        strategy.submit(Message(Value(0.1, {})))

    start = time.time()

    strategy.close()
    strategy.join(timeout=3)

    time_taken = time.time() - start

    assert 2 < time_taken < 4

    assert next_step.submit.call_count > 0
