import multiprocessing
import signal
import time
from datetime import datetime
from multiprocessing.managers import SharedMemoryManager
from typing import Any, Generator
from unittest.mock import Mock, call

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MessageBatch,
    MultiprocessingPool,
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


@pytest.fixture(autouse=True)
def does_not_leak_sigchild_handler() -> Generator[None, None, None]:
    yield
    assert isinstance(signal.getsignal(signal.SIGCHLD), int)


def test_message_batch() -> None:
    partition = Partition(Topic("test"), 0)

    smm = SharedMemoryManager()
    smm.start()

    block = smm.SharedMemory(16384)
    assert block.size == 16384

    message = Message(
        Value(KafkaPayload(None, b"\x00" * 16000, []), {partition: 1}, datetime.now())
    )

    batch: MessageBatch[Message[KafkaPayload]] = MessageBatch(block, "test")
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
                datetime.now(),
            )
        )
        for i, size in enumerate([4000, 4000, 8000, 12000])
    ]

    smm = SharedMemoryManager()
    smm.start()
    input_block = smm.SharedMemory(32768)
    assert input_block.size == 32768

    input_batch = MessageBatch[Message[Any]](input_block, "test")
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
                datetime.now(),
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
            IncrementCall(
                name="arroyo.strategies.run_task_with_multiprocessing.pool.create",
                value=1,
                tags=None,
            ),
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
        pool = MultiprocessingPool(worker_processes)
        transform_step = RunTaskWithMultiprocessing(
            transform_payload_expand,
            next_step,
            max_batch_size=5,
            max_batch_time=60,
            pool=pool,
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
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
                value=2,
                tags=None,
            ),
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
                value=16000,
                tags=None,
            ),
            IncrementCall(
                name="arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow",
                value=1,
                tags=None,
            ),
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
                value=1,
                tags=None,
            ),
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
                value=16000,
                tags=None,
            ),
            GaugeCall(
                "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
                1.0,
                tags=None,
            ),
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
                value=1,
                tags=None,
            ),
            TimingCall(
                name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
                value=4000,
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
        pool.close()

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
        pool = MultiprocessingPool(worker_processes)
        transform_step = RunTaskWithMultiprocessing(
            transform_payload_expand,  # doesn't matter
            next_step,
            max_batch_size=5,
            max_batch_time=60,
            pool=pool,
            input_block_size=4096,
            output_block_size=4096,
        )

    with assert_changes(
        get_subprocess_count,
        starting_processes + worker_processes + manager_processes,
        starting_processes,
    ), assert_changes(lambda: int(next_step.terminate.call_count), 0, 1):
        transform_step.terminate()
        pool.close()


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

    now = datetime.now()
    pool = MultiprocessingPool(num_processes=1)
    strategy = RunTaskWithMultiprocessing(
        count_calls,
        next_step,
        max_batch_size=1,
        max_batch_time=60,
        pool=pool,
        input_block_size=4096,
        output_block_size=4096,
    )

    strategy.submit(Message(Value(1, {}, now)))
    strategy.submit(Message(Value(-100, {}, now)))

    start_time = time.time()

    while next_step.submit.call_count < 5:
        time.sleep(0.1)
        strategy.poll()

        if time.time() - start_time > 5:
            raise AssertionError("took too long to poll")

    # The strategy keeps trying to submit the same message
    # since it's continually rejected
    assert next_step.submit.call_args_list == [
        call(Message(Value(2, {}, now))),
        call(Message(Value(2, {}, now))),
        call(Message(Value(2, {}, now))),
        call(Message(Value(2, {}, now))),
        call(Message(Value(2, {}, now))),
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
        call(Message(Value(2, {}, now))),
        call(Message(Value(-98, {}, now))),
    ]

    strategy.close()
    strategy.join()
    assert next_step.submit.call_args_list == [
        call(Message(Value(2, {}, now))),
        call(Message(Value(-98, {}, now))),
    ]

    assert TestingMetricsBackend.calls == [
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.pool.create",
            value=1,
            tags=None,
        ),
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
    ] + [
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
            value=2,
            tags=None,
        ),
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
            value=0,
            tags=None,
        ),
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
            value=1,
            tags=None,
        ),
    ] * 5 + [
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
            value=2,
            tags=None,
        ),
        TimingCall(
            name="arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
            value=0,
            tags=None,
        ),
        GaugeCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
            value=0.0,
            tags=None,
        ),
    ]

    pool.close()


def run_sleep(value: Message[float]) -> float:
    time.sleep(value.payload)
    return value.payload


def test_regression_join_timeout_one_message() -> None:
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()
    pool = MultiprocessingPool(num_processes=1)
    strategy = RunTaskWithMultiprocessing(
        run_sleep,
        next_step,
        max_batch_size=1,
        max_batch_time=60,
        pool=pool,
        input_block_size=4096,
        output_block_size=4096,
    )

    strategy.poll()
    strategy.submit(Message(Value(10, {}, datetime.now())))

    start = time.time()

    strategy.close()
    strategy.join(timeout=3)

    time_taken = time.time() - start

    assert time_taken < 4

    assert next_step.submit.call_count == 0

    pool.close()


def test_regression_join_timeout_many_messages() -> None:
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    pool = MultiprocessingPool(num_processes=1)
    strategy = RunTaskWithMultiprocessing(
        run_sleep,
        next_step,
        max_batch_size=1,
        pool=pool,
        max_batch_time=60,
        input_block_size=4096,
        output_block_size=4096,
    )

    for _ in range(10):
        strategy.submit(Message(Value(0.1, {}, datetime.now())))

    start = time.time()

    strategy.close()
    strategy.join(timeout=3)

    time_taken = time.time() - start

    assert 2 < time_taken < 4

    assert next_step.submit.call_count > 0

    pool.close()


def run_multiply_times_two(x: Message[KafkaPayload]) -> KafkaPayload:
    return KafkaPayload(None, x.payload.value * 2, [])


def test_input_block_resizing_max_size() -> None:
    INPUT_SIZE = 36 * 1024 * 1024
    MSG_SIZE = 10 * 1024
    NUM_MESSAGES = INPUT_SIZE // MSG_SIZE
    next_step = Mock()
    pool = MultiprocessingPool(num_processes=2)
    strategy = RunTaskWithMultiprocessing(
        run_multiply_times_two,
        next_step,
        max_batch_size=NUM_MESSAGES,
        max_batch_time=60,
        pool=pool,
        input_block_size=None,
        output_block_size=INPUT_SIZE // 2,
        max_input_block_size=16000,
    )

    with pytest.raises(MessageRejected):
        for _ in range(NUM_MESSAGES):
            strategy.submit(Message(Value(KafkaPayload(None, b"x" * MSG_SIZE, []), {})))

    strategy.close()
    strategy.join(timeout=3)

    assert not any(
        x.name == "arroyo.strategies.run_task_with_multiprocessing.batch.input.resize"
        for x in TestingMetricsBackend.calls
    )

    pool.close()


def test_input_block_resizing_without_limits() -> None:
    INPUT_SIZE = 36 * 1024 * 1024
    MSG_SIZE = 10 * 1024
    NUM_MESSAGES = INPUT_SIZE // MSG_SIZE
    next_step = Mock()

    pool = MultiprocessingPool(num_processes=2)
    strategy = RunTaskWithMultiprocessing(
        run_multiply_times_two,
        next_step,
        max_batch_size=NUM_MESSAGES,
        max_batch_time=60,
        pool=pool,
        input_block_size=None,
        output_block_size=INPUT_SIZE // 2,
    )

    with pytest.raises(MessageRejected):
        for _ in range(NUM_MESSAGES):
            strategy.submit(Message(Value(KafkaPayload(None, b"x" * MSG_SIZE, []), {})))

    strategy.close()
    strategy.join(timeout=3)

    assert (
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.input.resize",
            value=1,
            tags=None,
        )
        in TestingMetricsBackend.calls
    )

    pool.close()


def test_output_block_resizing_max_size() -> None:
    INPUT_SIZE = 72 * 1024 * 1024
    MSG_SIZE = 10 * 1024
    NUM_MESSAGES = INPUT_SIZE // MSG_SIZE
    next_step = Mock()
    pool = MultiprocessingPool(num_processes=2)

    strategy = RunTaskWithMultiprocessing(
        run_multiply_times_two,
        next_step,
        max_batch_size=NUM_MESSAGES,
        max_batch_time=60,
        pool=pool,
        input_block_size=INPUT_SIZE,
        output_block_size=None,
        max_output_block_size=16000,
    )

    for _ in range(NUM_MESSAGES):
        strategy.submit(Message(Value(KafkaPayload(None, b"x" * MSG_SIZE, []), {})))

    strategy.close()
    strategy.join(timeout=3)

    assert not any(
        x.name == "arroyo.strategies.run_task_with_multiprocessing.batch.output.resize"
        for x in TestingMetricsBackend.calls
    )
    pool.close()


def test_output_block_resizing_without_limits() -> None:
    INPUT_SIZE = 144 * 1024 * 1024
    MSG_SIZE = 10 * 1024
    NUM_MESSAGES = INPUT_SIZE // MSG_SIZE
    next_step = Mock()

    now = datetime.now()
    pool = MultiprocessingPool(num_processes=2)
    strategy = RunTaskWithMultiprocessing(
        run_multiply_times_two,
        next_step,
        max_batch_size=NUM_MESSAGES,
        max_batch_time=60,
        pool=pool,
        input_block_size=INPUT_SIZE,
        output_block_size=None,
    )

    for _ in range(NUM_MESSAGES):
        strategy.submit(
            Message(Value(KafkaPayload(None, b"x" * MSG_SIZE, []), {}, now))
        )

    strategy.close()
    strategy.join(timeout=3)
    pool.close()

    assert (
        next_step.submit.call_args_list
        == [
            call(Message(Value(KafkaPayload(None, b"x" * 2 * MSG_SIZE, []), {}, now))),
        ]
        * NUM_MESSAGES
    )

    assert (
        IncrementCall(
            name="arroyo.strategies.run_task_with_multiprocessing.batch.output.resize",
            value=1,
            tags=None,
        )
        in TestingMetricsBackend.calls
    )


def message_processor_raising_invalid_message(x: Message[KafkaPayload]) -> KafkaPayload:
    raise InvalidMessage(
        Partition(topic=Topic("test_topic"), index=0),
        offset=1000,
    )


def test_multiprocessing_with_invalid_message() -> None:
    next_step = Mock()
    pool = MultiprocessingPool(num_processes=2)
    strategy = RunTaskWithMultiprocessing(
        message_processor_raising_invalid_message,
        next_step,
        max_batch_size=1,
        max_batch_time=60,
        pool=pool,
    )

    strategy.submit(
        Message(Value(KafkaPayload(None, b"x" * 10, []), {}, datetime.now()))
    )

    strategy.poll()
    strategy.close()
    with pytest.raises(InvalidMessage):
        strategy.join(timeout=3)
    pool.close()


def test_reraise_invalid_message() -> None:
    next_step = Mock()
    partition = Partition(Topic("test"), 0)
    offset = 5
    now = datetime.now()
    next_step.poll.side_effect = InvalidMessage(partition, offset)
    pool = MultiprocessingPool(num_processes=2)

    strategy = RunTaskWithMultiprocessing(
        run_multiply_times_two,
        next_step,
        max_batch_size=1,
        max_batch_time=60,
        pool=pool,
    )

    strategy.submit(Message(Value(KafkaPayload(None, b"x" * 10, []), {}, now)))

    with pytest.raises(InvalidMessage):
        strategy.poll()

    next_step.poll.reset_mock(side_effect=True)
    strategy.close()
    strategy.join()
    pool.close()


def slow_func(message: Message[int]) -> int:
    time.sleep(0.2)
    return message.payload


def test_reuse_pool() -> None:
    # To be reused in strategy_one and strategy_two
    pool = MultiprocessingPool(num_processes=2)
    next_step = Mock()

    strategy_one = RunTaskWithMultiprocessing(
        slow_func,
        next_step,
        max_batch_size=2,
        max_batch_time=5,
        pool=pool,
    )

    strategy_one.submit(Message(Value(10, committable={})))

    strategy_one.close()

    # Join with timeout=0.0 to ensure there will be unprocessed pending messages
    # in the first batch
    strategy_one.join(0.0)

    strategy_two = RunTaskWithMultiprocessing(
        slow_func,
        next_step,
        max_batch_size=2,
        max_batch_time=5,
        pool=pool,
    )

    strategy_two.submit(Message(Value(10, committable={})))

    strategy_two.close()

    # Join with no timeout so the pending task will complete and message gets submitted
    strategy_two.join()

    assert next_step.submit.call_count == 1
    pool.close()
