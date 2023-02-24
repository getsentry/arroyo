import multiprocessing
import time
from datetime import datetime
from multiprocessing.managers import SharedMemoryManager
from typing import Any
from unittest import mock
from unittest.mock import Mock

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidMessages,
    InvalidRawMessage,
)
from arroyo.processing.strategies.run_task import (
    MessageBatch,
    RunTask,
    RunTaskInThreads,
    RunTaskWithMultiprocessing,
    ValueTooLarge,
    parallel_run_task_worker_apply,
)
from arroyo.types import BrokerValue, Message, Partition, Topic, Value
from tests.assertions import assert_changes, assert_does_not_change
from tests.metrics import Gauge as GaugeCall
from tests.metrics import TestingMetricsBackend
from tests.metrics import Timing as TimingCall


def test_run_task() -> None:
    mock_func = mock.Mock()
    next_step = mock.Mock()

    strategy = RunTask(mock_func, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(Value(b"hello", {partition: 1})))
    strategy.poll()
    strategy.submit(Message(Value(b"world", {partition: 2})))
    strategy.poll()

    # Wait for async functions to finish
    retries = 10

    for _i in range(0, retries):
        if mock_func.call_count < 2 or next_step.submit.call_count < 2:
            strategy.poll()
            time.sleep(0.1)
        else:
            break

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2

    strategy.join()
    strategy.close()

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2


def test_run_task_in_threads() -> None:
    mock_func = mock.Mock()
    next_step = mock.Mock()

    strategy = RunTaskInThreads(mock_func, 2, 4, next_step)
    partition = Partition(Topic("topic"), 0)

    strategy.submit(Message(BrokerValue(b"hello", partition, 0, datetime.now())))
    strategy.poll()
    strategy.submit(Message(BrokerValue(b"world", partition, 1, datetime.now())))
    strategy.poll()

    # Wait for async functions to finish
    retries = 10

    for _i in range(0, retries):
        if mock_func.call_count < 2 or next_step.submit.call_count < 2:
            strategy.poll()
            time.sleep(0.1)
        else:
            break

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2

    strategy.join()
    strategy.close()

    assert mock_func.call_count == 2
    assert next_step.poll.call_count == 2
    assert next_step.submit.call_count == 2


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

    batch: MessageBatch[KafkaPayload] = MessageBatch(block)
    with assert_changes(lambda: len(batch), 0, 1):
        batch.append(message)

    assert batch[0] == message
    assert list(batch) == [message]

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

    input_batch = MessageBatch[Any](input_block)
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


def fail_bad_messages(value: Message[KafkaPayload]) -> KafkaPayload:
    if value.payload.key is None:
        raise InvalidMessages(
            [
                InvalidRawMessage(
                    payload=str(value.payload),
                    reason=NO_KEY,
                )
            ]
        )

    return value.payload


def test_parallel_transform_worker_bad_messages() -> None:
    smm = SharedMemoryManager()
    smm.start()
    input_block = smm.SharedMemory(128)
    output_block = smm.SharedMemory(128)

    # every other message has a key
    messages = [
        Message(
            Value(
                KafkaPayload(None if i % 2 == 0 else b"key", b"\x00", []),
                {Partition(Topic("test"), 0): i + 1},
            )
        )
        for i in range(9)
    ]

    input_batch = MessageBatch[Any](input_block)
    for message in messages:
        input_batch.append(message)
    assert len(input_batch) == 9
    # process entire batch
    result = parallel_run_task_worker_apply(
        fail_bad_messages, input_batch, output_block
    )
    # all 9 messages processed
    assert result.next_index_to_process == 9
    # 5 were bad, 4 were good
    assert len(result.invalid_messages) == 5
    assert len(result.valid_messages_transformed) == 4

    input_batch = MessageBatch[Any](input_block)
    for message in messages:
        input_batch.append(message)
    assert len(input_batch) == 9
    # process batch from halfway through
    result = parallel_run_task_worker_apply(
        fail_bad_messages, input_batch, output_block, start_index=5
    )
    # all 9 messages processed
    assert result.next_index_to_process == 9
    # Out of remaining 4, 2 were bad, 2 were good
    assert len(result.invalid_messages) == 2
    assert len(result.valid_messages_transformed) == 2
    smm.shutdown()


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
            GaugeCall("batches_in_progress", 0.0, tags=None),
            GaugeCall("transform.processes", 2.0, tags=None),
            GaugeCall("batches_in_progress", 1.0, tags=None),
            TimingCall("batch.size.msg", 3, None),
            TimingCall("batch.size.bytes", 16000, None),
            GaugeCall("batches_in_progress", 2.0, tags=None),
            TimingCall("batch.size.msg", 1, None),
            TimingCall("batch.size.bytes", 2000, None),
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
        [GaugeCall("batches_in_progress", value, tags=None) for value in [1.0, 0.0]],
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


def test_parallel_run_task_bad_messages() -> None:
    next_step = Mock()

    starting_processes = get_subprocess_count()
    worker_processes = 5
    manager_processes = 1

    # every other message has a key
    messages = [
        Message(
            Value(
                KafkaPayload(None if i % 2 == 0 else b"key", b"\x00", []),
                {Partition(Topic("test"), 0): 1},
            )
        )
        for i in range(9)
    ]

    # everything should be processed in parallel, 5 workers should spawn
    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ):
        # create transform step with multiple processes
        transform_step = RunTaskWithMultiprocessing(
            function=fail_bad_messages,
            next_step=next_step,
            num_processes=worker_processes,
            max_batch_size=9,
            max_batch_time=60,
            input_block_size=4096,
            output_block_size=4096,
        )

        # submit 9 messages: 4 good ones 5 bad ones
        for message in messages:
            transform_step.submit(message)
            transform_step.poll()

    # wait for all processes to finish
    with pytest.raises(InvalidMessages) as e_info:
        transform_step.close()
        transform_step.join()

    # An exception should have been thrown with the 5 bad messages
    assert len(e_info.value.messages) == 5
    # Test exception pickles and decodes correctly
    invalid_message = e_info.value.messages[0]
    assert isinstance(invalid_message, InvalidRawMessage)
    assert invalid_message.reason == NO_KEY
    assert invalid_message.payload == str(messages[0].payload)
    # The 4 good ones should not have been blocked
    assert next_step.submit.call_count == 4


def add_one(value: Message[int]) -> int:
    return value.payload + 1


def test_message_rejected() -> None:
    # Handles MessageRejected from subsequent steps
    next_step = Mock()
    next_step.submit.side_effect = MessageRejected()

    strategy = RunTaskWithMultiprocessing(
        add_one,
        next_step,
        num_processes=1,
        max_batch_size=1,
        max_batch_time=60,
        input_block_size=4096,
        output_block_size=4096,
    )

    strategy.submit(Message(Value(1, {})))

    for _ in range(5):
        time.sleep(0.1)
        strategy.poll()

    # The strategy keeps trying to submit the same message
    # since it's continually rejected
    assert next_step.submit.call_count > 1

    # clear the side effect, let the message through now
    next_step.submit.reset_mock(side_effect=True)

    for _ in range(5):
        time.sleep(0.1)
        strategy.poll()

    # The message should have been submitted successfully now
    assert next_step.submit.call_count == 1

    strategy.close()
    strategy.join()
    # still 1, no more messages to submit
    assert next_step.submit.call_count == 1
