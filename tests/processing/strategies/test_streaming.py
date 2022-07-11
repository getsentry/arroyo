import itertools
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing.managers import SharedMemoryManager
from threading import Semaphore
from typing import Any, Iterator, Mapping, Optional
from unittest.mock import Mock, call

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidKafkaMessage,
    InvalidMessages,
)
from arroyo.processing.strategies.streaming.collect import (
    CollectStep,
    ParallelCollectStep,
)
from arroyo.processing.strategies.streaming.filter import FilterStep
from arroyo.processing.strategies.streaming.transform import (
    MessageBatch,
    ParallelTransformStep,
    TransformStep,
    ValueTooLarge,
    parallel_transform_worker_apply,
)
from arroyo.types import Message, Partition, Position, Topic
from tests.assertions import assert_changes, assert_does_not_change
from tests.metrics import Gauge as GaugeCall
from tests.metrics import TestingMetricsBackend
from tests.metrics import Timing as TimingCall


def test_filter() -> None:
    next_step = Mock()

    def test_function(message: Message[bool]) -> bool:
        return message.payload

    filter_step = FilterStep(test_function, next_step)

    fail_message = Message(Partition(Topic("topic"), 0), 0, False, datetime.now())

    with assert_does_not_change(lambda: int(next_step.submit.call_count), 0):
        filter_step.submit(fail_message)

    pass_message = Message(Partition(Topic("topic"), 0), 0, True, datetime.now())

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        filter_step.submit(pass_message)

    assert next_step.submit.call_args == call(pass_message)

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        filter_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        filter_step.join()


def test_transform() -> None:
    next_step = Mock()

    def transform_function(message: Message[int]) -> int:
        return message.payload * 2

    transform_step = TransformStep(transform_function, next_step)

    original_message = Message(Partition(Topic("topic"), 0), 0, 1, datetime.now())

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_message)

    assert next_step.submit.call_args == call(
        Message(
            original_message.partition,
            original_message.offset,
            transform_function(original_message),
            original_message.timestamp,
        )
    )

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        transform_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        transform_step.join()


def message_generator(
    partition: Partition, starting_offset: int = 0
) -> Iterator[Message[int]]:
    for i in itertools.count(starting_offset):
        yield Message(partition, i, i, datetime.now())


@pytest.mark.parametrize("parallel", [0, 1])
def test_collect(parallel: int) -> None:
    step_factory = Mock()
    step_factory.return_value = inner_step = Mock()

    commit_function = Mock()
    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = (
        ParallelCollectStep(step_factory, commit_function, 2, 60)
        if parallel
        else CollectStep(step_factory, commit_function, 2, 60)
    )

    # A batch should be started the first time the step receives a message.
    with assert_changes(lambda: step_factory.call_count, 0, 1):
        collect_step.poll()
        collect_step.submit(next(messages))  # offset 0

    # Subsequent messages should reuse the existing batch, ...
    with assert_does_not_change(lambda: step_factory.call_count, 1):
        second_message = next(messages)
        collect_step.poll()
        collect_step.submit(second_message)  # offset 1

    # ...until we hit the batch size limit.
    with assert_changes(lambda: int(inner_step.close.call_count), 0, 1), assert_changes(
        lambda: int(inner_step.join.call_count), 0, 1
    ), assert_changes(lambda: commit_function.call_count, 0, 1):
        collect_step.poll()
        # Give the threadpool some time to do processing
        time.sleep(1) if parallel else None
        assert commit_function.call_args == call(
            {partition: Position(2, second_message.timestamp)}
        )

    step_factory.return_value = inner_step = Mock()

    # The next message should create a new batch.
    with assert_changes(lambda: step_factory.call_count, 1, 2):
        collect_step.submit(next(messages))

    with assert_changes(lambda: int(inner_step.close.call_count), 0, 1):
        collect_step.close()

    with assert_changes(lambda: int(inner_step.join.call_count), 0, 1), assert_changes(
        lambda: commit_function.call_count, 1, 2
    ):
        collect_step.join()


class WaitProcessingStep(ProcessingStrategy[int]):
    """
    ProcessingStep implementation that acquires a lock when join is called to mimic a long wait.
    """

    def __init__(self, semaphore: Semaphore):
        self.semaphore = semaphore

    def submit(self, message: Message[int]) -> None:
        pass

    def poll(self) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        self.semaphore.acquire()


def test_parallel_collect_blocks_when_previous_batch_still_running() -> None:
    """
    This test verifies that for parallel collect while the batch being processed by the threadpool is still running,
    the newer batch will have to wait for the previous batch to finish.
    """

    def create_step_factory() -> ProcessingStrategy[int]:
        return WaitProcessingStep(test_semaphore)

    test_semaphore = Semaphore()

    threadpool = ThreadPoolExecutor(max_workers=1)

    commit_function = Mock()
    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = ParallelCollectStep(create_step_factory, commit_function, 1, 60)
    collect_step.submit(next(messages))
    # Semaphore will be acquired here.
    collect_step.poll()

    collect_step.submit(next(messages))
    # The following call to poll will block. The poll will check that the batch has reached size 1
    # and will try to close the existing batch. But since the future of CollectStep has the semaphore
    # it will block.
    future = threadpool.submit(collect_step.poll)

    time.sleep(1)
    # CollectStep's future cannot be completed until the semaphore is released.
    assert collect_step.future.done() is False if collect_step.future else True

    # Release the semaphore.
    test_semaphore.release()
    time.sleep(1)

    # CollectStep's future should now be completed.
    assert collect_step.future.done() is True if collect_step.future else True

    # The next call to poll should have succeeded by now since the previous future is complete.
    assert future.done() is True


def test_parallel_collect_fills_new_batch_when_previous_batch_still_running() -> None:
    """
    This test verifies that parallel collect continues to fill in new batch while the batch in threadpool is
    still running.
    """

    def create_step_factory() -> ProcessingStrategy[int]:
        return WaitProcessingStep(test_semaphore)

    test_semaphore = Semaphore()

    commit_function = Mock()
    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    # Batch size of 2
    collect_step = ParallelCollectStep(create_step_factory, commit_function, 2, 60)

    # Batch 1: Send 1st message
    collect_step.submit(next(messages))
    collect_step.poll()

    # Batch 1: Send 2nd message will will close the batch
    collect_step.submit(next(messages))
    assert len(collect_step.batch) == 2 if collect_step.batch else False
    prev_batch = collect_step.batch
    # This poll will close the batch and will cause the Semaphore lock to be acquired.
    collect_step.poll()

    # Batch 2: Send 1st message
    collect_step.submit(next(messages))
    assert collect_step.batch != prev_batch
    assert len(collect_step.batch) == 1 if collect_step.batch else False
    collect_step.poll()

    # Batch 2: Send 2nd message which will close the batch
    collect_step.submit(next(messages))
    assert len(collect_step.batch) == 2 if collect_step.batch else False
    prev_batch = collect_step.batch
    # Release the semaphore to indicate that previous batch has completed.
    test_semaphore.release()
    collect_step.poll()

    # Batch 3: Send 1st message
    collect_step.submit(next(messages))
    assert collect_step.batch != prev_batch


class ByPassProcessingStep(ProcessingStrategy[int]):
    """
    ProcessingStep implementation that does nothing
    """

    def submit(self, message: Message[int]) -> None:
        pass

    def poll(self) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass


def test_parallel_collect_throws_exception_when_commit_fails_for_previous_batch() -> None:
    """
    Test that when the commit fails for a previous batch, the exception thrown from
    the future makes the collect step throw an exception.
    """

    def commit_function(commit_map: Mapping[Partition, Position]) -> None:
        raise Exception

    def create_step_factory() -> ProcessingStrategy[int]:
        return ByPassProcessingStep()

    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = ParallelCollectStep(create_step_factory, commit_function, 1, 60)
    collect_step.submit(next(messages))
    # This step will close the batch and let threadpool finish it.
    collect_step.poll()

    collect_step.submit(next(messages))
    with pytest.raises(Exception):
        # This step will close the new batch. While doing so it will check the status of the
        # future. The future should throw an exception since commit_function will raise one.
        # The exception should be raised now.
        collect_step.poll()


def test_message_batch() -> None:
    partition = Partition(Topic("test"), 0)

    smm = SharedMemoryManager()
    smm.start()

    block = smm.SharedMemory(16384)
    assert block.size == 16384

    message = Message(
        partition, 0, KafkaPayload(None, b"\x00" * 16000, []), datetime.now()
    )

    batch: MessageBatch[KafkaPayload] = MessageBatch(block)
    with assert_changes(lambda: len(batch), 0, 1):
        batch.append(message)

    assert batch[0] == message
    assert list(batch) == [message]

    with assert_does_not_change(lambda: len(batch), 1), pytest.raises(ValueTooLarge):
        batch.append(message)

    smm.shutdown()


def transform_payload_expand(message: Message[KafkaPayload]) -> KafkaPayload:
    return KafkaPayload(
        message.payload.key,
        message.payload.value * 2,
        message.payload.headers,
    )


def test_parallel_transform_worker_apply() -> None:
    messages = [
        Message(
            Partition(Topic("test"), 0),
            i,
            KafkaPayload(None, b"\x00" * size, []),
            datetime.now(),
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

    result = parallel_transform_worker_apply(
        transform_payload_expand,
        input_batch,
        output_block,
    )

    # The first batch should be able to fit 2 messages.
    assert result.next_index_to_process == 2
    assert len(result.valid_messages_transformed) == 2

    result = parallel_transform_worker_apply(
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
        parallel_transform_worker_apply(
            transform_payload_expand,
            input_batch,
            output_block,
            result.next_index_to_process,
        )
    smm.shutdown()


NO_KEY = "No Key"


def fail_bad_messages(message: Message[KafkaPayload]) -> KafkaPayload:
    if message.payload.key is None:
        raise InvalidMessages(
            [
                InvalidKafkaMessage(
                    payload=str(message.payload),
                    timestamp=message.timestamp,
                    topic=message.partition.topic.name,
                    consumer_group="",
                    partition=message.partition.index,
                    offset=message.offset,
                    headers=message.payload.headers,
                    reason=NO_KEY,
                )
            ]
        )
    return message.payload


def test_parallel_transform_worker_bad_messages() -> None:
    smm = SharedMemoryManager()
    smm.start()
    input_block = smm.SharedMemory(128)
    output_block = smm.SharedMemory(128)

    # every other message has a key
    messages = [
        Message(
            Partition(Topic("test"), 0),
            i,
            KafkaPayload(None if i % 2 == 0 else b"key", b"\x00", []),
            datetime.now(),
        )
        for i in range(9)
    ]

    input_batch = MessageBatch[Any](input_block)
    for message in messages:
        input_batch.append(message)
    assert len(input_batch) == 9
    # process entire batch
    result = parallel_transform_worker_apply(
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
    result = parallel_transform_worker_apply(
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
            Partition(Topic("test"), 0),
            i,
            KafkaPayload(None, b"\x00" * size, []),
            datetime.now(),
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
        transform_step = ParallelTransformStep(
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


def test_parallel_transform_step_terminate_workers() -> None:
    next_step = Mock()

    starting_processes = get_subprocess_count()
    worker_processes = 2
    manager_processes = 1

    with assert_changes(
        get_subprocess_count,
        starting_processes,
        starting_processes + worker_processes + manager_processes,
    ):
        transform_step = ParallelTransformStep(
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


def test_parallel_transform_step_bad_messages() -> None:
    next_step = Mock()

    starting_processes = get_subprocess_count()
    worker_processes = 5
    manager_processes = 1

    # every other message has a key
    messages = [
        Message(
            Partition(Topic("test"), 0),
            0,
            KafkaPayload(None if i % 2 == 0 else b"key", b"\x00", []),
            datetime.now(),
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
        transform_step = ParallelTransformStep(
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
    assert isinstance(invalid_message, InvalidKafkaMessage)
    assert invalid_message.reason == NO_KEY
    assert invalid_message.payload == str(messages[0].payload)
    # The 4 good ones should not have been blocked
    assert next_step.submit.call_count == 4
