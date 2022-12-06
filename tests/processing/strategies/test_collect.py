import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Semaphore
from typing import Iterator, Optional
from unittest.mock import ANY, Mock, call

import pytest

from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.collect import CollectStep, ParallelCollectStep
from arroyo.types import BrokerValue, Message, Partition, Position, Topic, Value
from tests.assertions import assert_changes, assert_does_not_change


def message_generator(
    partition: Partition, starting_offset: int = 0
) -> Iterator[Message[int]]:
    for i in itertools.count(starting_offset):
        yield Message(BrokerValue(i, partition, i, datetime.utcnow()))


@pytest.mark.parametrize("parallel", [0, 1])
def test_collect(parallel: int) -> None:
    step_factory = Mock()
    step_factory.return_value = inner_step = Mock()

    next_step = Mock()
    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = (
        ParallelCollectStep(step_factory, next_step, 2, 60)
        if parallel
        else CollectStep(step_factory, next_step, 2, 60)
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
    ), assert_changes(lambda: next_step.submit.call_count, 0, 1):
        collect_step.poll()
        # Give the threadpool some time to do processing
        time.sleep(1) if parallel else None

        assert next_step.submit.call_args == call(
            Message(Value(ANY, {partition: Position(2, ANY)}))
        )

    step_factory.return_value = inner_step = Mock()

    # The next message should create a new batch.
    with assert_changes(lambda: step_factory.call_count, 1, 2):
        collect_step.submit(next(messages))

    with assert_changes(lambda: int(inner_step.close.call_count), 0, 1):
        collect_step.close()

    with assert_changes(lambda: int(inner_step.join.call_count), 0, 1), assert_changes(
        lambda: next_step.submit.call_count, 1, 2
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

    next_step = Mock()
    next_step.submit = Mock(side_effect=Exception())

    def create_step_factory() -> ProcessingStrategy[int]:
        return ByPassProcessingStep()

    partition = Partition(Topic("topic"), 0)
    messages = message_generator(partition, 0)

    collect_step = ParallelCollectStep(create_step_factory, next_step, 1, 60)
    collect_step.submit(next(messages))
    # This step will close the batch and let threadpool finish it.
    collect_step.poll()

    collect_step.submit(next(messages))
    with pytest.raises(Exception):
        # This step will close the new batch. While doing so it will check the status of the
        # future. The future should throw an exception since next_step.submit will raise one.
        # The exception should be raised now.
        collect_step.poll()
