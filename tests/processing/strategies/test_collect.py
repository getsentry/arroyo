import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Semaphore
from typing import Any, Iterator, Mapping, Optional, Sequence
from unittest.mock import Mock, call, patch

import pytest

from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.collect import (
    BatchBuilder,
    BatchStep,
    CollectStep,
    MessageBatch,
    OffsetRange,
    ParallelCollectStep,
    UnbatchStep,
)
from arroyo.types import Message, Partition, Position, Topic
from tests.assertions import assert_changes, assert_does_not_change


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


def message(partition: int, offset: int, payload: str) -> Message[str]:
    return Message(
        partition=Partition(topic=Topic("test"), index=partition),
        offset=offset,
        payload=payload,
        timestamp=datetime(2022, 1, 1, 0, 0, 1),
    )


test_builder = [
    pytest.param(
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
            message(0, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 2),
        {Partition(Topic("test"), 0): OffsetRange(1, 4, datetime(2022, 1, 1, 0, 0, 1))},
        False,
        id="partially full batch",
    ),
    pytest.param(
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
            message(0, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 12),
        {Partition(Topic("test"), 0): OffsetRange(1, 4, datetime(2022, 1, 1, 0, 0, 1))},
        True,
        id="Batch closed by time",
    ),
    pytest.param(
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
            message(0, 3, "Message 3"),
            message(0, 4, "Message 2"),
            message(0, 5, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 3),
        {Partition(Topic("test"), 0): OffsetRange(1, 6, datetime(2022, 1, 1, 0, 0, 1))},
        True,
        id="Batch closed by size",
    ),
    pytest.param(
        [
            message(0, 1, "Message 1"),
            message(1, 2, "Message 2"),
            message(2, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 2),
        {
            Partition(Topic("test"), 0): OffsetRange(
                1, 2, datetime(2022, 1, 1, 0, 0, 1)
            ),
            Partition(Topic("test"), 1): OffsetRange(
                2, 3, datetime(2022, 1, 1, 0, 0, 1)
            ),
            Partition(Topic("test"), 2): OffsetRange(
                3, 4, datetime(2022, 1, 1, 0, 0, 1)
            ),
        },
        False,
        id="Messages on multiple partitions",
    ),
]


@pytest.mark.parametrize(
    "messages_in, time_create, time_end, expected_offsets, expected_ready", test_builder
)
@patch("time.time")
def test_batch_builder(
    mock_time: Any,
    messages_in: Sequence[Message[str]],
    time_create: datetime,
    time_end: datetime,
    expected_offsets: Mapping[Partition, OffsetRange],
    expected_ready: bool,
) -> None:
    start = time.mktime(time_create.timetuple())
    mock_time.return_value = start
    batch_builder = BatchBuilder[str](10.0, 5)
    for m in messages_in:
        batch_builder.append(m)

    flush = time.mktime(time_end.timetuple())
    mock_time.return_value = flush
    assert batch_builder.is_ready() == expected_ready

    batch = batch_builder.build()
    assert len(batch) == len(messages_in)
    assert batch.offsets == expected_offsets


test_batch = [
    pytest.param(
        datetime(2022, 1, 1, 0, 0, 1),
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
        ],
        [],
        id="Half full batch",
    ),
    pytest.param(
        datetime(2022, 1, 1, 0, 0, 1),
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
            message(0, 3, "Message 3"),
        ],
        [
            call(
                Message(
                    partition=Partition(topic=Topic("test"), index=0),
                    offset=3,
                    timestamp=datetime(2022, 1, 1, 0, 0, 1),
                    payload=MessageBatch(
                        messages=[
                            message(0, 1, "Message 1"),
                            message(0, 2, "Message 2"),
                            message(0, 3, "Message 3"),
                        ],
                        offsets={
                            Partition(Topic("test"), 0): OffsetRange(
                                1, 4, datetime(2022, 1, 1, 0, 0, 1)
                            )
                        },
                    ),
                )
            )
        ],
        id="One full batch",
    ),
    pytest.param(
        datetime(2022, 1, 1, 0, 0, 1),
        [
            message(0, 1, "Message 1"),
            message(0, 2, "Message 2"),
            message(0, 3, "Message 3"),
            message(1, 1, "Message 1"),
            message(1, 2, "Message 2"),
            message(1, 3, "Message 3"),
        ],
        [
            call(
                Message(
                    partition=Partition(topic=Topic("test"), index=0),
                    offset=3,
                    timestamp=datetime(2022, 1, 1, 0, 0, 1),
                    payload=MessageBatch(
                        messages=[
                            message(0, 1, "Message 1"),
                            message(0, 2, "Message 2"),
                            message(0, 3, "Message 3"),
                        ],
                        offsets={
                            Partition(Topic("test"), 0): OffsetRange(
                                1, 4, datetime(2022, 1, 1, 0, 0, 1)
                            )
                        },
                    ),
                )
            ),
            call(
                Message(
                    partition=Partition(topic=Topic("test"), index=1),
                    offset=3,
                    timestamp=datetime(2022, 1, 1, 0, 0, 1),
                    payload=MessageBatch(
                        messages=[
                            message(1, 1, "Message 1"),
                            message(1, 2, "Message 2"),
                            message(1, 3, "Message 3"),
                        ],
                        offsets={
                            Partition(Topic("test"), 1): OffsetRange(
                                1, 4, datetime(2022, 1, 1, 0, 0, 1)
                            )
                        },
                    ),
                )
            ),
        ],
        id="Two full batches",
    ),
]


@pytest.mark.parametrize("start_time, messages_in, expected_batches", test_batch)
@patch("time.time")
def test_batch_step(
    mock_time: Any,
    start_time: datetime,
    messages_in: Sequence[Message[str]],
    expected_batches: Sequence[Message[MessageBatch[str]]],
) -> None:
    start = time.mktime(start_time.timetuple())
    mock_time.return_value = start
    next_step = Mock()
    batch_step = BatchStep[str](10.0, 3, next_step)
    for message in messages_in:
        batch_step.submit(message)
        batch_step.poll()

    if expected_batches:
        next_step.submit.assert_has_calls(expected_batches)
    else:
        next_step.submit.assert_not_called()


def test_unbatch_step() -> None:
    msg = Message(
        partition=Partition(topic=Topic("test"), index=1),
        offset=3,
        timestamp=datetime(2022, 1, 1, 0, 0, 1),
        payload=MessageBatch(
            messages=[
                message(1, 1, "Message 1"),
                message(1, 2, "Message 2"),
                message(1, 3, "Message 3"),
            ],
            offsets={
                Partition(Topic("test"), 1): OffsetRange(
                    1, 4, datetime(2022, 1, 1, 0, 0, 1)
                )
            },
        ),
    )

    next_step = Mock()
    unbatch_step = UnbatchStep[str](next_step)
    unbatch_step.submit(msg)
    next_step.submit.assert_has_calls(
        [
            call(message(1, 1, "Message 1")),
            call(message(1, 2, "Message 2")),
            call(message(1, 3, "Message 3")),
        ]
    )

    next_step.reset_mock(side_effect=True)
    next_step.submit.side_effect = MessageRejected()
    unbatch_step = UnbatchStep[str](next_step)
    with pytest.raises(MessageRejected):
        unbatch_step.submit(msg)

    next_step.submit.reset_mock(side_effect=True)

    unbatch_step.poll()
    next_step.submit.assert_has_calls(
        [
            call(message(1, 1, "Message 1")),
            call(message(1, 2, "Message 2")),
            call(message(1, 3, "Message 3")),
        ]
    )
