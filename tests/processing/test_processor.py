from datetime import datetime
from typing import Mapping, Optional
from unittest import mock

import pytest

from arroyo.commit import CommitPolicy
from arroyo.processing.processor import InvalidStateError, StreamProcessor
from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Commit, Message, Partition, Position, Topic
from tests.assertions import assert_changes, assert_does_not_change
from tests.metrics import TestingMetricsBackend, Timing


def test_stream_processor_lifecycle() -> None:
    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    metrics = TestingMetricsBackend

    with assert_changes(lambda: int(consumer.subscribe.call_count), 0, 1):
        processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    # The processor should accept heartbeat messages without an assignment or
    # active processor.
    consumer.poll.return_value = None
    processor._run_once()

    message = Message(Partition(topic, 0), 0, 0, datetime.now())

    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    assignment_callback = subscribe_kwargs["on_assign"]
    revocation_callback = subscribe_kwargs["on_revoke"]

    # Assignment should succeed if no assignment already exists.
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    # If ``Consumer.poll`` doesn't return a message, we should poll the
    # processing strategy, but not submit anything for processing.
    consumer.poll.return_value = None
    with assert_changes(
        lambda: int(strategy.poll.call_count), 0, 1
    ), assert_does_not_change(lambda: int(strategy.submit.call_count), 0):
        processor._run_once()

    # If ``Consumer.poll`` **does** return a message, we should poll the
    # processing strategy and submit the message for processing.
    consumer.poll.return_value = message
    with assert_changes(lambda: int(strategy.poll.call_count), 1, 2), assert_changes(
        lambda: int(strategy.submit.call_count), 0, 1
    ):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

    # If the message is rejected by the processing strategy, the consumer
    # should be paused and the message should be held for later.
    consumer.tell.return_value = offsets
    consumer.poll.return_value = message
    strategy.submit.side_effect = MessageRejected()
    with assert_changes(lambda: int(consumer.pause.call_count), 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

    # If ``Consumer.poll`` returns a message when we expect it to be paused,
    # we should raise an exception.
    with pytest.raises(InvalidStateError):
        processor._run_once()

    # Once the message is accepted by the processing strategy, the consumer
    # should be resumed.
    consumer.poll.return_value = None
    strategy.submit.return_value = None
    strategy.submit.side_effect = None
    with assert_changes(lambda: int(consumer.resume.call_count), 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)
    metric = metrics.calls[0]
    assert isinstance(metric, Timing)
    assert metric.name == "pause_duration_ms"

    # Strategy should be closed and recreated if it already exists and
    # we got another partition assigned.
    with assert_changes(lambda: int(strategy.close.call_count), 0, 1):
        assignment_callback({Partition(topic, 0): 0})

    # Revocation should succeed with an active assignment, and cause the
    # strategy instance to be closed.
    consumer.tell.return_value = {}

    with assert_changes(lambda: int(strategy.close.call_count), 1, 2):
        revocation_callback([Partition(topic, 0)])

    # Revocation should fail without an active assignment.
    with pytest.raises(InvalidStateError):
        revocation_callback([Partition(topic, 0)])

    # The processor should not accept non-heartbeat messages without an
    # assignment or active processor.
    consumer.poll.return_value = message
    with pytest.raises(InvalidStateError):
        processor._run_once()

    with assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor._shutdown()


def test_stream_processor_termination_on_error() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    consumer.poll.return_value = Message(Partition(topic, 0), 0, 0, datetime.now())

    exception = NotImplementedError("error")

    strategy = mock.Mock()
    strategy.submit.side_effect = exception

    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(
        consumer,
        topic,
        factory,
    )

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    with pytest.raises(Exception) as e, assert_changes(
        lambda: int(strategy.terminate.call_count), 0, 1
    ), assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor.run()

    assert e.value == exception


def test_stream_processor_incremental_assignment_revocation() -> None:
    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    with assert_changes(lambda: int(consumer.subscribe.call_count), 0, 1):
        processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    assignment_callback = subscribe_kwargs["on_assign"]
    revocation_callback = subscribe_kwargs["on_revoke"]

    # First partition assigned
    offsets_p0 = {Partition(topic, 0): 0}
    assignment_callback(offsets_p0)

    # Second partition assigned
    offsets_p1 = {Partition(topic, 1): 0}
    assignment_callback(offsets_p1)

    processor._run_once()

    # First partition revoked
    consumer.tell.return_value = {**offsets_p0, **offsets_p1}
    revocation_callback([Partition(topic, 0)])

    # Second partition revoked
    consumer.tell.return_value = {**offsets_p1}
    revocation_callback([Partition(topic, 1)])

    # Attempting to re-revoke a partition fails
    consumer.tell.return_value = {}
    with pytest.raises(InvalidStateError):
        revocation_callback([Partition(topic, 1)])

    # No assigned partitions, processor won't run
    with pytest.raises(InvalidStateError):
        processor._run_once()

    # Shutdown
    with assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor._shutdown()


def test_stream_processor_create_with_partitions() -> None:
    """
    Similar to test_stream_processor_incremental_assignment_revocation
    but instead validates the partitions for the calls to
    `create_with_partitions`.
    """
    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    with assert_changes(lambda: int(consumer.subscribe.call_count), 0, 1):
        processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    assignment_callback = subscribe_kwargs["on_assign"]
    revocation_callback = subscribe_kwargs["on_revoke"]

    # First partition assigned
    offsets_p0 = {Partition(topic, 0): 0}
    assignment_callback(offsets_p0)

    create_args, _ = factory.create_with_partitions.call_args
    assert factory.create_with_partitions.call_count == 1
    assert create_args[1] == offsets_p0

    # Second partition assigned
    offsets_p1 = {Partition(topic, 1): 0}
    assignment_callback(offsets_p1)

    create_args, _ = factory.create_with_partitions.call_args
    assert factory.create_with_partitions.call_count == 2
    assert create_args[1] == offsets_p1

    processor._run_once()

    # First partition revoked
    consumer.tell.return_value = {**offsets_p0, **offsets_p1}
    revocation_callback([Partition(topic, 0)])

    create_args, _ = factory.create_with_partitions.call_args
    assert factory.create_with_partitions.call_count == 3
    assert create_args[1] == offsets_p1

    # Second partition revoked - no partitions left
    consumer.tell.return_value = {**offsets_p1}
    revocation_callback([Partition(topic, 1)])

    # No partitions left means we don't re-create the strategy
    # so `create_with_partitions` call count shouldn't increase
    assert factory.create_with_partitions.call_count == 3


class CommitOffsets(ProcessingStrategy[int]):
    def __init__(self, commit: Commit) -> None:
        self.__commit = commit

    def submit(self, message: Message[int]) -> None:
        # If we get a message with value of 1, force commit
        if message.payload == 1:
            self.__commit(
                {message.partition: Position(message.next_offset, message.timestamp)},
                force=True,
            )

        self.__commit(
            {message.partition: Position(message.next_offset, message.timestamp)}
        )

    def poll(self) -> None:
        pass

    def close(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def terminate(self) -> None:
        pass


class CommitOffsetsFactory(ProcessingStrategyFactory[int]):
    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[int]:
        return CommitOffsets(commit)


def test_stream_processor_commit_policy() -> None:
    topic = Topic("topic")
    commit = mock.Mock()
    consumer = mock.Mock()
    consumer.commit_positions = commit

    factory = CommitOffsetsFactory()

    commit_every_second_message = CommitPolicy(None, 2)

    processor: StreamProcessor[int] = StreamProcessor(
        consumer, topic, factory, commit_every_second_message
    )

    # Assignment
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    assert commit.call_count == 0

    # Does not commit first message
    message = Message(Partition(topic, 0), 0, 0, datetime.now())
    consumer.poll.return_value = message
    processor._run_once()
    assert commit.call_count == 0

    # Commits second message
    message = Message(Partition(topic, 0), 1, 0, datetime.now())
    consumer.poll.return_value = message
    processor._run_once()
    assert commit.call_count == 1

    # Test force flag
    message = Message(Partition(topic, 0), 1, 0, datetime.now())
    consumer.poll.return_value = message
    processor._run_once()
    assert commit.call_count == 1
