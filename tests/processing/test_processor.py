from datetime import datetime, timedelta
from typing import Any, Mapping, Optional, Sequence, cast
from unittest import mock

import pytest

from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.abstract import MessageStorage
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.commit import IMMEDIATE, CommitPolicy
from arroyo.processing.processor import InvalidStateError, StreamProcessor
from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import BrokerValue, Commit, Message, Partition, Topic
from arroyo.utils.clock import TestingClock
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
        processor: StreamProcessor[int] = StreamProcessor(
            consumer, topic, factory, IMMEDIATE
        )

    # The processor should accept heartbeat messages without an assignment or
    # active processor.
    consumer.poll.return_value = None
    processor._run_once()

    partition = Partition(topic, 0)
    offset = 0
    now = datetime.now()
    payload = 0

    message = Message(BrokerValue(payload, partition, offset, now))

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
    consumer.poll.return_value = message.value
    with assert_changes(lambda: int(strategy.poll.call_count), 1, 2), assert_changes(
        lambda: int(strategy.submit.call_count), 0, 1
    ):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

    # If the message is rejected by the processing strategy, the consumer
    # should be paused and the message should be held for later.
    consumer.tell.return_value = offsets
    consumer.poll.return_value = message.value
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
    consumer.poll.return_value = message.value
    with pytest.raises(InvalidStateError):
        processor._run_once()

    with assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor._shutdown()

    poll_metric, processing_metric, pause_metric = metrics.calls
    assert isinstance(poll_metric, Timing)
    assert poll_metric.name == "arroyo.consumer.poll.time"
    assert isinstance(processing_metric, Timing)
    assert processing_metric.name == "arroyo.consumer.processing.time"
    assert isinstance(pause_metric, Timing)
    assert pause_metric.name == "arroyo.consumer.paused.time"


def test_stream_processor_termination_on_error() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    partition = Partition(topic, 0)
    offset = 0
    now = datetime.now()

    consumer.poll.return_value = BrokerValue(0, partition, offset, now)

    exception = NotImplementedError("error")

    strategy = mock.Mock()
    strategy.submit.side_effect = exception

    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(
        consumer, topic, factory, IMMEDIATE
    )

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    with pytest.raises(Exception) as e, assert_changes(
        lambda: int(strategy.terminate.call_count), 0, 1
    ), assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor.run()

    assert e.value == exception


def test_stream_processor_create_with_partitions() -> None:
    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    with assert_changes(lambda: int(consumer.subscribe.call_count), 0, 1):
        processor: StreamProcessor[int] = StreamProcessor(
            consumer, topic, factory, IMMEDIATE
        )

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
                message.committable,
                force=True,
            )

        self.__commit(
            message.committable,
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


def run_commit_policy_test(
    topic: Topic, given_messages: Sequence[Message[int]], policy: CommitPolicy
) -> Sequence[int]:
    commit = mock.Mock()
    consumer = mock.Mock()
    consumer.commit_offsets = commit

    factory = CommitOffsetsFactory()

    processor: StreamProcessor[int] = StreamProcessor(
        consumer,
        topic,
        factory,
        policy,
    )

    # Assignment
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    assert commit.call_count == 0

    commit_calls = []

    for message in given_messages:
        consumer.poll.return_value = message.value
        message_timestamp = cast(BrokerValue[int], message.value).timestamp
        with mock.patch("time.time", return_value=message_timestamp.timestamp()):
            processor._run_once()
        commit_calls.append(commit.call_count)

    return commit_calls


def test_stream_processor_commit_policy() -> None:
    topic = Topic("topic")

    commit_every_second_message = CommitPolicy(None, 2)

    assert run_commit_policy_test(
        topic,
        [
            Message(BrokerValue(0, Partition(topic, 0), 0, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 0), 1, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 0), 2, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 0), 5, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 0), 10, datetime.now())),
        ],
        commit_every_second_message,
    ) == [
        # Does not commit first message
        0,
        # Does commit second message
        1,
        # Does not commit third message
        1,
        # Should always commit if we are committing more than 2 messages at once.
        2,
        3,
    ]


def test_stream_processor_commit_policy_multiple_partitions() -> None:
    topic = Topic("topic")

    commit_every_second_message = CommitPolicy(None, 2)

    assert run_commit_policy_test(
        topic,
        [
            Message(BrokerValue(0, Partition(topic, 0), 200, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 1), 400, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 0), 400, datetime.now())),
            Message(BrokerValue(0, Partition(topic, 1), 400, datetime.now())),
        ],
        commit_every_second_message,
    ) == [
        # Does not commit first message even if the offset is super large
        0,
        # Does not commit first message on other partition even if the offset is super large
        0,
        # Does commit second message on first partition since the offset delta is super large
        1,
        # Does not commit second message on second partition since the offset delta is zero
        1,
    ]


def test_stream_processor_commit_policy_always() -> None:
    topic = Topic("topic")

    assert run_commit_policy_test(
        topic,
        [Message(BrokerValue(0, Partition(topic, 0), 200, datetime.now()))],
        IMMEDIATE,
    ) == [
        # IMMEDIATE policy can commit on the first message (even
        # though there is no previous offset stored)
        #
        # Indirectly assert that an offset delta of 1 is passed to
        # the commit policy, not 0
        1
    ]


def test_stream_processor_commit_policy_every_two_seconds() -> None:
    topic = Topic("topic")
    commit_every_two_seconds = CommitPolicy(2, None)

    now = datetime.now()

    assert run_commit_policy_test(
        topic,
        [
            Message(BrokerValue(0, Partition(topic, 0), 0, now)),
            Message(BrokerValue(0, Partition(topic, 0), 1, now + timedelta(seconds=1))),
            Message(BrokerValue(0, Partition(topic, 0), 2, now + timedelta(seconds=2))),
            Message(BrokerValue(0, Partition(topic, 0), 3, now + timedelta(seconds=3))),
            Message(BrokerValue(0, Partition(topic, 0), 4, now + timedelta(seconds=4))),
            Message(BrokerValue(0, Partition(topic, 0), 5, now + timedelta(seconds=5))),
            Message(BrokerValue(0, Partition(topic, 0), 6, now + timedelta(seconds=6))),
        ],
        commit_every_two_seconds,
    ) == [
        0,
        0,
        0,
        1,
        1,
        2,
        2,
    ]


@pytest.mark.parametrize(
    "commit_seconds", [0, 1000], ids=("commit_always", "commit_never")
)
@pytest.mark.parametrize("num_messages", [1, 100, 1000])
def test_commit_policy_bench(
    benchmark: Any, commit_seconds: int, num_messages: int
) -> None:
    topic = Topic("topic")
    commit_policy = CommitPolicy(commit_seconds, None)
    num_partitions = 1
    now = datetime.now()

    storage: MessageStorage[int] = MemoryMessageStorage()
    storage.create_topic(topic, num_partitions)

    broker = LocalBroker(storage, TestingClock())

    consumer = broker.get_consumer("test-group", enable_end_of_partition=True)

    factory = CommitOffsetsFactory()

    processor: StreamProcessor[int] = StreamProcessor(
        consumer,
        topic,
        factory,
        commit_policy,
    )

    def inner() -> None:
        for i in range(num_messages):
            storage.produce(Partition(topic, i % num_partitions), i, now)

        for _ in range(num_messages):
            processor._run_once()

    benchmark(inner)
