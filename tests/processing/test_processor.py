import time
from datetime import datetime
from typing import Any, Optional
from unittest import mock

import pytest

from arroyo.dlq import DlqPolicy, InvalidMessage
from arroyo.processing.processor import InvalidStateError, StreamProcessor
from arroyo.processing.strategies import Healthcheck
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import BrokerValue, Commit, Message, Partition, Topic
from tests.assertions import assert_changes, assert_does_not_change
from tests.metrics import Increment, TestingMetricsBackend, Timing


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
    consumer.tell.return_value = {}
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

        with mock.patch("time.time", return_value=time.time() + 5):
            # time.sleep(5)
            processor._run_once()  # Should pause now

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

    # Revocation should noop without an active assignment.
    revocation_callback([Partition(topic, 0)])
    revocation_callback([Partition(topic, 0)])

    # The processor should not accept non-heartbeat messages without an
    # assignment or active processor.
    consumer.poll.return_value = message.value
    with pytest.raises(InvalidStateError):
        processor._run_once()

    with assert_changes(lambda: int(consumer.close.call_count), 0, 1), assert_changes(
        lambda: int(factory.shutdown.call_count), 0, 1
    ):
        processor._shutdown()

    assert list((type(call), call.name) for call in metrics.calls) == [
        (Increment, "arroyo.consumer.partitions_assigned.count"),
        (Timing, "arroyo.consumer.run.create_strategy"),
        (Timing, "arroyo.consumer.run.callback"),
        (Timing, "arroyo.consumer.poll.time"),
        (Timing, "arroyo.consumer.callback.time"),
        (Timing, "arroyo.consumer.processing.time"),
        (Increment, "arroyo.consumer.run.count"),
        (Increment, "arroyo.consumer.partitions_assigned.count"),
        (Timing, "arroyo.consumer.run.close_strategy"),
        (Timing, "arroyo.consumer.run.create_strategy"),
        (Timing, "arroyo.consumer.run.callback"),
        (Increment, "arroyo.consumer.partitions_revoked.count"),
        (Timing, "arroyo.consumer.run.close_strategy"),
        (Timing, "arroyo.consumer.run.callback"),
        (Increment, "arroyo.consumer.partitions_revoked.count"),
        (Timing, "arroyo.consumer.run.callback"),
        (Increment, "arroyo.consumer.partitions_revoked.count"),
        (Timing, "arroyo.consumer.run.callback"),
        (Timing, "arroyo.consumer.processing.time"),
        (Timing, "arroyo.consumer.backpressure.time"),
        (Timing, "arroyo.consumer.join.time"),
        (Timing, "arroyo.consumer.shutdown.time"),
        (Timing, "arroyo.consumer.callback.time"),
        (Timing, "arroyo.consumer.poll.time"),
        (Increment, "arroyo.consumer.pause"),
        (Increment, "arroyo.consumer.run.count"),
        (Increment, "arroyo.consumer.resume"),
    ]


def test_stream_processor_termination_on_error() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    partition = Partition(topic, 0)
    offset = 0
    now = datetime.now()

    consumer.tell.return_value = {}
    consumer.poll.return_value = BrokerValue(0, partition, offset, now)

    exception = NotImplementedError("error")

    strategy = mock.Mock()
    strategy.submit.side_effect = exception

    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    with pytest.raises(Exception) as e, assert_changes(
        lambda: int(strategy.terminate.call_count), 0, 1
    ), assert_changes(lambda: int(consumer.close.call_count), 0, 1):
        processor.run()

    assert e.value == exception


def test_stream_processor_invalid_message_from_poll() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    partition = Partition(topic, 0)
    offset = 1
    now = datetime.now()

    consumer.tell.return_value = {}
    consumer.poll.side_effect = [BrokerValue(0, partition, offset, now)]

    strategy = mock.Mock()
    strategy.poll.side_effect = [
        InvalidMessage(partition, 0, needs_commit=False),
        None,
    ]

    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    # We need to ensure that poll() is called again after it raises
    # InvalidMessage the first time. This gives the strategy time to commit
    # offsets for that message before the next one is submitted.
    processor._run_once()
    assert strategy.poll.call_count == 1

    processor._run_once()
    assert strategy.poll.call_count == 2
    assert strategy.submit.call_count == 1


def test_stream_processor_invalid_message_from_submit() -> None:
    topic = Topic("test")

    consumer = mock.Mock()
    partition = Partition(topic, 0)
    offset = 1
    now = datetime.now()

    consumer.tell.return_value = {}
    consumer.poll.side_effect = [
        BrokerValue(0, partition, offset, now),
        BrokerValue(1, partition, offset + 1, now),
    ]

    strategy = mock.Mock()
    strategy.submit.side_effect = [
        InvalidMessage(partition, 0, needs_commit=False),
        None,
        None,
    ]

    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    assignment_callback = consumer.subscribe.call_args.kwargs["on_assign"]
    assignment_callback({Partition(topic, 0): 0})

    processor._run_once()
    assert strategy.poll.call_count == 1
    assert consumer.pause.call_count == 0
    assert strategy.submit.call_count == 1

    processor._run_once()
    assert strategy.submit.call_count == 2
    assert strategy.submit.call_args_list == [
        mock.call(Message(BrokerValue(0, partition, offset, now))),
        mock.call(Message(BrokerValue(0, partition, offset, now))),
    ]

    processor._run_once()
    assert strategy.submit.call_count == 3
    assert strategy.submit.call_args_list == [
        mock.call(Message(BrokerValue(0, partition, offset, now))),
        mock.call(Message(BrokerValue(0, partition, offset, now))),
        mock.call(Message(BrokerValue(1, partition, offset + 1, now))),
    ]


def test_stream_processor_create_with_partitions() -> None:
    topic = Topic("topic")

    consumer = mock.Mock()
    consumer.tell.return_value = {}
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

    consumer.tell.return_value = {**offsets_p0}

    # Second partition assigned
    offsets_p1 = {Partition(topic, 1): 0}
    assignment_callback(offsets_p1)

    create_args, _ = factory.create_with_partitions.call_args
    assert factory.create_with_partitions.call_count == 2
    assert create_args[1] == {**offsets_p1, **offsets_p0}

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
        self.__commit(message.committable)

    def poll(self) -> None:
        pass

    def close(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def terminate(self) -> None:
        pass


def test_dlq() -> None:
    topic = Topic("topic")
    partition = Partition(topic, 0)
    consumer = mock.Mock()
    consumer.poll.return_value = BrokerValue(0, partition, 1, datetime.now())
    consumer.tell.return_value = {}
    strategy = mock.Mock()
    strategy.submit.side_effect = InvalidMessage(partition, 1)
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    dlq_policy: Any = DlqPolicy(producer=mock.Mock())

    processor: StreamProcessor[int] = StreamProcessor(
        consumer, topic, factory, dlq_policy
    )

    # Assignment
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    processor._run_once()

    assert dlq_policy.producer.produce.call_count == 1


def test_healthcheck(tmp_path: Any) -> None:
    """
    Test healthcheck strategy e2e with StreamProcessor, to ensure the
    combination of both actually touches the file often enough.
    """

    topic = Topic("topic")
    partition = Partition(topic, 0)
    consumer = mock.Mock()
    now = datetime.now()
    consumer.poll.return_value = BrokerValue(0, partition, 1, now)
    consumer.tell.return_value = {}
    strategy = mock.Mock()
    strategy.submit.side_effect = InvalidMessage(partition, 1)
    factory = mock.Mock()
    factory.create_with_partitions.return_value = Healthcheck(
        healthcheck_file=str(tmp_path / "health.txt"), next_step=strategy
    )

    processor: StreamProcessor[int] = StreamProcessor(
        consumer,
        topic,
        factory,
    )

    # Assignment
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {Partition(topic, 0): 0}
    assignment_callback(offsets)

    processor._run_once()
    health_mtime = (tmp_path / "health.txt").stat().st_mtime
    assert health_mtime < time.time() + 1

    processor._run_once()
    assert (tmp_path / "health.txt").stat().st_mtime == health_mtime


def test_processor_pause_with_invalid_message() -> None:

    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(consumer, topic, factory)

    # Subscribe to topic
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    # Partition assignment
    partition = Partition(topic, 0)
    consumer.tell.return_value = {}
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {partition: 0}
    assignment_callback(offsets)

    # Message that we will get from polling
    message = Message(BrokerValue(0, partition, 0, datetime.now()))

    # Message will be rejected
    consumer.poll.return_value = message.value
    strategy.submit.side_effect = MessageRejected()
    with assert_changes(lambda: int(consumer.pause.call_count), 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

        with mock.patch("time.time", return_value=time.time() + 5):
            processor._run_once()  # Should pause now

    # Consumer is in paused state
    # The same rejected message should be carried over

    # All partitions are paused
    consumer.paused.return_value = set(p for p in offsets)
    # Simulate a continuous backpressure state where messages are being rejected
    strategy.submit.side_effect = MessageRejected()

    # Simulate Kafka returning nothing since the consumer is paused
    consumer.poll.return_value = None

    # The next poll returns nothing, but we are still carrying over the rejected message
    processor._run_once()
    assert consumer.poll.return_value is None

    # At this point, let's say the message carried over is invalid (e.g. it could be stale)
    strategy.submit.side_effect = InvalidMessage(partition, 0, needs_commit=False)

    # Handles the invalid message and unpauses the consumer
    with assert_changes(lambda: int(consumer.resume.call_count), 0, 1):
        processor._run_once()

    # Poll for the next message from Kafka
    new_message = Message(BrokerValue(0, partition, 1, datetime.now()))
    consumer.poll.return_value = new_message.value
    strategy.submit.return_value = None
    strategy.submit.side_effect = None

    processor._run_once()
    assert strategy.submit.call_args_list[-1] == mock.call(new_message)


def test_processor_poll_while_paused() -> None:

    topic = Topic("topic")

    consumer = mock.Mock()
    strategy = mock.Mock()
    factory = mock.Mock()
    factory.create_with_partitions.return_value = strategy

    processor: StreamProcessor[int] = StreamProcessor(
        consumer, topic, factory, handle_poll_while_paused=True
    )

    # Subscribe to topic
    subscribe_args, subscribe_kwargs = consumer.subscribe.call_args
    assert subscribe_args[0] == [topic]

    # Partition assignment
    partition = Partition(topic, 0)
    new_partition = Partition(topic, 1)
    consumer.tell.return_value = {}
    assignment_callback = subscribe_kwargs["on_assign"]
    offsets = {partition: 0}
    assignment_callback(offsets)

    # Message that we will get from polling
    message = Message(BrokerValue(0, partition, 0, datetime.now()))

    # Message will be rejected
    consumer.poll.return_value = message.value
    strategy.submit.side_effect = MessageRejected()
    with assert_changes(lambda: int(consumer.pause.call_count), 0, 1):
        processor._run_once()
        assert strategy.submit.call_args_list[-1] == mock.call(message)

        with mock.patch("time.time", return_value=time.time() + 5):
            processor._run_once()  # Should pause now

    # Consumer is in paused state
    # The same rejected message should be carried over

    # All partitions are paused
    consumer.paused.return_value = set(p for p in offsets)
    # Simulate a continuous backpressure state where messages are being rejected
    strategy.submit.side_effect = MessageRejected()

    # Simulate Kafka returning nothing since the consumer is paused
    consumer.poll.return_value = None

    # The next poll returns nothing, but we are still carrying over the rejected message
    processor._run_once()
    assert consumer.poll.return_value is None

    # At this point, let's say the message carried over is invalid (e.g. it could be stale)
    strategy.submit.side_effect = InvalidMessage(partition, 0, needs_commit=False)

    # Handles the invalid message and unpauses the consumer
    with assert_changes(lambda: int(consumer.resume.call_count), 0, 1):
        processor._run_once()

    # Poll for the next message from Kafka, but this time the partition has changed
    new_message = Message(BrokerValue(0, new_partition, 1, datetime.now()))
    consumer.poll.return_value = new_message.value
    processor._run_once()
    assert processor._StreamProcessor__is_paused is False  # type:ignore

    strategy.submit.return_value = None
    strategy.submit.side_effect = None

    processor._run_once()
    assert strategy.submit.call_args_list[-1] == mock.call(new_message)
