import time
from datetime import datetime
from typing import Any, Mapping, MutableSequence, Sequence
from unittest.mock import Mock, call, patch

import pytest

from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.backend import LocalConsumer
from arroyo.commit import IMMEDIATE
from arroyo.processing.processor import StreamProcessor
from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.batching import (
    AbstractBatchWorker,
    BatchBuilder,
    BatchProcessingStrategyFactory,
    BatchStep,
    MessageBatch,
    UnbatchStep,
)
from arroyo.types import Message, OffsetRange, Partition, Topic


class FakeWorker(AbstractBatchWorker[int, int]):
    def __init__(self) -> None:
        self.processed: MutableSequence[int] = []
        self.flushed: MutableSequence[Sequence[int]] = []

    def process_message(self, message: Message[int]) -> int:
        self.processed.append(message.payload)
        return message.payload

    def flush_batch(self, batch: Sequence[int]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self, broker: Broker[int]) -> None:
        topic = Topic("topic")
        broker.create_topic(topic, partitions=1)
        producer = broker.get_producer()
        for i in [1, 2, 3]:
            producer.produce(topic, i).result()

        consumer = broker.get_consumer("group")
        assert isinstance(consumer, LocalConsumer)

        worker = FakeWorker()
        batching_consumer = StreamProcessor(
            consumer,
            topic,
            BatchProcessingStrategyFactory(
                worker=worker,
                max_batch_size=2,
                max_batch_time=100,
            ),
            IMMEDIATE,
        )

        for _ in range(3):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3]
        assert worker.flushed == [[1, 2]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1

    @patch("time.time")
    def test_batch_time(self, mock_time: Any, broker: Broker[int]) -> None:
        topic = Topic("topic")
        broker.create_topic(topic, partitions=1)
        producer = broker.get_producer()
        consumer = broker.get_consumer("group")
        assert isinstance(consumer, LocalConsumer)

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        worker = FakeWorker()
        batching_consumer = StreamProcessor(
            consumer,
            topic,
            BatchProcessingStrategyFactory(
                worker=worker, max_batch_size=100, max_batch_time=2000
            ),
            IMMEDIATE,
        )

        for i in [1, 2, 3]:
            producer.produce(topic, i).result()

        for _ in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())

        for i in [4, 5, 6]:
            producer.produce(topic, i).result()

        for _ in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())

        for i in [7, 8, 9]:
            producer.produce(topic, i).result()

        for _ in range(3):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1


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
    batch = batch_builder.build_if_ready()
    if expected_ready:
        assert batch is not None
        assert len(batch.messages) == len(messages_in)
        assert batch.offsets == expected_offsets
    else:
        assert batch is None


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
    # The first submit should succeed. The step accumulates the
    # messages. The following one fails as messages are already
    # pending.
    unbatch_step.submit(msg)
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
