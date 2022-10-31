import time
from datetime import datetime
from typing import Any, Mapping, Sequence
from unittest.mock import Mock, call, patch

import pytest

from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.collect import (
    BatchBuilder,
    BatchStep,
    MessageBatch,
    OffsetRange,
    UnbatchStep,
)
from arroyo.types import Message, Partition, Topic


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

    batch = batch_builder.flush()
    assert len(batch) == len(messages_in)
    assert batch.offsets == expected_offsets
    assert batch.duration == flush - start


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
                        duration=0.0,
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
                        duration=0.0,
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
                        duration=0.0,
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
            duration=0.0,
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
