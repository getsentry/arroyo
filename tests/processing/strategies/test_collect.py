import time
from datetime import datetime
from typing import Any, Mapping, Sequence
from unittest.mock import patch

import pytest

from arroyo.processing.strategies.collect import BatchBuilder, OffsetRange
from arroyo.types import Message, Partition, Topic


def message(partition: int, offset: int, payload: str) -> Message[str]:
    return Message(
        partition=Partition(topic=Topic("test"), index=partition),
        offset=offset,
        payload=payload,
        timestamp=datetime(2022, 1, 1, 0, 0, 1),
    )


test_cases = [
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
    )
]


@pytest.mark.parametrize(
    "messages_in, time_create, time_end, expected_offsets, expected_ready", test_cases
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
