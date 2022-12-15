import time
from datetime import datetime
from typing import Any, Mapping, Sequence
from unittest.mock import Mock, call, patch

import pytest

from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.batching import BatchStep, UnbatchStep, ValuesBatch
from arroyo.processing.strategies.reduce import BatchBuilder
from arroyo.processing.strategies.transform import TransformStep
from arroyo.types import BaseValue, BrokerValue, Message, Partition, Topic, Value


def broker_value(partition: int, offset: int, payload: str) -> BrokerValue[str]:
    return BrokerValue(
        partition=Partition(topic=Topic("test"), index=partition),
        offset=offset,
        payload=payload,
        timestamp=datetime(2022, 1, 1, 0, 0, 1),
    )


def value(committable: Mapping[Partition, int], payload: str) -> Value[str]:
    return Value(payload=payload, committable=committable)


test_builder = [
    pytest.param(
        [
            broker_value(0, 1, "Message 1"),
            broker_value(0, 2, "Message 2"),
            broker_value(0, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 2),
        {},
        False,
        id="partially full batch",
    ),
    pytest.param(
        [
            broker_value(0, 1, "Message 1"),
            broker_value(0, 2, "Message 2"),
            broker_value(0, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 12),
        {Partition(Topic("test"), 0): 4},
        True,
        id="Batch closed by time",
    ),
    pytest.param(
        [
            broker_value(0, 1, "Message 1"),
            broker_value(0, 2, "Message 2"),
            broker_value(0, 3, "Message 3"),
            broker_value(0, 4, "Message 2"),
            broker_value(0, 5, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 3),
        {Partition(Topic("test"), 0): 6},
        True,
        id="Batch closed by size",
    ),
    pytest.param(
        [
            broker_value(0, 1, "Message 1"),
            broker_value(1, 2, "Message 2"),
            broker_value(2, 3, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 12),
        {
            Partition(Topic("test"), 0): 2,
            Partition(Topic("test"), 1): 3,
            Partition(Topic("test"), 2): 4,
        },
        True,
        id="Messages on multiple partitions",
    ),
    pytest.param(
        [
            value(
                payload="test",
                committable={
                    Partition(Topic("test"), 0): 2,
                    Partition(Topic("test"), 1): 3,
                    Partition(Topic("test"), 2): 4,
                },
            ),
            broker_value(2, 6, "Message 3"),
        ],
        datetime(2022, 1, 1, 0, 0, 1),
        datetime(2022, 1, 1, 0, 0, 12),
        {
            Partition(Topic("test"), 0): 2,
            Partition(Topic("test"), 1): 3,
            Partition(Topic("test"), 2): 7,
        },
        True,
        id="Messages with multiple offsets to commit",
    ),
]


@pytest.mark.parametrize(
    "values_in, time_create, time_end, expected_offsets, expected_ready", test_builder
)
@patch("time.time")
def test_batch_builder(
    mock_time: Any,
    values_in: Sequence[BaseValue[str]],
    time_create: datetime,
    time_end: datetime,
    expected_offsets: Mapping[Partition, int],
    expected_ready: bool,
) -> None:
    start = time.mktime(time_create.timetuple())
    mock_time.return_value = start

    def accumulator(
        result: ValuesBatch[str], value: BaseValue[str]
    ) -> ValuesBatch[str]:
        result.append(value)
        return result

    batch_builder: BatchBuilder[str, ValuesBatch[str]] = BatchBuilder(
        accumulator, [], 5, 10.0
    )
    for m in values_in:
        batch_builder.append(m)

    flush = time.mktime(time_end.timetuple())
    mock_time.return_value = flush
    batch = batch_builder.build_if_ready()
    if expected_ready:
        assert batch is not None
        assert len(batch.payload) == len(values_in)
        assert batch.committable == expected_offsets
    else:
        assert batch is None


def message(partition: int, offset: int, payload: str) -> Message[str]:
    return Message(broker_value(partition=partition, offset=offset, payload=payload))


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
                    Value(
                        payload=[
                            broker_value(0, 1, "Message 1"),
                            broker_value(0, 2, "Message 2"),
                            broker_value(0, 3, "Message 3"),
                        ],
                        committable={Partition(Topic("test"), 0): 4},
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
                    Value(
                        payload=[
                            broker_value(0, 1, "Message 1"),
                            broker_value(0, 2, "Message 2"),
                            broker_value(0, 3, "Message 3"),
                        ],
                        committable={Partition(Topic("test"), 0): 4},
                    ),
                )
            ),
            call(
                Message(
                    Value(
                        payload=[
                            broker_value(1, 1, "Message 1"),
                            broker_value(1, 2, "Message 2"),
                            broker_value(1, 3, "Message 3"),
                        ],
                        committable={Partition(Topic("test"), 1): 4},
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
    expected_batches: Sequence[ValuesBatch[str]],
) -> None:
    start = time.mktime(start_time.timetuple())
    mock_time.return_value = start
    next_step = Mock()
    batch_step = BatchStep[str](3, 10.0, next_step)
    for message in messages_in:
        batch_step.submit(message)
        batch_step.poll()

    if expected_batches:
        next_step.submit.assert_has_calls(expected_batches)
    else:
        next_step.submit.assert_not_called()


def test_batch_join() -> None:
    next_step = Mock()
    batch_step = BatchStep[str](5, 10.0, next_step)
    messages = [
        message(0, 1, "Message 1"),
        message(0, 2, "Message 2"),
    ]

    for msg in messages:
        batch_step.submit(msg)
        batch_step.poll()
        next_step.submit.assert_not_called()

    batch_step.join(None)
    next_step.submit.assert_has_calls(
        [
            call(
                Message(
                    Value(
                        payload=[
                            broker_value(0, 1, "Message 1"),
                            broker_value(0, 2, "Message 2"),
                        ],
                        committable={Partition(Topic("test"), 0): 3},
                    ),
                )
            )
        ]
    )


def test_unbatch_step() -> None:
    msg: Message[ValuesBatch[str]] = Message(
        Value(
            payload=[
                broker_value(1, 1, "Message 1"),
                broker_value(1, 2, "Message 2"),
                broker_value(1, 3, "Message 3"),
            ],
            committable={Partition(Topic("test"), 1): 4},
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

    next_step.submit.reset_mock(side_effect=True)
    unbatch_step = UnbatchStep[str](next_step)
    unbatch_step.submit(msg)
    unbatch_step.join(None)

    next_step.submit.assert_has_calls(
        [
            call(message(1, 1, "Message 1")),
            call(message(1, 2, "Message 2")),
            call(message(1, 3, "Message 3")),
        ]
    )


def test_batch_unbatch() -> None:
    """
    Tests a full end to end batch and unbatch pipeline.
    """

    def transformer(
        msg: Message[ValuesBatch[str]],
    ) -> ValuesBatch[str]:
        return [sub_msg.replace("Transformed") for sub_msg in msg.payload]

    final_step = Mock()
    next_step: TransformStep[ValuesBatch[str], ValuesBatch[str]] = TransformStep(
        function=transformer, next_step=UnbatchStep(final_step)
    )

    pipeline = BatchStep[str](max_batch_size=3, max_batch_time=10, next_step=next_step)

    input_msgs = [
        message(1, 1, "Message 1"),
        message(1, 2, "Message 2"),
        message(1, 3, "Message 3"),
    ]
    for m in input_msgs:
        pipeline.submit(m)
        final_step.submit.assert_not_called()
        pipeline.poll()

    final_step.submit.assert_has_calls(
        [
            call(message(1, 1, "Transformed")),
            call(message(1, 2, "Transformed")),
            call(message(1, 3, "Transformed")),
        ]
    )
