import time
from datetime import datetime
from typing import Any, Mapping, Sequence, cast
from unittest.mock import Mock, call, patch

import pytest

from arroyo.processing.strategies.abstract import MessageRejected
from arroyo.processing.strategies.batching import BatchStep, UnbatchStep, ValuesBatch
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import BrokerValue, Message, Partition, Topic, Value

NOW = datetime(2022, 1, 1, 0, 0, 1)


def broker_value(partition: int, offset: int, payload: str) -> BrokerValue[str]:
    return BrokerValue(
        partition=Partition(topic=Topic("test"), index=partition),
        offset=offset,
        payload=payload,
        timestamp=NOW,
    )


def value(committable: Mapping[Partition, int], payload: str) -> Value[str]:
    return Value(
        payload=payload,
        committable=committable,
        timestamp=NOW,
    )


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
                        timestamp=datetime(2022, 1, 1, 0, 0, 1),
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
                        timestamp=NOW,
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
                        timestamp=NOW,
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
                        timestamp=NOW,
                    ),
                )
            )
        ]
    )


def test_unbatch_step() -> None:
    msg: Message[ValuesBatch[str]] = Message(
        Value(
            payload=cast(
                ValuesBatch[str],
                [
                    broker_value(1, 1, "Message 1"),
                    broker_value(1, 2, "Message 2"),
                    broker_value(1, 3, "Message 3"),
                ],
            ),
            committable={Partition(Topic("test"), 1): 4},
            timestamp=NOW,
        ),
    )

    partition = Partition(Topic("test"), 1)

    next_step = Mock()
    unbatch_step = UnbatchStep[str](next_step)
    unbatch_step.submit(msg)
    next_step.submit.assert_has_calls(
        [
            call(Message(Value("Message 1", {}, NOW))),
            call(Message(Value("Message 2", {}, NOW))),
            call(Message(Value("Message 3", {partition: 4}, NOW))),
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
            call(Message(Value("Message 1", {}, NOW))),
            call(Message(Value("Message 2", {}, NOW))),
            call(Message(Value("Message 3", {partition: 4}, NOW))),
        ]
    )

    next_step.submit.reset_mock(side_effect=True)
    unbatch_step = UnbatchStep[str](next_step)
    unbatch_step.submit(msg)
    unbatch_step.join(None)

    next_step.submit.assert_has_calls(
        [
            call(Message(Value("Message 1", {}, NOW))),
            call(Message(Value("Message 2", {}, NOW))),
            call(Message(Value("Message 3", {partition: 4}, NOW))),
        ]
    )


def test_batch_unbatch() -> None:
    """
    Tests a full end to end batch and unbatch pipeline.
    """

    def transformer(
        batch: Message[ValuesBatch[str]],
    ) -> ValuesBatch[str]:
        return [sub_msg.replace("Transformed") for sub_msg in batch.payload]

    final_step = Mock()
    next_step: RunTask[ValuesBatch[str], ValuesBatch[str]] = RunTask(
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

    partition = Partition(Topic("test"), 1)

    final_step.submit.assert_has_calls(
        [
            call(Message(Value("Transformed", {}, NOW))),
            call(Message(Value("Transformed", {}, NOW))),
            call(Message(Value("Transformed", {partition: 4}, NOW))),
        ]
    )
