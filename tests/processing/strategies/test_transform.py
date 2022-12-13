from datetime import datetime
from unittest.mock import Mock, call

from arroyo.processing.strategies.transform import TransformStep
from arroyo.types import BrokerValue, Message, Partition, Topic, Value
from tests.assertions import assert_changes


def test_transform() -> None:
    next_step = Mock()

    def transform_function(message: Message[int]) -> int:
        return message.payload * 2

    transform_step = TransformStep(transform_function, next_step)

    original_message = Message(Value(1, {Partition(Topic("topic"), 0): 1}))

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_message)

    assert next_step.submit.call_args == call(
        Message(
            Value(transform_function(original_message), original_message.committable)
        )
    )

    with assert_changes(lambda: int(next_step.poll.call_count), 0, 1):
        transform_step.poll()

    with assert_changes(lambda: int(next_step.close.call_count), 0, 1), assert_changes(
        lambda: int(next_step.join.call_count), 0, 1
    ):
        transform_step.close()
        transform_step.join()

    next_step.reset_mock()
    next_step.submit.reset_mock()

    broker_payload = BrokerValue(1, Partition(Topic("topic"), 0), 0, datetime.now())

    original_broker_message = Message(broker_payload)

    with assert_changes(lambda: int(next_step.submit.call_count), 0, 1):
        transform_step.submit(original_broker_message)

    assert next_step.submit.call_args == call(
        Message(
            BrokerValue(
                transform_function(original_broker_message),
                broker_payload.partition,
                broker_payload.offset,
                broker_payload.timestamp,
            )
        )
    )
