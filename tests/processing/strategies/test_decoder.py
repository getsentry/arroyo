from unittest.mock import Mock

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.decoder import (
    DecodedKafkaMessage,
    JsonCodec,
    KafkaMessageDecoder,
    ValidationError,
)
from arroyo.types import Message, Partition, Topic, Value

schema = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string", "description": "32 character hex string"},
        "project_id": {"type": "integer", "description": "Project ID"},
        "group_id": {"type": "integer", "description": "Group ID"},
    },
}


def make_kafka_message(raw_data: bytes) -> Message[KafkaPayload]:
    return Message(
        Value(
            KafkaPayload(
                None,
                raw_data,
                [],
            ),
            {Partition(Topic("test"), 0): 1},
        )
    )


def test_json_decoder() -> None:
    next_step = Mock()
    json_codec = JsonCodec(schema)
    strategy = KafkaMessageDecoder(json_codec, True, next_step)
    strategy_skip_validation = KafkaMessageDecoder(json_codec, False, next_step)

    # Valid message
    valid_message = make_kafka_message(
        b'{"event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "project_id": 1, "group_id": 2}'
    )
    strategy.submit(valid_message)

    assert next_step.submit.call_args[0][0] == Message(
        Value(
            DecodedKafkaMessage(
                None,
                {
                    "event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "project_id": 1,
                    "group_id": 2,
                },
                [],
            ),
            valid_message.committable,
        ),
    )

    next_step.submit.reset_mock()

    # Invalid message
    invalid_message = make_kafka_message(
        b'{"event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "project_id": "bbb", "group_id": 2}'
    )

    with pytest.raises(ValidationError):
        strategy.submit(invalid_message)

    strategy_skip_validation.submit(invalid_message)
