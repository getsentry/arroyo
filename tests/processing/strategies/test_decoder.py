from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.codecs import ValidationError
from arroyo.codecs.json import JsonCodec
from arroyo.processing.strategies.decoder import (
    DecodedKafkaMessage,
    KafkaMessageDecoder,
)
from arroyo.types import Message, Partition, Topic, Value


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
    schema_path = Path.joinpath(
        Path(__file__).parents[2], "codecs", "serializers", "test.schema.json"
    )

    next_step = Mock()
    json_codec: JsonCodec[Any] = JsonCodec(schema_path=schema_path)
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

    # Json codec without schema should not fail with invalid message
    json_codec_no_schema: JsonCodec[Any] = JsonCodec()
    strategy = KafkaMessageDecoder(json_codec_no_schema, True, next_step)
    strategy.submit(valid_message)
    strategy.submit(invalid_message)
