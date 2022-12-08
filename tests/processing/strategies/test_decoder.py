import pickle
from datetime import datetime
from unittest.mock import Mock

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.decoder import (
    DecodedKafkaMessage,
    JsonCodec,
    KafkaMessageDecoder,
    ValidationError,
)
from arroyo.types import Message, Partition, Position, Topic, Value

schema = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string", "description": "32 character hex string"},
        "project_id": {"type": "integer", "description": "Project ID"},
        "group_id": {"type": "integer", "description": "Group ID"},
    },
}


def test_codec() -> None:
    # pickleable
    codec = JsonCodec(schema)
    pickle.loads(pickle.dumps(codec))


def make_kafka_message(raw_data: bytes) -> Message[KafkaPayload]:
    return Message(
        Value(
            KafkaPayload(
                None,
                raw_data,
                [],
            ),
            {Partition(Topic("test"), 0): Position(0, datetime.now())},
        )
    )


def test_json_decoder() -> None:
    next_step = Mock()
    json_codec = JsonCodec(schema)
    block_size = 4096

    strategy = KafkaMessageDecoder(
        json_codec, True, next_step, 1, 1, 1, block_size, block_size
    )

    # Valid message
    valid_message = make_kafka_message(
        b'{"event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "project_id": 1, "group_id": 2}'
    )
    strategy.submit(valid_message)
    strategy.poll()

    # Wait for all messages to get processed
    strategy.close()
    strategy.join()

    assert next_step.submit.call_count == 1

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

    strategy = KafkaMessageDecoder(
        json_codec, True, next_step, 1, 1, 1, block_size, block_size
    )

    with pytest.raises(ValidationError):
        strategy.submit(invalid_message)
        strategy.poll()
        strategy.close()
        strategy.join()

    # Without validation
    strategy_skip_validation = KafkaMessageDecoder(
        json_codec, False, next_step, 1, 1, 1, block_size, block_size
    )
    strategy_skip_validation.submit(invalid_message)
    strategy_skip_validation.poll()
    strategy_skip_validation.close()
    strategy_skip_validation.join()
