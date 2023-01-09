import io
import json
import os
from unittest.mock import Mock

import avro
import avro.io
import avro.schema
import msgpack
import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.decoder import (
    AvroCodec,
    DecodedKafkaMessage,
    JsonCodec,
    KafkaMessageDecoder,
    MsgpackCodec,
    ValidationError,
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
    schema_path = os.path.join(
        os.path.dirname(__file__), "serializers", "test.schema.json"
    )

    with open(schema_path, mode="r") as f:
        schema = json.loads(f.read())
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

        # Json codec without schema should not fail with invalid message
        json_codec_no_schema = JsonCodec()
        strategy = KafkaMessageDecoder(json_codec_no_schema, True, next_step)
        strategy.submit(valid_message)
        strategy.submit(invalid_message)


def test_msgpack_decoder() -> None:
    schema_path = os.path.join(
        os.path.dirname(__file__), "serializers", "test.schema.json"
    )

    with open(schema_path, mode="r") as f:
        schema = json.loads(f.read())
        next_step = Mock()
        msgpack_codec = MsgpackCodec(schema)

        # Valid message
        valid_message = make_kafka_message(
            msgpack.packb(
                {
                    "event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "project_id": 1,
                    "group_id": 2,
                }
            )
        )

        strategy = KafkaMessageDecoder(msgpack_codec, True, next_step)
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


def test_avro_decoder() -> None:
    schema_path = os.path.join(os.path.dirname(__file__), "serializers", "test.avsc")

    with open(schema_path, mode="rb") as f:
        schema = avro.schema.parse(f.read())

        bytes_writer = io.BytesIO()
        writer = avro.io.DatumWriter(schema)
        data = {
            "event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "project_id": 1,
            "group_id": 2,
        }
        writer.write(data, avro.io.BinaryEncoder(bytes_writer))
        valid_message = make_kafka_message(bytes_writer.getvalue())

        avro_codec = AvroCodec(schema)
        next_step = Mock()
        strategy = KafkaMessageDecoder(avro_codec, True, next_step)
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

        # Invalid schema
        invalid_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "invalid_thing", "type": ["int"]}],
                }
            )
        )

        bytes_writer = io.BytesIO()
        data = {"invalid_thing": 1}
        avro.io.DatumWriter(invalid_schema).write(
            data, avro.io.BinaryEncoder(bytes_writer)
        )

        # Invalid message
        invalid_message = make_kafka_message(bytes_writer.getvalue())

        with pytest.raises(ValidationError):
            strategy.submit(invalid_message)
