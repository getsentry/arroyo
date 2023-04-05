import json
from pathlib import Path
from typing import TypedDict

from arroyo.codecs.avro import AvroCodec
from arroyo.codecs.json import JsonCodec


class Example(TypedDict):
    event_id: str
    project_id: int
    group_id: int


def get_example_data() -> Example:
    return {
        "event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "project_id": 1,
        "group_id": 2,
    }


def test_json_codec() -> None:
    schema_path = Path.joinpath(
        Path(__file__).parent, "serializers", "test.schema.json"
    )
    with open(schema_path, mode="r") as f:
        schema = json.loads(f.read())

    codec: JsonCodec[Example] = JsonCodec(schema=schema)

    data = get_example_data()

    assert codec.decode(codec.encode(data, validate=False), validate=True) == data


def test_avro_codec() -> None:
    schema_path = Path.joinpath(Path(__file__).parent, "serializers", "test.avsc")
    codec: AvroCodec[Example] = AvroCodec(schema_path=schema_path)

    data = get_example_data()
    assert codec.decode(codec.encode(data, validate=True), validate=True) == data
