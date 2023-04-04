from __future__ import annotations

import os
from io import BytesIO
from typing import Any, TypeVar, cast

import avro  # pip install sentry-arroyo[avro]
from avro.errors import SchemaResolutionException
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter

from arroyo.codecs import Codec, ValidationError

T = TypeVar("T")


class AvroCodec(Codec[T]):
    def __init__(self, *, schema_path: os.PathLike[Any]) -> None:
        with open(schema_path, mode="r") as f:
            schema = avro.schema.parse(f.read())
        self.__schema = schema
        self.__reader = DatumReader(self.__schema)
        self.__writer = DatumWriter(self.__schema)

    def encode(self, data: T, validate: bool) -> bytes:
        bytes_writer = BytesIO()
        self.__writer.write(data, BinaryEncoder(bytes_writer))
        return bytes_writer.getvalue()

    def decode(self, raw_data: bytes, validate: bool) -> T:
        decoder = BinaryDecoder(BytesIO(raw_data))
        try:
            return cast(T, self.__reader.read(decoder))
        except SchemaResolutionException as exc:
            raise ValidationError from exc
