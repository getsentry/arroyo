from __future__ import annotations

import os
from typing import Any, Optional, TypeVar, cast

import fastjsonschema  # pip install sentry-arroyo[json]
import rapidjson

from arroyo.codecs import Codec, ValidationError

T = TypeVar("T")


class JsonCodec(Codec[T]):
    def __init__(
        self,
        *,
        schema: Optional[object] = None,
        schema_path: Optional[os.PathLike[Any]] = None,
    ) -> None:
        if schema is not None and schema_path is not None:
            raise ValueError("schema and schema_path are mutually exclusive")

        if schema_path is not None:
            with open(schema_path, mode="r") as f:
                schema = rapidjson.loads(f.read())

        if schema is not None:
            self.__validate = fastjsonschema.compile(schema)
        else:
            self.__validate = lambda _: _

    def encode(self, data: T, validate: bool) -> bytes:
        if validate:
            self.validate(data)
        return cast(bytes, rapidjson.dumps(data).encode("utf-8"))

    def decode(self, raw_data: bytes, validate: bool) -> Any:
        decoded = rapidjson.loads(raw_data)
        if validate:
            self.validate(decoded)
        return decoded

    def validate(self, data: T) -> None:
        try:
            self.__validate(data)
        except Exception as exc:
            raise ValidationError from exc
