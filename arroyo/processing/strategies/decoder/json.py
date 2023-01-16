from typing import Optional

import fastjsonschema  # pip install sentry-arroyo[json]
import rapidjson

from arroyo.processing.strategies.decoder.base import Codec, ValidationError


class JsonCodec(Codec[object]):
    def __init__(self, json_schema: Optional[object] = None) -> None:
        if json_schema is not None:
            self.__validate = fastjsonschema.compile(json_schema)
        else:
            self.__validate = lambda _: _

    def decode(self, raw_data: bytes, validate: bool) -> object:
        decoded = rapidjson.loads(raw_data)
        if validate:
            self.validate(decoded)
        return decoded

    def validate(self, data: object) -> None:
        try:
            self.__validate(data)
        except Exception as exc:
            raise ValidationError from exc
