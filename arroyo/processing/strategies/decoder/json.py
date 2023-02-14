import time
from typing import Optional

import fastjsonschema  # pip install sentry-arroyo[json]
import rapidjson

from arroyo.processing.strategies.decoder.base import Codec, ValidationError
from arroyo.utils.metrics import get_metrics


class JsonCodec(Codec[object]):
    def __init__(self, json_schema: Optional[object] = None) -> None:
        self.__metrics = get_metrics()

        if json_schema is not None:
            self.__validate = fastjsonschema.compile(json_schema)
        else:
            self.__validate = lambda _: _

    def decode(self, raw_data: bytes, validate: bool) -> object:
        start = time.time()
        decoded = rapidjson.loads(raw_data)

        after_decoded = time.time() - start

        self.__metrics.timing("arroyo.strategies.decoder.json.loads", after_decoded)

        if validate:
            self.validate(decoded)
            self.__metrics.timing(
                "arroyo.strategies.decoder.json.validate", time.time() - after_decoded
            )
        return decoded

    def validate(self, data: object) -> None:
        try:
            self.__validate(data)
        except Exception as exc:
            raise ValidationError from exc
