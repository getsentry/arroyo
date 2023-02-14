import logging
import time
from typing import Optional

import fastjsonschema  # pip install sentry-arroyo[json]
import rapidjson

from arroyo.processing.strategies.decoder.base import Codec, ValidationError
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)


class JsonCodec(Codec[object]):
    def __init__(
        self,
        json_schema: Optional[object] = None,
        json_schema_name: Optional[str] = None,
        json_schema_raise_failures: bool = True,
    ) -> None:
        self.__metrics = get_metrics()
        self.__json_schema_raise_failures = json_schema_raise_failures
        self.__json_schema_name = json_schema_name

        if json_schema is not None:
            self.__validate = fastjsonschema.compile(json_schema)
        else:
            self.__validate = lambda _: _

    def decode(self, raw_data: bytes, validate: bool) -> object:
        start = time.time()
        decoded = rapidjson.loads(raw_data)

        after_decoded = time.time()

        self.__metrics.timing(
            "arroyo.strategies.decoder.json.loads",
            after_decoded - start,
        )

        if validate:
            self.validate(decoded)
            self.__metrics.timing(
                "arroyo.strategies.decoder.json.validate",
                time.time() - after_decoded,
            )
        return decoded

    def validate(self, data: object) -> None:
        try:
            self.__validate(data)
        except Exception as exc:
            self.__metrics.increment(
                "arroyo.strategies.decoder.json.validate.failure",
            )
            if self.__json_schema_raise_failures:
                raise ValidationError from exc
            else:
                logger.error(
                    "Failed to validate JSON message with schema %s",
                    self.__json_schema_name,
                    exc_info=True,
                )
