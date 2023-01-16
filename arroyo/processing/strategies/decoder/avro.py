from io import BytesIO

from avro.errors import SchemaResolutionException  # pip install sentry-arroyo[avro]
from avro.io import BinaryDecoder, DatumReader

from arroyo.processing.strategies.decoder.base import Codec, ValidationError


class AvroCodec(Codec[object]):
    def __init__(self, avro_schema: object) -> None:
        self.__schema = avro_schema
        self.__reader = DatumReader(self.__schema)

    def decode(self, raw_data: bytes, validate: bool) -> object:
        assert validate is True, "Validation cannot be skipped for AvroCodec"
        decoder = BinaryDecoder(BytesIO(raw_data))
        try:
            return self.__reader.read(decoder)
        except SchemaResolutionException as exc:
            raise ValidationError from exc
