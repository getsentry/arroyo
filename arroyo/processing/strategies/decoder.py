from __future__ import annotations

from avro.io import BinaryDecoder, DatumReader
from avro.errors import SchemaResolutionException
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, Tuple, TypeVar
from io import BytesIO

import fastjsonschema
import msgpack
import rapidjson

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Message

T = TypeVar("T")


@dataclass(frozen=True)
class DecodedKafkaMessage(Generic[T]):
    key: Optional[bytes]
    decoded: T
    headers: Sequence[Tuple[str, bytes]]


class KafkaMessageDecoder(ProcessingStrategy[KafkaPayload]):
    """
    Decode messages to be forwarded to the next step. Optional validation.
    This strategy accepts a KafkaPayload and only performs validation on the
    message value. Headers and keys are forwarded without being validated or parsed.
    """

    def __init__(
        self,
        codec: Codec[T],
        validate: bool,
        next_step: ProcessingStrategy[DecodedKafkaMessage[T]],
    ) -> None:
        self.__codec = codec
        self.__validate = validate
        self.__next_step = next_step

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        decoded_value = self.__codec.decode(
            message.payload.value, validate=self.__validate
        )

        decoded = DecodedKafkaMessage(
            message.payload.key, decoded_value, message.payload.headers
        )
        self.__next_step.submit(Message(message.value.replace(decoded)))

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)


class ValidationError(Exception):
    """
    Placeholder. May eventually be replaced by a DLQ specific exception
    once that feature is finalized.
    """

    pass


class Codec(ABC, Generic[T]):
    @abstractmethod
    def decode(self, raw_data: bytes, validate: bool) -> T:
        """
        Decode bytes from Kafka message.
        If validate is true, validation is performed.
        """
        raise NotImplementedError


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


class MsgpackCodec(Codec[object]):
    """
    This codec assumes the schema is json.
    """

    def __init__(self, json_schema: Optional[object] = None) -> None:
        if json_schema is not None:
            self.__validate = fastjsonschema.compile(json_schema)
        else:
            self.__validate = lambda _: _

    def decode(self, raw_data: bytes, validate: bool) -> object:
        decoded = msgpack.unpackb(raw_data)
        if validate:
            self.validate(decoded)
        return decoded

    def validate(self, data: object) -> None:
        try:
            self.__validate(data)
        except Exception as exc:
            raise ValidationError from exc


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
