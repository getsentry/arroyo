from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, NamedTuple, Optional, Sequence, Tuple

import fastjsonschema
import rapidjson

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTaskWithMultiprocessing
from arroyo.types import Message


class DecodedKafkaMessage(NamedTuple):
    key: Optional[bytes]
    decoded: object
    headers: Sequence[Tuple[str, bytes]]


def validation_func(
    codec: Codec, validate: bool, message: Message[KafkaPayload]
) -> DecodedKafkaMessage:
    return DecodedKafkaMessage(
        message.payload.key,
        codec.decode(message.payload.value, validate),
        message.payload.headers,
    )


class KafkaMessageDecoder(ProcessingStrategy[KafkaPayload]):
    """
    Decode messages to be forwarded to the next step. Optional validation.
    This strategy accepts a KafkaPayload and only performs validation on the
    message value. Headers and keys are forwarded without being validated or parsed.
    """

    def __init__(
        self,
        codec: Codec,
        validate: bool,
        next_step: ProcessingStrategy[DecodedKafkaMessage],
        num_processes: int,
        max_batch_size: int,
        max_batch_time: float,
        input_block_size: int,
        output_block_size: int,
        initializer: Optional[Callable[[], None]] = None,
    ) -> None:
        self.__task_runner = RunTaskWithMultiprocessing(
            partial(validation_func, codec, validate),
            next_step,
            num_processes,
            max_batch_size,
            max_batch_time,
            input_block_size,
            output_block_size,
            initializer,
        )

    def poll(self) -> None:
        self.__task_runner.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        self.__task_runner.submit(message)

    def close(self) -> None:
        self.__task_runner.close()

    def terminate(self) -> None:
        self.__task_runner.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__task_runner.join(timeout)


class ValidationError(Exception):
    """
    Placeholder. May eventually be replaced by a DLQ specific exception
    once that feature is finalized.
    """

    pass


class Codec(ABC):
    @abstractmethod
    def decode(self, raw_data: bytes, validate: bool) -> object:
        """
        Decode bytes from Kafka message into Python object.
        If validate is true, the validation is run.
        """
        raise NotImplementedError

    @abstractmethod
    def validate(self, data: object) -> None:
        """
        Runs the validation. Raises a ValidationError if the data is not valid.
        """
        raise NotImplementedError


class JsonCodec(Codec):
    def __init__(self, json_schema: object) -> None:
        self.__json_schema = json_schema
        # Initially set to none as the validate function is not pickleable
        self.__validate_func: Optional[Callable[[object], None]] = None

    def __validate(self, data: object) -> None:
        if self.__validate_func is None:
            self.__validate_func = fastjsonschema.compile(self.__json_schema)

        self.__validate_func(data)

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
