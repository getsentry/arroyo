from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Generic,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from arroyo.backends.kafka import KafkaPayload
from arroyo.codecs import Codec
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FILTERED_PAYLOAD, FilteredPayload, Message, Value

T = TypeVar("T")


@dataclass(frozen=True)
class DecodedKafkaMessage(Generic[T]):
    key: Optional[bytes]
    decoded: T
    headers: Sequence[Tuple[str, bytes]]


class KafkaMessageDecoder(ProcessingStrategy[KafkaPayload], Generic[T]):
    """
    Decode messages to be forwarded to the next step. Optional validation.
    This strategy accepts a KafkaPayload and only performs validation on the
    message value. Headers and keys are forwarded without being validated or parsed.

    Raises ``InvalidMessage`` if the message fails validation.
    """

    def __init__(
        self,
        codec: Codec[T],
        validate: bool,
        next_step: ProcessingStrategy[Union[FilteredPayload, DecodedKafkaMessage[T]]],
    ) -> None:
        self.__codec = codec
        self.__validate = validate
        self.__next_step = next_step
        self.__invalid_messages: MutableSequence[InvalidMessage] = []

    def __forward_invalid_offsets(self) -> None:
        if self.__invalid_messages:
            committable = {m.partition: m.offset + 1 for m in self.__invalid_messages}
            self.__next_step.poll()
            self.__next_step.submit(Message(Value(FILTERED_PAYLOAD, committable)))
            self.__invalid_messages = []

    def poll(self) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        if isinstance(message.payload, FilteredPayload):
            self.__next_step.submit(cast(Message[DecodedKafkaMessage[T]], message))
        else:
            try:
                decoded_value = self.__codec.decode(
                    message.payload.value, validate=self.__validate
                )
            except InvalidMessage as e:
                self.__invalid_messages.append(e)
                raise e

            decoded = DecodedKafkaMessage(
                message.payload.key, decoded_value, message.payload.headers
            )
            self.__next_step.submit(Message(message.value.replace(decoded)))

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.join(timeout)
