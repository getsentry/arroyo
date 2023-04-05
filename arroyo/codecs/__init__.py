from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class Codec(ABC, Generic[T]):
    @abstractmethod
    def encode(self, data: T, validate: bool) -> bytes:
        """
        Decode bytes from Kafka message.
        If validate is true, validation is performed.
        """
        raise NotImplementedError

    @abstractmethod
    def decode(self, raw_data: bytes, validate: bool) -> T:
        """
        Decode bytes from Kafka message.
        If validate is true, validation is performed.
        """
        raise NotImplementedError


class ValidationError(Exception):
    """
    Raised by the codec when a message fails validation.
    """

    pass
