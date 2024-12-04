import time
from typing import Callable, Generic, Optional, TypeVar, Union

from arroyo.processing.strategies.buffer import Buffer
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.types import BaseValue, FilteredPayload, Message

TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")


Accumulator = Callable[[TResult, BaseValue[TPayload]], TResult]


class ReduceBuffer(Generic[TPayload, TResult]):
    """Reduce strategy buffer class."""

    def __init__(
        self,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        max_batch_size: int,
        max_batch_time: float,
        compute_batch_size: Optional[Callable[[BaseValue[TPayload]], int]] = None,
    ):
        self.accumulator = accumulator
        self.initial_value = initial_value
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.compute_batch_size = compute_batch_size

        self._buffer = initial_value()
        self._buffer_size = 0
        self._buffer_until = time.time() + max_batch_time

    @property
    def buffer(self) -> TResult:
        return self._buffer

    @property
    def is_empty(self) -> bool:
        return self._buffer_size == 0

    @property
    def is_ready(self) -> bool:
        return (
            self._buffer_size >= self.max_batch_size
            or time.time() >= self._buffer_until
        )

    def append(self, message: BaseValue[TPayload]) -> None:
        self._buffer = self.accumulator(self._buffer, message)
        if self.compute_batch_size:
            buffer_increment = self.compute_batch_size(message)
        else:
            buffer_increment = 1
        self._buffer_size += buffer_increment

    def new(self) -> "ReduceBuffer[TPayload, TResult]":
        return ReduceBuffer(
            accumulator=self.accumulator,
            initial_value=self.initial_value,
            max_batch_size=self.max_batch_size,
            max_batch_time=self.max_batch_time,
            compute_batch_size=self.compute_batch_size,
        )


class Reduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    """
    Accumulates messages until the max size or max time condition is hit.
    The accumulator function is run on each message in the order it is received.

    Once the "batch" is full, the accumulated value is submitted to the next step.

    This strategy propagates `MessageRejected` exceptions from the
    downstream steps if they are thrown.

    :param max_batch_size: How many messages should be reduced into one at maximum.
    :param max_batch_time: How much time (in seconds) should be spent reducing
        messages together before flushing the batch.
    """

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        next_step: ProcessingStrategy[TResult],
        compute_batch_size: Optional[Callable[[BaseValue[TPayload]], int]] = None,
    ) -> None:
        self.__buffer_step = Buffer(
            buffer=ReduceBuffer(
                max_batch_size=max_batch_size,
                max_batch_time=max_batch_time,
                accumulator=accumulator,
                initial_value=initial_value,
                compute_batch_size=compute_batch_size,
            ),
            next_step=next_step,
        )

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        self.__buffer_step.submit(message)

    def poll(self) -> None:
        self.__buffer_step.poll()

    def close(self) -> None:
        self.__buffer_step.close()

    def terminate(self) -> None:
        self.__buffer_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__buffer_step.join(timeout)


__all__ = ["Reduce"]
