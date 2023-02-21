import time
from typing import Callable, Generic, MutableMapping, Optional, TypeVar, Union, cast

from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.types import BaseValue, FilteredPayload, Message, Partition, Value
from arroyo.utils.metrics import get_metrics

TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")


Accumulator = Callable[[TResult, BaseValue[TPayload]], TResult]


class BatchBuilder(Generic[TPayload, TResult]):
    """
    Accumulates values into a batch.
    """

    def __init__(
        self,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: TResult,
        max_batch_size: int,
        max_batch_time: float,
    ) -> None:
        self.__max_batch_time = max_batch_time
        self.__max_batch_size = max_batch_size
        self.__accumulator = accumulator
        self.__accumulated_value = initial_value
        self.__count = 0
        self.__offsets: MutableMapping[Partition, int] = {}
        self.init_time = time.time()

    def append(self, value: BaseValue[Union[FilteredPayload, TPayload]]) -> None:
        self.__offsets.update(value.committable)
        if not isinstance(value.payload, FilteredPayload):
            self.__accumulated_value = self.__accumulator(
                self.__accumulated_value, cast(BaseValue[TPayload], value)
            )

            self.__count += 1

    def build_if_ready(self) -> Optional[Value[TResult]]:
        if (
            self.__count >= self.__max_batch_size
            or time.time() > self.init_time + self.__max_batch_time
        ):

            return Value(payload=self.__accumulated_value, committable=self.__offsets)
        else:
            return None

    def build(self) -> Value[TResult]:
        return Value(payload=self.__accumulated_value, committable=self.__offsets)


class Reduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    """
    Accumulates messages until the max size or max time condition is hit.
    The accumulator function is run on each message in the order it is received.

    Once the "batch" is full, the accumulated value is submitted to the next step.

    This strategy propagates `MessageRejected` exceptions from the
    downstream steps if they are thrown.
    """

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        accumulator: Accumulator[TResult, TPayload],
        initial_value: Callable[[], TResult],
        next_step: ProcessingStrategy[TResult],
    ) -> None:
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__accumulator = accumulator
        self.__initial_value = initial_value
        self.__next_step = next_step
        self.__batch_builder: Optional[BatchBuilder[TPayload, TResult]] = None
        self.__metrics = get_metrics()
        self.__closed = False

    def __flush(self, force: bool) -> None:
        assert self.__batch_builder is not None
        batch = (
            self.__batch_builder.build_if_ready()
            if not force
            else self.__batch_builder.build()
        )
        if batch is None:
            return

        batch_msg = Message(batch)

        self.__next_step.submit(batch_msg)

        self.__metrics.timing(
            "arroyo.strategies.reduce.batch_time",
            time.time() - self.__batch_builder.init_time,
        )

        self.__batch_builder = None

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        """
        Accumulates messages in the current batch.
        A new batch is created at the first message received.

        This method tries to flush before adding the message
        to the current batch. This is so that, if we receive
        `MessageRejected` exception from the following step,
        we can propagate the exception without processing the
        new message. This allows the previous step to try again
        without introducing duplications.
        """
        assert not self.__closed

        if isinstance(message.payload, FilteredPayload):
            self.__next_step.submit(cast(Message[TResult], message))
            return

        if self.__batch_builder is not None:
            self.__flush(force=False)

        if self.__batch_builder is None:
            self.__batch_builder = BatchBuilder(
                self.__accumulator,
                self.__initial_value(),
                max_batch_size=self.__max_batch_size,
                max_batch_time=self.__max_batch_time,
            )

        self.__batch_builder.append(cast(BaseValue[TPayload], message.value))

    def poll(self) -> None:
        assert not self.__closed

        if self.__batch_builder is not None:
            try:
                self.__flush(force=False)
            except MessageRejected:
                pass

        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__batch_builder = None

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Terminates the strategy by joining the following step.
        This method tries to flush the current batch no matter
        whether the batch is ready or not.
        """
        deadline = time.time() + timeout if timeout is not None else None
        if self.__batch_builder is not None:
            while deadline is None or time.time() < deadline:
                try:
                    self.__flush(force=True)
                    break
                except MessageRejected:
                    pass

        self.__next_step.close()
        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
