from __future__ import annotations

import time
from collections import deque
from typing import Deque, Generic, MutableMapping, MutableSequence, Optional, Sequence

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import BaseValue, Message, Partition, Position, TPayload, Value

ValuesBatch = Sequence[BaseValue[TPayload]]


class BatchBuilder(Generic[TPayload]):
    """
    Accumulates Values in a Sequence of BaseValue.

    It expects messages to be in monotonic order per partition.
    Out of order offsets will generate an invalid watermark.
    """

    def __init__(
        self,
        max_batch_time: float,
        max_batch_size: int,
    ) -> None:
        self.__max_batch_time = max_batch_time
        self.__max_batch_size = max_batch_size
        self.__values: MutableSequence[BaseValue[TPayload]] = []
        self.__offsets: MutableMapping[Partition, Position] = {}
        self.__init_time = time.time()

    def append(self, value: BaseValue[TPayload]) -> None:
        self.__offsets.update(value.committable)
        self.__values.append(value)

    def build_if_ready(self) -> Optional[Value[ValuesBatch[TPayload]]]:
        if (
            len(self.__values) >= self.__max_batch_size
            or time.time() > self.__init_time + self.__max_batch_time
        ):
            return Value(payload=self.__values, committable=self.__offsets)
        else:
            return None

    def build(self) -> Value[ValuesBatch[TPayload]]:
        return Value(payload=self.__values, committable=self.__offsets)


class BatchStep(ProcessingStrategy[TPayload]):
    """
    Accumulates messages into a batch. When the batch is full, this
    strategy submits it to the next step.

    A batch is represented as a `ValuesBatch` object which is a sequence
    of BaseValue. This includes both the messages and the high offset
    watermark.

    A messages batch is closed and submitted when the maximum number of
    messages is received or when the max_batch_time has passed since the
    first message was received.

    This step does not require in order processing. If messages are sent
    out of order, though, the highest observed offset per partition is
    still the committable one, whether or not all messages with lower
    offsets have been observed by this step.

    This strategy propagates `MessageRejected` exceptions from the
    downstream steps if they are thrown.
    """

    def __init__(
        self,
        max_batch_time_sec: float,
        max_batch_size: int,
        next_step: ProcessingStrategy[Sequence[BaseValue[TPayload]]],
    ) -> None:
        self.__max_batch_time_sec = max_batch_time_sec
        self.__max_batch_size = max_batch_size
        self.__next_step = next_step
        self.__batch_builder: Optional[BatchBuilder[TPayload]] = None
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
        self.__batch_builder = None

    def submit(self, message: Message[TPayload]) -> None:
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

        if self.__batch_builder is not None:
            self.__flush(force=False)

        if self.__batch_builder is None:
            self.__batch_builder = BatchBuilder(
                max_batch_time=self.__max_batch_time_sec,
                max_batch_size=self.__max_batch_size,
            )

        self.__batch_builder.append(message.value)

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

        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )


class UnbatchStep(ProcessingStrategy[ValuesBatch[TPayload]]):
    """
    This processing step receives batches and explodes them thus sending
    the content to the next step message by message.

    A batch is represented as a `ValuesBatch` object.

    If this step receives a `MessageRejected` exception from the next
    step it would keep the remaining messages and attempt to submit
    them at the following call to `poll`
    """

    def __init__(
        self,
        next_step: ProcessingStrategy[TPayload],
    ) -> None:
        self.__next_step = next_step
        self.__batch_to_send: Deque[BaseValue[TPayload]] = deque()
        self.__closed = False

    def __flush(self) -> None:
        while self.__batch_to_send:
            msg = self.__batch_to_send[0]
            self.__next_step.submit(Message(msg))
            self.__batch_to_send.popleft()

    def submit(self, message: Message[ValuesBatch[TPayload]]) -> None:
        assert not self.__closed
        if self.__batch_to_send:
            raise MessageRejected

        self.__batch_to_send.extend(message.payload)
        try:
            self.__flush()
        except MessageRejected:
            # The messages are stored in self.__batch_to_send ready for
            # the next call to `poll`.
            pass

    def poll(self) -> None:
        assert not self.__closed

        try:
            self.__flush()
        except MessageRejected:
            pass

        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        """
        Terminates the strategy by joining the following step.
        This method throws away the current batch if any.
        """
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None
        while deadline is None or time.time() < deadline:
            try:
                self.__flush()
                break
            except MessageRejected:
                pass

        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
