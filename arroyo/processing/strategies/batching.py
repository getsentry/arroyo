from __future__ import annotations

import time
from collections import deque
from typing import Deque, MutableSequence, Optional, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import BaseValue, FilteredPayload, Message, TStrategyPayload

ValuesBatch = MutableSequence[BaseValue[TStrategyPayload]]


class BatchStep(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
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
        max_batch_size: int,
        max_batch_time: float,
        next_step: ProcessingStrategy[ValuesBatch[TStrategyPayload]],
    ) -> None:
        def accumulator(
            result: ValuesBatch[TStrategyPayload], value: BaseValue[TStrategyPayload]
        ) -> ValuesBatch[TStrategyPayload]:
            result.append(value)
            return result

        self.__reduce_step: Reduce[
            TStrategyPayload, ValuesBatch[TStrategyPayload]
        ] = Reduce(
            max_batch_size,
            max_batch_time,
            accumulator,
            lambda: [],
            next_step,
        )

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
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
        self.__reduce_step.submit(message)

    def poll(self) -> None:
        self.__reduce_step.poll()

    def close(self) -> None:
        self.__reduce_step.close()

    def terminate(self) -> None:
        self.__reduce_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Terminates the strategy by joining the following step.
        This method tries to flush the current batch no matter
        whether the batch is ready or not.
        """
        self.__reduce_step.join(timeout)


class UnbatchStep(
    ProcessingStrategy[Union[FilteredPayload, ValuesBatch[TStrategyPayload]]]
):
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
        next_step: ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    ) -> None:
        self.__next_step = next_step
        self.__batch_to_send: Deque[
            Message[Union[FilteredPayload, TStrategyPayload]]
        ] = deque()
        self.__closed = False

    def __flush(self) -> None:
        while self.__batch_to_send:
            msg = self.__batch_to_send[0]
            self.__next_step.submit(msg)
            self.__batch_to_send.popleft()

    def submit(
        self, message: Message[Union[FilteredPayload, ValuesBatch[TStrategyPayload]]]
    ) -> None:
        assert not self.__closed
        if self.__batch_to_send:
            raise MessageRejected

        # XXX: BatchStep puts the committable offsets of FilteredPayloads onto
        # `message.committable`. Those offsets are currently discarded
        # entirely. We unbatch the inner list of payloads of `message` and
        # discard the message wrapper.
        #
        # A correct solution would take the `committable` of the outer message
        # and "meld" it onto the last inner message, and/or fabricate a new
        # Message[FilteredPayload]

        if isinstance(message.payload, FilteredPayload):
            self.__batch_to_send.append(
                cast(Message[Union[FilteredPayload, TStrategyPayload]], message)
            )
        else:
            self.__batch_to_send.extend(map(Message, message.payload))
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
