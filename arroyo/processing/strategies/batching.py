from __future__ import annotations

from typing import Callable, MutableSequence, Optional, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.reduce import Reduce
from arroyo.processing.strategies.unfold import Unfold
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

    :param max_batch_size: How many messages should be reduced into one at maximum.
    :param max_batch_time: How much time (in seconds) should be spent reducing
        messages together before flushing the batch.
    """

    def __init__(
        self,
        max_batch_size: int,
        max_batch_time: float,
        next_step: ProcessingStrategy[ValuesBatch[TStrategyPayload]],
        compute_batch_size: Optional[
            Callable[[BaseValue[TStrategyPayload]], int]
        ] = None,
    ) -> None:
        def accumulator(
            result: ValuesBatch[TStrategyPayload], value: BaseValue[TStrategyPayload]
        ) -> ValuesBatch[TStrategyPayload]:
            result.append(value)
            return result

        self.__reduce_step: Reduce[TStrategyPayload, ValuesBatch[TStrategyPayload]] = (
            Reduce(
                max_batch_size,
                max_batch_time,
                accumulator,
                lambda: [],
                next_step,
                compute_batch_size,
            )
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
        def generator(
            values: ValuesBatch[TStrategyPayload],
        ) -> ValuesBatch[TStrategyPayload]:
            return values

        self.__unfold_step = Unfold(generator, next_step)

    def submit(
        self, message: Message[Union[FilteredPayload, ValuesBatch[TStrategyPayload]]]
    ) -> None:
        self.__unfold_step.submit(message)

    def poll(self) -> None:
        self.__unfold_step.poll()

    def close(self) -> None:
        self.__unfold_step.poll()

    def terminate(self) -> None:
        self.__unfold_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__unfold_step.join(timeout)
