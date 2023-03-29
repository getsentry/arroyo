from typing import Callable, Generic, MutableSequence, Optional, TypeVar, Union, cast

from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import (
    FILTERED_PAYLOAD,
    FilteredPayload,
    Message,
    TStrategyPayload,
    Value,
)

TResult = TypeVar("TResult")


class RunTask(
    ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    Generic[TStrategyPayload, TResult],
):
    """
    Basic strategy to run a custom processing function on a message.

    The processing function provided can raise ``InvalidMessage`` to indicate that
    the message is invalid and should be put in a dead letter queue.
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
    ) -> None:
        self.__function = function
        self.__next_step = next_step
        # Invalid message offsets pending commit
        self.__invalid_messages: MutableSequence[InvalidMessage] = []

    def __forward_invalid_offsets(self) -> None:
        if self.__invalid_messages:
            committable = {m.partition: m.offset + 1 for m in self.__invalid_messages}
            self.__next_step.poll()
            self.__next_step.submit(Message(Value(FILTERED_PAYLOAD, committable)))
            self.__invalid_messages = []

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        if isinstance(message.payload, FilteredPayload):
            self.__next_step.submit(cast(Message[FilteredPayload], message))
        else:
            try:
                result = self.__function(cast(Message[TStrategyPayload], message))
                value = message.value.replace(result)
                self.__next_step.submit(Message(value))
            except InvalidMessage as e:
                self.__invalid_messages.append(e)
                raise e

    def poll(self) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()
