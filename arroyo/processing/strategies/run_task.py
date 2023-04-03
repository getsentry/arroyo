from typing import Callable, Generic, Optional, TypeVar, Union, cast

from arroyo.dlq import InvalidMessage, InvalidMessageState
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload

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
        self.__invalid_messages = InvalidMessageState()

    def __forward_invalid_offsets(self) -> None:
        if len(self.__invalid_messages):
            self.__next_step.poll()
            filter_msg = self.__invalid_messages.build()
            if filter_msg:
                try:
                    self.__next_step.submit(filter_msg)
                    self.__invalid_messages.reset()
                except MessageRejected:
                    pass

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
