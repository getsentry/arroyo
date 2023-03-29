from typing import Callable, Generic, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload

TResult = TypeVar("TResult")


class RunTask(
    ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    Generic[TStrategyPayload, TResult],
):
    """
    Basic strategy to run a custom processing function on a message.
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
    ) -> None:
        self.__function = function
        self.__next_step = next_step

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        if isinstance(message.payload, FilteredPayload):
            self.__next_step.submit(cast(Message[FilteredPayload], message))
        else:
            result = self.__function(cast(Message[TStrategyPayload], message))
            value = message.value.replace(result)
            self.__next_step.submit(Message(value))

    def poll(self) -> None:
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()
