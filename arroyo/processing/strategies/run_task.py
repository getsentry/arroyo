from __future__ import annotations

from typing import Callable, Generic, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.guard import StrategyGuard
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

    def __new__(
        cls,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
        better_backpressure: bool = False,
    ) -> RunTask[TStrategyPayload, TResult]:
        def build_self(
            next_step: ProcessingStrategy[Union[FilteredPayload, TResult]]
        ) -> ProcessingStrategy[Union[FilteredPayload, TResult]]:
            self = object.__new__(RunTask)
            self.__init__(function, next_step, better_backpressure)  # type: ignore
            return self

        return cast(
            RunTask[TStrategyPayload, TResult], StrategyGuard(build_self, next_step)
        )

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
        better_backpressure: bool = False,
    ) -> None:
        self.__function = function
        self.__next_step = next_step
        self.__better_backpressure = better_backpressure
        self.__message_carried_over: Optional[Message[TResult]] = None

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        if self.__better_backpressure:
            if self.__message_carried_over is not None:
                raise MessageRejected(message)

            result = self.__function(cast(Message[TStrategyPayload], message))
            value = message.value.replace(result)
            transformed: Message[TResult] = Message(value)

            try:
                self.__next_step.submit(transformed)
            except MessageRejected:
                self.__message_carried_over = transformed
        else:
            result = self.__function(cast(Message[TStrategyPayload], message))
            value = message.value.replace(result)
            self.__next_step.submit(Message(value))

    def poll(self) -> None:
        self.__next_step.poll()

        if self.__better_backpressure and self.__message_carried_over is not None:
            try:
                self.__next_step.submit(self.__message_carried_over)
                self.__message_carried_over = None
            except MessageRejected:
                pass

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout=timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()
