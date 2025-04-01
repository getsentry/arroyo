import time
from collections import deque
from typing import Callable, Deque, Generic, Iterable, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import BaseValue, FilteredPayload, Message, Value

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class Unfold(
    ProcessingStrategy[Union[FilteredPayload, TInput]], Generic[TInput, TOutput]
):
    """
    Unfold receives a message and explodes it to generate a collection of
    messages submitting them one by one to the next step. The generated
    messages are created according to the generator function provided by the user.

    ::

        def generator(num: int) -> Sequence[Value[int]]:
            return [Value(i, {}, None) for i in range(num)]

        unfold = Unfold(generator, next_step)

    The generator function provided must return an iterable (i.e. a class that
    implements `__iter__` ).

    The generator can choose to set its own committable on the return value. If
    the `committable` is empty, `Unfold` will use the offsets of the
    original message.

    If this step receives a `MessageRejected` exception from the next
    step it keeps the remaining messages and attempts to submit
    them on subsequent calls to `poll`
    """

    def __init__(
        self,
        generator: Callable[[TInput], Iterable[BaseValue[TOutput]]],
        next_step: ProcessingStrategy[Union[FilteredPayload, TOutput]],
    ) -> None:
        self.__generator = generator
        self.__next_step = next_step
        self.__closed = False
        # If we get MessageRejected from the next step, we put the pending messages here
        self.__pending: Deque[Message[TOutput]] = deque()

    def submit(self, message: Message[Union[FilteredPayload, TInput]]) -> None:
        assert not self.__closed
        if self.__pending:
            raise MessageRejected

        if isinstance(message.payload, FilteredPayload):
            self.__next_step.submit(
                cast(Message[Union[FilteredPayload, TOutput]], message)
            )
            return

        iterable = list(self.__generator(message.payload))
        num_messages = len(iterable)

        store_remaining_messages = False

        for i, value in enumerate(iterable):
            next_message = Message(value=value)
            # If generator did not provide committable, patch our own
            # committable onto it
            if i == num_messages - 1 and not next_message.committable:
                next_message = Message(
                    Value(
                        next_message.payload,
                        message.committable,
                        next_message.timestamp,
                    )
                )

            if store_remaining_messages == False:
                try:
                    self.__next_step.submit(next_message)
                except MessageRejected:
                    # All remaining messages are stored in self.__pending ready for
                    # the next call to `poll`.
                    store_remaining_messages = True
                    self.__pending.append(next_message)
            else:
                self.__pending.append(next_message)

    def __flush(self) -> None:
        while self.__pending:
            try:
                message = self.__pending[0]
                self.__next_step.submit(message)
                self.__pending.popleft()
            except MessageRejected:
                break

    def poll(self) -> None:
        self.__flush()
        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None
        while deadline is None or time.time() < deadline:
            if self.__pending:
                self.__flush()
            else:
                break

        self.__next_step.close()
        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
