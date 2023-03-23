import time
from collections import deque
from typing import Callable, Collection, Deque, Generic, Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Value

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class Unfold(
    ProcessingStrategy[Union[FilteredPayload, TInput]], Generic[TInput, TOutput]
):
    """
    Unfold receives a message and explodes it to generate a collection of
    messages submitting them one by one to the next step. The generated
    messages are created according to the generator function provided by the user.

    The generator function provided must return a collection (i.e. a class that
    implements sized + iterable).

    If this step receives a `MessageRejected` exception from the next
    step it keeps the remaining messages and attempts to submit
    them on subsequent calls to `poll`
    """

    def __init__(
        self,
        generator: Callable[[TInput], Collection[TOutput]],
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

        iterable = self.__generator(message.payload)
        num_messages = len(iterable)

        store_remaining_messages = False

        for idx, value in enumerate(iterable):
            # Last message is special because offsets can be committed along with it
            if idx == num_messages - 1:
                next_message = Message(Value(value, message.committable))
            else:
                next_message = Message(Value(value, {}))

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
                pass

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
