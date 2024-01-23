import time
from datetime import datetime
from typing import (
    Generic,
    MutableMapping,
    Optional,
    TypeVar,
    Union,
    cast,
    Protocol,
)

from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.types import BaseValue, FilteredPayload, Message, Partition, Value
from arroyo.utils.metrics import get_metrics

TPayload = TypeVar("TPayload", contravariant=True)
TResult = TypeVar("TResult", covariant=True)


# Does the protocol need to be generic over the payload and result?
class BufferProtocol(Protocol[TPayload, TResult]):
    """Reduce strategy buffer protocol.

    Buffer is agnostic to the caller. It accepts messages and describes its state to caller's
    using its internal state. This is by design to reduce coupling between the Buffer and its
    implementer.
    """

    @property
    def buffer(self) -> TResult:
        """Return a TResult buffer which will (likely) be committed by the next-step."""
        ...

    @property
    def is_empty(self) -> bool:
        """Return "True" if the buffer is empty.

        Because a buffer could have a complex implementation, the caller has no idea if the
        buffer is empty unless the buffer exposes it.
        """
        ...

    @property
    def is_ready(self) -> bool:
        """Return "True" if the buffer is ready to be committed.

        Determined by message count, buffer size, the time of day, the phase of the moon,
        whatever you want.
        """
        ...

    def append(self, message: BaseValue[TPayload]) -> None:
        """Accept a TPayload mutating the internal state of the batch builder."""
        ...

    def new(self) -> "BufferProtocol[TPayload, TResult]":
        """Return a new instance of the "BufferProtocol" class.

        This is defined as an instance method (as opposed to a factory method) because we do
        not want the Reduce strategy to know _anything_ about the buffer's implementation. The
        buffer is passed to the stategy as an instance. The strategy does not need to concern
        itself with the arguments required to construct the instance.

        This method could be replaced with a "reset" method but a reset may not be so simple and
        may lead to unexpected bugs. However, constructing a new instance should be a relatively
        simple proposition by comparison. This also introduces the possibility of a buffer which
        dynamically manages its commit interval (intentionally or not). Responsibile
        implementation is required.
        """
        ...


class Buffer(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    """Accumulates messages until the buffer declares its ready to commit. The accumulator
    function is run on each message in the order it is received.

    Once the "batch" is full, the accumulated value is submitted to the next step.

    This strategy propagates `MessageRejected` exceptions from the
    downstream steps if they are thrown.
    """

    def __init__(
        self,
        buffer: BufferProtocol[TPayload, TResult],
        next_step: ProcessingStrategy[TResult],
    ) -> None:
        self.__buffer = buffer
        self.__next_step = next_step
        self.__metrics = get_metrics()
        self.__closed = False

        # Previously these values were tracked in the "BatchBuilder" class. That's fine if
        # BatchBuilder is internal to the Arroyo library however if the "BatchBuilder" is defined
        # by the implementer, you are then asking the implementer to manage responsibly manage
        # offsets (which can be tricky). Therefore, we manage it within the strategy and allow
        # the buffer to be ignorant of its caller.
        self.__last_timestamp: Optional[datetime] = None
        self.__offsets: MutableMapping[Partition, int] = {}
        self.__init_time = time.time()

    def __flush(self, force: bool) -> None:
        # You might want to ask the buffer if its empty or not rather than assuming. It may
        # very well be holding multiple buffers which fill at non-uniform rates.
        if self.__buffer.is_empty:
            return None

        # If you're not forcing me and I'm not ready to commit then I'm not doing anything.
        if not force and not self.__buffer.is_ready:
            return None

        # Push the buffer to the next step.
        assert isinstance(self.__last_timestamp, datetime)

        buffer_msg = Message(
            Value(
                payload=self.__buffer.buffer,
                committable=self.__offsets,
                timestamp=self.__last_timestamp,
            )
        )
        self.__next_step.submit(buffer_msg)
        self.__metrics.timing(
            "arroyo.strategies.reduce.batch_time",
            time.time() - self.__init_time,
        )

        # Reset to the empty state.
        self.__buffer = self.__buffer.new()
        self.__last_timestamp = None
        self.__offsets = {}
        self.__init_time = time.time()

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

        self.__flush(force=False)

        self.__offsets.update(message.value.committable)
        self.__last_timestamp = message.value.timestamp
        self.__buffer.append(cast(BaseValue[TPayload], message.value))

    def poll(self) -> None:
        assert not self.__closed

        try:
            self.__flush(force=False)
        except MessageRejected:
            pass

        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Terminates the strategy by joining the following step.
        This method tries to flush the current batch no matter
        whether the batch is ready or not.
        """
        deadline = time.time() + timeout if timeout is not None else None
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


__all__ = ["Buffer"]
