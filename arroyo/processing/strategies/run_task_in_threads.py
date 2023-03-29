import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Deque, Generic, Optional, Tuple, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload

logger = logging.getLogger(__name__)

TResult = TypeVar("TResult")


class RunTaskInThreads(
    ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    Generic[TStrategyPayload, TResult],
):
    """
    This strategy can be used to run IO-bound tasks in parallel.

    The user specifies a processing function (a callable that takes a message). For each message received
    in the submit method, it runs that processing function. Once completed, the message is submitted
    to the next step (with the payload containing the result of the processing function).

    Since the processing function will be run in threads, avoid using objects which can be modified
    by different threads or protect it using locks.

    If there are too many pending futures, we MessageRejected will be raised to notify the stream processor
    to slow down.

    On poll we check for completion of futures. If processing is done, we submit to the next step.
    If an error occured the original exception will be raised.
    """

    def __init__(
        self,
        processing_function: Callable[[Message[TStrategyPayload]], TResult],
        concurrency: int,
        max_pending_futures: int,
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
    ) -> None:
        self.__executor = ThreadPoolExecutor(max_workers=concurrency)
        self.__function = processing_function
        self.__queue: Deque[
            Tuple[
                Message[Union[FilteredPayload, TStrategyPayload]],
                Optional[Future[TResult]],
            ]
        ] = deque()
        self.__max_pending_futures = max_pending_futures
        self.__next_step = next_step
        self.__closed = False

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed
        # The list of pending futures is too long, tell the stream processor to slow down
        if len(self.__queue) > self.__max_pending_futures:
            raise MessageRejected

        future: Optional[Future[TResult]]
        if not isinstance(message.payload, FilteredPayload):
            future = self.__executor.submit(
                self.__function, cast(Message[TStrategyPayload], message)
            )
        else:
            future = None

        self.__queue.append((message, future))

    def poll(self) -> None:
        while self.__queue:
            message, future = self.__queue[0]
            next_message: Message[TResult]

            if future is not None:
                if not future.done():
                    break

                # Will raise if the future errored
                result = future.result()
                payload = message.value.replace(result)
                next_message = Message(payload)
            else:
                # The message is filtered, and therefore the payload is
                # FilteredPayload
                next_message = cast(Message[TResult], message)

            self.__queue.popleft()
            self.__next_step.poll()
            self.__next_step.submit(next_message)

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, future = self.__queue.popleft()
            next_message: Message[TResult]

            if future is not None:
                # Will raise if the future errored
                result = future.result(remaining)
                payload = message.value.replace(result)
                next_message = Message(payload)
            else:
                # The message is filtered, and therefore the payload is
                # FilteredPayload
                next_message = cast(Message[TResult], message)

            self.__next_step.poll()
            self.__next_step.submit(next_message)

        self.__executor.shutdown()
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__closed = True
        self.__next_step.close()

    def terminate(self) -> None:
        self.__closed = True
        self.__executor.shutdown()
        self.__next_step.terminate()
