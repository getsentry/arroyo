import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Deque, Optional, Tuple, TypeVar

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import Commit, Message, TPayload

logger = logging.getLogger(__name__)

TOutput = TypeVar("TOutput")


class RunTaskInThreads(ProcessingStrategy[TPayload]):
    """
    This strategy can be used to run IO-bound tasks in parallel, then commit offsets.

    The user specifies a processing function (a callable that takes a message). For each message received
    in the submit method, it runs that processing function. Once completed, offsets are committed.
    Note that the return value of the processing function is discarded so this is not an appropriate
    strategy if something else needs to happen after offsets are committed and the function returns.

    Since the processing function will be run in threads, avoid using objects which can be modified
    by different threads or protect it using locks.

    If there are too many pending futures, we MessageRejected will be raised to notify the stream processor
    to slow down.

    On poll we check for completion of futures. If processing is done, we commit the offsetes.
    If an error occured the original exception will be raised.

    Caution: MessageRejected is not properly handled by the ParallelTransform step. Exercise
    caution if chaining this step anywhere after a parallel transform.
    """

    def __init__(
        self,
        processing_function: Callable[[Message[TPayload]], TOutput],
        concurrency: int,
        max_pending_futures: int,
        commit: Commit,
    ) -> None:
        self.__executor = ThreadPoolExecutor(max_workers=concurrency)
        self.__function = processing_function
        self.__queue: Deque[Tuple[Message[TPayload], Future[TOutput]]] = deque()
        self.__max_pending_futures = max_pending_futures
        self.__commit = commit
        self.__closed = False

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed
        # The list of pending futures is too long, tell the stream processor to slow down
        if len(self.__queue) > self.__max_pending_futures:
            raise MessageRejected

        self.__queue.append((message, self.__executor.submit(self.__function, message)))

    def poll(self) -> None:
        while self.__queue:
            message, future = self.__queue[0]

            if not future.done():
                break

            # Ensure exception gets raised
            future.result()

            self.__queue.popleft()

            self.__commit({message.partition: message.position_to_commit})

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        self.__commit({}, force=True)

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            message, future = self.__queue.popleft()

            future.result(remaining)

            self.__commit(
                {message.partition: message.position_to_commit},
                force=True,
            )

        self.__executor.shutdown()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__executor.shutdown()
