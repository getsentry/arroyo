import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Deque, Optional, Tuple

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import Commit, Message, TPayload

logger = logging.getLogger(__name__)


class RunTask(ProcessingStrategy[TPayload]):
    """
    Runs a task in a thread. Commits offsets once futures are done.
    """

    def __init__(
        self,
        processing_function: Callable[[Message[TPayload]], None],
        concurrency: int,
        commit: Commit,
    ) -> None:
        self.__executor = ThreadPoolExecutor(max_workers=concurrency)
        self.__function = processing_function
        self.__queue: Deque[Tuple[Message[TPayload], Future[None]]] = deque()
        self.__max_pending_futures = concurrency * 2
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
            message, future = self.__queue.popleft()

            if not future.done():
                break

            exc = future.exception()

            if exc is not None:
                raise exc

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
