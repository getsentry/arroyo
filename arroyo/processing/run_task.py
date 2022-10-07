import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Deque, Optional, Tuple

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import Commit, Message, Position, TPayload

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
        self.__futures: Deque[Tuple[Message[TPayload], Future[None]]] = deque()
        self.__max_pending_futures = concurrency * 2
        self.__commit = commit
        self.__closed = False

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed
        # The list of pending futures is too long, tell the stream processor to slow down
        if len(self.__futures) > self.__max_pending_futures:
            raise MessageRejected

        self.__futures.append(
            (message, self.__executor.submit(self.__function, message))
        )

    def poll(self) -> None:
        # Remove completed futures in order
        while self.__futures and self.__futures[0][1].done():
            message, _ = self.__futures.popleft()

            self.__commit(
                {message.partition: Position(message.next_offset, message.timestamp)}
            )

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        self.__commit({}, force=True)

        while self.__futures:
            remaining = timeout - (time.time() - start) if timeout is not None else None
            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__futures)} futures in queue")
                break

            message, future = self.__futures.popleft()

            future.result(remaining)

            self.__commit(
                {message.partition: Position(message.offset, message.timestamp)},
                force=True,
            )

        self.__executor.shutdown()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__executor.shutdown()
