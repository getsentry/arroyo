import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from typing import Callable, Generic, MutableMapping, Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.types import Message, Partition, Position, TPayload, Value
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)


class Batch(Generic[TPayload]):
    def __init__(
        self,
        step: ProcessingStep[TPayload],
    ) -> None:
        self.__step = step

        self.__created = time.time()
        self.__length = 0
        self.offsets: MutableMapping[Partition, Position] = {}
        self.__closed = False

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} message{'s' if len(self) != 1 else ''}, open for {self.duration():0.2f} seconds>"

    def __len__(self) -> int:
        return self.__length

    def duration(self) -> float:
        return time.time() - self.__created

    def poll(self) -> None:
        self.__step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        self.__step.submit(message)
        self.__length += 1

        for (partition, position) in message.committable.items():
            self.offsets[partition] = position

    def close(self) -> None:
        self.__closed = True
        self.__step.close()

    def terminate(self) -> None:
        self.__closed = True

        logger.info("Terminating %r...", self.__step)
        self.__step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__step.join(timeout)


class CollectStep(ProcessingStep[TPayload]):
    """
    Collects messages into batches, periodically closing the batch and
    committing the offsets once the batch has successfully been closed.
    """

    def __init__(
        self,
        step_factory: Callable[[], ProcessingStep[TPayload]],
        next_step: ProcessingStep[Batch[TPayload]],
        max_batch_size: int,
        max_batch_time: float,
    ) -> None:
        self.__step_factory = step_factory
        self.__next_step = next_step
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        self.batch: Optional[Batch[TPayload]] = None
        self.__closed = False
        self._metrics = get_metrics()
        self._collect_poll_time = 0

    def close_and_reset_batch(self) -> None:
        assert self.batch is not None
        self.batch.close()
        self.batch.join()
        logger.info("Completed processing %r.", self.batch)
        self.__next_step.submit(Message(Value(self.batch, self.batch.offsets)))
        self.batch = None
        self._metrics.timing("collect.poll.time", self._collect_poll_time)
        self._collect_poll_time = 0

    def poll(self) -> None:
        start_time = time.time()
        if self.batch is None:
            return

        self.batch.poll()

        # XXX: This adds a substantially blocking operation to the ``poll``
        # method which is bad.
        if len(self.batch) >= self.__max_batch_size:
            logger.debug("Size limit reached, closing %r...", self.batch)
            start_time = time.time()
            self.close_and_reset_batch()
            self._metrics.timing(
                "collect.reset_batch", (time.time() - start_time) * 1000
            )
        elif self.batch.duration() >= self.__max_batch_time:
            logger.debug("Time limit reached, closing %r...", self.batch)
            start_time = time.time()
            self.close_and_reset_batch()
            self._metrics.timing(
                "collect.reset_batch", (time.time() - start_time) * 1000
            )

        self._collect_poll_time += int(time.time() - start_time) * 1000

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.batch is None:
            self.batch = Batch(self.__step_factory())

        self.batch.submit(message)

    def close(self) -> None:
        self.__closed = True

        if self.batch is not None:
            logger.debug("Closing %r...", self.batch)
            self.batch.close()

    def terminate(self) -> None:
        self.__closed = True

        if self.batch is not None:
            self.batch.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        if self.batch is not None:
            self.batch.join(timeout)
            self.__next_step.submit(Message(Value(self.batch, self.batch.offsets)))
            logger.info("Completed processing %r.", self.batch)
            self.batch = None


class ParallelCollectStep(CollectStep[TPayload]):
    """
    ParallelCollectStep is similar to CollectStep except it allows the closing and reset of the
    batch to happen in a threadpool. This is useful if closing the batch involes IO-bound tasks
    such as flushing to a remote storage system. The next batch can start getting filled while
    the previous batch is being closed.

    The threadpool has 1 worker since we want to ensure batches are processed sequentially
    and passed to the next step in order.
    """

    def __init__(
        self,
        step_factory: Callable[[], ProcessingStep[TPayload]],
        next_step: ProcessingStep[Batch[TPayload]],
        max_batch_size: int,
        max_batch_time: float,
        wait_timeout: float = 10.0,
    ):
        super().__init__(step_factory, next_step, max_batch_size, max_batch_time)
        self.__threadpool = ThreadPoolExecutor(max_workers=1)
        self.future: Optional[Future[None]] = None
        self.wait_timeout = wait_timeout
        self.__finish_batch = partial(self.__finish_batch_and_submit, next_step)

    def close_and_reset_batch(self) -> None:
        """
        Closes the current batch in an asynchronous manner. Waits for previous work to be completed before
        proceeding the next one. We can provide the existing batch to the threadpool since the collector
        is going to make a new batch.
        """
        if self.future:
            # If any exceptions are raised they should get bubbled up.
            self.future.result(timeout=self.wait_timeout)

        assert self.batch is not None
        self.future = self.__threadpool.submit(self.__finish_batch, batch=self.batch)
        self.batch = None
        self._metrics.timing("collect.poll.time", self._collect_poll_time)
        self._collect_poll_time = 0

    @staticmethod
    def __finish_batch_and_submit(
        next_step: ProcessingStep[Batch[TPayload]], batch: Batch[TPayload]
    ) -> None:
        assert batch is not None

        batch.close()
        batch.join()
        logger.info("Completed processing %r.", batch)
        next_step.submit(Message(Value(batch, batch.offsets)))

    def join(self, timeout: Optional[float] = None) -> None:
        work_time = 0.0
        # We should finish the previous batch before proceeding to the finish the existing one.
        if self.future is not None:
            previous_time = time.time()
            self.future.result(timeout)
            work_time = time.time() - previous_time

        self.__threadpool.shutdown()

        super().join((timeout - work_time) if timeout else None)
