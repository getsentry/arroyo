import logging
import multiprocessing
import pickle
import signal
import time
from collections import deque
from dataclasses import dataclass
from functools import partial
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.pool import AsyncResult, Pool
from multiprocessing.shared_memory import SharedMemory
from pickle import PickleBuffer
from typing import (
    Any,
    Callable,
    Deque,
    Generic,
    Iterator,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from arroyo.dlq import InvalidMessage, InvalidMessageState
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload
from arroyo.utils.metrics import Gauge, get_metrics

logger = logging.getLogger(__name__)

__all__ = ["RunTaskWithMultiprocessing"]

TResult = TypeVar("TResult")
TBatchValue = TypeVar("TBatchValue")

LOG_THRESHOLD_TIME = 20  # In seconds


class ChildProcessTerminated(RuntimeError):
    pass


class NextStepTimeoutError(RuntimeError):
    pass


# A serialized message is composed of a pickled ``Message`` instance (bytes)
# and a sequence of ``(offset, length)`` that referenced locations in a shared
# memory block for out of band buffer transfer.
SerializedBatchValue = Tuple[bytes, Sequence[Tuple[int, int]]]


class ValueTooLarge(ValueError):
    """
    Raised when a value is too large to be written to a shared memory block.
    """


class MessageBatch(Generic[TBatchValue]):
    """
    Contains a sequence of ``Message`` instances that are intended to be
    shared across processes.
    """

    def __init__(self, block: SharedMemory) -> None:
        self.block = block
        self.__items: MutableSequence[SerializedBatchValue] = []
        self.__offset = 0
        self.__iter_offset = 0

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items, {self.__offset} bytes>"

    def __len__(self) -> int:
        return len(self.__items)

    def get_content_size(self) -> int:
        return self.__offset

    def __getitem__(self, index: int) -> TBatchValue:
        """
        Get a message in this batch by its index.

        The message returned by this method is effectively a copy of the
        original message within this batch, and may be safely passed
        around without requiring any special accomodation to keep the shared
        block open or free from conflicting updates.
        """
        data, buffers = self.__items[index]
        # The buffers read from the shared memory block are converted to
        # ``bytes`` rather than being forwarded as ``memoryview`` for two
        # reasons. First, true buffer support protocol is still pretty rare (at
        # writing, it is not supported by either standard library ``json`` or
        # ``rapidjson``, nor by the Confluent Kafka producer), so we'd be
        # copying these values at a later stage anyway. Second, copying these
        # values from the shared memory block (rather than referencing location
        # within it) means that we do not have to ensure that the shared memory
        # block is not recycled during the life of one of these ``Message``
        # instances. If the shared memory block was reused for a different
        # batch while one of the ``Message`` instances returned by this method
        # was still "alive" in a different part of the processing pipeline, the
        # contents of the message would be liable to be corrupted (at best --
        # possibly causing a data leak/security issue at worst.)
        return cast(
            TBatchValue,
            pickle.loads(
                data,
                buffers=[
                    self.block.buf[offset : offset + length].tobytes()
                    for offset, length in buffers
                ],
            ),
        )

    def __iter__(self) -> Iterator[Tuple[int, TBatchValue]]:
        """
        Iterate through the messages contained in this batch.

        See ``__getitem__`` for more details about the ``Message`` instances
        yielded by the iterator returned by this method.
        """
        for i in range(self.__iter_offset, len(self.__items)):
            yield i, self[i]

    def reset_iterator(self, iter_offset: int) -> None:
        self.__iter_offset = iter_offset

    def append(self, message: TBatchValue) -> None:
        """
        Add a message to this batch.

        Internally, this serializes the message using ``pickle`` (effectively
        creating a copy of the input), writing any data that supports
        out-of-band buffer transfer via the ``PickleBuffer`` interface to the
        shared memory block associated with this batch. If there is not
        enough space in the shared memory block to write all buffers to be
        transferred out-of-band, this method will raise a ``ValueTooLarge``
        error.
        """
        buffers: MutableSequence[Tuple[int, int]] = []

        def buffer_callback(buffer: PickleBuffer) -> None:
            value = buffer.raw()
            offset = self.__offset
            length = len(value)
            if offset + length > self.block.size:
                raise ValueTooLarge(
                    f"value exceeds available space in block, {length} "
                    f"bytes needed but {self.block.size - offset} bytes free"
                )
            self.block.buf[offset : offset + length] = value
            self.__offset += length
            buffers.append((offset, length))

        data = pickle.dumps(message, protocol=5, buffer_callback=buffer_callback)

        self.__items.append((data, buffers))


class BatchBuilder(Generic[TBatchValue]):
    def __init__(
        self,
        batch: MessageBatch[TBatchValue],
        max_batch_size: int,
        max_batch_time: float,
    ) -> None:
        self.__batch = batch
        self.__max_batch_size = max_batch_size
        self.__deadline = time.time() + max_batch_time

    def __len__(self) -> int:
        return len(self.__batch)

    def append(self, message: TBatchValue) -> None:
        self.__batch.append(message)

    def ready(self) -> bool:
        if len(self.__batch) >= self.__max_batch_size:
            return True
        elif time.time() >= self.__deadline:
            return True
        else:
            return False

    def build(self) -> MessageBatch[TBatchValue]:
        return self.__batch


def parallel_worker_initializer(
    custom_initialize_func: Optional[Callable[[], None]] = None
) -> None:
    # Worker process should ignore ``SIGINT`` so that processing is not
    # interrupted by ``KeyboardInterrupt`` during graceful shutdown.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    if custom_initialize_func is not None:
        custom_initialize_func()


@dataclass
class ParallelRunTaskResult(Generic[TResult]):
    """
    Results of applying the run task function to a batch
    of messages in parallel.
    """

    next_index_to_process: int
    valid_messages_transformed: MessageBatch[
        Union[InvalidMessage, Message[Union[FilteredPayload, TResult]]]
    ]


def parallel_run_task_worker_apply(
    function: Callable[[Message[TStrategyPayload]], TResult],
    input_batch: MessageBatch[Message[Union[FilteredPayload, TStrategyPayload]]],
    output_block: SharedMemory,
    start_index: int = 0,
) -> ParallelRunTaskResult[TResult]:

    valid_messages_transformed: MessageBatch[
        Union[InvalidMessage, Message[Union[FilteredPayload, TResult]]]
    ] = MessageBatch(output_block)

    next_index_to_process = start_index
    while next_index_to_process < len(input_batch):
        message = input_batch[next_index_to_process]

        # Theory: Doing this check in the subprocess is cheaper because we
        # shouldn't get a high volume of FilteredPayloads on average.
        if isinstance(message.value.payload, FilteredPayload):
            valid_messages_transformed.append(
                cast(Message[Union[FilteredPayload, TResult]], message)
            )
            next_index_to_process += 1
            continue

        payload: Union[InvalidMessage, Message[Union[FilteredPayload, TResult]]]

        try:
            payload = message.replace(
                function(cast(Message[TStrategyPayload], message))
            )
        except InvalidMessage as e:
            payload = e
        except Exception:
            # The remote traceback thrown when retrieving the result from the
            # pool omits a lot of useful data (and usually includes a
            # truncated traceback), logging it here allows us to get this
            # information at the expense of sending duplicate events to Sentry
            # (one from the child and one from the parent.)
            logger.warning(
                "Caught exception while applying %r to %r!",
                function,
                message,
                exc_info=True,
            )
            raise

        try:
            valid_messages_transformed.append(payload)
        except ValueTooLarge:
            # If the output batch cannot accept the transformed message when
            # the batch is empty, we'll never be able to write it and should
            # error instead of retrying. Otherwise, we need to return the
            # values we've already accumulated and continue processing later.
            if len(valid_messages_transformed) == 0:
                raise
            else:
                break
        else:
            next_index_to_process += 1

    return ParallelRunTaskResult(next_index_to_process, valid_messages_transformed)


class RunTaskWithMultiprocessing(
    ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    Generic[TStrategyPayload, TResult],
):
    """
    Run a function in parallel across messages using subprocesses.

    ``RunTaskWithMultiprocessing`` uses the ``multiprocessing`` stdlib module to
    transform messages in parallel.

    :param function: The function to use for transforming.
    :param next_step: The processing strategy to forward transformed messages to.
    :param num_processes: The number of processes to spawn.
    :param max_batch_size: Wait at most for this many messages before "closing" a batch.
    :param max_batch_time: Wait at most for this many seconds before closing a batch.

    :param input_block_size: For each subprocess, a shared memory buffer of
        ``input_block_size`` is allocated. This value should be at least
        `message_size * max_batch_size` large, where `message_size` is the expected
        average message size.

        If the value is too small, the batch is implicitly broken up. In that
        case, the ``batch.input.overflow`` metric is emitted.

    :param output_block_size: Size of the shared memory buffer used to store
        results. Like with input data, the batch is implicitly broken up on
        overflow, and ``batch.output.overflow`` metric is incremented.

        While not entirely avoidable, it is best to avoid hitting overflow too
        often on either buffer. The performance implications of running into
        overflow on the output buffer can be particularly significant.

    :param initializer: A function to run at the beginning of each subprocess.

        Subprocesses are spawned without any of the state of the parent
        process, they are entirely new Python interpreters. You might want to
        re-initialize your Django application here.
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
        num_processes: int,
        max_batch_size: int,
        max_batch_time: float,
        input_block_size: int,
        output_block_size: int,
        initializer: Optional[Callable[[], None]] = None,
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        self.__shared_memory_manager = SharedMemoryManager()
        self.__shared_memory_manager.start()

        self.__pool = Pool(
            num_processes,
            initializer=partial(parallel_worker_initializer, initializer),
            context=multiprocessing.get_context("spawn"),
        )

        self.__input_blocks = [
            self.__shared_memory_manager.SharedMemory(input_block_size)
            for _ in range(num_processes)
        ]

        self.__output_blocks = [
            self.__shared_memory_manager.SharedMemory(output_block_size)
            for _ in range(num_processes)
        ]

        self.__batch_builder: Optional[
            BatchBuilder[
                Union[InvalidMessage, Message[Union[FilteredPayload, TStrategyPayload]]]
            ]
        ] = None

        self.__processes: Deque[
            Tuple[
                MessageBatch[
                    Union[
                        InvalidMessage,
                        Message[Union[FilteredPayload, TStrategyPayload]],
                    ]
                ],
                AsyncResult[ParallelRunTaskResult[TResult]],
            ]
        ] = deque()
        self.__invalid_messages = InvalidMessageState()

        self.__metrics = get_metrics()
        self.__batches_in_progress = Gauge(self.__metrics, "batches_in_progress")
        self.__pool_waiting_time: Optional[float] = None
        self.__metrics.gauge("transform.processes", num_processes)

        self.__closed = False

        def handle_sigchld(signum: int, frame: Any) -> None:
            # Terminates the consumer if any child process of the
            # consumer is terminated.
            # This is meant to detect the unexpected termination of
            # multiprocessor pool workers.
            if not self.__closed:
                self.__metrics.increment("sigchld.detected")
                raise ChildProcessTerminated(signum)

        signal.signal(signal.SIGCHLD, handle_sigchld)

    def __submit_batch(self) -> None:
        assert self.__batch_builder is not None
        batch = self.__batch_builder.build()
        logger.debug("Submitting %r to %r...", batch, self.__pool)
        self.__processes.append(
            (
                batch,
                self.__pool.apply_async(
                    parallel_run_task_worker_apply,
                    (self.__transform_function, batch, self.__output_blocks.pop()),
                ),
            )
        )
        self.__batches_in_progress.increment()
        self.__metrics.timing("batch.size.msg", len(batch))
        self.__metrics.timing("batch.size.bytes", batch.get_content_size())
        self.__batch_builder = None

    def __forward_invalid_offsets(self) -> None:
        if len(self.__invalid_messages):
            self.__next_step.poll()
            filter_msg = self.__invalid_messages.build()
            if filter_msg:
                try:
                    self.__next_step.submit(filter_msg)
                    self.__invalid_messages.reset()
                except MessageRejected:
                    pass

    def __check_for_results(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None

        while self.__processes:
            try:
                self.__check_for_results_impl(
                    timeout=max(deadline - time.time(), 0)
                    if deadline is not None
                    else None
                )
            except NextStepTimeoutError:
                if deadline is None or deadline > time.time():
                    continue
                break
            except multiprocessing.TimeoutError:
                if self.__pool_waiting_time is None:
                    self.__pool_waiting_time = time.time()
                else:
                    current_time = time.time()
                    if current_time - self.__pool_waiting_time > LOG_THRESHOLD_TIME:
                        logger.warning(
                            "Waited on the process pool longer than %d seconds. Waiting for %d results. Pool: %r",
                            LOG_THRESHOLD_TIME,
                            len(self.__processes),
                            self.__pool,
                        )
                        self.__pool_waiting_time = current_time
                break
            else:
                self.__pool_waiting_time = None

    def __check_for_results_impl(self, timeout: Optional[float] = None) -> None:
        input_batch, async_result = self.__processes[0]

        # If this call is being made in a context where it is intended to be
        # nonblocking, checking if the result is ready (rather than trying to
        # retrieve the result itself) avoids costly synchronization.
        if timeout == 0 and not async_result.ready():
            # ``multiprocessing.TimeoutError`` (rather than builtin
            # ``TimeoutError``) maintains consistency with ``AsyncResult.get``.
            raise multiprocessing.TimeoutError()

        result = async_result.get(timeout=timeout)

        for idx, message in result.valid_messages_transformed:
            if isinstance(message, InvalidMessage):
                # For the next invocation of __check_for_results, skip over this message
                result.valid_messages_transformed.reset_iterator(idx + 1)
                self.__invalid_messages.append(message)
                raise message

            try:
                self.__next_step.poll()
                self.__next_step.submit(message)

            except MessageRejected:
                # For the next invocation of __check_for_results, start at this message
                result.valid_messages_transformed.reset_iterator(idx)

                self.__metrics.increment(
                    "arroyo.strategies.run_task_with_multiprocessing.batch.backpressure"
                )
                raise NextStepTimeoutError()

        if result.next_index_to_process != len(input_batch):
            self.__metrics.increment(
                "arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow"
            )

            logger.warning(
                "Received incomplete batch (%0.2f%% complete), resubmitting",
                result.next_index_to_process / len(input_batch) * 100,
            )

            # TODO: This reserializes all the ``SerializedMessage`` data prior
            # to the processed index even though the values at those indices
            # will never be unpacked. It probably makes sense to remove that
            # data from the batch to avoid unnecessary serialization overhead.
            self.__processes[0] = (
                input_batch,
                self.__pool.apply_async(
                    parallel_run_task_worker_apply,
                    (
                        self.__transform_function,
                        input_batch,
                        result.valid_messages_transformed.block,
                        result.next_index_to_process,
                    ),
                ),
            )
            return

        logger.debug("Completed %r, reclaiming blocks...", input_batch)
        self.__input_blocks.append(input_batch.block)
        self.__output_blocks.append(result.valid_messages_transformed.block)
        self.__batches_in_progress.decrement()

        del self.__processes[0]

    def poll(self) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.poll()

        self.__check_for_results(timeout=0)

        if self.__batch_builder is not None and self.__batch_builder.ready():
            self.__submit_batch()

    def __reset_batch_builder(self) -> None:
        try:
            input_block = self.__input_blocks.pop()
        except IndexError as e:
            raise MessageRejected("no available input blocks") from e

        self.__batch_builder = BatchBuilder(
            MessageBatch(input_block),
            self.__max_batch_size,
            self.__max_batch_time,
        )

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed

        if self.__batch_builder is None:
            self.__reset_batch_builder()
            assert self.__batch_builder is not None

        try:
            self.__batch_builder.append(message)
        except ValueTooLarge as e:
            logger.debug("Caught %r, closing batch and retrying...", e)
            self.__metrics.increment(
                "arroyo.strategies.run_task_with_multiprocessing.batch.input.overflow"
            )
            self.__submit_batch()

            # This may raise ``MessageRejected`` (if all of the shared memory
            # is in use) and create backpressure.
            self.__reset_batch_builder()
            assert self.__batch_builder is not None

            # If this raises ``ValueTooLarge``, that means that the input block
            # size is too small (smaller than the Kafka payload limit without
            # compression.)
            self.__batch_builder.append(message)

    def close(self) -> None:
        self.__closed = True

        if self.__batch_builder is not None and len(self.__batch_builder) > 0:
            self.__submit_batch()

    def terminate(self) -> None:
        self.__closed = True

        logger.info("Terminating %r...", self.__pool)
        self.__pool.terminate()

        logger.info("Shutting down %r...", self.__shared_memory_manager)
        self.__shared_memory_manager.shutdown()

        logger.info("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None
        self.__forward_invalid_offsets()

        logger.debug("Waiting for %s batches...", len(self.__processes))

        self.__check_for_results(
            timeout=timeout,
        )

        logger.debug("Waiting for %s...", self.__pool)
        self.__pool.terminate()

        self.__shared_memory_manager.shutdown()

        self.__next_step.close()
        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
