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

DEFAULT_INPUT_BLOCK_SIZE = 16 * 1024 * 1024
DEFAULT_OUTPUT_BLOCK_SIZE = 16 * 1024 * 1024

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

    def __init__(self, block: SharedMemory, overflow_error_message: str) -> None:
        self.block = block
        self.__items: MutableSequence[SerializedBatchValue] = []
        self.__offset = 0
        self.__iter_offset = 0
        self.__overflow_error_message = overflow_error_message

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
                    f"{self.__overflow_error_message}\n\n"
                    f"Value exceeds available space in block, {length} "
                    f"bytes needed but {self.block.size - offset} bytes free."
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
    custom_initialize_func: Optional[Callable[[], None]] = None,
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
    ] = MessageBatch(
        output_block,
        "A message was too large for the output block. Consider increasing --output-block-size",
    )

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


class MultiprocessingPool:
    """
    Multiprocessing pool for the RunTaskWithMultiprocessing strategy.
    It can be re-used each time the strategy is created on assignments.

    NOTE: The close() method must be called when shutting down the consumer.

    The `close()` method is also called by `RunTaskWithMultiprocessing` when
    there are uncompleted pending tasks to ensure no state is carried over.
    The `maybe_create_pool` function is called on every assignment to ensure
    the pool is re-created again, in case it was closed on the previous recovation.

    :param num_processes: The number of processes to spawn.

    :param initializer: A function to run at the beginning of each subprocess.

        Subprocesses are spawned without any of the state of the parent
        process, they are entirely new Python interpreters. You might want to
        re-initialize your Django application here.

    """

    def __init__(
        self,
        num_processes: int,
        initializer: Optional[Callable[[], None]] = None,
    ) -> None:
        self.__num_processes = num_processes
        self.__initializer = initializer
        self.__pool: Optional[Pool] = None
        self.__metrics = get_metrics()
        self.maybe_create_pool()

    def maybe_create_pool(self) -> None:
        if self.__pool is None:
            self.__metrics.increment(
                "arroyo.strategies.run_task_with_multiprocessing.pool.create"
            )
            self.__pool = Pool(
                self.__num_processes,
                initializer=partial(parallel_worker_initializer, self.__initializer),
                context=multiprocessing.get_context("spawn"),
            )

    @property
    def num_processes(self) -> int:
        return self.__num_processes

    @property
    def initializer(self) -> Optional[Callable[[], None]]:
        return self.__initializer

    def apply_async(self, *args: Any, **kwargs: Any) -> Any:
        if self.__pool:
            return self.__pool.apply_async(*args, **kwargs)
        else:
            raise RuntimeError("No pool available")

    def close(self) -> None:
        """
        Must be called manually when shutting down the consumer.
        Also called from strategy.join() if there are pending futures in order
        ensure state is completely cleaned up.
        """
        if self.__pool:
            self.__pool.terminate()
            self.__pool = None


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
    :param max_batch_size: Wait at most for this many messages before "closing" a batch.
    :param max_batch_time: Wait at most for this many seconds before closing a batch.

    :param pool: The multiprocessing pool to use for parallel processing. The same pool
        instance can be re-used each time ``RunTaskWithMultiprocessing`` is created on
        rebalance.

    :param input_block_size: For each subprocess, a shared memory buffer of
        ``input_block_size`` is allocated. This value should be at least
        `message_size * max_batch_size` large, where `message_size` is the expected
        average message size.

        If the value is too small, the batch is implicitly broken up. In that
        case, the
        ``arroyo.strategies.run_task_with_multiprocessing.batch.input.overflow``
        metric is emitted.

        If the value is set to `None`, the `input_block_size` is automatically
        adjusted to adapt to traffic. Keep in mind that this is a rather
        experimental feature and less productionized than explicitly setting a
        value.

    :param output_block_size: Size of the shared memory buffer used to store
        results. Like with input data, the batch is implicitly broken up on
        overflow, and
        ``arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow``
        metric is incremented.

        Like with `input_block_size`, the value can be set to `None` to enable
        automatic resizing.

    :param max_input_block_size: If automatic resizing is enabled, this sets an
        upper limit on how large those blocks can get.

    :param max_output_block_size: Same as `max_input_block_size` but for output
        blocks.


    Number of processes
    ~~~~~~~~~~~~~~~~~~~

    The metric
    ``arroyo.strategies.run_task_with_multiprocessing.batches_in_progress``
    shows you how many processes arroyo is able to effectively use at any given
    point.

    The metric ``arroyo.strategies.run_task_with_multiprocessing.processes``
    shows how many processes arroyo was configured with.

    If those two metrics don't line up, your consumer is not bottlenecked on
    number of processes. That's a good thing, you want to have some reserve
    capacity. But it means that increasing ``num_processes`` will not make your
    consumer faster.

    Batching
    ~~~~~~~~

    Arroyo sends messages in batches to subprocesses. ``max_batch_size`` and ``max_batch_time``
    should be tweaked for optimal performance. You can observe the effect in the following metrics:

    * ``arroyo.strategies.run_task_with_multiprocessing.batch.size.msg``: The number of messages per batch.
    * ``arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes``: The number of bytes used per batch.

    The cost of batches (locking, synchronization) generally amortizes with
    increased batch sizes. Too small batches, and this strategy will spend a
    lot of time synchronizing between processes. Too large batches, however,
    can cause your consumer to not use all processes effectively, as a lot of
    time may be spent waiting for batches to fill up.

    If ``batch.size.msg`` is flat (as in, it's a perfectly straight line at a
    constant), you are hitting ``max_batch_size``. If ``batch.size.bytes`` is
    flat, you are hitting input buffer overflow (see next section). If neither
    are flat, you are hitting ``max_batch_time``.

    Input and output buffers
    ~~~~~~~~~~~~~~~~~~~~~~~~

    You want to keep an eye on these metrics:

    1. ``arroyo.strategies.run_task_with_multiprocessing.batch.input.overflow``
    2. ``arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow``
    3. ``arroyo.strategies.run_task_with_multiprocessing.batch.backpressure``

    If ``batch.input.overflow`` is emitted at all, arroyo ran out of memory for
    batching and started breaking up your batches into smaller ones. You want
    to increase ``input_block_size`` in response. Note that when you do this,
    you may have to re-tweak ``max_batch_size`` and ``max_batch_time``, as you
    were never hitting those configured limits before. Input overflow is not
    really all that expensive in Arroyo, but since it affects how batching
    works it can still make performance tuning of your consumer more confusing.
    Best to avoid it anyway.

    If ``batch.output.overflow`` is emitted at all, arroyo ran out of memory
    when *fetching* the data from subprocesses, and so the response from
    subprocesses to the main processes is chunked. Output overflow is very
    expensive, and you want to avoid it. Increase ``output_block_size`` in
    response.

    If ``batch.backpressure`` is continuously emitted, you are not bottlenecked
    on multiprocessing at all, but instead the next strategy can't keep up and
    is applying backpressure. You can likely reduce ``num_processes`` and won't
    notice a performance regression.

    Prefetching
    ~~~~~~~~~~~

    If you set ``prefetch_batches`` to `True`, Arroyo will allocate twice as
    many input blocks as processes, and will prefetch the next batch while the
    current batch is being processed. This can help saturate the process pool to
    increase throughput, but it also increases memory usage.

    Use this option if your consumer is bottlenecked on the multiprocessing step
    but also runs time-consuming tasks in the other steps, like ``Produce`` or
    ``Unfold``. By prefetching batches, the pool can immediately start working
    on the next batch while the current batch is being sent through the next
    steps.

    How to tune your consumer
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Note that it doesn't make sense to fix output overflow without fixing input
    overflow first. If you increase output block size to get rid of output
    overflows, then increase input block size, your effective batch size may
    increase to a point where you encounter output overflow again. If you
    encounter a lot of issues at once, best to fix them in this order:

    1. First, tune ``input_block_size`` to fix input overflow. This will
       increase average/effective batch size. Alternatively, set it to `None`
       (default) to let arroyo auto-tune it.
    2. Then, tune ``max_batch_size`` and ``max_batch_time`` so that you get the
       highest throughput. Test this by running your consumer on a backlog of
       messages and look at consumer offset rate (vs broker/total offset rate),
       or time it takes to get consumer lag back to normal. For as long as you
       have enough RAM, increment it in large steps (like 2x) and fine-tune
       afterwards.
    3. Then, tune ``output_block_size`` to fix output overflow. If in your
       previous tests there was a lot of output overflow, this will remove a lot
       of CPU load from your consumer and potentially also increase throughput.
    4. Now take a look at the ``batch.backpressure`` metric. If it is emitted,
       you need to optimize the next strategy (``next_step``) because that's what
       you're bottlenecked on. If it is not emitted, you may need to increase
       ``num_processes`` or increase batch size, so that
       `RunTaskWithMultiprocessing` itself is not the bottleneck.
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], TResult],
        next_step: ProcessingStrategy[Union[FilteredPayload, TResult]],
        max_batch_size: int,
        max_batch_time: float,
        pool: MultiprocessingPool,
        input_block_size: Optional[int] = None,
        output_block_size: Optional[int] = None,
        max_input_block_size: Optional[int] = None,
        max_output_block_size: Optional[int] = None,
        prefetch_batches: bool = False,
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        self.__resize_input_blocks = input_block_size is None
        self.__resize_output_blocks = output_block_size is None
        self.__max_input_block_size = max_input_block_size
        self.__max_output_block_size = max_output_block_size

        self.__pool = pool
        self.__pool.maybe_create_pool()
        num_processes = self.__pool.num_processes

        self.__shared_memory_manager = SharedMemoryManager()
        self.__shared_memory_manager.start()

        block_count = num_processes
        if prefetch_batches:
            # Allocate twice as many blocks as processes to ensure that every
            # process can immediately continue to handle another batch while the
            # main strategy is busy to submit the transformed messages to the
            # next step.
            block_count *= 2

        self.__input_blocks = [
            self.__shared_memory_manager.SharedMemory(
                input_block_size or DEFAULT_INPUT_BLOCK_SIZE
            )
            for _ in range(block_count)
        ]

        self.__output_blocks = [
            self.__shared_memory_manager.SharedMemory(
                output_block_size or DEFAULT_OUTPUT_BLOCK_SIZE
            )
            for _ in range(block_count)
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
                bool,  # was the input block too small?
                bool,  # was the output block too small?
            ]
        ] = deque()
        self.__invalid_messages = InvalidMessageState()

        self.__metrics = get_metrics()
        self.__batches_in_progress = Gauge(
            self.__metrics,
            "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
        )
        self.__pool_waiting_time: Optional[float] = None
        self.__metrics.gauge(
            "arroyo.strategies.run_task_with_multiprocessing.processes", num_processes
        )

        self.__closed = False

        def handle_sigchld(signum: int, frame: Any) -> None:
            # Terminates the consumer if any child process of the
            # consumer is terminated.
            # This is meant to detect the unexpected termination of
            # multiprocessor pool workers.
            if not self.__closed:
                self.__metrics.increment("sigchld.detected")
                raise ChildProcessTerminated(signum)

        self.original_sigchld = signal.signal(signal.SIGCHLD, handle_sigchld)

    def __submit_batch(self, input_block_too_small: bool) -> None:
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
                input_block_too_small,
                False,
            )
        )
        self.__batches_in_progress.increment()
        self.__metrics.timing(
            "arroyo.strategies.run_task_with_multiprocessing.batch.size.msg", len(batch)
        )
        self.__metrics.timing(
            "arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
            batch.get_content_size(),
        )
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
                    timeout=(
                        max(deadline - time.time(), 0) if deadline is not None else None
                    )
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
        (
            input_batch,
            async_result,
            input_block_too_small,
            output_block_too_small,
        ) = self.__processes[0]

        # If this call is being made in a context where it is intended to be
        # nonblocking, checking if the result is ready (rather than trying to
        # retrieve the result itself) avoids costly synchronization.
        if timeout == 0 and not async_result.ready():
            # ``multiprocessing.TimeoutError`` (rather than builtin
            # ``TimeoutError``) maintains consistency with ``AsyncResult.get``.
            raise multiprocessing.TimeoutError()

        result = async_result.get(timeout=timeout)

        self.__metrics.timing(
            "arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
            len(result.valid_messages_transformed),
        )
        self.__metrics.timing(
            "arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
            result.valid_messages_transformed.get_content_size(),
        )

        for idx, message in result.valid_messages_transformed:
            if isinstance(message, InvalidMessage):
                # For the next invocation of __check_for_results, skip over this message
                result.valid_messages_transformed.reset_iterator(idx + 1)
                self.__invalid_messages.append(message)
                raise message

            try:
                self.__next_step.poll()
            except InvalidMessage as e:
                # For the next invocation of __check_for_results, start at this message
                result.valid_messages_transformed.reset_iterator(idx)
                self.__invalid_messages.append(e)
                raise e

            try:
                self.__next_step.submit(message)

            except MessageRejected:
                # For the next invocation of __check_for_results, start at this message
                result.valid_messages_transformed.reset_iterator(idx)

                self.__metrics.increment(
                    "arroyo.strategies.run_task_with_multiprocessing.batch.backpressure"
                )
                raise NextStepTimeoutError()
            except InvalidMessage as e:
                # For the next invocation of __check_for_results, skip over this message
                # since we do not want to re-submit it.
                result.valid_messages_transformed.reset_iterator(idx + 1)
                self.__invalid_messages.append(e)
                raise e

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
                input_block_too_small,
                True,
            )
            return

        old_input_block = input_batch.block

        if (
            input_block_too_small
            and self.__resize_input_blocks
            and (
                self.__max_input_block_size is None
                or self.__max_input_block_size > old_input_block.size * 2
            )
        ):
            self.__metrics.increment(
                "arroyo.strategies.run_task_with_multiprocessing.batch.input.resize"
            )
            new_input_block = self.__shared_memory_manager.SharedMemory(
                old_input_block.size * 2
            )
            old_input_block.unlink()
        else:
            new_input_block = old_input_block

        old_output_block = result.valid_messages_transformed.block

        if (
            output_block_too_small
            and self.__resize_output_blocks
            and (
                self.__max_output_block_size is None
                or self.__max_output_block_size > old_output_block.size * 2
            )
        ):
            self.__metrics.increment(
                "arroyo.strategies.run_task_with_multiprocessing.batch.output.resize"
            )
            new_output_block = self.__shared_memory_manager.SharedMemory(
                old_output_block.size * 2
            )
            old_output_block.unlink()
        else:
            new_output_block = old_output_block

        logger.debug("Completed %r, reclaiming blocks...", input_batch)
        self.__input_blocks.append(new_input_block)
        self.__output_blocks.append(new_output_block)
        self.__batches_in_progress.decrement()

        del self.__processes[0]

    def poll(self) -> None:
        self.__forward_invalid_offsets()
        self.__next_step.poll()

        self.__check_for_results(timeout=0)

        if self.__batch_builder is not None and self.__batch_builder.ready():
            self.__submit_batch(False)

    def __reset_batch_builder(self) -> None:
        try:
            input_block = self.__input_blocks.pop()
        except IndexError as e:
            raise MessageRejected("no available input blocks") from e

        self.__batch_builder = BatchBuilder(
            MessageBatch(
                input_block,
                "A message is too large for the input block. Consider increasing --input-block-size",
            ),
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
            self.__submit_batch(True)

            # This may raise ``MessageRejected`` (if all of the shared memory
            # is in use) and create backpressure.
            self.__reset_batch_builder()
            assert self.__batch_builder is not None

            # If this raises ``ValueTooLarge``, that means that the input block
            # size is too small (smaller than the Kafka payload limit without
            # compression.)
            self.__batch_builder.append(message)

    def _do_close(self) -> None:
        self.__closed = True

        signal.signal(signal.SIGCHLD, self.original_sigchld)

    def close(self) -> None:
        self._do_close()

        if self.__batch_builder is not None and len(self.__batch_builder) > 0:
            self.__submit_batch(False)

    def terminate(self) -> None:
        self._do_close()

        logger.info("Terminating %r...", self.__pool)

        logger.info("Shutting down %r...", self.__shared_memory_manager)
        self.__pool.close()

        self.__shared_memory_manager.shutdown()

        logger.info("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        start_join = time.time()
        deadline = time.time() + timeout if timeout is not None else None
        self.__forward_invalid_offsets()

        logger.debug("Waiting for %s batches...", len(self.__processes))

        while True:
            elapsed = time.time() - start_join
            try:
                self.__check_for_results(
                    timeout=timeout - elapsed if timeout is not None else None,
                )
                break
            except InvalidMessage:
                raise

        logger.debug("Waiting for %s...", self.__pool)

        # XXX: We need to recreate the pool if there are still pending futures, to avoid
        # state from the previous assignment not being properly cleaned up.
        if len(self.__processes):
            self.__pool.close()

        self.__shared_memory_manager.shutdown()

        self.__next_step.close()
        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
