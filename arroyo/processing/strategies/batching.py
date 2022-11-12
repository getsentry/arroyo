from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Callable,
    Deque,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    TypeVar,
)

from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Message, OffsetRange, Partition, Position, Topic, TPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)


TResult = TypeVar("TResult")


class AbstractBatchWorker(ABC, Generic[TPayload, TResult]):
    """
    The ``BatchProcessingStrategy`` requires an instance of this class to
    handle user provided work such as processing raw messages and flushing
    processed batches to a custom backend.
    """

    @abstractmethod
    def process_message(self, message: Message[TPayload]) -> Optional[TResult]:
        """
        Called with each raw message, allowing the worker to do incremental
        (preferably local!) work on events. The object returned is put into
        the batch maintained internally by the ``BatchProcessingStrategy``.

        If this method returns `None` it is not added to the batch.

        A simple example would be decoding the message payload value and
        extracting a few fields.
        """
        pass

    @abstractmethod
    def flush_batch(self, batch: Sequence[TResult]) -> None:
        """
        Called with a list of processed (by ``process_message``) objects.
        The worker should write the batch of processed messages into whatever
        store(s) it is maintaining. Afterwards the offsets are committed by
        the ``BatchProcessingStrategy``.

        A simple example would be writing the batch to another topic.
        """
        pass


@dataclass
class Offsets:
    __slots__ = ["lo", "hi", "timestamp"]

    lo: int  # inclusive
    hi: int  # exclusive
    timestamp: datetime


@dataclass
class Batch(Generic[TResult]):
    results: MutableSequence[TResult] = field(default_factory=list)
    offsets: MutableMapping[Partition, Offsets] = field(default_factory=dict)
    created: float = field(default_factory=lambda: time.time())
    messages_processed_count: int = 0
    # the total amount of time, in milliseconds, that it took to process
    # the messages in this batch (does not included time spent waiting for
    # new messages)
    processing_time_ms: float = 0.0


class BatchProcessingStrategy(ProcessingStrategy[TPayload]):
    """
    Do not use for new consumers.
    This is deprecated and will be removed in a future version.

    The ``BatchProcessingStrategy`` is a processing strategy that accumulates
    processed message values, periodically flushing them after a given
    duration of time has passed or number of output values have been
    accumulated. Users need only provide an implementation of what it means
    to process a raw message and flush a batch of events via an
    ``AbstractBatchWorker`` instance.

    Messages are processed as they are read from the consumer, then added to
    an in-memory batch. These batches are flushed based on the batch size or
    time sent since the first message in the batch was recieved (e.g. "500
    items or 1000ms"), whichever threshold is reached first. When a batch of
    items is flushed, the consumer offsets are synchronously committed.
    """

    def __init__(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
    ) -> None:
        self.__commit = commit
        self.__worker = worker
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__metrics = get_metrics()

        self.__batch: Optional[Batch[TResult]] = None
        self.__closed = False
        self.__flush_done = time.time()

    def poll(self) -> None:
        """
        Check if the current in-flight batch should be flushed.
        """
        assert not self.__closed

        if self.__batch is not None and (
            len(self.__batch.results) >= self.__max_batch_size
            or time.time() > self.__batch.created + self.__max_batch_time / 1000.0
        ):

            self.__metrics.timing("processing_phase", time.time() - self.__flush_done)
            self.__flush()
            self.__flush_done = time.time()

    def submit(self, message: Message[TPayload]) -> None:
        """
        Process a message.
        """
        assert not self.__closed

        start = time.time()

        self.__metrics.timing(
            "receive_latency",
            (start - message.timestamp.timestamp()) * 1000,
            tags={
                "topic": message.partition.topic.name,
                "partition": str(message.partition.index),
            },
        )

        # Create the batch only after the first message is seen.
        if self.__batch is None:
            self.__batch = Batch()

        result = self.__worker.process_message(message)

        # XXX: ``None`` is indistinguishable from a potentially valid return
        # value of ``TResult``!
        if result is not None:
            self.__batch.results.append(result)

        duration = (time.time() - start) * 1000
        self.__batch.messages_processed_count += 1
        self.__batch.processing_time_ms += duration
        self.__metrics.timing("process_message", duration)

        if message.partition in self.__batch.offsets:
            self.__batch.offsets[message.partition].hi = message.next_offset
            self.__batch.offsets[message.partition].timestamp = message.timestamp
        else:
            self.__batch.offsets[message.partition] = Offsets(
                message.offset,
                message.next_offset,
                message.timestamp,
            )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        # The active batch is discarded when exiting without attempting to
        # write or commit, so this method can exit immediately without
        # blocking.
        pass

    def __flush(self) -> None:
        """
        Flush the active batch and reset the batch state.
        """
        assert not self.__closed
        assert self.__batch is not None, "cannot flush without active batch"

        logger.info(
            "Flushing %s items (from %r)",
            len(self.__batch.results),
            self.__batch.offsets,
        )

        self.__metrics.timing(
            "process_message.normalized",
            self.__batch.processing_time_ms / self.__batch.messages_processed_count,
        )

        batch_results_length = len(self.__batch.results)
        if batch_results_length > 0:
            logger.debug("Flushing batch via worker")
            flush_start = time.time()
            self.__worker.flush_batch(self.__batch.results)
            flush_duration = (time.time() - flush_start) * 1000
            logger.info("Worker flush took %dms", flush_duration)
            self.__metrics.increment("batch.flush.items", batch_results_length)
            self.__metrics.timing("batch.flush", flush_duration)
            self.__metrics.timing(
                "batch.flush.normalized", flush_duration / batch_results_length
            )

        logger.debug("Committing offsets for batch")
        commit_start = time.time()
        offsets = {
            partition: Position(offsets.hi, offsets.timestamp)
            for partition, offsets in self.__batch.offsets.items()
        }
        self.__commit(offsets)
        logger.debug("Committed offsets: %s", offsets)
        commit_duration = (time.time() - commit_start) * 1000
        logger.debug("Offset commit took %dms", commit_duration)

        self.__batch = None


class BatchProcessingStrategyFactory(ProcessingStrategyFactory[TPayload]):
    """
    Do not use for new consumers.
    This is deprecated and will be removed in a future version.
    """

    def __init__(
        self,
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
    ) -> None:
        self.__worker = worker
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

    def create_with_partitions(
        self,
        commit: Callable[[Mapping[Partition, Position]], None],
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[TPayload]:
        return BatchProcessingStrategy(
            commit,
            self.__worker,
            self.__max_batch_size,
            self.__max_batch_time,
        )


@dataclass(frozen=True)
class MessageBatch(Generic[TPayload]):
    """
    Represents a batch of messages with the range of offsets covered.
    The range of offsets is represented as a mapping between partitions
    and offset ranges.
    The lowest offset in each offset range is inclusize. The highest is
    esclusive.

    This class intentionally does not implement __len__ or __iter__ as
    generally, when using it, we want to deal with None batch and empty
    batches differently. This way it is less likely to make mistakes
    when checking for an empty batch.
    """

    messages: Sequence[Message[TPayload]]
    offsets: Mapping[Partition, OffsetRange]

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self.messages)} message{'s' if len(self.messages) != 1 else ''}>"

    def last(self) -> Optional[Message[TPayload]]:
        return self.messages[-1] if self.messages else None

    def is_empty(self) -> bool:
        return len(self.messages) == 0


class OutOfOrderMessage(Exception):
    pass


class BatchBuilder(Generic[TPayload]):
    """
    Accumulates messages in a `MessageBatch` object.
    It requires offsets to be in monotonic order in each partition.
    """

    def __init__(
        self,
        max_batch_time: float,
        max_batch_size: int,
    ) -> None:
        self.__max_batch_time = max_batch_time
        self.__max_batch_size = max_batch_size
        self.__messages: MutableSequence[Message[TPayload]] = []
        self.__offsets: MutableMapping[Partition, OffsetRange] = {}
        self.__init_time = time.time()

    def append(self, message: Message[TPayload]) -> None:
        if message.partition in self.__offsets:
            if self.__offsets[message.partition].hi >= message.next_offset:
                raise OutOfOrderMessage(
                    f"Received offset {message.next_offset}. Current watermark {self.__offsets[message.partition].hi}"
                )

            self.__offsets[message.partition].hi = message.next_offset
            self.__offsets[message.partition].timestamp = message.timestamp
        else:
            self.__offsets[message.partition] = OffsetRange(
                message.offset, message.next_offset, message.timestamp
            )

        self.__messages.append(message)

    def build_if_ready(self) -> Optional[MessageBatch[TPayload]]:
        if (
            len(self.__messages) >= self.__max_batch_size
            or time.time() > self.__init_time + self.__max_batch_time
        ):
            return MessageBatch(
                messages=self.__messages,
                offsets=self.__offsets,
            )
        else:
            return None

    def force_build(self) -> MessageBatch[TPayload]:
        return MessageBatch(
            messages=self.__messages,
            offsets=self.__offsets,
        )


class BatchStep(ProcessingStrategy[TPayload]):
    """
    Accumulates messages into a batch. When the batch is full this
    strategy submits it to the next step.

    A batch is represented as a `MessageBatch` object, which includes
    both the messages and the offset ranges.

    A message is closed and submitted when the maximum number of messages
    is received or when the max_batch_time has passed since the first
    message was received.

    This step does not allow out of order processing. The batch can only
    be built with monotonically increasing offsets per partition.
    If messages out of order are spot, this strategy will raise an
    `OutOfOrderMessage` exception.

    If a batch is closed empty (no message received within `max_batch_time_sec`)
    An empty batch is submitted to the following step. The following step,
    thus needs to know how to deal with empty batches.

    This strategy propagates `MessageRejected` exceptions from the
    downstream steps if they are thrown.
    """

    def __init__(
        self,
        max_batch_time_sec: float,
        max_batch_size: int,
        next_step: ProcessingStrategy[MessageBatch[TPayload]],
    ) -> None:
        self.__max_batch_time_sec = max_batch_time_sec
        self.__max_batch_size = max_batch_size
        self.__next_step = next_step
        self.__batch_builder: Optional[BatchBuilder[TPayload]] = None
        self.__closed = False

    def __flush(self, force: bool) -> None:
        assert self.__batch_builder is not None
        batch = (
            self.__batch_builder.build_if_ready()
            if not force
            else self.__batch_builder.force_build()
        )

        if batch is None:
            return

        last_msg = batch.last()
        if last_msg is None:
            # TODO: PR #134 will fix this problem and make it unnecessary
            # to hack partition id = 0 and offset = 0 for empty batches.
            batch_msg = Message(
                partition=Partition(Topic(""), 0),
                offset=0,
                timestamp=datetime.now(),
                payload=batch,
            )
        else:
            batch_msg = Message(
                partition=last_msg.partition,
                offset=last_msg.offset,
                timestamp=last_msg.timestamp,
                payload=batch,
            )
        self.__next_step.submit(batch_msg)
        self.__batch_builder = None

    def submit(self, message: Message[TPayload]) -> None:
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

        if self.__batch_builder is not None:
            self.__flush(force=False)

        if self.__batch_builder is None:
            self.__batch_builder = BatchBuilder(
                max_batch_time=self.__max_batch_time_sec,
                max_batch_size=self.__max_batch_size,
            )

        self.__batch_builder.append(message)

    def poll(self) -> None:
        assert not self.__closed

        if self.__batch_builder is not None:
            try:
                self.__flush(force=False)
            except MessageRejected:
                pass

        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__batch_builder = None

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None
        if self.__batch_builder is not None:
            self.__flush(force=True)

        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )


class UnbatchStep(ProcessingStrategy[MessageBatch[TPayload]]):
    """
    This processing step receives batches and explodes them sending
    the content message by message to the next step.

    A batch is represented as a `MessageBatch` object.

    If this step receives a `MessageRejected` exception from the next
    step it would keep the remaining messages and attempt to submit
    them at the following call to `poll`
    """

    def __init__(
        self,
        next_step: ProcessingStrategy[TPayload],
    ) -> None:
        self.__next_step = next_step
        self.__batch_to_send: Deque[Message[TPayload]] = deque()
        self.__closed = False

    def __flush(self) -> None:
        while self.__batch_to_send:
            msg = self.__batch_to_send[0]
            self.__next_step.submit(msg)
            self.__batch_to_send.popleft()

    def submit(self, message: Message[MessageBatch[TPayload]]) -> None:
        assert not self.__closed

        if self.__batch_to_send:
            raise MessageRejected

        self.__batch_to_send.extend(message.payload.messages)
        self.__flush()

    def poll(self) -> None:
        assert not self.__closed

        try:
            self.__flush()
        except MessageRejected:
            pass

        self.__next_step.poll()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__current_batch = None

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None
        self.__flush()

        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )
