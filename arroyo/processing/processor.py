from __future__ import annotations

import functools
import logging
import time
from collections import defaultdict
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from arroyo.backends.abstract import Consumer
from arroyo.commit import CommitPolicy
from arroyo.dlq import BufferedMessages, DlqPolicy, InvalidMessage
from arroyo.errors import RecoverableError
from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import BrokerValue, Message, Partition, Topic, TStrategyPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)

METRICS_FREQUENCY_SEC = 1.0  # In seconds

F = TypeVar("F", bound=Callable[[Any], Any])


def _rdkafka_callback(metrics: MetricsBuffer) -> Callable[[F], F]:
    def decorator(f: F) -> F:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            try:
                return f(*args, **kwargs)
            except Exception:
                logger.exception(f"{f.__name__} crashed")
                raise
            finally:
                metrics.incr_timing(
                    ConsumerTiming.CONSUMER_CALLBACK_TIME, time.time() - start_time
                )

        return cast(F, wrapper)

    return decorator


class InvalidStateError(RuntimeError):
    pass


class ConsumerTiming(Enum):
    CONSUMER_POLL_TIME = "arroyo.consumer.poll.time"
    CONSUMER_PROCESSING_TIME = "arroyo.consumer.processing.time"
    CONSUMER_PAUSED_TIME = "arroyo.consumer.paused.time"
    CONSUMER_DLQ_TIME = "arroyo.consumer.dlq.time"
    CONSUMER_JOIN_TIME = "arroyo.consumer.join.time"

    # This metric's timings overlap with DLQ/join time.
    CONSUMER_CALLBACK_TIME = "arroyo.consumer.callback.time"
    CONSUMER_SHUTDOWN_TIME = "arroyo.consumer.shutdown.time"


class ConsumerCounter(Enum):
    CONSUMER_RUN_COUNT = "arroyo.consumer.run.count"


class MetricsBuffer:
    def __init__(self) -> None:
        self.__metrics = get_metrics()
        self.__timers: MutableMapping[ConsumerTiming, float] = defaultdict(float)
        self.__counters: MutableMapping[ConsumerCounter, int] = defaultdict(int)
        self.__reset()

    def incr_timing(self, metric: ConsumerTiming, duration: float) -> None:
        self.__timers[metric] += duration
        self.__throttled_record()

    def incr_counter(self, metric: ConsumerCounter, delta: int) -> None:
        self.__counters[metric] += delta
        self.__throttled_record()

    def flush(self) -> None:
        metric: Union[ConsumerTiming, ConsumerCounter]
        value: Union[float, int]

        for metric, value in self.__timers.items():
            self.__metrics.timing(metric.value, value)
        for metric, value in self.__counters.items():
            self.__metrics.increment(metric.value, value)
        self.__reset()

    def __reset(self) -> None:
        self.__timers.clear()
        self.__counters.clear()
        self.__last_record_time = time.time()

    def __throttled_record(self) -> None:
        if time.time() - self.__last_record_time > METRICS_FREQUENCY_SEC:
            self.flush()


class StreamProcessor(Generic[TStrategyPayload]):
    """
    A stream processor manages the relationship between a ``Consumer``
    instance and a ``ProcessingStrategy``, ensuring that processing
    strategies are instantiated on partition assignment and closed on
    partition revocation.
    """

    def __init__(
        self,
        consumer: Consumer[TStrategyPayload],
        topic: Topic,
        processor_factory: ProcessingStrategyFactory[TStrategyPayload],
        commit_policy: CommitPolicy,
        dlq_policy: Optional[DlqPolicy[TStrategyPayload]] = None,
        join_timeout: Optional[float] = None,
    ) -> None:
        self.__consumer = consumer
        self.__processor_factory = processor_factory
        self.__metrics_buffer = MetricsBuffer()

        self.__processing_strategy: Optional[
            ProcessingStrategy[TStrategyPayload]
        ] = None

        self.__message: Optional[BrokerValue[TStrategyPayload]] = None

        # If the consumer is in the paused state, this is when the last call to
        # ``pause`` occurred or the time the pause metric was last recorded.
        self.__paused_timestamp: Optional[float] = None

        self.__commit_policy_state = commit_policy.get_state_machine()
        self.__join_timeout = join_timeout
        self.__shutdown_requested = False

        # Buffers messages for DLQ. Messages are added when they are submitted for processing and
        # removed once the commit callback is fired as they are guaranteed to be valid at that point.
        self.__dlq_policy = dlq_policy
        self.__buffered_messages: BufferedMessages[TStrategyPayload] = BufferedMessages(
            dlq_policy
        )

        def _close_strategy() -> None:
            start_close = time.time()

            if self.__processing_strategy is None:
                raise InvalidStateError(
                    "received unexpected revocation without active processing strategy"
                )

            logger.info("Closing %r...", self.__processing_strategy)
            self.__processing_strategy.close()

            logger.info("Waiting for %r to exit...", self.__processing_strategy)

            while True:
                start_join = time.time()

                try:
                    self.__processing_strategy.join(self.__join_timeout)
                    self.__metrics_buffer.incr_timing(
                        ConsumerTiming.CONSUMER_JOIN_TIME, time.time() - start_join
                    )
                    break
                except InvalidMessage as e:
                    self.__metrics_buffer.incr_timing(
                        ConsumerTiming.CONSUMER_JOIN_TIME, time.time() - start_join
                    )
                    self._handle_invalid_message(e)

            logger.info(
                "%r exited successfully, releasing assignment.",
                self.__processing_strategy,
            )
            self.__processing_strategy = None
            self.__message = None  # avoid leaking buffered messages across assignments

            self.__metrics_buffer.incr_timing(
                ConsumerTiming.CONSUMER_SHUTDOWN_TIME, time.time() - start_close
            )

        def _create_strategy(partitions: Mapping[Partition, int]) -> None:
            self.__processing_strategy = (
                self.__processor_factory.create_with_partitions(
                    self.__commit, partitions
                )
            )
            logger.debug(
                "Initialized processing strategy: %r", self.__processing_strategy
            )

        @_rdkafka_callback(metrics=self.__metrics_buffer)
        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            logger.info("New partitions assigned: %r", partitions)
            self.__buffered_messages.reset()
            if partitions:
                if self.__processing_strategy is not None:
                    logger.exception(
                        "Partition assignment while processing strategy active"
                    )
                    _close_strategy()
                _create_strategy(partitions)

        @_rdkafka_callback(metrics=self.__metrics_buffer)
        def on_partitions_revoked(partitions: Sequence[Partition]) -> None:
            logger.info("Partitions revoked: %r", partitions)

            if partitions:
                _close_strategy()

                # Recreate the strategy if the consumer still has other partitions
                # assigned and is not closed or errored
                try:
                    current_partitions = self.__consumer.tell()
                    if len(current_partitions.keys() - set(partitions)):
                        active_partitions = {
                            partition: offset
                            for partition, offset in current_partitions.items()
                            if partition not in partitions
                        }
                        _create_strategy(active_partitions)
                except RuntimeError:
                    pass

        self.__consumer.subscribe(
            [topic], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked
        )

    def __commit(self, offsets: Mapping[Partition, int], force: bool = False) -> None:
        """
        If force is passed, commit immediately and do not throttle. This should
        be used during consumer shutdown where we do not want to wait before committing.
        """
        for (partition, offset) in offsets.items():
            self.__buffered_messages.pop(partition, offset - 1)

        self.__consumer.stage_offsets(offsets)
        now = time.time()

        if force or self.__commit_policy_state.should_commit(
            now,
            offsets,
        ):
            self.__consumer.commit_offsets()
            logger.debug(
                "Waited %0.4f seconds for offsets to be committed to %r.",
                time.time() - now,
                self.__consumer,
            )
            self.__commit_policy_state.did_commit(now, offsets)

    def run(self) -> None:
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")
        try:
            while not self.__shutdown_requested:
                self._run_once()

            self._shutdown()
        except Exception:
            logger.exception("Caught exception, shutting down...")

            if self.__processing_strategy is not None:
                logger.debug("Terminating %r...", self.__processing_strategy)
                self.__processing_strategy.terminate()
                self.__processing_strategy = None

            logger.info("Closing %r...", self.__consumer)
            self.__consumer.close()
            logger.info("Processor terminated")
            raise

    def _handle_invalid_message(self, exc: InvalidMessage) -> None:
        # Do not "carry over" message if it is the invalid one. Every other
        # message should be re-submitted to the strategy.
        if (
            self.__message is not None
            and exc.partition == self.__message.partition
            and exc.offset == self.__message.offset
        ):
            self.__message = None

        logger.exception(exc)
        if self.__dlq_policy:
            start_dlq = time.time()
            invalid_message = self.__buffered_messages.pop(exc.partition, exc.offset)
            if invalid_message is None:
                raise Exception(
                    f"Invalid message not found in buffer {exc.partition} {exc.offset}",
                ) from None

            # XXX: This blocks until the message is produced. This will be slow
            # if there is a very large volume of invalid messages to be produced.
            self.__dlq_policy.producer.produce(invalid_message).result()

            self.__metrics_buffer.incr_timing(
                ConsumerTiming.CONSUMER_DLQ_TIME, time.time() - start_dlq
            )

    def _run_once(self) -> None:
        self.__metrics_buffer.incr_counter(ConsumerCounter.CONSUMER_RUN_COUNT, 1)

        message_carried_over = self.__message is not None

        if message_carried_over:
            # If a message was carried over from the previous run, there are two reasons:
            #
            # * MessageRejected. the consumer should be paused and not
            #   returning any messages on ``poll``.
            # * InvalidMessage. the message should be resubmitted.
            #   _handle_invalid_message is responsible for clearing out
            #   self.__message if it was the invalid one.
            if (
                self.__paused_timestamp is not None
                and self.__consumer.poll(timeout=0) is not None
            ):
                raise InvalidStateError(
                    "received message when consumer was expected to be paused"
                )
        else:
            # Otherwise, we need to try fetch a new message from the consumer,
            # even if there is no active assignment and/or processing strategy.
            try:
                start_poll = time.time()
                self.__message = self.__consumer.poll(timeout=1.0)
                self.__metrics_buffer.incr_timing(
                    ConsumerTiming.CONSUMER_POLL_TIME, time.time() - start_poll
                )
            except RecoverableError:
                return

        if self.__processing_strategy is not None:
            start_poll = time.time()
            try:
                self.__processing_strategy.poll()
            except InvalidMessage as e:
                self._handle_invalid_message(e)
                return

            self.__metrics_buffer.incr_timing(
                ConsumerTiming.CONSUMER_PROCESSING_TIME, time.time() - start_poll
            )
            if self.__message is not None:
                try:
                    start_submit = time.time()
                    message = (
                        Message(self.__message) if self.__message is not None else None
                    )
                    if not message_carried_over:
                        self.__buffered_messages.append(self.__message)
                    self.__processing_strategy.submit(message)

                    self.__metrics_buffer.incr_timing(
                        ConsumerTiming.CONSUMER_PROCESSING_TIME,
                        time.time() - start_submit,
                    )
                except MessageRejected as e:
                    # If the processing strategy rejected our message, we need
                    # to pause the consumer and hold the message until it is
                    # accepted, at which point we can resume consuming.
                    if not message_carried_over:
                        logger.debug(
                            "Caught %r while submitting %r, pausing consumer...",
                            e,
                            self.__message,
                        )
                        self.__consumer.pause([*self.__consumer.tell().keys()])

                        self.__paused_timestamp = time.time()
                    else:
                        current_time = time.time()
                        if self.__paused_timestamp:
                            self.__metrics_buffer.incr_timing(
                                ConsumerTiming.CONSUMER_PAUSED_TIME,
                                current_time - self.__paused_timestamp,
                            )
                            self.__paused_timestamp = current_time
                except InvalidMessage as e:
                    self._handle_invalid_message(e)

                else:
                    # If we were trying to submit a message that failed to be
                    # submitted on a previous run, we can resume accepting new
                    # messages.
                    if message_carried_over and self.__paused_timestamp is not None:
                        self.__consumer.resume([*self.__consumer.tell().keys()])

                        self.__metrics_buffer.incr_timing(
                            ConsumerTiming.CONSUMER_PAUSED_TIME,
                            time.time() - self.__paused_timestamp,
                        )

                        self.__paused_timestamp = None

                    self.__message = None
        else:
            if self.__message is not None:
                raise InvalidStateError(
                    "received message without active processing strategy"
                )

    def signal_shutdown(self) -> None:
        """
        Tells the stream processor to shutdown on the next run loop
        iteration.

        Typically called from a signal handler.
        """
        logger.debug("Shutdown signalled")

        self.__shutdown_requested = True

    def _shutdown(self) -> None:
        # close the consumer
        logger.debug("Stopping consumer")
        self.__metrics_buffer.flush()
        self.__consumer.close()
        logger.debug("Stopped")

        # if there was an active processing strategy, it should be shut down
        # and unset when the partitions are revoked during consumer close
        assert (
            self.__processing_strategy is None
        ), "processing strategy was not closed on shutdown"
