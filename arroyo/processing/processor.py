from __future__ import annotations

import functools
import logging
import time
from collections import defaultdict
from typing import (
    Any,
    Callable,
    Generic,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from arroyo.backends.abstract import Consumer
from arroyo.commit import ONCE_PER_SECOND, CommitPolicy
from arroyo.dlq import BufferedMessages, DlqPolicy, DlqPolicyWrapper, InvalidMessage
from arroyo.errors import RecoverableError
from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import BrokerValue, Message, Partition, Topic, TStrategyPayload
from arroyo.utils.logging import handle_internal_error
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)

METRICS_FREQUENCY_SEC = 1.0  # In seconds
BACKPRESSURE_THRESHOLD = 5.0  # In seconds

F = TypeVar("F", bound=Callable[[Any], Any])


def _rdkafka_callback(metrics: MetricsBuffer) -> Callable[[F], F]:
    def decorator(f: F) -> F:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            try:
                return f(*args, **kwargs)
            except Exception as e:
                handle_internal_error(e)
                logger.exception(f"{f.__name__} crashed")
                raise
            finally:
                value = time.time() - start_time
                metrics.metrics.timing(
                    "arroyo.consumer.run.callback",
                    value,
                    tags={"callback_name": f.__name__},
                )
                metrics.incr_timing("arroyo.consumer.callback.time", value)

        return cast(F, wrapper)

    return decorator


class InvalidStateError(RuntimeError):
    pass


ConsumerTiming = Literal[
    "arroyo.consumer.poll.time",
    "arroyo.consumer.processing.time",
    "arroyo.consumer.backpressure.time",
    "arroyo.consumer.dlq.time",
    "arroyo.consumer.join.time",
    # This metric's timings overlap with DLQ/join time.
    "arroyo.consumer.callback.time",
    "arroyo.consumer.shutdown.time",
]

ConsumerCounter = Literal[
    "arroyo.consumer.run.count",
    "arroyo.consumer.invalid_message.count",
    "arroyo.consumer.pause",
    "arroyo.consumer.resume",
    "arroyo.consumer.dlq.dropped_messages",
]


class MetricsBuffer:
    def __init__(self) -> None:
        self.metrics = get_metrics()
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
            self.metrics.timing(metric, value)
        for metric, value in self.__counters.items():
            self.metrics.increment(metric, value)
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
        commit_policy: CommitPolicy = ONCE_PER_SECOND,
        dlq_policy: Optional[DlqPolicy[TStrategyPayload]] = None,
        join_timeout: Optional[float] = None,
        shutdown_strategy_before_consumer: bool = False,
    ) -> None:
        self.__consumer = consumer
        self.__processor_factory = processor_factory
        self.__metrics_buffer = MetricsBuffer()

        self.__processing_strategy: Optional[
            ProcessingStrategy[TStrategyPayload]
        ] = None

        self.__message: Optional[BrokerValue[TStrategyPayload]] = None

        # The timestamp when backpressure state started
        self.__backpressure_timestamp: Optional[float] = None
        # Consumer is paused after it is in backpressure state for > BACKPRESSURE_THRESHOLD seconds
        self.__is_paused = False

        self.__commit_policy_state = commit_policy.get_state_machine()
        self.__join_timeout = join_timeout
        self.__shutdown_requested = False
        self.__shutdown_strategy_before_consumer = shutdown_strategy_before_consumer

        # Buffers messages for DLQ. Messages are added when they are submitted for processing and
        # removed once the commit callback is fired as they are guaranteed to be valid at that point.
        self.__buffered_messages: BufferedMessages[TStrategyPayload] = BufferedMessages(
            dlq_policy
        )

        self.__dlq_policy: Optional[DlqPolicyWrapper[TStrategyPayload]] = (
            DlqPolicyWrapper(dlq_policy) if dlq_policy is not None else None
        )

        def _close_strategy() -> None:
            self._close_processing_strategy()

        def _create_strategy(partitions: Mapping[Partition, int]) -> None:
            start_create = time.time()

            self.__processing_strategy = (
                self.__processor_factory.create_with_partitions(
                    self.__commit, partitions
                )
            )

            self.__metrics_buffer.metrics.timing(
                "arroyo.consumer.run.create_strategy", time.time() - start_create
            )

            logger.debug(
                "Initialized processing strategy: %r", self.__processing_strategy
            )

        @_rdkafka_callback(metrics=self.__metrics_buffer)
        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            logger.info("New partitions assigned: %r", partitions)
            logger.info("Member id: %r", self.__consumer.member_id)

            self.__metrics_buffer.metrics.increment(
                "arroyo.consumer.partitions_assigned.count", len(partitions)
            )

            current_partitions = dict(self.__consumer.tell())
            current_partitions.update(partitions)

            if self.__dlq_policy:
                self.__dlq_policy.reset_dlq_limits(current_partitions)

            if current_partitions:
                if self.__processing_strategy is not None:
                    # TODO: for cooperative-sticky rebalancing this can happen
                    # quite often. we should port the changes to
                    # ProcessingStrategyFactory that we made in Rust: Remove
                    # create_with_partitions, replace with create +
                    # update_partitions
                    logger.warning(
                        "Partition assignment while processing strategy active"
                    )
                    _close_strategy()
                _create_strategy(current_partitions)

        @_rdkafka_callback(metrics=self.__metrics_buffer)
        def on_partitions_revoked(partitions: Sequence[Partition]) -> None:
            logger.info("Partitions to revoke: %r", partitions)

            self.__metrics_buffer.metrics.increment(
                "arroyo.consumer.partitions_revoked.count", len(partitions)
            )

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
                        logger.info(
                            "Recreating strategy since there are still active partitions: %r",
                            active_partitions,
                        )
                        _create_strategy(active_partitions)
                except RuntimeError:
                    pass

            for partition in partitions:
                self.__buffered_messages.remove(partition)

            # Partition revocation can happen anytime during the consumer lifecycle and happen
            # multiple times. What we want to know is that the consumer is not stuck somewhere.
            # The presence of this message as the last message of a consumer
            # indicates that the consumer was not stuck.
            logger.info("Partition revocation complete.")

        self.__consumer.subscribe(
            [topic], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked
        )

    def _close_processing_strategy(self) -> None:
        """Close the processing strategy and wait for it to exit."""
        start_close = time.time()

        if self.__processing_strategy is None:
            # Partitions are revoked when the consumer is shutting down, at
            # which point we already have closed the consumer.
            return

        logger.info("Closing %r...", self.__processing_strategy)
        logger.info("Member id: %r", self.__consumer.member_id)
        self.__processing_strategy.close()

        logger.info("Waiting for %r to exit...", self.__processing_strategy)

        while True:
            start_join = time.time()

            try:
                self.__processing_strategy.join(self.__join_timeout)
                self.__metrics_buffer.incr_timing(
                    "arroyo.consumer.join.time", time.time() - start_join
                )
                break
            except InvalidMessage as e:
                self.__metrics_buffer.incr_timing(
                    "arroyo.consumer.join.time", time.time() - start_join
                )
                self._handle_invalid_message(e)

        logger.info("%r exited successfully", self.__processing_strategy)
        self.__processing_strategy = None
        self.__message = None
        self.__is_paused = False
        self._clear_backpressure()

        value = time.time() - start_close
        self.__metrics_buffer.metrics.timing(
            "arroyo.consumer.run.close_strategy", value
        )
        self.__metrics_buffer.incr_timing("arroyo.consumer.shutdown.time", value)

    def __commit(self, offsets: Mapping[Partition, int], force: bool = False) -> None:
        """
        If force is passed, commit immediately and do not throttle. This should
        be used during consumer shutdown where we do not want to wait before committing.
        """
        for partition, offset in offsets.items():
            self.__buffered_messages.pop(partition, offset - 1)

        self.__consumer.stage_offsets(offsets)
        now = time.time()

        if force or self.__commit_policy_state.should_commit(
            now,
            offsets,
        ):
            if self.__dlq_policy:
                self.__dlq_policy.flush(offsets)

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
            self.__processor_factory.shutdown()
            logger.info("Processor terminated")
            raise

    def _clear_backpressure(self) -> None:
        if self.__backpressure_timestamp is not None:
            self.__metrics_buffer.incr_timing(
                "arroyo.consumer.backpressure.time",
                time.time() - self.__backpressure_timestamp,
            )
            self.__backpressure_timestamp = None

    def _handle_invalid_message(self, exc: InvalidMessage) -> None:
        # Do not "carry over" message if it is the invalid one. Every other
        # message should be re-submitted to the strategy.
        if (
            self.__message is not None
            and exc.partition == self.__message.partition
            and exc.offset == self.__message.offset
        ):
            self.__message = None

        if exc.log_exception:
            logger.exception(exc)
        self.__metrics_buffer.incr_counter("arroyo.consumer.invalid_message.count", 1)
        if self.__dlq_policy:
            start_dlq = time.time()
            invalid_message = self.__buffered_messages.pop(exc.partition, exc.offset)
            if invalid_message is None:
                raise Exception(
                    f"Invalid message not found in buffer {exc.partition} {exc.offset}",
                ) from None

            # XXX: This blocks if there are more than MAX_PENDING_FUTURES in the queue.
            try:
                self.__dlq_policy.produce(invalid_message, exc.reason)
            except Exception:
                logger.exception(
                    f"Failed to produce message (partition: {exc.partition} offset: {exc.offset}) to DLQ topic, dropping"
                )
                self.__metrics_buffer.incr_counter(
                    "arroyo.consumer.dlq.dropped_messages", 1
                )

            self.__metrics_buffer.incr_timing(
                "arroyo.consumer.dlq.time", time.time() - start_dlq
            )

    def _run_once(self) -> None:
        self.__metrics_buffer.incr_counter("arroyo.consumer.run.count", 1)

        message_carried_over = self.__message is not None

        if not message_carried_over:
            # Poll for a new message from the consumer only if there is no carried
            # over message which we need to successfully submit first.
            try:
                start_poll = time.time()
                self.__message = self.__consumer.poll(timeout=1.0)
                if self.__message:
                    self.__buffered_messages.append(self.__message)
                self.__metrics_buffer.incr_timing(
                    "arroyo.consumer.poll.time", time.time() - start_poll
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
                "arroyo.consumer.processing.time", time.time() - start_poll
            )
            if self.__message is not None:
                try:
                    start_submit = time.time()
                    message = Message(self.__message)
                    self.__processing_strategy.submit(message)

                    self.__metrics_buffer.incr_timing(
                        "arroyo.consumer.processing.time",
                        time.time() - start_submit,
                    )
                except MessageRejected as e:
                    # If the processing strategy rejected our message, we need
                    # to pause the consumer and hold the message until it is
                    # accepted, at which point we can resume consuming.
                    # if not message_carried_over:
                    if self.__backpressure_timestamp is None:
                        self.__backpressure_timestamp = time.time()

                    elif not self.__is_paused and (
                        time.time() - self.__backpressure_timestamp
                        > BACKPRESSURE_THRESHOLD
                    ):
                        self.__metrics_buffer.incr_counter("arroyo.consumer.pause", 1)
                        logger.debug(
                            "Caught %r while submitting %r, pausing consumer...",
                            e,
                            self.__message,
                        )
                        self.__consumer.pause([*self.__consumer.tell().keys()])
                        self.__is_paused = True

                    elif self.__is_paused:
                        paused_partitions = set(self.__consumer.paused())
                        unpaused_partitions = (
                            set(self.__consumer.tell()) - paused_partitions
                        )
                        if unpaused_partitions:
                            logger.warning(
                                "Processor in paused state while consumer is partially unpaused: %s, paused: %s",
                                unpaused_partitions,
                                paused_partitions,
                            )
                            self.__is_paused = False
                            # unpause paused partitions... just in case a subset is paused
                            self.__metrics_buffer.incr_counter(
                                "arroyo.consumer.resume", 1
                            )
                            self.__consumer.resume([*paused_partitions])
                        else:
                            # A paused consumer should still poll periodically to avoid it's partitions
                            # getting revoked by the broker after reaching the max.poll.interval.ms
                            # Polling a paused consumer should never yield a message.
                            assert self.__consumer.poll(0.1) is None
                    else:
                        time.sleep(0.01)

                except InvalidMessage as e:
                    self._handle_invalid_message(e)

                    if self.__is_paused:
                        self.__metrics_buffer.incr_counter("arroyo.consumer.resume", 1)
                        self.__consumer.resume([*self.__consumer.tell().keys()])
                        self.__is_paused = False

                else:
                    # Resume if we are currently in a paused state
                    if self.__is_paused:
                        self.__metrics_buffer.incr_counter("arroyo.consumer.resume", 1)
                        self.__consumer.resume([*self.__consumer.tell().keys()])
                        self.__is_paused = False

                    # Clear backpressure timestamp if it is set
                    self._clear_backpressure()

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
        logger.info("Shutdown signalled")

        self.__shutdown_requested = True

    def _shutdown(self) -> None:
        # If shutdown_strategy_before_consumer is set, work around an issue
        # where rdkafka would revoke our partition, but then also immediately
        # revoke our member ID as well, causing join() of the CommitStrategy
        # (that is running in the partition revocation callback) to crash.
        if self.__shutdown_strategy_before_consumer:
            self._close_processing_strategy()

        # close the consumer
        logger.info("Stopping consumer")
        self.__metrics_buffer.flush()
        self.__consumer.close()
        self.__processor_factory.shutdown()
        logger.info("Stopped")

        # if there was an active processing strategy, it should be shut down
        # and unset when the partitions are revoked during consumer close
        assert (
            self.__processing_strategy is None
        ), "processing strategy was not closed on shutdown"
