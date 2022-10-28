from __future__ import annotations

import logging
import time
from typing import Generic, Mapping, Optional, Sequence

from arroyo.backends.abstract import Consumer
from arroyo.commit import CommitPolicy
from arroyo.errors import RecoverableError
from arroyo.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Message, Partition, Position, Topic, TPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)

LOG_THRESHOLD_TIME = 5  # In seconds


class InvalidStateError(RuntimeError):
    pass


class MetricsBuffer:
    def __init__(self) -> None:
        self.__metrics = get_metrics()
        self.__metrics_frequency_sec = 1.0
        self.__reset()

    def add_poll_time(self, duration: float) -> None:
        self.__consumer_poll_time += duration
        self.__throttled_record()

    def add_processing_time(self, duration: float) -> None:
        self.__processing_time += duration
        self.__throttled_record()

    def add_pause_time(self, duration: float) -> None:
        self.__paused_time += duration
        self.__throttled_record()

    def flush(self) -> None:
        self.__metrics.timing("arroyo.consumer.poll.time", self.__consumer_poll_time)
        self.__metrics.timing("arroyo.consumer.processing.time", self.__processing_time)
        self.__metrics.timing("arroyo.consumer.paused.time", self.__paused_time)
        self.__reset()

    def __reset(self) -> None:
        self.__consumer_poll_time = 0.0
        self.__processing_time = 0.0
        self.__paused_time = 0.0
        self.__last_record_time = time.time()

    def __throttled_record(self) -> None:
        if time.time() - self.__last_record_time > self.__metrics_frequency_sec:
            self.flush()


class StreamProcessor(Generic[TPayload]):
    """
    A stream processor manages the relationship between a ``Consumer``
    instance and a ``ProcessingStrategy``, ensuring that processing
    strategies are instantiated on partition assignment and closed on
    partition revocation.
    """

    def __init__(
        self,
        consumer: Consumer[TPayload],
        topic: Topic,
        processor_factory: ProcessingStrategyFactory[TPayload],
        commit_policy: CommitPolicy,
    ) -> None:
        self.__consumer = consumer
        self.__processor_factory = processor_factory
        self.__metrics_buffer = MetricsBuffer()

        self.__processing_strategy: Optional[ProcessingStrategy[TPayload]] = None

        self.__message: Optional[Message[TPayload]] = None

        # If the consumer is in the paused state, this is when the last call to
        # ``pause`` occurred.
        self.__paused_timestamp: Optional[float] = None
        self.__last_log_timestamp: Optional[float] = None

        self.__commit_policy = commit_policy
        self.__last_committed_time: float = time.time()
        self.__messages_since_last_commit = 0

        self.__shutdown_requested = False

        def _close_strategy() -> None:
            if self.__processing_strategy is None:
                raise InvalidStateError(
                    "received unexpected revocation without active processing strategy"
                )

            logger.debug("Closing %r...", self.__processing_strategy)
            self.__processing_strategy.close()

            logger.debug("Waiting for %r to exit...", self.__processing_strategy)
            self.__processing_strategy.join()

            logger.debug(
                "%r exited successfully, releasing assignment.",
                self.__processing_strategy,
            )
            self.__processing_strategy = None
            self.__message = None  # avoid leaking buffered messages across assignments

        def _create_strategy(partitions: Mapping[Partition, int]) -> None:
            self.__processing_strategy = (
                self.__processor_factory.create_with_partitions(
                    self.__commit, partitions
                )
            )
            logger.debug(
                "Initialized processing strategy: %r", self.__processing_strategy
            )

        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            logger.info("New partitions assigned: %r", partitions)
            if partitions:
                if self.__processing_strategy is not None:
                    _close_strategy()
                _create_strategy(partitions)

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

    def __commit(
        self, positions: Mapping[Partition, Position], force: bool = False
    ) -> None:
        """
        If force is passed, commit immediately and do not throttle. This should
        be used during consumer shutdown where we do not want to wait before committing.
        """
        self.__consumer.stage_positions(positions)
        self.__messages_since_last_commit += 1

        if force or self.__commit_policy.should_commit(
            self.__last_committed_time, self.__messages_since_last_commit
        ):
            start = time.time()
            self.__consumer.commit_positions()
            logger.debug(
                "Waited %0.4f seconds for offsets to be committed to %r.",
                time.time() - start,
                self.__consumer,
            )
            self.__last_committed_time = start
            self.__messages_since_last_commit = 0

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

    def _run_once(self) -> None:
        message_carried_over = self.__message is not None

        if message_carried_over:
            # If a message was carried over from the previous run, the consumer
            # should be paused and not returning any messages on ``poll``.
            if self.__consumer.poll(timeout=0) is not None:
                raise InvalidStateError(
                    "received message when consumer was expected to be paused"
                )
        else:
            # Otherwise, we need to try fetch a new message from the consumer,
            # even if there is no active assignment and/or processing strategy.
            try:
                start_poll = time.time()
                self.__message = self.__consumer.poll(timeout=1.0)
                self.__metrics_buffer.add_poll_time(time.time() - start_poll)
            except RecoverableError:
                return

        if self.__processing_strategy is not None:
            start_poll = time.time()
            self.__processing_strategy.poll()
            self.__metrics_buffer.add_processing_time(time.time() - start_poll)
            if self.__message is not None:
                try:
                    start_submit = time.time()
                    self.__processing_strategy.submit(self.__message)
                    self.__metrics_buffer.add_processing_time(
                        time.time() - start_submit
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
                        # Log paused condition every 5 seconds at most
                        current_time = time.time()
                        if self.__last_log_timestamp:
                            paused_duration: Optional[float] = (
                                current_time - self.__last_log_timestamp
                            )
                        elif self.__paused_timestamp:
                            paused_duration = current_time - self.__paused_timestamp
                        else:
                            paused_duration = None

                        if (
                            paused_duration is not None
                            and paused_duration > LOG_THRESHOLD_TIME
                        ):
                            self.__last_log_timestamp = current_time
                            logger.info(
                                "Paused for longer than %d seconds", LOG_THRESHOLD_TIME
                            )
                            self.__metrics_buffer.add_pause_time(paused_duration)

                else:
                    # If we were trying to submit a message that failed to be
                    # submitted on a previous run, we can resume accepting new
                    # messages.
                    if message_carried_over:
                        assert self.__paused_timestamp is not None
                        paused_duration = time.time() - self.__paused_timestamp
                        logger.debug(
                            "Successfully submitted %r, resuming consumer after %0.4f seconds...",
                            self.__message,
                            paused_duration,
                        )
                        self.__consumer.resume([*self.__consumer.tell().keys()])
                        last_recorded = (
                            self.__last_log_timestamp
                            if self.__last_log_timestamp is not None
                            else self.__paused_timestamp
                        )

                        self.__metrics_buffer.add_pause_time(
                            time.time() - last_recorded
                        )

                        self.__paused_timestamp = None
                        self.__last_log_timestamp = None

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
