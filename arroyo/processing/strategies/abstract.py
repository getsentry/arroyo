from abc import ABC, abstractmethod
from typing import Generic, Optional

from arroyo.types import Commit, Message, TStrategyPayload


class MessageRejected(Exception):
    """
    MessageRejected should be raised in a processing strategy's submit method
    if it is unable to keep up with the rate of incoming messages. It tells
    the consumer to slow down and retry the message later.
    """

    pass


class ProcessingStrategy(ABC, Generic[TStrategyPayload]):
    """
    A processing strategy defines how a stream processor processes messages
    during the course of a single assignment. The processor is instantiated
    when the assignment is received, and closed when the assignment is
    revoked.

    This interface is intentionally not prescriptive, and affords a
    significant degree of flexibility for the various implementations.
    """

    @abstractmethod
    def poll(self) -> None:
        """
        Poll the processor to check on the status of asynchronous tasks or
        perform other scheduled work.

        This method is called on each consumer loop iteration, so this method
        should not be used to perform work that may block for a significant
        amount of time and block the progress of the consumer or exceed the
        consumer poll interval timeout.

        This method may raise exceptions that were thrown by asynchronous
        tasks since the previous call to ``poll``.
        """
        raise NotImplementedError

    @abstractmethod
    def submit(self, message: Message[TStrategyPayload]) -> None:
        """
        Submit a message for processing.

        Messages may be processed synchronously or asynchronously, depending
        on the implementation of the processing strategy. Callers of this
        method should not assume that this method returning successfully
        implies that the message was successfully processed.

        If the processing strategy is unable to accept a message (due to it
        being at or over capacity, for example), this method will raise a
        ``MessageRejected`` exception.
        """
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        """
        Shutdown this instance. No more messages should be accepted by the
        instance after this method has been called.

        Called by the shutdown method of the stream processor.
        """
        raise NotImplementedError

    @abstractmethod
    def terminate(self) -> None:
        """
        Close the processing strategy immediately, abandoning any work in
        progress. No more messages should be accepted by the instance after
        this method has been called.
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: Optional[float]) -> None:
        """
        Block until the processing strategy has completed all previously
        submitted work, or the provided timeout has been reached.

        This method is called synchronously by the stream processor during
        assignment revocation, and blocks the assignment from being released
        until this function exits, allowing any work in progress to be
        completed and committed before the continuing the rebalancing
        process.
        """
        raise NotImplementedError


class LegacyProcessingStrategy(ProcessingStrategy[TStrategyPayload]):
    """
    Old style processing strategy, which is closed and recreated
    on every rebalance. To be deprecated.
    """

    @abstractmethod
    def join(self, timeout: Optional[float]) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def flush(self, timeout: Optional[float]) -> None:
        self.close()
        self.join(timeout)

    def shutdown(self) -> None:
        self.close()
        self.join(None)


class ProcessingStrategyFactory(ABC, Generic[TStrategyPayload]):
    """
    A ProcessingStrategyFactory is used to wrap a series of
    ProcessingStrategy steps, and calls `create_with_partitions`
    to instantiate the ProcessingStrategy on partition assignment
    or partition revocation if the strategy needs to be recreated.
    """

    @abstractmethod
    def create(self, commit: Commit) -> ProcessingStrategy[TStrategyPayload]:
        raise NotImplementedError
