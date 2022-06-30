from abc import ABC, abstractmethod
from typing import Optional

from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidMessages,
)


class DeadLetterQueuePolicy(ABC):
    """
    A DLQ Policy defines how to handle invalid messages.
    """

    @abstractmethod
    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        """
        Decide what to do with invalid messages.
        """
        raise NotImplementedError()

    @abstractmethod
    def join(self, timeout: Optional[float]) -> None:
        """
        Cleanup any asynchronous tasks that may be running.
        """
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        """
        Close the policy from handling any new messages.
        """
        raise NotImplementedError()

    @abstractmethod
    def terminate(self) -> None:
        """
        Immediately close this policy, abandoning any work in
        progress
        """
        raise NotImplementedError()
