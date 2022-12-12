from typing import Any, Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Commit, Message


class CommitOffsets(ProcessingStrategy[Any]):
    """
    Just commits offsets.

    This should always be used as the last step in a chain of processing
    strategies. It commits offsets back to the broker after all prior
    processing of that message is completed.
    """

    def __init__(self, commit: Commit) -> None:
        self.__commit = commit

    def poll(self) -> None:
        pass

    def submit(self, message: Message[Any]) -> None:
        self.__commit(message.committable)

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        # Commit all previously staged offsets
        self.__commit({}, force=True)
