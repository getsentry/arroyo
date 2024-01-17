import time
from typing import Any, Optional

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Commit, Message
from arroyo.utils.metrics import get_metrics


class CommitOffsets(ProcessingStrategy[Any]):
    """
    Commits offset and records consumer latency metric.

    This should always be used as the last step in a chain of processing
    strategies. It commits offsets back to the broker after all prior
    processing of that message is completed.
    """

    def __init__(self, commit: Commit) -> None:
        self.__commit = commit
        self.__metrics = get_metrics()
        self.__last_record_time: Optional[float] = None

    def poll(self) -> None:
        self.__commit({})

    def submit(self, message: Message[Any]) -> None:
        now = time.time()
        if self.__last_record_time is None or now - self.__last_record_time > 1:
            if message.timestamp is not None:
                self.__metrics.timing(
                    "arroyo.consumer.latency", now - message.timestamp.timestamp()
                )
                self.__last_record_time = now
        self.__commit(message.committable)

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        # Commit all previously staged offsets
        self.__commit({}, force=True)
