from typing import Optional
import time

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import TStrategyPayload, Message
from arroyo.utils.metrics import get_metrics

HEALTHCHECK_MAX_FREQUENCY_SEC = 1.0  # In seconds


class Healthcheck(ProcessingStrategy[TStrategyPayload]):
    """
    A strategy that takes a filepath, and touches that file everytime
    `Strategy.poll` is called. If that function is not called multiple times
    per minute, it indicates that the consumer is overall unhealthy.

    File touches are debounced to happen once per second at most.
    """

    def __init__(
        self, healthcheck_file: str, next_step: ProcessingStrategy[TStrategyPayload]
    ):
        self.__healthcheck_file = healthcheck_file
        self.__healthcheck_file_touched_at: Optional[float] = None
        self.__next_step = next_step
        self.__metrics = get_metrics()

    def submit(self, message: Message[TStrategyPayload]) -> None:
        self.__next_step.submit(message)

    def poll(self) -> None:
        self.__next_step.poll()
        now = time.time()

        if (
            self.__healthcheck_file_touched_at is not None
            and self.__healthcheck_file_touched_at + HEALTHCHECK_MAX_FREQUENCY_SEC > now
        ):
            return

        with open(self.__healthcheck_file, "w"):
            # touch the file
            self.__metrics.increment("arroyo.processing.strategies.healthcheck.touch")
            pass

        self.__healthcheck_file_touched_at = now

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()
