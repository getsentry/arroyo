from typing import Optional

from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    InvalidMessages,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
)


class RaiseInvalidMessagePolicy(DeadLetterQueuePolicy):
    def __init__(self) -> None:
        self.__closed = False

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        assert not self.__closed
        raise e

    def join(self, timeout: Optional[float]) -> None:
        return

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()
