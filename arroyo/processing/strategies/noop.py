from __future__ import annotations

from typing import Optional, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload


class Noop(
    ProcessingStrategy[Union[FilteredPayload, object]],
):
    """
    Noop strategy that takes a message and does nothing.
    """

    def __init__(self) -> None:
        pass

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        pass

    def poll(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass
