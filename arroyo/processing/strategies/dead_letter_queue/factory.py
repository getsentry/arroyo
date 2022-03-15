from typing import Callable, Mapping

from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.dead_letter_queue.dead_letter_queue import (
    DeadLetterQueue,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
)
from arroyo.types import Partition, Position, TPayload


class DeadLetterQueueFactory(ProcessingStrategyFactory[TPayload]):
    """
    DLQ Factory which wraps a given ProcessingStrategyFactory with
    a given DLQ Policy.
    """

    def __init__(
        self,
        processing_strategy_factory: ProcessingStrategyFactory[TPayload],
        policy: DeadLetterQueuePolicy[TPayload],
    ) -> None:
        self.__next_step_factory = processing_strategy_factory
        self.__policy = policy

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[TPayload]:
        next_step = self.__next_step_factory.create(commit)
        return DeadLetterQueue(next_step, self.__policy)
