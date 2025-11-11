import logging
from typing import Callable, Optional, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message, TStrategyPayload
from arroyo.utils.metrics import get_metrics

logger = logging.getLogger(__name__)


class FilterStep(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    """
    Determines if a message should be submitted to the next processing step.

    `FilterStep` takes a callback, `function`, and if that callback returns
    `False`, the message is dropped.
    """

    def __init__(
        self,
        function: Callable[[Message[TStrategyPayload]], bool],
        next_step: ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]],
    ):
        self.__test_function = function
        self.__next_step = next_step
        self.__closed = False
        self.__metrics = get_metrics()

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed

        if not isinstance(message.payload, FilteredPayload) and self.__test_function(
            cast(Message[TStrategyPayload], message)
        ):
            self.__next_step.submit(message)
        else:
            self.__metrics.increment("arroyo.strategies.filter.dropped_messages")

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout=timeout)
