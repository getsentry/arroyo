import logging
from typing import Callable, Optional, Union, cast

from arroyo.processing.strategies.abstract import FILTERED_PAYLOAD, FilteredPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.types import Message, TPayload

logger = logging.getLogger(__name__)


class FilterStep(ProcessingStep[TPayload]):
    """
    Determines if a message should be submitted to the next processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], bool],
        next_step: ProcessingStep[Union[FilteredPayload, TPayload]],
        consecutive_drop_limit: Optional[int] = None,
    ):
        self.__test_function = function
        self.__next_step = next_step
        self.__consecutive_drop_limit = consecutive_drop_limit

        self.__closed = False
        self.__consecutive_drop_count = 0

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.__test_function(message):
            cast_message = cast(Message[Union[FilteredPayload, TPayload]], message)
            self.__next_step.submit(cast_message)
            self.__consecutive_drop_count = 0
        else:
            self.__consecutive_drop_count += 1

            if (
                self.__consecutive_drop_limit is not None
                and self.__consecutive_drop_count >= self.__consecutive_drop_limit
            ):
                cast_message = cast(
                    Message[Union[FilteredPayload, TPayload]],
                    message.replace(FILTERED_PAYLOAD),
                )
                self.__next_step.submit(cast_message)
                self.__consecutive_drop_count = 0

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
