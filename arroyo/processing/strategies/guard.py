import logging
from typing import Callable, MutableMapping, Optional, Union, cast

from arroyo.dlq import InvalidMessage, InvalidMessageState
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Partition, TStrategyPayload

BasicStrategy = ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]
BasicMessage = Message[Union[FilteredPayload, TStrategyPayload]]
BuildProcessingStrategy = Callable[
    [BasicStrategy[TStrategyPayload]], BasicStrategy[TStrategyPayload]
]

logger = logging.getLogger(__name__)


class _StrategyGuardAfter(BasicStrategy[TStrategyPayload]):
    def __init__(self, next_step: BasicStrategy[TStrategyPayload]):
        self.__next_step = next_step
        self.__committable: MutableMapping[Partition, int] = {}

    def submit(self, message: BasicMessage[TStrategyPayload]) -> None:
        for partition, offset in message.committable.items():
            if self.__committable.setdefault(partition, offset) > offset:
                logger.warning(
                    "Submitted a message with committable {%s: %s}, "
                    "but we already submitted a message with a higher offset before.\n\n"
                    "Either Arroyo has a bug, or you are writing a custom "
                    "strategy that has odd control flow and cannot use "
                    "StrategyGuard",
                    partition,
                    offset,
                )
        self.__next_step.submit(message)

    def poll(self) -> None:
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout=timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()


class StrategyGuard(BasicStrategy[TStrategyPayload]):
    """
    A wrapper around a strategy class that implements message filtering and
    dead letter queue support for the strategy.

    Can only be used in certain situations where the strategy's own control
    flow is simple and immediate, i.e. does not involve any batching.

    This is currently only an experiment in that we are not sure about its API.
    Only for internal use.

    Note that custom strategies generally don't have to deal with FilteredPayload and
    InvalidMessage exceptions in simple consumers. If there is no filter step
    or DLQing, there will be no filtered messages. If there is no explicit
    handling of InvalidMessage, DLQed messages are not counted against commit
    policy, but otherwise DLQing works.
    """

    def __init__(
        self,
        build_inner_strategy: BuildProcessingStrategy[TStrategyPayload],
        next_step: BasicStrategy[TStrategyPayload],
    ) -> None:
        self.__next_step = next_step
        self.__next_step_wrapper = _StrategyGuardAfter(next_step)
        self.__inner_strategy = build_inner_strategy(self.__next_step_wrapper)
        self.__invalid_messages = InvalidMessageState()

    def submit(self, message: BasicMessage[TStrategyPayload]) -> None:
        if isinstance(message.payload, FilteredPayload):
            self.__next_step_wrapper.submit(cast(Message[FilteredPayload], message))
        else:
            try:
                self.__inner_strategy.submit(message)
            except InvalidMessage as e:
                self.__invalid_messages.append(e)
                raise e

    def __forward_invalid_offsets(self) -> None:
        if len(self.__invalid_messages):
            self.__inner_strategy.poll()
            filter_msg = self.__invalid_messages.build()
            if filter_msg:
                try:
                    self.__next_step_wrapper.submit(filter_msg)
                    self.__invalid_messages.reset()
                except MessageRejected:
                    pass

    def poll(self) -> None:
        self.__forward_invalid_offsets()
        try:
            self.__inner_strategy.poll()
        except InvalidMessage as e:
            self.__invalid_messages.append(e)
            raise e

    def join(self, timeout: Optional[float] = None) -> None:
        try:
            self.__inner_strategy.join(timeout)
        except InvalidMessage:
            # We cannot forward offsets here since the inner strategy is already
            # marked as closed. Log the exception and move on. The message will get
            # reprocessed properly when the consumer is restarted.
            logger.warning("Invalid message in join", exc_info=True)

    def close(self) -> None:
        # Forward invalid offsets first. Once the inner strategy is closed, we can no
        # longer submit invalid messages to it.
        self.__forward_invalid_offsets()
        self.__inner_strategy.close()

    def terminate(self) -> None:
        self.__inner_strategy.terminate()
