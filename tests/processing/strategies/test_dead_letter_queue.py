import time
from datetime import datetime
from typing import Callable, Mapping, MutableSequence, Optional, Tuple

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.dead_letter_queue.dead_letter_queue import (
    DeadLetterQueue,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    InvalidMessage,
)
from arroyo.processing.strategies.dead_letter_queue.policies.count import (
    CountInvalidMessagePolicy,
)
from arroyo.processing.strategies.dead_letter_queue.policies.ignore import (
    IgnoreInvalidMessagePolicy,
)
from arroyo.processing.strategies.dead_letter_queue.policies.raise_e import (
    RaiseInvalidMessagePolicy,
)
from arroyo.types import Message, Partition, Position, Topic


class FakeProcessingStep(ProcessingStrategy[KafkaPayload]):
    """
    Raises InvalidMessage if a submitted message has no key in payload.
    """

    def poll(self) -> None:
        raise InvalidMessage(
            Message(
                Partition(Topic(""), 0), 0, KafkaPayload(None, b"", []), datetime.now()
            )
        )

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def terminate(self) -> None:
        pass

    def close(self) -> None:
        pass

    def submit(self, message: Message[KafkaPayload]) -> None:
        """
        Valid message is one with a key.
        """
        if message.payload.key is None:
            raise InvalidMessage(message)


class FakeProcessingStepFactory(ProcessingStrategyFactory[KafkaPayload]):
    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[KafkaPayload]:
        return FakeProcessingStep()


@pytest.fixture
def processing_step() -> ProcessingStrategy[KafkaPayload]:
    return FakeProcessingStep()


@pytest.fixture
def valid_message() -> Message[KafkaPayload]:
    valid_payload = KafkaPayload(b"", b"", [])
    return Message(Partition(Topic(""), 0), 0, valid_payload, datetime.now())


@pytest.fixture
def invalid_message() -> Message[KafkaPayload]:
    invalid_payload = KafkaPayload(None, b"", [])
    return Message(Partition(Topic(""), 0), 0, invalid_payload, datetime.now())


def test_raise(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_raise: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step, RaiseInvalidMessagePolicy()
    )
    dlq_raise.submit(valid_message)
    with pytest.raises(InvalidMessage):
        dlq_raise.submit(invalid_message)
    with pytest.raises(InvalidMessage):
        dlq_raise.poll()


def test_ignore(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_ignore: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step, IgnoreInvalidMessagePolicy()
    )
    dlq_ignore.submit(valid_message)
    dlq_ignore.submit(invalid_message)


def test_count(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step, CountInvalidMessagePolicy(5)
    )
    dlq_count.submit(valid_message)
    for _ in range(5):
        dlq_count.submit(invalid_message)
    with pytest.raises(InvalidMessage):
        dlq_count.submit(invalid_message)


def test_count_short(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count_short: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step, CountInvalidMessagePolicy(5, 1)
    )
    dlq_count_short.submit(valid_message)
    for _ in range(5):
        dlq_count_short.submit(invalid_message)
    with pytest.raises(InvalidMessage):
        dlq_count_short.submit(invalid_message)
    time.sleep(1)
    dlq_count_short.submit(invalid_message)


def test_stateful_count(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:

    now = int(datetime.now().timestamp())
    state: MutableSequence[Tuple[int, int]] = [(now - 1, 2), (now, 2)]

    # Stateful count DLQ intialized with 4 hits in the state
    dlq_count_load_state: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(
            limit=5,
            load_state=state,
        ),
    )

    dlq_count_load_state.submit(valid_message)

    # Limit is 5, 4 hits exist, 1 more should be added without exception raised
    dlq_count_load_state.submit(invalid_message)

    # Limit is 5, 5 hits exist, next invalid message should cause exception
    with pytest.raises(InvalidMessage):
        dlq_count_load_state.submit(invalid_message)
