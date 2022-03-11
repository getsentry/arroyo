import time
from datetime import datetime
from typing import MutableSequence, Optional, Tuple

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.dead_letter_queue.dead_letter_queue import DeadLetterQueue
from arroyo.dead_letter_queue.policies.abstract import InvalidMessage
from arroyo.dead_letter_queue.policies.count import CountInvalidMessagePolicy
from arroyo.dead_letter_queue.policies.ignore import IgnoreInvalidMessagePolicy
from arroyo.dead_letter_queue.policies.raise_e import RaiseInvalidMessagePolicy
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Message, Partition, Topic


class FakeProcessingStep(ProcessingStrategy[KafkaPayload]):
    """
    Raises InvalidMessage if a submitted message has no key in payload.
    """

    def poll(self) -> None:
        pass

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
            raise InvalidMessage


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
    dlq_raise = DeadLetterQueue(processing_step, RaiseInvalidMessagePolicy())  # type: ignore
    with pytest.raises(InvalidMessage):
        dlq_raise.submit(invalid_message)
    dlq_raise.submit(valid_message)


def test_ignore(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_ignore = DeadLetterQueue(processing_step, IgnoreInvalidMessagePolicy())  # type: ignore
    dlq_ignore.submit(valid_message)
    dlq_ignore.submit(invalid_message)


def test_count(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count = DeadLetterQueue(processing_step, CountInvalidMessagePolicy(5))  # type: ignore
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
    dlq_count_short = DeadLetterQueue(processing_step, CountInvalidMessagePolicy(5, 1))  # type: ignore
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

    state: MutableSequence[Tuple[int, int]] = []

    def add_hit(timestamp: int) -> None:
        """
        Callback func for stateful count policy.
        """
        for i, hit in enumerate(state):
            if hit[0] == timestamp:
                state[i] = (timestamp, hit[1] + 1)
                return
        state.append((timestamp, 1))

    # Stateful count DLQ initialized with empty state
    dlq_stateful_count = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(limit=5, load_state=state, add_hit_callback=add_hit),
    )  # type: ignore

    dlq_stateful_count.submit(valid_message)

    # Limit is 5, should raise on 6th invalid message
    for i in range(5):
        dlq_stateful_count.submit(invalid_message)
        assert state == [(int(datetime.now().timestamp()), i + 1)]
    with pytest.raises(InvalidMessage):
        dlq_stateful_count.submit(invalid_message)

    now = int(datetime.now().timestamp())
    state = [(now - 1, 2), (now, 2)]

    # Stateful count DLQ intialized with 4 hits in the state
    dlq_stateful_count_current_state = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(
            limit=5,
            load_state=state,
            add_hit_callback=add_hit,
        ),
    )  # type: ignore

    dlq_stateful_count_current_state.submit(valid_message)

    # Limit is 5, 4 hits exist, 1 more should be added without exception raised
    dlq_stateful_count_current_state.submit(invalid_message)

    # test callback worked
    assert state == [(now - 1, 2), (now, 3)]

    # Limit is 5, 5 hits exist, next invalid message should cause exception
    with pytest.raises(InvalidMessage):
        dlq_stateful_count_current_state.submit(invalid_message)
