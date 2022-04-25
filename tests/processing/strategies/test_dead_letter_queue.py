import json
import time
from datetime import datetime
from typing import MutableSequence, Optional, Tuple

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.dead_letter_queue.dead_letter_queue import (
    DeadLetterQueue,
)
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    InvalidMessage,
    InvalidMessages,
)
from arroyo.processing.strategies.dead_letter_queue.policies.count import (
    CountInvalidMessagePolicy,
)
from arroyo.processing.strategies.dead_letter_queue.policies.ignore import (
    IgnoreInvalidMessagePolicy,
)
from arroyo.processing.strategies.dead_letter_queue.policies.produce import (
    ProduceInvalidMessagePolicy,
)
from arroyo.processing.strategies.dead_letter_queue.policies.raise_e import (
    RaiseInvalidMessagePolicy,
)
from arroyo.types import Message, Partition, Topic

NO_KEY = "No key"


class FakeProcessingStep(ProcessingStrategy[KafkaPayload]):
    """
    Raises InvalidMessages if a submitted message has no key in payload.
    """

    def poll(self) -> None:
        raise InvalidMessages(
            [InvalidMessage("a bad message", datetime.now(), reason=NO_KEY)]
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
            raise InvalidMessages(
                [
                    InvalidMessage(
                        payload=str(message.payload),
                        timestamp=message.timestamp,
                        offset=message.offset,
                        partition=message.partition.index,
                        reason=NO_KEY,
                    )
                ]
            )


class FakeBatchingProcessingStep(FakeProcessingStep):
    """
    Batches up to 5 messages.
    """

    def __init__(self) -> None:
        self._batch: MutableSequence[Message[KafkaPayload]] = []

    def submit(self, message: Message[KafkaPayload]) -> None:
        self._batch.append(message)
        if len(self._batch) > 4:
            self._submit_multiple()

    def _submit_multiple(self) -> None:
        """
        Valid message is one with a key.
        """
        bad_messages = [
            InvalidMessage(payload=str(message.payload), timestamp=message.timestamp)
            for message in self._batch
            if message.payload.key is None
        ]
        self._batch = []
        if bad_messages:
            raise InvalidMessages(bad_messages)


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
    with pytest.raises(InvalidMessages):
        dlq_raise.submit(invalid_message)
    with pytest.raises(InvalidMessages):
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
        processing_step,
        CountInvalidMessagePolicy(next_policy=IgnoreInvalidMessagePolicy(), limit=5),
    )
    dlq_count.submit(valid_message)
    for _ in range(5):
        dlq_count.submit(invalid_message)
    with pytest.raises(InvalidMessages):
        dlq_count.submit(invalid_message)


def test_count_short(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count_short: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(
            next_policy=IgnoreInvalidMessagePolicy(), limit=5, seconds=1
        ),
    )
    dlq_count_short.submit(valid_message)
    for _ in range(5):
        dlq_count_short.submit(invalid_message)
    with pytest.raises(InvalidMessages):
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
            next_policy=IgnoreInvalidMessagePolicy(),
            limit=5,
            load_state=state,
        ),
    )

    dlq_count_load_state.submit(valid_message)

    # Limit is 5, 4 hits exist, 1 more should be added without exception raised
    dlq_count_load_state.submit(invalid_message)

    # Limit is 5, 5 hits exist, next invalid message should cause exception
    with pytest.raises(InvalidMessages):
        dlq_count_load_state.submit(invalid_message)


def test_multiple_invalid_messages(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
) -> None:
    fake_batching_processor = FakeBatchingProcessingStep()
    count_policy = CountInvalidMessagePolicy(
        next_policy=IgnoreInvalidMessagePolicy(), limit=5
    )
    dlq_count: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        fake_batching_processor, count_policy
    )

    """
    Batch submits on 5th message, count policy raises on 6th invalid message processed.

    First batch submitted on 3rd iteration with 3 invalid and 2 valid messages
    - count policy now holds 3 invalid messages

    Second batch submitted on 5th iteration with 3 valid and and 2 invalid messages
    - count policy now holds 5 invalid messages
    """
    for _ in range(5):
        dlq_count.submit(invalid_message)
        dlq_count.submit(valid_message)

    # build the next batch with 4 invalid messages, count policy still only sees 5 invalid messages
    for _ in range(4):
        dlq_count.submit(invalid_message)
    assert count_policy._count() == 5

    """
    Next message submitted triggers batch to submit
    - submits 4 batched invalid messages to the count policy, triggering it to raise
    """
    with pytest.raises(InvalidMessages) as e_info:
        dlq_count.submit(valid_message)

    assert len(e_info.value.messages) == 4
    assert count_policy._count() == 9


def test_produce_invalid_messages(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
    broker: LocalBroker[KafkaPayload],
) -> None:
    producer = broker.get_producer()
    topic = Topic("test-dead-letter-topic")
    broker.create_topic(topic, 1)
    produce_policy = ProduceInvalidMessagePolicy(
        producer, Topic("test-dead-letter-topic")
    )
    dlq_produce: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step, produce_policy
    )

    consumer = broker.get_consumer("test-group")
    consumer.subscribe([topic])

    # valid message should not be produced to dead-letter topic
    dlq_produce.submit(valid_message)
    assert consumer.poll() is None

    # invalid message should
    dlq_produce.submit(invalid_message)

    produced_message = consumer.poll()
    assert produced_message is not None

    # produced message should have appropriate info
    dead_letter_payload = produced_message.payload.value
    dead_letter_dict = json.loads(dead_letter_payload)
    assert dead_letter_dict["reason"] == NO_KEY
