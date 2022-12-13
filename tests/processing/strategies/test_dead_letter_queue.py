import json
import time
from datetime import datetime
from typing import Any, Mapping, MutableSequence, Optional, Tuple
from unittest.mock import patch

import pytest

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.dead_letter_queue.dead_letter_queue import (
    DeadLetterQueue,
)
from arroyo.processing.strategies.dead_letter_queue.invalid_messages import (
    DATE_TIME_FORMAT,
    InvalidKafkaMessage,
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
from arroyo.types import BrokerValue, Message, Partition, Topic

NO_KEY = "No key"
BAD_PAYLOAD = "Bad payload"
NOW = datetime.now()


def kafka_message_to_invalid_kafka_message(
    message: Message[KafkaPayload], reason: str
) -> InvalidKafkaMessage:
    consumer_payload = message.value
    assert isinstance(consumer_payload, BrokerValue)
    return InvalidKafkaMessage(
        payload=consumer_payload.payload.value,
        timestamp=consumer_payload.timestamp,
        topic=consumer_payload.partition.topic.name,
        consumer_group="",
        partition=consumer_payload.partition.index,
        offset=consumer_payload.offset,
        headers=consumer_payload.payload.headers,
        reason=reason,
    )


class FakeProcessingStep(ProcessingStrategy[KafkaPayload]):
    """
    Raises InvalidMessages if a submitted message has no key in payload.
    """

    def __raise(self) -> None:
        raise InvalidMessages(
            [
                InvalidKafkaMessage(
                    payload=b"a bad message",
                    timestamp=NOW,
                    topic="",
                    consumer_group="",
                    partition=0,
                    offset=0,
                    headers=[],
                    reason=NO_KEY,
                )
            ]
        )

    def poll(self) -> None:
        self.__raise()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__raise()

    def terminate(self) -> None:
        pass

    def close(self) -> None:
        pass

    def submit(self, message: Message[KafkaPayload]) -> None:
        """
        Valid message is one with a key and decodable value.
        """
        reason: str = ""

        try:
            message.payload.value.decode("utf-8")
        except UnicodeDecodeError:
            reason = BAD_PAYLOAD
        else:
            if message.payload.key is None:
                reason = NO_KEY

        if reason:
            raise InvalidMessages(
                [kafka_message_to_invalid_kafka_message(message, reason)]
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

    def _process_message(self, message: Message[KafkaPayload]) -> None:
        """
        Some processing we want to happen per message.
        """
        if message.payload.key is None:
            raise InvalidMessages(
                [kafka_message_to_invalid_kafka_message(message, NO_KEY)]
            )

    def _submit_multiple(self) -> None:
        """
        Valid message is one with a key.
        """
        bad_messages: MutableSequence[InvalidMessage] = []
        for message in self._batch:
            try:
                self._process_message(message)
            except InvalidMessages as e:
                bad_messages += e.messages
        # At this point, we have some bad messages but the
        # good ones have been processed without failing entire batch
        self._batch = []
        if bad_messages:
            raise InvalidMessages(bad_messages)


@pytest.fixture
def processing_step() -> ProcessingStrategy[KafkaPayload]:
    return FakeProcessingStep()


@pytest.fixture
def valid_message() -> Message[KafkaPayload]:
    partition = Partition(Topic(""), 0)
    offset = 0

    return Message(
        BrokerValue(
            KafkaPayload(b"Key", b"Value", []),
            partition,
            offset,
            NOW,
        )
    )


@pytest.fixture
def invalid_message_no_key() -> Message[KafkaPayload]:
    partition = Partition(Topic(""), 0)
    offset = 0

    return Message(
        BrokerValue(
            KafkaPayload(None, b"Value", []),
            partition,
            offset,
            NOW,
        )
    )


@pytest.fixture
def invalid_message_bad_value() -> Message[KafkaPayload]:
    partition = Partition(Topic(""), 0)
    offset = 0

    return Message(
        BrokerValue(
            KafkaPayload(key=b"Key", value=b"\xff", headers=[]),
            partition,
            offset,
            NOW,
        )
    )


def test_raise(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_raise = DeadLetterQueue(processing_step, RaiseInvalidMessagePolicy())
    dlq_raise.submit(valid_message)
    with pytest.raises(InvalidMessages):
        dlq_raise.submit(invalid_message_no_key)
    with pytest.raises(InvalidMessages):
        dlq_raise.poll()


def test_ignore(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_ignore = DeadLetterQueue(processing_step, IgnoreInvalidMessagePolicy())
    dlq_ignore.submit(valid_message)
    dlq_ignore.submit(invalid_message_no_key)


def test_dlq_join(processing_step: FakeProcessingStep) -> None:
    # processing step should raise, dlq should handle within join
    policy = IgnoreInvalidMessagePolicy()
    dlq_ignore = DeadLetterQueue(processing_step, policy)
    with patch.object(policy, "handle_invalid_messages") as mock:
        dlq_ignore.join()
    mock.assert_called_once()


def test_count(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(next_policy=IgnoreInvalidMessagePolicy(), limit=5),
    )
    dlq_count.submit(valid_message)
    for _ in range(5):
        dlq_count.submit(invalid_message_no_key)
    with pytest.raises(InvalidMessages):
        dlq_count.submit(invalid_message_no_key)


def test_count_short(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count_short = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(
            next_policy=IgnoreInvalidMessagePolicy(), limit=5, seconds=1
        ),
    )
    dlq_count_short.submit(valid_message)
    for _ in range(5):
        dlq_count_short.submit(invalid_message_no_key)
    with pytest.raises(InvalidMessages):
        dlq_count_short.submit(invalid_message_no_key)
    time.sleep(1)
    dlq_count_short.submit(invalid_message_no_key)


def test_stateful_count(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:

    now = int(datetime.now().timestamp())
    state: MutableSequence[Tuple[int, int]] = [(now - 1, 2), (now, 2)]

    # Stateful count DLQ intialized with 4 hits in the state
    dlq_count_load_state = DeadLetterQueue(
        processing_step,
        CountInvalidMessagePolicy(
            next_policy=IgnoreInvalidMessagePolicy(),
            limit=5,
            load_state=state,
        ),
    )

    dlq_count_load_state.submit(valid_message)

    # Limit is 5, 4 hits exist, 1 more should be added without exception raised
    dlq_count_load_state.submit(invalid_message_no_key)

    # Limit is 5, 5 hits exist, next invalid message should cause exception
    with pytest.raises(InvalidMessages):
        dlq_count_load_state.submit(invalid_message_no_key)


def test_invalid_batched_messages(
    valid_message: Message[KafkaPayload],
    invalid_message_no_key: Message[KafkaPayload],
) -> None:
    fake_batching_processor = FakeBatchingProcessingStep()
    count_policy = CountInvalidMessagePolicy(
        next_policy=IgnoreInvalidMessagePolicy(), limit=5
    )
    dlq_count = DeadLetterQueue(fake_batching_processor, count_policy)

    """
    Batch submits on 5th message, count policy raises on 6th invalid message processed.

    First batch submitted on 3rd iteration with 3 invalid and 2 valid messages
    - count policy now holds 3 invalid messages

    Second batch submitted on 5th iteration with 3 valid and and 2 invalid messages
    - count policy now holds 5 invalid messages
    """
    for _ in range(5):
        dlq_count.submit(invalid_message_no_key)
        dlq_count.submit(valid_message)

    # build the next batch with 4 invalid messages, count policy still only sees 5 invalid messages
    for _ in range(4):
        dlq_count.submit(invalid_message_no_key)
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
    invalid_message_no_key: Message[KafkaPayload],
    invalid_message_bad_value: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
    broker: LocalBroker[KafkaPayload],
) -> None:
    producer = broker.get_producer()
    topic = Topic("test-dead-letter-topic")
    broker.create_topic(topic, 1)
    produce_policy = ProduceInvalidMessagePolicy(
        producer, Topic("test-dead-letter-topic")
    )
    dlq_produce = DeadLetterQueue(processing_step, produce_policy)

    consumer = broker.get_consumer("test-group")
    consumer.subscribe([topic])

    # valid message should not be produced to dead-letter topic
    dlq_produce.submit(valid_message)
    assert consumer.poll() is None

    # invalid messages should
    dlq_produce.submit(invalid_message_no_key)
    dlq_produce.submit(invalid_message_bad_value)

    value = consumer.poll()
    assert value is not None
    produced_message = Message(value)
    assert_produced_message_is_expected(
        produced_message,
        {
            "payload": "Value",
            "timestamp": NOW.strftime(DATE_TIME_FORMAT),
            "topic": "",
            "consumer_group": "",
            "partition": 0,
            "offset": 0,
            "headers": [],
            "key": None,
            "reason": NO_KEY,
        },
    )

    value = consumer.poll()
    assert value is not None
    produced_message = Message(value)
    assert_produced_message_is_expected(
        produced_message,
        {
            "payload": "(base64) /w==",
            "timestamp": NOW.strftime(DATE_TIME_FORMAT),
            "topic": "",
            "consumer_group": "",
            "partition": 0,
            "offset": 0,
            "headers": [],
            "key": None,
            "reason": BAD_PAYLOAD,
        },
    )


def assert_produced_message_is_expected(
    produced_message: Optional[Message[KafkaPayload]],
    expected_dict: Mapping[str, Any],
) -> None:
    assert produced_message is not None
    # produced message should have appropriate info
    dead_letter_payload = produced_message.payload.value
    dead_letter_dict = json.loads(dead_letter_payload)
    assert dead_letter_dict == expected_dict
