from datetime import datetime
from typing import Optional
from arroyo.dead_letter_queue.policies.abstract import (
    InvalidMessage,
)

from arroyo.dead_letter_queue.dead_letter_queue import DeadLetterQueue

from arroyo.backends.kafka import KafkaPayload
from arroyo.dead_letter_queue.policies.raise_e import RaiseInvalidMessagePolicy
from arroyo.dead_letter_queue.policies.ignore import IgnoreInvalidMessagePolicy
from arroyo.dead_letter_queue.policies.count import CountInvalidMessagePolicy
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Message, Partition, Topic

import pytest


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


def test_dlq() -> None:
    dlq_raise = DeadLetterQueue(FakeProcessingStep(), RaiseInvalidMessagePolicy())
    dlq_ignore = DeadLetterQueue(FakeProcessingStep(), IgnoreInvalidMessagePolicy())
    dlq_count = DeadLetterQueue(FakeProcessingStep(), CountInvalidMessagePolicy(5))

    partition = Partition(Topic(""), 0)
    valid_payload = KafkaPayload(b"", b"", [])
    invalid_payload = KafkaPayload(None, b"", [])
    timestamp = datetime.now()

    valid_message = Message(partition, 0, valid_payload, timestamp)
    invalid_message = Message(partition, 0, invalid_payload, timestamp)

    with pytest.raises(InvalidMessage):
        dlq_raise.submit(invalid_message)

    dlq_raise.submit(valid_message)

    dlq_ignore.submit(valid_message)
    dlq_ignore.submit(invalid_message)

    dlq_count.submit(valid_message)
    for _ in range(5):
        dlq_count.submit(invalid_message)
    with pytest.raises(InvalidMessage):
        dlq_count.submit(invalid_message)
