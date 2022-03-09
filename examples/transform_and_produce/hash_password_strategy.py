import hashlib
import json
import logging
from typing import Optional

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategy as ProcessingStep
from arroyo.types import Message

logger = logging.getLogger(__name__)


class HashPasswordStrategy(ProcessingStrategy[KafkaPayload]):
    """
    Hash Password Step.

    Given a message, hashes password, submits hashed credentials to next step.
    """

    def __init__(self, next_step: ProcessingStep[KafkaPayload]) -> None:
        self.__next_step = next_step
        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[KafkaPayload]) -> None:
        assert not self.__closed

        # Expected format of the message is {"username": "<username>", "password": "<password>"}
        auth = json.loads(message.payload.value)
        hashed = self._hash_password(auth["password"])
        data = json.dumps({"username": auth["username"], "password": hashed}).encode(
            "utf-8"
        )

        # Build a new message and submit to next step
        self.__next_step.submit(
            Message(
                message.partition,
                message.offset,
                KafkaPayload(key=None, value=data, headers=[]),
                message.timestamp,
            )
        )

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.close()

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
