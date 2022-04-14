import json

from arroyo.backends.kafka.consumer import KafkaPayload, KafkaProducer
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessages,
)
from arroyo.types import Topic
from arroyo.utils.metrics import get_metrics


class ProduceInvalidMessagePolicy(DeadLetterQueuePolicy):
    """
    Produces given InvalidMessages to a dead letter topic.

    Meant to be used as a baseclass for policies needing to produce
    invalid messages to a dead letter topic.
    """

    def __init__(self, producer: KafkaProducer, dead_letter_topic: Topic) -> None:
        self.__metrics = get_metrics()
        self.__dead_letter_topic = dead_letter_topic
        self.__producer = producer

    def handle_invalid_messages(self, e: InvalidMessages) -> None:
        """
        Produces a message to the given dead letter topic for each
        invalid message in the form:
        {
            "topic": <original topic the bad message was produced to>,
            "timestamp": <time at which exception was thrown>,
            "message": <original bad message>
        }
        """
        for message in e.messages:
            data = json.dumps(
                {"topic": e.topic, "timestamp": e.timestamp, "message": message}
            ).encode("utf-8")
            payload = KafkaPayload(key=None, value=data, headers=[])
            self.__producer.produce(
                destination=self.__dead_letter_topic, payload=payload
            )
            self.__metrics.increment("dlq.produced_messages")
