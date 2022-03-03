import hashlib
import json
from arroyo.backends.kafka.configuration import build_kafka_consumer_configuration
from arroyo.backends.kafka.consumer import (
    KafkaConsumer,
    KafkaPayload,
    KafkaProducer,
)
from arroyo.processing.processor import StreamProcessor

from arroyo.processing.strategies.simple_producer.simple_produce import (
    SimpleStrategyFactory,
)
from arroyo.types import Message, Topic
import socket


RAW_TOPIC = Topic("raw-topic")
HASHED_TOPIC = Topic("hashed-topic")


def hash_password(message: Message[KafkaPayload]) -> Message[KafkaPayload]:
    """
    Hashes password in Kafka message containing a username and password.
    """
    auth = json.loads(message.payload.value)
    hashed = hashlib.sha256(auth["password"].encode("utf-8")).hexdigest()
    data = json.dumps({"username": auth["username"], "password": hashed}).encode(
        "utf-8"
    )
    payload = KafkaPayload(message.payload.key, data, headers=message.payload.headers)
    return Message(
        message.partition,
        message.offset,
        payload,
        message.timestamp,
        message.next_offset,
    )


if __name__ == "__main__":
    producer = KafkaProducer(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": socket.gethostname(),
        }
    )
    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            default_config={
                "bootstrap.servers": "localhost:9092",
            },
            auto_offset_reset="latest",
            group_id="rahul-group",
        )
    )

    processor = StreamProcessor(
        consumer=consumer,
        topic=RAW_TOPIC,
        processor_factory=SimpleStrategyFactory(hash_password, producer, HASHED_TOPIC),
    )

    processor.run()
