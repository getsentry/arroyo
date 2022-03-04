from arroyo.backends.kafka.configuration import build_kafka_consumer_configuration
from arroyo.backends.kafka.consumer import (
    KafkaConsumer,
    KafkaProducer,
)
from arroyo.processing.processor import StreamProcessor


from arroyo.types import Topic
import socket

from arroyo.examples.transform_and_produce.factory import (
    HashPasswordAndProduceStrategyFactory,
)


RAW_TOPIC = Topic("raw-topic")
HASHED_TOPIC = Topic("hashed-topic")


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
            group_id="hash-password-group",
        )
    )

    processor = StreamProcessor(
        consumer=consumer,
        topic=RAW_TOPIC,
        processor_factory=HashPasswordAndProduceStrategyFactory(
            producer=producer, topic=HASHED_TOPIC
        ),
    )

    processor.run()
