import os

from examples.transform_and_produce.factory import HashPasswordAndProduceStrategyFactory

from arroyo.backends.kafka.configuration import (
    build_kafka_configuration,
    build_kafka_consumer_configuration,
)
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaProducer
from arroyo.processing.processor import StreamProcessor
from arroyo.types import Topic

RAW_TOPIC = Topic("raw-topic")
HASHED_TOPIC = Topic("hashed-topic")

# Where the broker is, localhost:9092 when running locally
BOOTSTRAP_SERVERS = [os.environ.get("BOOTSTRAP_SERVERS") or "localhost:9092"]


if __name__ == "__main__":

    # Create a Producer, KafkaProducer is a wrapper on confluent_kafka.Producer
    producer = KafkaProducer(
        build_kafka_configuration(
            default_config={},
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )
    )

    # KafkaConsumer is a wrapper on confluent_kafka.Consumer
    consumer = KafkaConsumer(
        # A function to bootstrap required configs to create a consumer
        build_kafka_consumer_configuration(
            default_config={},
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            group_id="hash-password-group",  # consumer group name
        )
    )

    # StreamProcessor is an Arroyo specific processor written which continously polls
    # the given producer and submits the message to the provided strategy
    processor = StreamProcessor(
        consumer=consumer,
        topic=RAW_TOPIC,  # topic the consumer should subscribe to
        processor_factory=HashPasswordAndProduceStrategyFactory(
            producer=producer, topic=HASHED_TOPIC
        ),
    )

    processor.run()
