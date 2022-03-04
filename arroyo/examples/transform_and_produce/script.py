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

    # Create a Producer, KafkaProducer is a wrapper on confluent_kafka.Producer
    producer = KafkaProducer(
        {
            "bootstrap.servers": "localhost:9092",  # Where the broker is, localhost:9092 when running locally
            "client.id": socket.gethostname(),
        }
    )

    # KafkaConsumer is a wrapper on confluent_kafka.Consumer
    consumer = KafkaConsumer(
        # A function to bootstrap required configs to create a consumer
        build_kafka_consumer_configuration(
            default_config={
                "bootstrap.servers": "localhost:9092",
            },
            auto_offset_reset="latest",  # "define the behavior of the consumer when there is no committed position" - confluent docs
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
