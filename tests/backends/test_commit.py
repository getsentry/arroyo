from datetime import datetime

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.commit import Commit
from arroyo.types import Partition, Topic


def test_encode_decode(broker: Broker[KafkaPayload]) -> None:
    topic = Topic("topic")
    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    message = producer.produce(
        topic, KafkaPayload(None, "hello".encode("utf8"), [])
    ).result(1.0)

    commit_codec = CommitCodec()

    commit = Commit(
        "leader-a",
        Partition(topic, 0),
        message.next_offset,
        datetime.now(),
    )

    encoded = commit_codec.encode(commit)

    assert commit_codec.decode(encoded) == commit
