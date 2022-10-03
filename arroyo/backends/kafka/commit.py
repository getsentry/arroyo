from datetime import datetime
from typing import Optional

from arroyo.backends.kafka import KafkaPayload
from arroyo.commit import Commit
from arroyo.types import Partition, Topic
from arroyo.utils.codecs import Codec

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class CommitCodec(Codec[KafkaPayload, Commit]):
    def encode(self, value: Commit) -> KafkaPayload:
        assert value.orig_message_ts is not None

        return KafkaPayload(
            f"{value.partition.topic.name}:{value.partition.index}:{value.group}".encode(
                "utf-8"
            ),
            f"{value.offset}".encode("utf-8"),
            [
                (
                    "orig_message_ts",
                    datetime.strftime(value.orig_message_ts, DATETIME_FORMAT).encode(
                        "utf-8"
                    ),
                )
            ],
        )

    def decode(self, value: KafkaPayload) -> Commit:
        key = value.key
        if not isinstance(key, bytes):
            raise TypeError("payload key must be a bytes object")

        val = value.value
        if not isinstance(val, bytes):
            raise TypeError("payload value must be a bytes object")

        headers = {k: v for (k, v) in value.headers}
        try:
            orig_message_ts: Optional[datetime] = datetime.strptime(
                headers["orig_message_ts"].decode("utf-8"), DATETIME_FORMAT
            )
        except KeyError:
            orig_message_ts = None

        topic_name, partition_index, group = key.decode("utf-8").split(":", 3)
        offset = int(val.decode("utf-8"))
        return Commit(
            group,
            Partition(Topic(topic_name), int(partition_index)),
            offset,
            orig_message_ts,
        )
