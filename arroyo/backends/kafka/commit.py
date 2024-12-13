import json
import logging
from datetime import datetime

from arroyo.backends.kafka import KafkaPayload
from arroyo.commit import Commit
from arroyo.types import Partition, Topic
from arroyo.utils.codecs import Codec

# Kept in decode method for backward compatibility. Will be
# remove in a future release of Arroyo
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)

max_times_to_log_legacy_message = 10


class CommitCodec(Codec[KafkaPayload, Commit]):
    def encode(self, value: Commit) -> KafkaPayload:
        assert value.orig_message_ts is not None

        payload = json.dumps(
            {
                "offset": value.offset,
                "orig_message_ts": value.orig_message_ts,
                "received_p99": value.received_p99,
            }
        ).encode("utf-8")

        return KafkaPayload(
            f"{value.partition.topic.name}:{value.partition.index}:{value.group}".encode(
                "utf-8"
            ),
            payload,
            [],
        )

    def decode(self, value: KafkaPayload) -> Commit:
        key = value.key
        if not isinstance(key, bytes):
            raise TypeError("payload key must be a bytes object")

        val = value.value
        if not isinstance(val, bytes):
            raise TypeError("payload value must be a bytes object")

        payload = val.decode("utf-8")

        if payload.isnumeric():
            return self.decode_legacy(value)

        decoded = json.loads(payload)
        offset = decoded["offset"]
        orig_message_ts = decoded["orig_message_ts"]

        if decoded.get("received_p99"):
            received_ts = decoded["received_p99"]
        else:
            received_ts = None

        topic_name, partition_index, group = key.decode("utf-8").split(":", 3)

        return Commit(
            group,
            Partition(Topic(topic_name), int(partition_index)),
            offset,
            orig_message_ts,
            received_ts,
        )

    def decode_legacy(self, value: KafkaPayload) -> Commit:
        global max_times_to_log_legacy_message
        key = value.key
        if not isinstance(key, bytes):
            raise TypeError("payload key must be a bytes object")

        val = value.value
        if not isinstance(val, bytes):
            raise TypeError("payload value must be a bytes object")

        headers = {k: v for (k, v) in value.headers}
        orig_message_ts = datetime.strptime(
            headers["orig_message_ts"].decode("utf-8"), DATETIME_FORMAT
        )

        topic_name, partition_index, group = key.decode("utf-8").split(":", 3)
        offset = int(val.decode("utf-8"))

        commit = Commit(
            group,
            Partition(Topic(topic_name), int(partition_index)),
            offset,
            orig_message_ts.timestamp(),
            None,
        )

        if max_times_to_log_legacy_message > 0:
            max_times_to_log_legacy_message -= 1
            logger.warning("Legacy commit message found: %s", commit)

        return commit
