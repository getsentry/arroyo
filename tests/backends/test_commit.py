import time

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit
from arroyo.types import Partition, Topic


def test_encode_decode() -> None:
    topic = Topic("topic")
    commit_codec = CommitCodec()

    offset_to_commit = 5

    now = time.time()

    commit = Commit(
        "leader-a",
        Partition(topic, 0),
        offset_to_commit,
        now,
        now - 5,
    )

    encoded = commit_codec.encode(commit)

    assert commit_codec.decode(encoded) == commit


def test_decode_legacy() -> None:
    legacy = KafkaPayload(
        b"topic:0:leader-a", b"5", [("orig_message_ts", b"2023-09-26T21:58:14.191325Z")]
    )
    decoded = CommitCodec().decode(legacy)
    assert decoded.offset == 5
    assert decoded.group == "leader-a"
