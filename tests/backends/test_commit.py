from datetime import datetime, timedelta

from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit
from arroyo.types import Partition, Topic


def test_encode_decode() -> None:
    topic = Topic("topic")
    commit_codec = CommitCodec()

    offset_to_commit = 5

    now = datetime.now()

    commit = Commit(
        "leader-a",
        Partition(topic, 0),
        offset_to_commit,
        now,
        now - timedelta(seconds=5),
    )

    encoded = commit_codec.encode(commit)

    assert commit_codec.decode(encoded) == commit
