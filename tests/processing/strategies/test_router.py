import pytest

from arroyo.processing.strategies.router import (
    CommitWatermarkTracker,
    PartitionWatermark,
)
from arroyo.types import Partition, Topic


def test_empty_watermark() -> None:
    watermark = PartitionWatermark(set())
    assert watermark.get_watermark() is None
    watermark.purge()
    assert watermark.get_watermark() is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 0

    watermark = PartitionWatermark({"route1", "route2", "route3"})
    assert watermark.get_watermark() is None
    watermark.purge()
    assert watermark.get_watermark() is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 0


def test_single_route() -> None:
    watermark = PartitionWatermark({"route1"})

    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)
    watermark.add_message("route1", 16)
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 3

    assert watermark.get_watermark() is None
    watermark.purge()
    assert watermark.get_watermark() is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 3

    watermark.advance_watermark("route1", 15)
    assert watermark.committed_offsets == 2
    assert watermark.uncommitted_offsets == 1
    assert watermark.get_watermark() == 15
    watermark.purge()
    assert watermark.get_watermark() is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 1

    # Empty the route
    watermark.advance_watermark("route1", 16)
    assert watermark.get_watermark() == 16
    watermark.purge()
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 0

    with pytest.raises(AssertionError):
        watermark.advance_watermark("route1", 17)

    # Refill. Test it keeps working consistently
    watermark.add_message("route1", 17)
    assert watermark.get_watermark() is None
    watermark.advance_watermark("route1", 17)
    assert watermark.get_watermark() == 17


def test_switching_routes() -> None:
    watermark = PartitionWatermark({"route1", "route2"})

    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)
    watermark.add_message("route1", 16)

    watermark.add_message("route2", 17)
    watermark.add_message("route2", 20)
    watermark.add_message("route2", 21)

    watermark.add_message("route1", 30)
    watermark.add_message("route1", 32)

    watermark.add_message("route2", 33)
    watermark.add_message("route2", 34)

    assert watermark.get_watermark() is None

    watermark.advance_watermark("route2", 17)
    assert watermark.committed_offsets == 1
    watermark.advance_watermark("route2", 21)
    assert watermark.committed_offsets == 3
    assert watermark.uncommitted_offsets == 7

    # We are still waiting for any commit on route 1
    assert watermark.get_watermark() is None

    # Now we unlock both route 1 and 2
    watermark.advance_watermark("route1", 16)
    assert watermark.get_watermark() == 21

    watermark.purge()
    # No watermark now
    assert watermark.get_watermark() is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 4

    watermark.advance_watermark("route2", 33)
    watermark.advance_watermark("route2", 34)
    assert watermark.get_watermark() is None

    watermark.advance_watermark("route1", 32)
    assert watermark.get_watermark() == 34


def test_commit_tracker() -> None:
    topic = Topic("mytopic")
    p1 = Partition(topic, 0)
    p2 = Partition(topic, 1)
    tracker = CommitWatermarkTracker(
        routes={"route1", "route2"},
        partitions={p1, p2},
    )

    tracker.add_message("route1", p1, 10)
    tracker.add_message("route1", p1, 15)
    tracker.add_message("route2", p1, 20)
    tracker.add_message("route1", p1, 25)

    tracker.add_message("route1", p2, 5)
    tracker.add_message("route1", p2, 10)
    tracker.add_message("route2", p2, 11)
    tracker.add_message("route1", p2, 20)

    assert tracker.add_commit("route1", {p1: 10}) == {p1: 10}
    assert tracker.add_commit("route1", {p2: 10}) == {p2: 10}
    assert tracker.add_commit("route1", {p1: 25, p2: 20}) == {p1: 15}

    tracker.add_message("route1", p1, 30)
    tracker.add_message("route1", p2, 21)

    assert tracker.add_commit("route2", {p1: 20, p2: 11}) == {p1: 25, p2: 20}
