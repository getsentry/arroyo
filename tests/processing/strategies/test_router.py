from arroyo.processing.strategies.router import (
    CommitWatermarkTracker,
    PartitionWatermark,
)
from arroyo.types import Partition, Topic


def test_base() -> None:
    watermark = PartitionWatermark({"route1", "route2", "route3"})

    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)
    watermark.add_message("route1", 16)
    watermark.add_message("route1", 17)
    watermark.add_message("route2", 25)
    watermark.add_message("route2", 26)
    watermark.add_message("route2", 27)
    watermark.add_message("route3", 28)

    assert watermark.get_watermark() is None

    watermark.advance_watermark("route2", 25)

    assert watermark.get_watermark() is None

    watermark.advance_watermark("route1", 16)

    assert watermark.get_watermark() == 16

    watermark.advance_watermark("route1", 17)

    assert watermark.get_watermark() == 25

    watermark.advance_watermark("route1", 27)

    assert watermark.get_watermark() == 27

    topic = Topic("mytopic")
    p1 = Partition(topic, 0)
    p2 = Partition(topic, 1)
    tracker = CommitWatermarkTracker(
        routes={"route1", "route2"},
        partitions={p1, p2},
    )

    tracker.add_message("route1", p1, 10)
    tracker.add_message("route1", p1, 15)

    tracker.add_message("route1", p2, 5)
    tracker.add_message("route1", p2, 10)

    tracker.add_message("route2", p1, 20)
    tracker.add_message("route2", p2, 11)

    tracker.add_message("route1", p1, 25)
    tracker.add_message("route1", p2, 20)

    assert tracker.add_commit("route1", {p1: 10}) == {p1: 10}
    assert tracker.add_commit("route1", {p2: 10}) == {p2: 10}
    assert tracker.add_commit("route1", {p1: 25, p2: 20}) == {}

    tracker.add_message("route1", p1, 30)
    tracker.add_message("route1", p2, 21)

    assert tracker.add_commit("route2", {p1: 20, p2: 11}) == {p1: 25, p2: 20}
