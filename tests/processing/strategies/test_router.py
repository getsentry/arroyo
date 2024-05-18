from dataclasses import dataclass
from typing import Mapping, MutableMapping, MutableSequence, Optional, Sequence, Union

import pytest

from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.processing.strategies.router import (
    PartitionWatermark,
    RouterStrategy,
    TopicCommitWatermark,
)
from arroyo.types import Commit as ArroyoCommit
from arroyo.types import (
    FilteredPayload,
    Message,
    Partition,
    Topic,
    TStrategyPayload,
    Value,
)


def test_empty_watermark() -> None:
    watermark = PartitionWatermark(set())
    assert watermark.high_watermark is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 0

    watermark = PartitionWatermark({"route1", "route2", "route3"})
    assert watermark.high_watermark is None
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 0


def test_invalid_offset() -> None:
    watermark = PartitionWatermark({"route1"})
    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)

    with pytest.raises(AssertionError):
        watermark.advance_watermark("route1", 13)


def test_single_route() -> None:
    watermark = PartitionWatermark({"route1"})

    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)
    watermark.add_message("route1", 16)
    watermark.add_message("route1", 17)
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 4

    assert watermark.high_watermark is None

    watermark.advance_watermark("route1", 16)
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 1
    assert watermark.high_watermark == 16

    # Empty the route
    watermark.advance_watermark("route1", 17)
    assert watermark.high_watermark == 17
    assert watermark.uncommitted_offsets == 0

    with pytest.raises(AssertionError):
        watermark.advance_watermark("route1", 18)

    # Refill. Test it keeps working consistently
    watermark.add_message("route1", 18)
    assert watermark.high_watermark == 17
    watermark.advance_watermark("route1", 18)
    assert watermark.high_watermark == 18


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

    assert watermark.high_watermark is None

    watermark.advance_watermark("route2", 17)
    # I committed one offset in route2. Those
    # on route 1 are not committed.
    assert watermark.committed_offsets == 1
    watermark.advance_watermark("route2", 21)
    # As I did not committed anything from route1
    # and they were lower than all offsets on route2
    # No offset has been purged. I still need them
    # around till I can advance the lower watermark
    assert watermark.committed_offsets == 3
    assert watermark.uncommitted_offsets == 7

    # We are still waiting for any commit on route 1
    assert watermark.high_watermark is None

    # Now we unlock both route 1 and 2
    watermark.advance_watermark("route1", 16)
    assert watermark.high_watermark == 21
    assert watermark.committed_offsets == 0
    assert watermark.uncommitted_offsets == 4

    watermark.advance_watermark("route2", 34)
    assert watermark.high_watermark == 21

    watermark.advance_watermark("route1", 32)
    assert watermark.high_watermark == 34


def test_basic_deletion() -> None:
    watermark = PartitionWatermark({"route1", "route2"})

    assert watermark.high_watermark is None

    watermark.add_message("route1", 10)
    watermark.add_message("route1", 15)
    watermark.add_message("route1", 16)

    assert watermark.uncommitted_offsets == 3
    assert watermark.committed_offsets == 0

    watermark.rewind("route1")
    assert watermark.uncommitted_offsets == 2

    watermark.advance_watermark("route1", 15)
    with pytest.raises(AssertionError):
        watermark.rewind("route1")


def test_delete_allow_commit() -> None:
    watermark = PartitionWatermark({"route1", "route2"})
    watermark.add_message("route1", 10)
    watermark.add_message("route2", 20)
    watermark.add_message("route1", 25)

    watermark.advance_watermark("route1", 25)
    assert watermark.high_watermark == 10
    watermark.rewind("route2")
    # I removed the only offset in route2 thus
    # unlocking route1
    assert watermark.high_watermark == 25


def test_commit_tracker() -> None:
    topic = Topic("mytopic")
    p1 = Partition(topic, 0)
    p2 = Partition(topic, 1)
    tracker = TopicCommitWatermark(
        routes={"route1", "route2"},
    )

    tracker.add_message("route1", p1, 10)
    tracker.add_message("route1", p1, 15)
    tracker.add_message("route2", p1, 20)
    tracker.add_message("route1", p1, 25)

    tracker.add_message("route1", p2, 5)
    tracker.add_message("route1", p2, 10)
    tracker.add_message("route2", p2, 11)
    tracker.add_message("route1", p2, 20)

    assert tracker.commit("route1", {p1: 10}) == {p1: 10}
    assert tracker.commit("route1", {p2: 10}) == {p1: 10, p2: 10}
    assert tracker.commit("route1", {p1: 25, p2: 20}) == {p1: 15, p2: 10}

    tracker.add_message("route1", p1, 30)
    tracker.add_message("route1", p2, 21)

    assert tracker.commit("route2", {p1: 20, p2: 11}) == {p1: 25, p2: 20}


@dataclass
class Event:
    pass


@dataclass
class Processed(Event):
    route: str
    offsets: Mapping[Partition, int]


@dataclass
class Commit(Event):
    offsets: Mapping[Partition, int]
    force: bool


@dataclass
class Join(Event):
    route: str


@dataclass
class Close(Event):
    route: str


@dataclass
class Terminate(Event):
    route: str


@dataclass
class Poll(Event):
    route: str


class EventRecorder:
    def __init__(self) -> None:
        self.__events: MutableSequence[Event] = []

    def add(self, event: Event) -> None:
        self.__events.append(event)

    def assert_sequence(self, events: Sequence[Event]) -> None:
        assert events == self.__events


class TestStrategy(ProcessingStrategy[TStrategyPayload]):
    def __init__(
        self,
        commit: ArroyoCommit,
        commit_interval: int,
        recorder: EventRecorder,
        route: str,
        reject_events: bool = False,
    ) -> None:
        self.__commit = commit
        self.__interval = commit_interval
        self.__recorder = recorder
        self.__route = route
        self.__op = 0
        self.__reject_events = reject_events
        self.__last_offsets: MutableMapping[Partition, int] = {}

    def poll(self) -> None:
        if self.__op % self.__interval == 0:
            self.__commit(self.__last_offsets)
            self.__last_offsets = {}
        self.__recorder.add(Poll(self.__route))

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        if self.__reject_events:
            raise MessageRejected
        self.__op += 1
        self.__recorder.add(Processed(self.__route, message.committable))
        for partition, offset in message.committable.items():
            if (
                partition not in self.__last_offsets
                or self.__last_offsets[partition] < offset
            ):
                self.__last_offsets[partition] = offset

    def close(self) -> None:
        self.__recorder.add(Close(self.__route))

    def terminate(self) -> None:
        self.__recorder.add(Terminate(self.__route))

    def join(self, timeout: Optional[float] = None) -> None:
        self.__recorder.add(Join(self.__route))
        self.__commit(self.__last_offsets, force=True)


def selector(message: Message[Union[FilteredPayload, TStrategyPayload]]) -> str:
    return str(message.payload)


def test_empty_strategy() -> None:
    recorder = EventRecorder()

    def commit_func(offsets: Mapping[Partition, int], force: bool = False) -> None:
        pass

    strategy: ProcessingStrategy[Union[FilteredPayload, str]] = RouterStrategy(
        {}, selector, commit_func
    )

    topic = Topic("topic")
    p1 = Partition(topic, 0)

    with pytest.raises(AssertionError):
        strategy.submit(Message(Value("route1", {p1: 1})))

    def build1(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 3, recorder, "route1")

    strategy = RouterStrategy({"route1": build1}, selector, commit_func)

    strategy.close()
    strategy.terminate()

    with pytest.raises(AssertionError):
        strategy.submit(Message(Value("route1", {p1: 1})))

    recorder.assert_sequence(
        [
            Close("route1"),
            Terminate("route1"),
        ]
    )


def test_sequence_one_partition() -> None:
    recorder = EventRecorder()

    def build1(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 3, recorder, "route1")

    def build2(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route2")

    def commit_func(offsets: Mapping[Partition, int], force: bool = False) -> None:
        recorder.add(Commit(offsets, force))

    strategy: ProcessingStrategy[Union[FilteredPayload, str]] = RouterStrategy(
        {"route1": build1, "route2": build2}, selector, commit_func
    )

    topic = Topic("topic")
    p1 = Partition(topic, 0)

    strategy.submit(Message(Value("route1", {p1: 1})))
    strategy.poll()
    strategy.submit(Message(Value("route1", {p1: 2})))
    strategy.poll()
    strategy.submit(Message(Value("route2", {p1: 3})))
    strategy.poll()
    strategy.submit(Message(Value("route1", {p1: 5})))
    strategy.poll()

    strategy.submit(Message(Value("route2", {p1: 10})))
    strategy.poll()

    strategy.submit(Message(Value("route1", {p1: 11})))
    strategy.submit(Message(Value("route1", {p1: 12})))
    strategy.poll()
    strategy.submit(Message(Value("route2", {p1: 14})))
    strategy.close()
    strategy.join()

    recorder.assert_sequence(
        [
            Processed("route1", {p1: 1}),
            Poll("route1"),
            Poll("route2"),
            Processed("route1", {p1: 2}),
            Poll("route1"),
            Poll("route2"),
            Processed("route2", {p1: 3}),
            # Do not commit here as I am waiting for route1 to commit
            Poll("route1"),
            Poll("route2"),
            Processed("route1", {p1: 5}),
            # Reached a commit point for route1 (3 messages) now I
            # commit everything
            Commit({p1: 5}, force=False),
            Poll("route1"),
            Poll("route2"),
            Processed("route2", {p1: 10}),
            Poll("route1"),
            # Route 2 commits every message, there is nothing waiting
            # from route 1 so commit.
            Commit({p1: 10}, force=False),
            Poll("route2"),
            Processed("route1", {p1: 11}),
            Processed("route1", {p1: 12}),
            Poll("route1"),
            Poll("route2"),
            Processed("route2", {p1: 14}),
            # Commit with force on Join
            Close("route1"),
            Close("route2"),
            Join("route1"),
            Commit({p1: 12}, force=True),
            Join("route2"),
            Commit({p1: 14}, force=True),
        ]
    )


def test_multi_partition() -> None:
    recorder = EventRecorder()

    def build1(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route1")

    def build2(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route2")

    def build3(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route3")

    def commit_func(offsets: Mapping[Partition, int], force: bool = False) -> None:
        recorder.add(Commit(offsets, force))

    strategy: ProcessingStrategy[Union[FilteredPayload, str]] = RouterStrategy(
        {"route1": build1, "route2": build2, "route3": build3}, selector, commit_func
    )

    topic = Topic("topic")
    p1 = Partition(topic, 0)
    p2 = Partition(topic, 1)
    p3 = Partition(topic, 2)

    def publish_all_routes(partition: Partition) -> None:
        strategy.submit(Message(Value("route1", {partition: 1})))
        strategy.submit(Message(Value("route2", {partition: 2})))
        strategy.submit(Message(Value("route3", {partition: 3})))

    publish_all_routes(p1)
    publish_all_routes(p2)
    publish_all_routes(p3)

    strategy.join()

    recorder.assert_sequence(
        [
            Processed("route1", {p1: 1}),
            Processed("route2", {p1: 2}),
            Processed("route3", {p1: 3}),
            Processed("route1", {p2: 1}),
            Processed("route2", {p2: 2}),
            Processed("route3", {p2: 3}),
            Processed("route1", {p3: 1}),
            Processed("route2", {p3: 2}),
            Processed("route3", {p3: 3}),
            # Nobody committed so far, so each route will commit
            # its offset. If we ran the joins in reverse order we
            # would only commit once with offset 3. That is because
            # route 3 would try to commit first and we would wait
            # till the other routes have committed.
            Join("route1"),
            Commit({p1: 1, p2: 1, p3: 1}, force=True),
            Join("route2"),
            Commit({p1: 2, p2: 2, p3: 2}, force=True),
            Join("route3"),
            Commit({p1: 3, p2: 3, p3: 3}, force=True),
        ]
    )


def test_rejecting_messages() -> None:
    recorder = EventRecorder()

    def build1(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route1")

    def build2(commit: ArroyoCommit) -> ProcessingStrategy[TStrategyPayload]:
        return TestStrategy(commit, 1, recorder, "route2", reject_events=True)

    def commit_func(offsets: Mapping[Partition, int], force: bool = False) -> None:
        recorder.add(Commit(offsets, force))

    strategy: ProcessingStrategy[Union[FilteredPayload, str]] = RouterStrategy(
        {"route1": build1, "route2": build2}, selector, commit_func
    )

    topic = Topic("topic")
    p1 = Partition(topic, 0)

    strategy.submit(Message(Value("route1", {p1: 1})))
    with pytest.raises(MessageRejected):
        strategy.submit(Message(Value("route2", {p1: 2})))
    strategy.join()

    recorder.assert_sequence(
        [
            Processed("route1", {p1: 1}),
            Join("route1"),
            Commit({p1: 1}, force=True),
            Join("route2"),
        ]
    )
