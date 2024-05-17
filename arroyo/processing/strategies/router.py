import logging
from collections import deque
from functools import partial
from typing import Deque, Mapping, MutableMapping, Optional, Protocol, Set, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Commit, FilteredPayload, Message, Partition, TStrategyPayload

logger = logging.getLogger(__name__)


class PartitionWatermark:
    """
    Keeps track of the highest committable offset for a partition when commits
    may not be seen in order.

    Commits may not be seen in order if messages are processed by independent
    strategies each of which can commit at different times and following
    different policies.

    Example:
    we are routing most messages to one strategy that commits in batches every
    1000 messages. A smaller part of those messages are routed to a second
    strategy that commits every offsets it sees.

    The two strategies do not know about each other, so the commits may be
    observed out of order.
    We cannot issue commits to Kafka out of order so we need to reorder them.

    This class keep the order by keeping track of the highest offset such that
    all the lower offsets have been committed.

    This class keeps a concept of route, which represents the strategies above.
    Commit for a route are still expected to be in order.
    """

    def __init__(self, routes: Set[str]):
        self.__committed: Mapping[str, Deque[int]] = {
            route: deque() for route in routes
        }
        self.__uncommitted: Mapping[str, Deque[int]] = {
            route: deque() for route in routes
        }

        self.__lowest_uncommitted: Optional[int] = None

    def __update_lowest_uncommitted(self) -> None:
        self.__lowest_uncommitted = None
        for _, queue in self.__uncommitted.items():
            if queue and (
                self.__lowest_uncommitted is None
                or self.__lowest_uncommitted > queue[0]
            ):
                self.__lowest_uncommitted = queue[0]

    def add_message(self, route: str, offset: int) -> None:
        """
        Adds one uncommitted offset to one route.
        """
        self.__uncommitted[route].append(offset)
        if self.__lowest_uncommitted is None:
            self.__update_lowest_uncommitted()

    def rewind(self, route: str) -> None:
        """
        Remove the last message we added
        """
        assert self.__uncommitted[route], "There are no uncommitted offsets to remove"
        val = self.__uncommitted[route].pop()
        if self.__lowest_uncommitted and val <= self.__lowest_uncommitted:
            self.__update_lowest_uncommitted()

    def advance_watermark(self, route: str, offset: int) -> None:
        """
        Records a commit of an offset on a route thus advancing the
        watermark.

        Not all offsets need to be committed (standard kafka commit
        behavior). Committing offset x on route y means committing all
        offsets up to x on route y.
        """
        assert len(self.__uncommitted[route]) > 0, "There are no watermarks to advance"
        found = False
        while self.__uncommitted[route] and self.__uncommitted[route][0] <= offset:
            uncommitted = self.__uncommitted[route].popleft()
            self.__committed[route].append(uncommitted)
            found = uncommitted == offset

        assert found, f"Requested offset {offset} was not in the uncommitted queue."
        self.__update_lowest_uncommitted()

    def get_high_watermark(self) -> Optional[int]:
        """
        Returns the highest committed offset across routes.

        Formally: The returned offset is the highest observed offset that
        is lower than any observed uncommitted offset.
        """
        high_watermark = None
        for _, queue in self.__committed.items():
            for committed in queue:
                if (
                    self.__lowest_uncommitted is None
                    or committed < self.__lowest_uncommitted
                ) and (high_watermark is None or committed > high_watermark):
                    high_watermark = committed

        return high_watermark

    def purge(self) -> None:
        """
        Drops all the offsets that are committed and we do not need anymore.

        Formally: it drops all the committed offsets that are lower than
        the lowest uncommitted offset.

        This is destructive. After calling this method, the result of
        `get_high_watermark` becomes None.
        """
        for _, queue in self.__committed.items():
            while queue and (
                self.__lowest_uncommitted is None
                or queue[0] < self.__lowest_uncommitted
            ):
                queue.popleft()

    @property
    def committed_offsets(self) -> int:
        return sum(len(queue) for queue in self.__committed.values())

    @property
    def uncommitted_offsets(self) -> int:
        return sum(len(queue) for queue in self.__uncommitted.values())


class TopicCommitWatermark:
    """
    Keeps track of the watermark of multiple partitions and decides
    when and what to commit.

    In an environment where messages are not processed in order or
    where commits are issued out of order, we need to reorder the
    commits before issuing them to Kafka to ensure the "at least
    once" semantics.

    This class manages the commits order. It still expects that commits
    for a single partition will happen in order.
    """

    def __init__(self, routes: Set[str]) -> None:
        self.__route_names = routes
        self.__watermarks: MutableMapping[Partition, PartitionWatermark] = {}

    def add_message(self, route: str, partition: Partition, offset: int) -> None:
        if partition not in self.__watermarks:
            self.__watermarks[partition] = PartitionWatermark(self.__route_names)
        self.__watermarks[partition].add_message(route, offset)

    def rewind(self, route: str, partition: Partition) -> None:
        """
        Remove the latest uncommitted message for a partition.
        """
        self.__watermarks[partition].rewind(route)

    def commit(
        self, route: str, offsets: Mapping[Partition, int]
    ) -> Mapping[Partition, int]:
        """
        Record a commit and decides whether it is time to issue a commit
        to the StreamProcessor.

        A commit is issued if, for each partition, all commits lower than
        the offset provided have been committed.
        """
        high_watermark: MutableMapping[Partition, int] = {}
        for partition, offset in offsets.items():
            assert (
                partition in self.__watermarks
            ), f"Partition {partition} is unknown. I have never received a message for it"

            watermark = self.__watermarks[partition]
            watermark.advance_watermark(route, offset)
            watermark_offset = watermark.get_high_watermark()
            if watermark_offset is not None:
                high_watermark[partition] = watermark_offset
            watermark.purge()

        return high_watermark

    @property
    def uncommitted_offsets(self) -> int:
        return sum(
            watermark.uncommitted_offsets for watermark in self.__watermarks.values()
        )


class RouteBuilder(Protocol):
    """
    Builds the strategy that is used on a route by the `RouterStrategy`.

    The `RouterStrategy` has to intercept the commit calls in order to
    sort them and commit at the right time.
    `RouterStrategy` intercepts the commit calls by wrapping the `commit`
    function call received from the `StreamProcessor`. This means that
    each of the destination route needs to be provided the wrapper and
    cannot be instantiated with the original `commit` function.

    Requiring users of `RouterStrategy` to provide a builder instead of
    the strategy objects makes it hard to mismanage the `commit` function
    and accidentally instantiate the destination strategies with the wrong
    `commit` function.
    """

    def __call__(self, commit: Commit) -> ProcessingStrategy[TStrategyPayload]:
        pass


class RouteSelector(Protocol):
    """
    Given a message it decides which route the message should take.
    """

    def __call__(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> str:
        pass


class RouterStrategy(ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]):
    """
    This strategy routes messages between a number of strategies depending on
    any property of the message itself. It then keeps track of the commit
    highest watermark to ensure we only commit offsets that have been committed
    by the destination strategies.

    As messages are routed to multiple independent strategies each of which
    can have its own commit policies, it is not given that commits would be
    issued in order by the destination strategies.

    This strategy solves the problem by intercepting all commits callback and
    ensure to reorder them.

    The destination strategy for a message is chosen through a `RouteSelector`
    function provided when instantiating this class.

    In order for this strategy to intercept callbacks to the commit function
    we need to let this class instantiate the destination strategies and pass
    them the `commit` callback.
    """

    def __init__(
        self,
        routes: Mapping[str, RouteBuilder],
        selector: RouteSelector,
        commit: Commit,
    ) -> None:
        self.__root_commit = commit
        self.__watermark_tracker = TopicCommitWatermark(set(routes.keys()))
        self.__force_commit = False

        def commit_func(
            route: str, offsets: Mapping[Partition, int], force: bool = False
        ) -> None:
            offsets_to_commit = self.__watermark_tracker.commit(route, offsets)
            if offsets_to_commit:
                self.__root_commit(offsets_to_commit, force or self.__force_commit)
                self.__force_commit = False
            else:
                if force:
                    self.__force_commit

        self.__selector = selector
        self.__routes: Mapping[
            str, ProcessingStrategy[Union[FilteredPayload, TStrategyPayload]]
        ] = {
            name: builder(partial(commit_func, name))
            for name, builder in routes.items()
        }

        self.__closed = False

    def poll(self) -> None:
        for route in self.__routes.values():
            route.poll()

    def submit(
        self, message: Message[Union[FilteredPayload, TStrategyPayload]]
    ) -> None:
        assert not self.__closed

        route = self.__selector(message)
        assert route in self.__routes, f"Invalid route {route}"
        # We have to add the message to the watermark before calling
        # `submit` on the destination strategy because, while unlikely,
        # it is possible that submit itself will commit the message,
        # thus the message has to be already in the watermark.
        for partition, offset in message.committable.items():
            self.__watermark_tracker.add_message(route, partition, offset)

        try:
            self.__routes[route].submit(message)
        except Exception:
            for partition, offset in message.committable.items():
                # When we receive a `MessageRejected` exception upon submit
                # the message is not supposed to have been committed, so
                # we need to remove it from the watermark.
                self.__watermark_tracker.rewind(route, partition)
            raise

    def close(self) -> None:
        self.__closed = True

        for route in self.__routes.values():
            route.close()

    def terminate(self) -> None:
        self.__closed = True

        for route in self.__routes.values():
            logger.debug("Terminating %r...", route)
            route.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        for route in self.__routes.values():
            # TODO: Should reduce the timeout at each route
            route.join(timeout)

        uncommitted = self.__watermark_tracker.uncommitted_offsets
        if uncommitted:
            logger.error(f"Terminating with {uncommitted} uncommitted offsets")
