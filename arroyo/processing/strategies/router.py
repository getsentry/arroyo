import logging
from collections import deque
from functools import partial
from typing import Deque, Mapping, MutableMapping, Optional, Protocol, Set, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Commit, FilteredPayload, Message, Partition, TStrategyPayload

logger = logging.getLogger(__name__)


class PartitionWatermark:
    """
    Keeps track of the highest committable offset when messages are processed
    by a discrete set of independent strategies (routes here) which can
    issue commit according to different strategies, thus out of order.

    Example:

    ```
    Offsets produced:
    Route 1:  ---- 10 --- 15 --- 20 --------------------
    Route 2:  ------------------------- 25 --- 30 --- 35
    ```

    Route1 offsets are processed by a different strategy independently from
    Route2 offsets. This makes it is totally possible that the strategy that
    processes route 2 would commit offset 25, 30, 35 before route 1 commits 20.

    This class keeps track that the highest committable offset is not going
    to be 35 until route one issued the commit for offset 20.

    This class has to keep track of all offsets above the highest committable
    at a given point in time in order as, depending on the order of the
    received commits, the watermark can land on any of the offsets. We cannot
    keep only the highest offset per route.
    """

    def __init__(self, routes: Set[str]):
        self.__committed: Mapping[str, Deque[int]] = {
            route: deque() for route in routes
        }
        self.__uncommitted: Mapping[str, Deque[int]] = {
            route: deque() for route in routes
        }

        self.__highest_committed: Optional[int] = None

    def add_message(self, route: str, offset: int) -> None:
        """
        Adds one uncommitted offset to one route.

        An uncommitted offset is added when the message is consumed, it is
        being processed but before the offset is committed.
        """
        self.__uncommitted[route].append(offset)

    def rewind(self, route: str) -> None:
        """
        Remove the last message we added.

        This message has to be uncommitted. It is a valid scenario when
        we try to process a message but backpressure makes us take it
        back
        """
        assert self.__uncommitted[route], "There are no uncommitted offsets to remove"
        highest_committed = None
        for _, queue in self.__committed.items():
            if queue and (highest_committed is None or queue[-1] > highest_committed):
                highest_committed = queue[-1]
        assert (
            highest_committed is None
            or self.__uncommitted[route][-1] > highest_committed
        ), "Cannot rewind an offset when highest offsets have been committed"

        self.__uncommitted[route].pop()

    def advance_watermark(self, route: str, offset: int) -> Optional[int]:
        """
        Records a commit of an offset on a route thus advancing the
        watermark.

        Not all offsets need to be committed (standard kafka commit
        behavior). Committing offset x on route y means committing all
        offsets up to x on route y.

        The worst case time complexity is `O(n)` with n being the number
        of uncommitted messages + those waiting for a commit.
        Though the amortized complexity should be constant.

        The only sources of `n` iterations in this algorithm are to scan
        offsets till the one we want to commit or scan offsets till the
        lowest uncommitted.

        The first can only be greater than one only if we commit rarely.
        And we do not commit this method is not called at all.

        We can do the same reasoning for the second one as well as we purge
        committed messages each time we advance the watermark so the only
        reason to have `n` messages to scan is if we did not scan
        them the `n - 1` previous calls.
        """
        queue = self.__uncommitted[route]
        found = False
        while queue and queue[0] <= offset:
            committed = queue.popleft()
            if committed == offset:
                found = True
            self.__committed[route].append(committed)
        assert found, f"Requested offset {offset} was not in the uncommitted queue."

        # The high watermark at this point is the highest offset in the
        # self__committed map that is also lower than the lowest uncommitted
        # offset. So we need the lowest uncommitted offset first.
        lowest_uncommitted = None
        for _, queue in self.__uncommitted.items():
            if queue and (lowest_uncommitted is None or lowest_uncommitted > queue[0]):
                lowest_uncommitted = queue[0]

        highest_committed = None
        for _, queue in self.__committed.items():
            while queue and (
                lowest_uncommitted is None or queue[0] < lowest_uncommitted
            ):
                to_drop = queue.popleft()
                if highest_committed is None or to_drop > highest_committed:
                    highest_committed = to_drop

        if highest_committed is not None:
            self.__highest_committed = highest_committed
        return highest_committed

    @property
    def high_watermark(self) -> Optional[int]:
        """
        Returns the highest committed offset across routes.

        Formally: The returned offset is the highest observed offset that
        is lower than any observed uncommitted offset.
        """
        return self.__highest_committed

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

    See `PartitionWatermark` for more details and examples.
    """

    def __init__(self, routes: Set[str]) -> None:
        self.__route_names = routes
        self.__watermarks: MutableMapping[Partition, PartitionWatermark] = {}

    def add_message(self, route: str, partition: Partition, offset: int) -> None:
        """
        Add a number of offsets to the watermarks.

        Partitions may not be known when this class is instantiated so we
        add them on the fly to the watermarks map.
        """
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

        This class returns the highest committed offsets per partition.
        The highest committed offset is the highest offset such that no
        lower offset has not been committed.

        The return value is the map of offsets we are allowed to commit to
        Kafka.
        """
        high_watermark: MutableMapping[Partition, int] = {}
        for partition, offset in offsets.items():
            assert (
                partition in self.__watermarks
            ), f"Partition {partition} is unknown. I have never received a message for it"

            watermark = self.__watermarks[partition]
            watermark_offset = watermark.advance_watermark(route, offset)

        for partition, watermark in self.__watermarks.items():
            watermark_offset = watermark.high_watermark
            if watermark_offset is not None:
                high_watermark[partition] = watermark_offset

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

    Messages are routed to multiple independent strategies each of which
    can have its own commit policies. It is not given that commits would be
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

        self.__last_committed: MutableMapping[Partition, int] = {}

        def commit_func(
            route: str, offsets: Mapping[Partition, int], force: bool = False
        ) -> None:
            current_offsets = self.__watermark_tracker.commit(route, offsets)
            offsets_to_commit: MutableMapping[Partition, int] = {}
            for partition, offset in current_offsets.items():
                if (
                    partition not in self.__last_committed
                    or self.__last_committed[partition] < offset
                ):
                    self.__last_committed[partition] = offset
                    offsets_to_commit[partition] = offset

            if offsets_to_commit:
                self.__root_commit(offsets_to_commit, force or self.__force_commit)
                self.__force_commit = False
            else:
                if force:
                    self.__force_commit = True

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
