from collections import deque
from typing import Deque, Mapping, MutableMapping, Optional, Set

from arroyo.types import Partition


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

    def add_message(self, route: str, offset: int) -> None:
        """
        Adds one uncommitted offset to one route.
        """
        self.__uncommitted[route].append(offset)

    def advance_watermark(self, route: str, offset: int) -> None:
        """
        Records a commit of an offset on a route thus advancing the
        watermark.

        Not all offsets need to be committed (standard kafka commit
        behavior). Committing offset x on route y means committing all
        offsets up to x on route y.
        """
        assert len(self.__uncommitted[route]) > 0, "There are no watermarks to advance"
        while self.__uncommitted[route] and self.__uncommitted[route][0] <= offset:
            uncommitted = self.__uncommitted[route].popleft()
            self.__committed[route].append(uncommitted)

        self.__lowest_uncommitted = None
        for _, queue in self.__uncommitted.items():
            if queue and (
                self.__lowest_uncommitted is None
                or self.__lowest_uncommitted > queue[0]
            ):
                self.__lowest_uncommitted = queue[0]

    def get_watermark(self) -> Optional[int]:
        """
        Returns the highest committed offset across routes.

        Formally: The returned offset is the highest observed offset that
        is lower than any observed uncommitted offset.
        """
        high_watermark = None
        for _, queue in self.__committed.items():
            # TODO: Avoid scanning the whole queue each time
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
        `get_watermark` becomes None.
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


class CommitWatermarkTracker:
    """
    Keeps track of the watermark of multiple partitions and decides
    when and what to commit.
    """

    def __init__(self, routes: Set[str], partitions: Set[Partition]) -> None:
        self.__watermarks: Mapping[Partition, PartitionWatermark] = {
            partition: PartitionWatermark(routes) for partition in partitions
        }

        self.__committed_offsets: MutableMapping[Partition, int] = {}

    def add_message(self, route: str, partition: Partition, offset: int) -> None:
        self.__watermarks[partition].add_message(route, offset)

    def add_commit(
        self, route: str, offsets: Mapping[Partition, int]
    ) -> Mapping[Partition, int]:
        """
        Figures out what needs tio be committed when a strategy calls the
        commit callback
        """
        high_watermark: MutableMapping[Partition, int] = {}
        for partition, offset in offsets.items():
            watermark = self.__watermarks[partition]
            watermark.advance_watermark(route, offset)
            watermark_offset = watermark.get_watermark()
            if watermark_offset is not None:
                high_watermark[partition] = watermark_offset
            watermark.purge()

        ret = {}
        for partition, offset in high_watermark.items():
            if self.__committed_offsets.get(partition, 0) < offset:
                self.__committed_offsets[partition] = offset
                ret[partition] = offset

        return ret
