from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import Callable, Generic, Mapping, Optional, Sequence, TypeVar, Union

from arroyo.types import BrokerValue, Partition, Topic, TStrategyPayload

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Consumer(Generic[TStrategyPayload], ABC):
    """
    This abstract class provides an interface for consuming messages from a
    multiplexed collection of partitioned topic streams.

    Partitions support sequential access, as well as random access by
    offsets. There are three types of offsets that a consumer interacts with:
    working offsets, staged offsets, and committed offsets. Offsets always
    represent the starting offset of the *next* message to be read. (For
    example, committing an offset of X means the next message fetched via
    poll will have a least an offset of X, and the last message read had an
    offset less than X.)

    The working offsets are used track the current read offset within a
    partition. This can be also be considered as a cursor, or as high
    watermark. Working offsets are local to the consumer process. They are
    not shared with other consumer instances in the same consumer group and
    do not persist beyond the lifecycle of the consumer instance, unless they
    are committed.

    Committed offsets are managed by an external arbiter/service, and are
    used as the starting point for a consumer when it is assigned a partition
    during the subscription process. To ensure that a consumer roughly "picks
    up where it left off" after restarting, or that another consumer in the
    same group doesn't read messages that have been processed by another
    consumer within the same group during a rebalance operation, offsets must
    be regularly committed by calling ``commit_offsets`` after they have been
    staged with ``stage_offsets``. Offsets are not staged or committed
    automatically!

    During rebalance operations, working offsets are rolled back to the
    latest committed offset for a partition, and staged offsets are cleared
    after the revocation callback provided to ``subscribe`` is called. (This
    occurs even if the consumer retains ownership of the partition across
    assignments.) For this reason, it is generally good practice to ensure
    offsets are committed as part of the revocation callback.
    """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[BrokerValue[TStrategyPayload]]:
        """
        Fetch a message from the consumer. If no message is available before
        the timeout, ``None`` is returned.

        This method may raise an ``OffsetOutOfRange`` exception if the
        consumer attempts to read from an invalid location in one of it's
        assigned partitions. (Additional details can be found in the
        docstring for ``Consumer.seek``.)
        """
        raise NotImplementedError

    @abstractmethod
    def pause(self, partitions: Sequence[Partition]) -> None:
        """
        Pause consuming from the provided partitions.

        A partition that is paused will be automatically resumed during
        reassignment. This ensures that the behavior is consistent during
        rebalances, regardless of whether or not this consumer retains
        ownership of the partition. (If this partition was assigned to a
        different consumer in the consumer group during a rebalance, that
        consumer would not have knowledge of whether or not the partition was
        previously paused and would start consuming from the partition.) If
        partitions should remain paused across rebalances, this should be
        implemented in the assignment callback.

        If any of the provided partitions are not in the assignment set, an
        exception will be raised and no partitions will be paused.
        """
        raise NotImplementedError

    @abstractmethod
    def resume(self, partitions: Sequence[Partition]) -> None:
        """
        Resume consuming from the provided partitions.

        If any of the provided partitions are not in the assignment set, an
        exception will be raised and no partitions will be resumed.
        """
        raise NotImplementedError

    @abstractmethod
    def paused(self) -> Sequence[Partition]:
        """
        Return the currently paused partitions.
        """
        raise NotImplementedError

    @abstractmethod
    def tell(self) -> Mapping[Partition, int]:
        """
        Return the working offsets for all currently assigned partitions.
        """
        raise NotImplementedError

    @abstractmethod
    def seek(self, offsets: Mapping[Partition, int]) -> None:
        """
        Update the working offsets for the provided partitions.

        When using this method, it is possible to set a partition to an
        invalid offset without an immediate error. (Examples of invalid
        offsets include an offset that is too low and has already been
        dropped by the broker due to data retention policies, or an offset
        that is too high which is not yet associated with a message.) Since
        this method only updates the local working offset (and does not
        communicate with the broker), setting an invalid offset will cause a
        subsequent ``poll`` call to raise ``OffsetOutOfRange`` exception,
        even though the call to ``seek`` succeeded.

        If any provided partitions are not in the assignment set, an
        exception will be raised and no offsets will be modified.
        """
        raise NotImplementedError

    @abstractmethod
    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        """
        Stage offsets to be committed. If an offset has already been staged
        for a given partition, that offset is overwritten (even if the offset
        moves in reverse.)
        """
        raise NotImplementedError

    @abstractmethod
    def commit_offsets(self) -> Mapping[Partition, int]:
        """
        Commit staged offsets. The return value of this method is a mapping
        of streams with their committed offsets as values.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, timeout: Optional[float] = None) -> None:
        raise NotImplementedError

    @abstractproperty
    def closed(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def member_id(self) -> str:
        """
        Return the member ID of the consumer as supplied by the broker.
        This is useful for debugging purposes.
        """
        raise NotImplementedError


class SimpleProducerFuture(Generic[T]):
    """
    A stub for concurrent.futures.Future that does not construct any Condition
    variables, therefore is faster to construct. However, some methods are
    missing, and result() in particular is not efficient with timeout > 0.
    """

    def __init__(self) -> None:
        self.result_value: T | None = None
        self.result_exception: Exception | None = None

    def done(self) -> bool:
        return self.result_value is not None or self.result_exception is not None

    def result(self, timeout: float | None = None) -> T:
        if timeout is not None:
            deadline = time.time() + timeout
        else:
            deadline = None

        # This implementation is bogus and shouldn't be used in production,
        # only in tests at most. It is only here for the sake of implementing
        # the contract. If you really need result with timeout>0, you should
        # use the stdlib future.
        #
        # If this becomes performance sensitive, we can potentially implement
        # something more sophisticated such as lazily creating the condition
        # variable, and synchronizing the creation of that using a global lock.
        while deadline is None or time.time() < deadline:
            if self.result_exception is not None:
                raise self.result_exception
            if self.result_value is not None:
                return self.result_value
            time.sleep(0.1)

        raise TimeoutError()

    def set_result(self, result: T) -> None:
        self.result_value = result

    def set_exception(self, exception: Exception) -> None:
        self.result_exception = exception


ProducerFuture = Union[SimpleProducerFuture[T], Future[T]]


class Producer(Generic[TStrategyPayload], ABC):
    @abstractmethod
    def produce(
        self, destination: Union[Topic, Partition], payload: TStrategyPayload
    ) -> ProducerFuture[BrokerValue[TStrategyPayload]]:
        """
        Produce to a topic or partition.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> Future[None]:
        """
        Close the producer.
        """
        raise NotImplementedError
