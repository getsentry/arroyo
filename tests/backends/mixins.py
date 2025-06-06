import time
import uuid
from abc import ABC, abstractmethod
from contextlib import closing
from typing import Any, ContextManager, Generic, Iterator, Mapping, Optional, Sequence
from unittest import mock

import pytest

from arroyo.backends.abstract import Consumer, Producer
from arroyo.errors import ConsumerError, EndOfPartition, OffsetOutOfRange
from arroyo.types import BrokerValue, Partition, Topic, TStrategyPayload
from tests.assertions import assert_changes, assert_does_not_change


class StreamsTestMixin(ABC, Generic[TStrategyPayload]):
    cooperative_sticky = False
    kip_848 = False

    @abstractmethod
    def get_topic(self, partitions: int = 1) -> ContextManager[Topic]:
        raise NotImplementedError

    @abstractmethod
    def get_consumer(
        self, group: Optional[str] = None, enable_end_of_partition: bool = True
    ) -> Consumer[TStrategyPayload]:
        raise NotImplementedError

    @abstractmethod
    def get_producer(self) -> Producer[TStrategyPayload]:
        raise NotImplementedError

    @abstractmethod
    def get_payloads(self) -> Iterator[TStrategyPayload]:
        raise NotImplementedError

    def test_consumer(self) -> None:
        group = uuid.uuid1().hex
        payloads = self.get_payloads()

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages = [
                    future.result(timeout=5.0)
                    for future in [
                        producer.produce(topic, next(payloads)) for i in range(2)
                    ]
                ]

            consumer = self.get_consumer(group)

            def _assignment_callback(partitions: Mapping[Partition, int]) -> None:
                assert partitions == {Partition(topic, 0): messages[0].offset}

                consumer.seek({Partition(topic, 0): messages[1].offset})

                with pytest.raises(ConsumerError):
                    consumer.seek({Partition(topic, 1): 0})

            assignment_callback = mock.Mock(side_effect=_assignment_callback)

            def _revocation_callback(partitions: Sequence[Partition]) -> None:
                assert partitions == [Partition(topic, 0)]
                assert consumer.tell() == {Partition(topic, 0): messages[1].offset}

                # Not sure why you'd want to do this, but it shouldn't error.
                consumer.seek({Partition(topic, 0): messages[0].offset})

            revocation_callback = mock.Mock(side_effect=_revocation_callback)

            # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
            # use to wait for assignment, but we'd need to be very careful to avoid
            # edge cases here. It's probably not worth the complexity for now.
            consumer.subscribe(
                [topic], on_assign=assignment_callback, on_revoke=revocation_callback
            )

            with assert_changes(
                lambda: assignment_callback.called, False, True
            ), assert_changes(
                consumer.tell,
                {},
                {Partition(topic, 0): messages[1].next_offset},
            ):
                value = consumer.poll(10.0)  # XXX: getting the subcription is slow

            assert isinstance(value, BrokerValue)
            assert value.committable == messages[1].committable
            assert value.partition == Partition(topic, 0)
            assert value.offset == messages[1].offset
            assert value == messages[1]

            consumer.seek({Partition(topic, 0): messages[0].offset})
            assert consumer.tell() == {Partition(topic, 0): messages[0].offset}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 1): 0})

            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            # Even if there is another message available, ``poll`` should
            # return ``None`` if the consumer is paused.
            assert consumer.poll(1.0) is None

            with assert_changes(consumer.paused, [Partition(topic, 0)], []):
                consumer.resume([Partition(topic, 0)])

            value = consumer.poll(5.0)
            assert isinstance(value, BrokerValue)
            assert value.partition == Partition(topic, 0)
            assert value.offset == messages[0].offset
            assert value.payload == messages[0].payload

            assert consumer.commit_offsets() == {}

            consumer.stage_offsets(value.committable)

            assert consumer.commit_offsets() == {Partition(topic, 0): value.next_offset}

            consumer.stage_offsets({Partition(Topic("invalid"), 0): 0})

            with pytest.raises(ConsumerError):
                consumer.commit_offsets()

            assert consumer.tell() == {Partition(topic, 0): messages[1].offset}

            consumer.unsubscribe()

            with assert_changes(lambda: revocation_callback.called, False, True):
                assert consumer.poll(1.0) is None

            assert consumer.tell() == {}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 0): messages[0].offset})

            revocation_callback.reset_mock()

            with assert_changes(
                lambda: consumer.closed, False, True
            ), assert_does_not_change(lambda: revocation_callback.called, False):
                consumer.close()

            # Make sure all public methods (except ``close```) error if called
            # after the consumer has been closed.

            with pytest.raises(RuntimeError):
                consumer.subscribe([topic])

            with pytest.raises(RuntimeError):
                consumer.unsubscribe()

            with pytest.raises(RuntimeError):
                consumer.poll()

            with pytest.raises(RuntimeError):
                consumer.tell()

            with pytest.raises(RuntimeError):
                consumer.seek({Partition(topic, 0): messages[0].offset})

            with pytest.raises(RuntimeError):
                consumer.pause([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.resume([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.paused()

            # stage_positions does not validate anything
            consumer.stage_offsets({})

            with pytest.raises(RuntimeError):
                consumer.commit_offsets()

            consumer.close()  # should be safe, even if the consumer is already closed

            consumer = self.get_consumer(group)

            revocation_callback = mock.MagicMock()

            consumer.subscribe([topic], on_revoke=revocation_callback)

            value = consumer.poll(10.0)  # XXX: getting the subscription is slow
            assert isinstance(value, BrokerValue)
            assert value.partition == Partition(topic, 0)
            assert value.offset == messages[1].offset
            assert value.payload == messages[1].payload
            assert value == messages[1]

            try:
                assert consumer.poll(1.0) is None
            except EndOfPartition as error:
                assert error.partition == Partition(topic, 0)
                assert error.offset == value.next_offset
            else:
                raise AssertionError("expected EndOfPartition error")

            with assert_changes(lambda: revocation_callback.called, False, True):
                consumer.close()

    def test_consumer_offset_out_of_range(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages = [producer.produce(topic, next(payloads)).result(5.0)]

            consumer = self.get_consumer()
            consumer.subscribe([topic])

            for i in range(5):
                message = consumer.poll(1.0)
                if message is not None:
                    break
                else:
                    time.sleep(1.0)
            else:
                raise Exception("assignment never received")

            with pytest.raises(EndOfPartition):
                consumer.poll()

            # Somewhat counterintuitively, seeking to an invalid offset
            # should be allowed -- we don't know it's invalid until we try and
            # read from it.
            consumer.seek({Partition(topic, 0): messages[-1].next_offset + 1000})

            with pytest.raises(OffsetOutOfRange):
                consumer.poll()

    def test_working_offsets(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages = [producer.produce(topic, next(payloads)).result(5.0)]

            def on_assign(partitions: Mapping[Partition, int]) -> None:
                # NOTE: This will eventually need to be controlled by a generalized
                # consumer auto offset reset setting.

                assert (
                    partitions
                    == consumer.tell()
                    == {messages[0].partition: messages[0].offset}
                )

            consumer = self.get_consumer()
            consumer.subscribe([topic], on_assign=on_assign)

            for i in range(5):
                value = consumer.poll(1.0)
                if value is not None:
                    break
                else:
                    time.sleep(1.0)
            else:
                raise Exception("assignment never received")

            assert isinstance(value, BrokerValue)
            assert value == messages[0]

            # The first call to ``poll`` should raise ``EndOfPartition``. It
            # should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            with assert_does_not_change(
                consumer.tell, {value.partition: value.next_offset}
            ), pytest.raises(EndOfPartition):
                assert consumer.poll(1.0) is None

            # It should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            with assert_does_not_change(
                consumer.tell, {value.partition: value.next_offset}
            ):
                assert consumer.poll(1.0) is None

            with assert_changes(
                consumer.tell,
                {value.partition: value.next_offset},
                {value.partition: value.offset},
            ):
                consumer.seek({value.partition: value.offset})

            with assert_changes(
                consumer.tell,
                {value.partition: value.offset},
                {value.partition: value.next_offset},
            ):
                value = consumer.poll()
                assert value == messages[0]

            # Seeking beyond the first missing index should work but subsequent
            # reads should error. (We don't know if this offset is valid or not
            # until we try to fetch a message.)
            assert isinstance(value, BrokerValue)
            with assert_changes(
                consumer.tell,
                {value.partition: value.next_offset},
                {value.partition: value.next_offset + 1},
            ):
                consumer.seek({value.partition: value.next_offset + 1})

            # Offsets should not be advanced after a failed poll.
            with assert_does_not_change(
                consumer.tell,
                {value.partition: value.next_offset + 1},
            ), pytest.raises(ConsumerError):
                consumer.poll(1.0)

            # Trying to seek on an unassigned partition should error.
            with assert_does_not_change(
                consumer.tell,
                {value.partition: value.next_offset + 1},
            ), pytest.raises(ConsumerError):
                consumer.seek({value.partition: 0, Partition(topic, -1): 0})

            # Trying to seek to a negative offset should error.
            with assert_does_not_change(
                consumer.tell,
                {value.partition: value.next_offset + 1},
            ), pytest.raises(ConsumerError):
                consumer.seek({value.partition: -1})

    def test_pause_resume(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic() as topic, closing(
            self.get_consumer()
        ) as consumer, closing(self.get_producer()) as producer:
            messages = [
                producer.produce(topic, next(payloads)).result(timeout=5.0)
                for i in range(5)
            ]

            consumer.subscribe([topic])

            assert consumer.poll(10.0) == messages[0]
            assert consumer.paused() == []

            # XXX: Unfortunately, there is really no way to prove that this
            # consumer would return the message other than by waiting a while.
            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            assert consumer.poll(1.0) is None

            # We should pick up where we left off when we resume the partition.
            with assert_changes(consumer.paused, [Partition(topic, 0)], []):
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[1]

            # Calling ``seek`` should have a side effect, even if no messages
            # are consumed before calling ``pause``.
            with assert_changes(
                consumer.tell,
                {Partition(topic, 0): messages[1].next_offset},
                {Partition(topic, 0): messages[3].offset},
            ):
                consumer.seek({Partition(topic, 0): messages[3].offset})
                consumer.pause([Partition(topic, 0)])
                assert consumer.poll(1.0) is None
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[3]

            # It is still allowable to call ``seek`` on a paused partition.
            # When consumption resumes, we would expect to see the side effect
            # of that seek.
            consumer.pause([Partition(topic, 0)])
            with assert_changes(
                consumer.tell,
                {Partition(topic, 0): messages[3].next_offset},
                {Partition(topic, 0): messages[0].offset},
            ):
                consumer.seek({Partition(topic, 0): messages[0].offset})
                assert consumer.poll(1.0) is None
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[0]

            with assert_does_not_change(consumer.paused, []), pytest.raises(
                ConsumerError
            ):
                consumer.pause([Partition(topic, 0), Partition(topic, 1)])

            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            with assert_does_not_change(
                consumer.paused, [Partition(topic, 0)]
            ), pytest.raises(ConsumerError):
                consumer.resume([Partition(topic, 0), Partition(topic, 1)])

    def test_pause_resume_rebalancing(self) -> None:
        payloads = self.get_payloads()

        consumer_a_on_assign = mock.Mock()
        consumer_a_on_revoke = mock.Mock()
        consumer_b_on_assign = mock.Mock()
        consumer_b_on_revoke = mock.Mock()

        with self.get_topic(2) as topic, closing(
            self.get_producer()
        ) as producer, closing(
            self.get_consumer("group", enable_end_of_partition=False)
        ) as consumer_a, closing(
            self.get_consumer("group", enable_end_of_partition=False)
        ) as consumer_b:
            messages = [
                producer.produce(Partition(topic, i), next(payloads)).result(
                    timeout=5.0
                )
                for i in [0, 1]
            ]

            def wait_until_rebalancing(
                from_consumer: Consumer[Any], to_consumer: Consumer[Any]
            ) -> None:
                for _ in range(20):
                    assert from_consumer.poll(0) is None
                    if to_consumer.poll(1.0) is not None:
                        return

                raise RuntimeError("no rebalancing happened")

            consumer_a.subscribe(
                [topic], on_assign=consumer_a_on_assign, on_revoke=consumer_a_on_revoke
            )

            # It doesn't really matter which message is fetched first -- we
            # just want to know the assignment occurred.
            assert (
                consumer_a.poll(10.0) in messages
            )  # XXX: getting the subcription is slow

            assert len(consumer_a.tell()) == 2
            assert len(consumer_b.tell()) == 0

            # Pause all partitions.
            consumer_a.pause([Partition(topic, 0), Partition(topic, 1)])
            assert set(consumer_a.paused()) == set(
                [Partition(topic, 0), Partition(topic, 1)]
            )

            consumer_b.subscribe(
                [topic], on_assign=consumer_b_on_assign, on_revoke=consumer_b_on_revoke
            )

            wait_until_rebalancing(consumer_a, consumer_b)

            if self.cooperative_sticky or self.kip_848:
                # within incremental rebalancing, only one partition should have been reassigned to the consumer_b, and consumer_a should remain paused
                # Either partition 0 or 1 might be the paused one
                assert len(consumer_a.paused()) == 1
                assert consumer_a.poll(10.0) is None
            else:
                # The first consumer should have had its offsets rolled back, as
                # well as should have had it's partition resumed during
                # rebalancing.
                assert consumer_a.paused() == []
                assert consumer_a.poll(10.0) is not None

            assert len(consumer_a.tell()) == 1
            assert len(consumer_b.tell()) == 1

            (consumer_a_partition,) = consumer_a.tell()
            (consumer_b_partition,) = consumer_b.tell()

            # Pause consumer_a again.
            consumer_a.pause(list(consumer_a.tell()))
            # if we close consumer_a, consumer_b should get all partitions
            producer.produce(next(iter(consumer_a.tell())), next(payloads)).result(
                timeout=5.0
            )
            consumer_a.unsubscribe()
            wait_until_rebalancing(consumer_a, consumer_b)

            assert len(consumer_b.tell()) == 2

            if self.cooperative_sticky or self.kip_848:
                consumer_a_on_assign.assert_has_calls(
                    [
                        mock.call({Partition(topic, 0): 0, Partition(topic, 1): 0}),
                    ]
                )

                consumer_a_on_revoke.assert_has_calls(
                    [
                        mock.call([Partition(topic, 0)]),
                        mock.call([Partition(topic, 1)]),
                    ],
                    any_order=True,
                )

                consumer_b_on_assign.assert_has_calls(
                    [
                        mock.call({Partition(topic, 0): 0}),
                        mock.call({Partition(topic, 1): 0}),
                    ],
                    any_order=True,
                )
                assert consumer_b_on_revoke.mock_calls == []
            else:
                assert consumer_a_on_assign.mock_calls == [
                    mock.call({Partition(topic, 0): 0, Partition(topic, 1): 0}),
                    mock.call({consumer_a_partition: 0}),
                ]
                assert consumer_a_on_revoke.mock_calls == [
                    mock.call([Partition(topic, 0), Partition(topic, 1)]),
                    mock.call([consumer_a_partition]),
                ]

                assert consumer_b_on_assign.mock_calls == [
                    mock.call({consumer_b_partition: 0}),
                    mock.call({Partition(topic, 0): 0, Partition(topic, 1): 0}),
                ]
                assert consumer_b_on_revoke.mock_calls == [
                    mock.call([consumer_b_partition])
                ]
