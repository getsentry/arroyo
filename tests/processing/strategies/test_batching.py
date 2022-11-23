import time
from datetime import datetime
from typing import Any, MutableSequence, Sequence
from unittest.mock import Mock, patch

from arroyo.processing.strategies.batching import (
    AbstractBatchWorker,
    BatchProcessingStrategy,
)
from arroyo.types import BrokerValue, Message, Partition, Topic


class FakeWorker(AbstractBatchWorker[int, int]):
    def __init__(self) -> None:
        self.processed: MutableSequence[int] = []
        self.flushed: MutableSequence[Sequence[int]] = []

    def process_message(self, message: Message[int]) -> int:
        self.processed.append(message.payload)
        return message.payload

    def flush_batch(self, batch: Sequence[int]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self) -> None:
        topic = Topic("topic")
        worker = FakeWorker()
        commit_func = Mock()

        strategy = BatchProcessingStrategy(
            commit_func, worker, max_batch_size=2, max_batch_time=100
        )

        for i in [1, 2, 3]:
            message = Message(BrokerValue(i, Partition(topic, 0), i, datetime.utcnow()))
            strategy.submit(message)
            strategy.poll()

        strategy.close()
        strategy.join()

        assert worker.processed == [1, 2, 3]
        assert worker.flushed == [[1, 2]]
        assert commit_func.call_count == 1

    @patch("time.time")
    def test_batch_time(self, mock_time: Any) -> None:
        topic = Topic("topic")

        t1 = datetime(2018, 1, 1, 0, 0, 0)
        t2 = datetime(2018, 1, 1, 0, 0, 1)
        t3 = datetime(2018, 1, 1, 0, 0, 5)

        mock_time.return_value = time.mktime(t1.timetuple())
        worker = FakeWorker()
        commit_func = Mock()

        strategy = BatchProcessingStrategy(
            commit_func, worker, max_batch_size=100, max_batch_time=2000
        )

        for i in [1, 2, 3]:
            message = Message(BrokerValue(i, Partition(topic, 0), i, t1))
            strategy.submit(message)

        mock_time.return_value = time.mktime(t2.timetuple())
        strategy.poll()

        for i in [4, 5, 6]:
            message = Message(BrokerValue(i, Partition(topic, 0), i, t2))
            strategy.submit(message)

        mock_time.return_value = time.mktime(t3.timetuple())
        strategy.poll()

        for i in [7, 8, 9]:
            message = Message(BrokerValue(i, Partition(topic, 0), i, t3))
            strategy.submit(message)
            strategy.poll()

        assert worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert commit_func.call_count == 1

        strategy.close()
        strategy.join()
