from datetime import datetime
from typing import Callable, List, Mapping, Set
from unittest.mock import Mock, call

from arroyo.commit import IMMEDIATE
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.filter import FilterStep
from arroyo.processing.strategies.reduce import Reduce
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import FILTERED_PAYLOAD, BaseValue, Message, Partition, Topic, Value


def test_reduce() -> None:
    now = datetime.now()

    def accumulator(result: Set[int], value: BaseValue[int]) -> Set[int]:
        result.add(value.payload)
        return result

    initial_value: Callable[[], Set[int]] = lambda: set()

    next_step = Mock()

    strategy = Reduce(3, 5.0, accumulator, initial_value, next_step)

    partition = Partition(Topic("topic"), 0)

    for i in range(6):
        strategy.submit(
            Message(
                Value(
                    i,
                    {
                        partition: i + 1,
                    },
                    now,
                )
            )
        )
        strategy.poll()

    next_step.submit.assert_has_calls(
        [
            call(Message(Value({0, 1, 2}, {partition: 3}, now))),
            call(Message(Value({3, 4, 5}, {partition: 6}, now))),
        ]
    )


def _add(result: Set[int], value: BaseValue[int]) -> Set[int]:
    result.add(value.payload)
    return result


def test_reduce_forwards_filtered_message_only_when_nothing_is_waiting() -> None:
    now = datetime.now()
    partition = Partition(Topic("topic"), 0)

    next_step = Mock()
    strategy = Reduce(3, 5.0, _add, set, next_step)
    filtered = Message(Value(FILTERED_PAYLOAD, {partition: 7}))
    strategy.submit(filtered)
    assert next_step.submit.call_args_list == [call(filtered)]

    # A message with batch size 0 leaves the buffer looking empty, but it is
    # still waiting for a flush.
    next_step = Mock()
    strategy = Reduce(3, 5.0, _add, set, next_step, compute_batch_size=lambda value: 0)
    strategy.submit(Message(Value(0, {partition: 1}, now)))
    strategy.submit(Message(Value(FILTERED_PAYLOAD, {partition: 5})))
    assert next_step.submit.call_args_list == []


def test_reduce_holds_filtered_offsets_until_the_batch_is_flushed() -> None:
    now = datetime.now()
    next_step = Mock()
    strategy = Reduce(3, 5.0, _add, set, next_step)
    topic = Topic("topic")
    p0 = Partition(topic, 0)
    p1 = Partition(topic, 1)

    strategy.submit(Message(Value(0, {p0: 1}, now)))
    strategy.submit(Message(Value(1, {p1: 2}, now)))
    strategy.submit(Message(Value(FILTERED_PAYLOAD, {p0: 5})))
    strategy.poll()

    # Offsets 0 and 1 are still buffered, so nothing may be committed past them.
    assert next_step.submit.call_args_list == []

    strategy.close()
    strategy.join()

    # The filtered message's offset travels with the batch; partition 1 keeps its own.
    assert next_step.submit.call_args_list == [
        call(Message(Value({0, 1}, {p0: 5, p1: 2}, now)))
    ]


def test_reduce_does_not_commit_filtered_offsets_ahead_of_the_batch() -> None:
    now = datetime.now()
    partition = Partition(Topic("topic"), 0)
    written: List[Set[int]] = []
    committed: List[Mapping[Partition, int]] = []

    def write(message: Message[Set[int]]) -> None:
        written.append(message.payload)

    def commit(offsets: Mapping[Partition, int], force: bool = False) -> None:
        if offsets:
            committed.append(dict(offsets))

    def accept(message: Message[int]) -> bool:
        return message.payload < 3

    reduce_step: Reduce[int, Set[int]] = Reduce(
        100, 60.0, _add, set, RunTask(write, CommitOffsets(commit))
    )

    # The same shape as the consumers in snuba and sentry: a filter that emits
    # commit-only messages, a Reduce that batches, and a commit at the end.
    strategy: FilterStep[int] = FilterStep(accept, reduce_step, commit_policy=IMMEDIATE)

    for offset in range(5):
        strategy.submit(Message(Value(offset, {partition: offset + 1}, now)))
        strategy.poll()

    # Offsets 0, 1 and 2 are accepted and sit in the buffer; 3 and 4 are filtered.
    assert written == []
    assert committed == []

    strategy.close()
    strategy.join()

    assert written == [{0, 1, 2}]
    assert committed == [{partition: 5}]
