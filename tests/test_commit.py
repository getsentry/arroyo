import pytest

from arroyo.commit import IMMEDIATE, ONCE_PER_SECOND, CommitPolicy
from arroyo.types import Partition, Topic


def test_commit_policy() -> None:
    partition = Partition(Topic("test"), 0)

    state = IMMEDIATE.get_state_machine()
    time = 0.5
    offset = 1
    assert state.should_commit(time, {partition: offset}) is True
    time += 2
    offset += 1
    assert state.should_commit(time, {partition: offset}) is True
    time += 0.5
    offset += 5
    assert state.should_commit(time, {partition: offset}) is True

    state = ONCE_PER_SECOND.get_state_machine()
    time = 0.5
    offset = 10
    assert state.should_commit(time, {partition: offset}) is False
    time += 1
    offset += 10
    assert state.should_commit(time, {partition: offset}) is True
    time += 1.5
    offset += 10
    assert state.should_commit(time, {partition: offset}) is True

    state = CommitPolicy(2, 100).get_state_machine()
    time = 1
    offset = 99
    assert state.should_commit(time, {partition: offset}) is False
    time += 2
    offset += 1
    assert state.should_commit(time, {partition: offset}) is True
    time += 1
    offset += 101
    assert state.should_commit(time, {partition: offset}) is True

    with pytest.raises(Exception):
        CommitPolicy(None, None)
