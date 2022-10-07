import pytest

from arroyo.commit import IMMEDIATE, ONCE_PER_SECOND, CommitPolicy


def test_commit_policy() -> None:
    assert IMMEDIATE.should_commit(0.5, 1) is True
    assert IMMEDIATE.should_commit(2, 1) is True
    assert IMMEDIATE.should_commit(0.5, 5) is True

    assert ONCE_PER_SECOND.should_commit(0.5, 10) is False
    assert ONCE_PER_SECOND.should_commit(1, 10) is True
    assert ONCE_PER_SECOND.should_commit(1.5, 10) is True

    custom_policy = CommitPolicy(2, 100)
    assert custom_policy.should_commit(1, 99) is False
    assert custom_policy.should_commit(2, 1) is True
    assert custom_policy.should_commit(1, 101) is True

    with pytest.raises(Exception):
        CommitPolicy(None, None)
