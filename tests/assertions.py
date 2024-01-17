from contextlib import contextmanager
from typing import Any, Callable, Iterator


@contextmanager
def assert_changes(
    callable: Callable[[], Any],
    before: Any,
    after: Any,
) -> Iterator[None]:
    actual = callable()
    assert actual == before

    yield

    actual = callable()
    assert actual == after


@contextmanager
def assert_does_not_change(
    callable: Callable[[], Any],
    value: Any,
) -> Iterator[None]:
    actual = callable()
    assert actual == value

    yield

    actual = callable()
    assert actual == value
