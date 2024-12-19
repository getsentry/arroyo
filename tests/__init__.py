from typing import ContextManager, Protocol, TypeVar

T = TypeVar("T")


class WithStmt(Protocol):
    def __call__(self, value: ContextManager[T]) -> T:
        ...
