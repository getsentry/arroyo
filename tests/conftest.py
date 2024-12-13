from typing import Generator, Iterator, List

import pytest

pytest.register_assert_rewrite("tests.assertions")

from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import TStrategyPayload
from arroyo.utils.clock import MockedClock
from arroyo.utils.metrics import configure_metrics
from tests.metrics import TestingMetricsBackend


def pytest_configure() -> None:
    configure_metrics(TestingMetricsBackend)


@pytest.fixture(autouse=True)
def clear_metrics_state() -> Iterator[None]:
    yield
    TestingMetricsBackend.calls.clear()


@pytest.fixture
def broker() -> Iterator[LocalBroker[TStrategyPayload]]:
    yield LocalBroker(MemoryMessageStorage(), MockedClock())


@pytest.fixture(autouse=True)
def assert_no_internal_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    from arroyo.utils import logging

    errors: List[Exception] = []
    monkeypatch.setattr(logging, "_handle_internal_error", errors.append)

    yield

    for e in errors:
        raise e
