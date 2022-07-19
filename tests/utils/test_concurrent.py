import threading
import time
from concurrent.futures import TimeoutError

import pytest

from arroyo.utils.concurrent import execute


def test_execute() -> None:
    assert execute(threading.current_thread).result() != threading.current_thread()

    with pytest.raises(ZeroDivisionError):
        assert execute(lambda: 1 / 0).result()

    with pytest.raises(TimeoutError):
        assert execute(lambda: time.sleep(10), daemon=True).result(timeout=0)
