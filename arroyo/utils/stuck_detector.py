from __future__ import annotations

import logging
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from typing import Iterator

logger = logging.getLogger(__name__)


def get_all_thread_stacks() -> str:
    """Get stack traces from all threads without using signals."""
    stacks = []
    frames = sys._current_frames()
    threads_by_id = {t.ident: t for t in threading.enumerate()}

    for thread_id, frame in frames.items():
        thread = threads_by_id.get(thread_id)
        thread_name = thread.name if thread else f"Unknown-{thread_id}"
        stack = "".join(traceback.format_stack(frame))
        stacks.append(f"Thread {thread_name} ({thread_id}):\n{stack}")

    return "\n\n".join(stacks)


@contextmanager
def stuck_detector(
    name: str = "stuck-detector",
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """
    Context manager that spawns a daemon thread to detect stuck operations.
    If the wrapped code block doesn't complete within timeout_seconds,
    logs all thread stack traces for debugging.
    """
    done = threading.Event()

    def detector() -> None:
        start = time.time()
        while not done.wait(timeout=1):
            if time.time() - start > timeout_seconds:
                logger.warning(
                    "%s: Operation stuck for %s seconds, stacks:\n%s",
                    name,
                    timeout_seconds,
                    get_all_thread_stacks(),
                )
                return

    thread = threading.Thread(target=detector, daemon=True, name=name)
    thread.start()
    try:
        yield
    finally:
        done.set()
