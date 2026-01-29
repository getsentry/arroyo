from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from arroyo.utils.stuck_detector import get_all_thread_stacks, stuck_detector


def test_get_all_thread_stacks() -> None:
    """Test that get_all_thread_stacks returns stack traces for all threads."""
    stacks = get_all_thread_stacks()
    assert "MainThread" in stacks
    assert "test_get_all_thread_stacks" in stacks  # Current function should be in stack


def test_stuck_detector_does_not_trigger_before_timeout() -> None:
    """Test that stuck detector doesn't trigger when operation completes quickly."""
    with patch.object(
        __import__("arroyo.utils.stuck_detector", fromlist=["logger"]).logger,
        "warning",
    ) as mock_warn:
        with stuck_detector(timeout_seconds=30):
            pass

        mock_warn.assert_not_called()


def test_stuck_detector_logs_after_timeout() -> None:
    """Test that stuck detector logs warning with stack traces when timeout exceeded."""
    warning_logged = threading.Event()

    def mock_warning(*args: object, **kwargs: object) -> None:
        warning_logged.set()

    with patch.object(
        __import__("arroyo.utils.stuck_detector", fromlist=["logger"]).logger,
        "warning",
        side_effect=mock_warning,
    ) as mock_warn:
        with stuck_detector(timeout_seconds=0.05):
            warning_logged.wait(timeout=2)

        mock_warn.assert_called_once()
        call_args = mock_warn.call_args
        assert "Operation stuck" in call_args[0][0]
        assert "MainThread" in call_args[0][3]
