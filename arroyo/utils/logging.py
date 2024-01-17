from logging import CRITICAL, DEBUG, ERROR, INFO, NOTSET, WARNING
from typing import Callable, Optional

PYTHON_TO_SYSLOG_MAP = {
    NOTSET: 7,
    DEBUG: 7,
    INFO: 6,
    WARNING: 4,
    ERROR: 3,
    CRITICAL: 2,
}


def pylog_to_syslog_level(level: int) -> int:
    return PYTHON_TO_SYSLOG_MAP.get(level, 7)


# A callback overwritten by testsuite so that we can assert there are no
# internal errors.
_handle_internal_error: Optional[Callable[[Exception], None]] = None


def handle_internal_error(e: Exception) -> None:
    if _handle_internal_error is not None:
        _handle_internal_error(e)
