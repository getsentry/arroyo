import logging

import structlog
from structlog.types import EventDict


def add_severity_attribute(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    Set the severity attribute for Google Cloud logging ingestion
    """
    event_dict["severity"] = event_dict["level"].upper()
    del event_dict["level"]
    return event_dict


def setup_logging() -> None:
    structlog.configure(
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            add_severity_attribute,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
    )
