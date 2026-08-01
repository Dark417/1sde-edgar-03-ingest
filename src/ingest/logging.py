"""L0: structured JSON logging to stdout.

Imports nothing internal. Does not handle: log shipping, metrics backends, or
deciding what to log — callers do that.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

__all__ = ["configure_logging", "get_logger"]


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog to emit one JSON object per line on stdout.

    JSON on stdout is what CloudWatch and ``docker logs`` both parse without
    help. Called once from the CLI; calling it again is harmless.

    Does not handle: log level per module, or file/rotating handlers — a batch
    container logs to stdout and nowhere else.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    # force=True so a second call rebinds to the current sys.stdout instead of
    # leaving a handler holding a stream that may since have been replaced or
    # closed.
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=numeric_level, force=True)

    # httpx and botocore log request lines as plain text at INFO, which would
    # interleave non-JSON lines into a stream that is contractually all-JSON
    # (AGENTS.md §5.9). Their content is already covered by our own events.
    for noisy in ("httpx", "httpcore", "botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        # No explicit file: the factory then resolves sys.stdout at write time.
        # Pinning the current sys.stdout here would capture whatever stream
        # happened to be installed at configure time and keep writing to it
        # after it is replaced or closed.
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> Any:
    """Return a bound structlog logger.

    Return type is ``Any`` because structlog's bound-logger type varies with the
    configured ``wrapper_class``; pinning it here would be a lie under --strict.
    """
    return structlog.get_logger(name)
