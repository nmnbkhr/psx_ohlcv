"""Structured JSON logging for the pakfindata API.

Designed for journald: every log line is a single JSON object so
`journalctl --user -u pakfindata-api -o cat | jq .` works.

We deliberately write our own formatter (~25 lines) rather than
pulling in python-json-logger — fewer deps, same result.

Levels follow Python's logging module. Set via PAKFINDATA_LOG_LEVEL.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from pakfindata.api.config import get_settings


class JSONFormatter(logging.Formatter):
    """Emit one JSON object per log record.

    Reserved fields: ts, level, logger, message, module, line.
    Extra fields supplied via `logger.info("msg", extra={...})` are
    merged into the top-level object.
    """

    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "asctime", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Merge extras passed via logger.info("msg", extra={"foo": "bar"})
        for k, v in record.__dict__.items():
            if k in self._RESERVED or k.startswith("_"):
                continue
            payload[k] = v

        return json.dumps(payload, default=str)


def configure_logging() -> None:
    """Install the JSON formatter on stdout and silence uvicorn's
    default access-log handlers (we use SyslogIdentifier instead).

    Safe to call multiple times — re-applies the configuration.
    """
    settings = get_settings()

    root = logging.getLogger()
    root.setLevel(getattr(logging, settings.log_level, logging.INFO))

    # Remove any pre-existing handlers (uvicorn installs some).
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)

    # uvicorn loggers — let them propagate to root.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers = []
        lg.propagate = True
