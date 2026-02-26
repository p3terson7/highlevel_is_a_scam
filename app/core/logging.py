import json
import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any


_REQUEST_ID: ContextVar[str] = ContextVar("request_id", default="-")
_RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": get_request_id(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED and not key.startswith("_")
        }
        if extra:
            payload["extra"] = extra
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            handler.setFormatter(JsonFormatter())
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def set_request_id(request_id: str) -> Token[str]:
    return _REQUEST_ID.set(request_id)


def reset_request_id(token: Token[str]) -> None:
    _REQUEST_ID.reset(token)


def get_request_id() -> str:
    return _REQUEST_ID.get()
