import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

# I'm using context variables to trace request IDs and scopes across the app.
trace_id_ctx = contextvars.ContextVar("trace_id", default=None)
conversation_id_ctx = contextvars.ContextVar("conversation_id", default=None)
consignee_scope_ctx = contextvars.ContextVar("consignee_scope", default=None)


class JSONFormatter(logging.Formatter):
    """
    I format logs as JSON so I can easily include tracing context.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": trace_id_ctx.get(),
            "conversation_id": conversation_id_ctx.get(),
            "consignee_scope": consignee_scope_ctx.get(),
        }

        # I include any extra data if I find it.
        if hasattr(record, "extra_data"):
            log_record.update(record.extra_data)

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def setup_logger(name: str = "shipment_qna_bot", level: str = "INFO") -> logging.Logger:
    """
    I set up the logger here with my JSON formatter and file rotation.
    """
    logger = logging.getLogger(name)

    # I clear old handlers so I don't get double logs.
    if logger.handlers:
        logger.handlers.clear()

    logger.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # I'll add a file handler for app.log
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)

    # I stop the logs from bubbling up to the root to avoid clutter.
    logger.propagate = False

    return logger


# Global logger instance
logger = setup_logger()


def set_log_context(
    conversation_id: Optional[str] = None,
    consignee_codes: Optional[list[str]] = None,
    intent: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> None:
    """
    I update the log context variables here.
    """
    conversation_id_ctx.set(conversation_id)
    consignee_scope_ctx.set(consignee_codes)
    trace_id_ctx.set(trace_id)
