import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

# Context variables for request tracing
trace_id_ctx = contextvars.ContextVar("trace_id", default=None)
conversation_id_ctx = contextvars.ContextVar("conversation_id", default=None)
consignee_scope_ctx = contextvars.ContextVar("consignee_scope", default=None)


class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings with context.
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

        # Add extra fields from record if available
        if hasattr(record, "extra_data"):
            log_record.update(record.extra_data)

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def setup_logger(name: str = "shipment_qna_bot", level: str = "INFO") -> logging.Logger:
    """
    Configures and returns a logger with JSON formatting.
    """
    logger = logging.getLogger(name)

    # clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    logger.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    # Add file handler for app.log
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)

    # Prevent propagation to root logger to avoid double logging if root is configured
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
    Updates the context variables for logging.
    """
    conversation_id_ctx.set(conversation_id)
    consignee_scope_ctx.set(consignee_codes)
    trace_id_ctx.set(trace_id)

    # Note: intent is passed but not currently stored in a dedicated context var
    # We could add an intent_ctx if needed, or rely on extra_data in node logs.
