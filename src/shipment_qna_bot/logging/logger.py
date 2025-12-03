# src/shimpemt_qna_bot/logging/logger.py

import os
import warnings

# from dotenv import load_dotenv, find_dotenv # type: ignore
# load_dotenv(find_dotenv(), override=True)


warnings.filterwarnings("ignore", category=DeprecationWarning)

import contextvars
import logging
from logging.handlers import RotatingFileHandler
from typing import Iterable, List, Optional  # type: ignore

# from shipment_qna_bot.logging.formatter import ShipmentQnaFormatter
from .formatter import ShipmentQnaFormatter

# context variables for logging
trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "trace_id", default="-"
)
conversation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "conversation_id", default="-"
)
intent_var: contextvars.ContextVar[str] = contextvars.ContextVar("intent", default="-")
consignee_codes_var: contextvars.ContextVar[str] = contextvars.ContextVar("consignee_codes", default=[])  # type: ignore


class ContextFilter(logging.Filter):
    """
    Injects contextvars into every LogRecord so formatter can access into the log.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        record.conversation_id = conversation_id_var.get()
        record.intent = intent_var.get()
        consignee_codes = consignee_codes_var.get() or []  # type: ignore
        if isinstance(consignee_codes, (list, tuple)):
            record.consignee_codes = ",".join(str(c) for c in consignee_codes)  # type: ignore
        else:
            record.consignee_codes = str(consignee_codes)
        return True


def set_log_context(
    *,
    trace_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    intent: Optional[str] = None,
    consignee_codes: Optional[Iterable[str]] = None,
    **extra: object,  # for any unexpected kwargs
) -> None:
    """
    Set or Update logging context for current request / graph execution.

    Call sign:
    - in FastAPI middleware (per request)
    - at graph entry when we know conversational_id / intent
    """
    if trace_id is not None:
        trace_id_var.set(trace_id)
    if conversation_id is not None:
        conversation_id_var.set(conversation_id)
    if intent is not None:
        intent_var.set(intent)
    if consignee_codes is not None:
        consignee_codes_var.set(list(consignee_codes))  # type: ignore


_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:  # type: ignore
    """_summary_: Store process execution steps in context variable for logging.

    Returns:
        logging.Logger: _description_: Return the singleton logger instance with custome formatter and context filter.
    """
    global _logger
    if _logger is not None:  # type: ignore
        return _logger  # type: ignore

    logger = logging.getLogger("shipment_qna_bot")
    # logger.setLevel(logging.DEBUG)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())  # type: ignore
    logger.propagate = False  # prevent double logging if root logger

    # clear if any previous broken handlers was present
    logger.handlers.clear()

    # ensure Logs direcory exists to hold log files
    logs_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
    )
    os.makedirs(logs_dir, exist_ok=True)

    # create formatter
    formatter = ShipmentQnaFormatter()

    # create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # rotating file handler
    file_path = os.path.join(logs_dir, "app.log")
    file_handler = RotatingFileHandler(  # type: ignore
        file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10 MB
    )
    file_handler.setFormatter(formatter)

    # add context filter
    context_filter = ContextFilter()
    console_handler.addFilter(context_filter)
    file_handler.addFilter(context_filter)

    # add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    _logger = logger
    return logger


# convenience alias module-level logger
logger = get_logger()  # type: ignore


# test cases and application:
# useage inside the app
##############################

# from shipment_qna_bot.logging.logger import logger

# logger.info("Starting HybridRetriever with k=8", extra={"step": "NODE:HybridRetriever"}) # type: ignore
# logger.error("Azure Search call failed", extra={"step": "TOOL:AzureSearch"}, exc_info=True) # type: ignore

# set context once per request / graph execution
################################################

# from shipment_qna_bot.logging.logger import set_log_context

# set_log_context(
#     trace_id="uuid-...",
#     conversation_id="conversation-uuid",
#     intent="status",
#     consignee_codes=["PARENT(0001)", "CHILD(0002)"]
# ) # type: ignore
