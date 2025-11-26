# src/shimpemt_qna_bot/logging/formatters.py

import datetime
import logging
import traceback as tb
from typing import Any  # type: ignore


class ShipmentQnaFormatter(logging.Formatter):
    """
    Custom formatter for logging in Shipment QnA Bot.
    produces logs line in the format:
    shipment_qna_bot - [2024-10-05 14:23:45] [ERROR] An error occurred
    shipment_qna_bot_24_Nov_25_14_12: NODE: RetrivalPlanner => INFO trace = ... conv= ... intent= ... - message ...
    including exception traceback if present.
    Deiberately keep it human-readable but structured enough to grep through logs.
    """

    # dd_MMM_yy_HH_MM e.g 24_Nov_25_14_12
    _time_format = "%d_%b_%y_%H_%M"

    def format(self, record: logging.LogRecord) -> str:
        # convert epoch to local time
        dt = datetime.datetime.fromtimestamp(record.created)
        timestamp_str = dt.strftime(self._time_format)

        # step: extra["step"] if provided, else logger name
        step = getattr(record, "step", record.name)

        # context aware field
        trace_id = getattr(record, "trace_id", "-")
        conversation_id = getattr(record, "conversation_id", "-")
        intent = getattr(record, "intent", "-")
        consignee_codes = getattr(record, "consignee_codes", "-")

        level = record.levelname

        # build base message, including formating
        msg = record.getMessage()

        # if exception hits, append it in traceback
        if record.exc_info:
            exc_text = "".join(tb.format_exception(*record.exc_info))
            msg = f"{msg} | Exception = {exc_text}"  # type: ignore

        line = (
            f"shipment_qna_bot_{timestamp_str}: {step} => "
            f"{level} trace={trace_id} conv={conversation_id} intent={intent}"
            f" consignees={consignee_codes} - {msg}"
        )

        return line
