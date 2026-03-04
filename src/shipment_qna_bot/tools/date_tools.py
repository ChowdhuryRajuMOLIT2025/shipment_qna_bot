# src/shipment_qna_bot/tools/date_tools.py

from datetime import datetime
from typing import Any, Dict  # type: ignore


def get_today_date() -> str:
    """
    Returns the today date in YYYY-MMM-DD format.
    """
    return datetime.now().strftime("%Y-%b-%d")


GET_TODAY_DATE_SCHEMA = {  # type: ignore
    "type": "function",
    "function": {
        "name": "get_today_date",
        "description": "Get the current date. Useful for resolving relative date queries like 'shipped today', 'arriving tomorrow', 'next Friday', etc.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
}
