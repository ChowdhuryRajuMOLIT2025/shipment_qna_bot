import re
from typing import Any, Dict, List

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_chat_tool: AzureOpenAIChatTool | None = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


def extractor_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts entities (Container, PO, OBL, Booking, Dates, Locations) from the normalized question.
    """
    text = state.get("normalized_question") or state.get("question_raw") or ""

    def _extract_time_window_days(raw: str) -> int | None:
        lowered = raw.lower()
        match = re.search(r"\b(?:next|in)\s+(\d+)\s+days?\b", lowered)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        if re.search(r"\bnext\s+week\b", lowered):
            return 7
        if re.search(r"\bnext\s+month\b", lowered):
            return 30
        return None

    # 1. Regex handles high-confidence ID formats
    # Container: 4 letters + 7 digits
    container_pattern = r"\b[a-zA-Z]{4}\d{7}\b"
    # PO: Alphanumeric, but usually has at least some numbers or specific separators.
    # Narrowing to avoid matching common 5-15 char words like 'WHERE'.
    # Typically POs have a mix of letters and numbers or start with specific prefixes.
    po_pattern = r"\b(?:PO\s*|#)?([a-zA-Z0-9]*\d+[a-zA-Z0-9]*)\b"
    # OBL: Usually carrier code (4 chars) + alphanumeric string.
    obl_pattern = r"\b(?:MAEU|MSCU|SGPV|KKFU|COSU)[a-zA-Z0-9]{8,15}\b"
    # Booking: Often similar to OBL but sometimes just 7+ digits.
    booking_pattern = r"\b(?:[a-zA-Z]{2,4}\d{7,10})\b"

    containers = [c.upper() for c in re.findall(container_pattern, text)]
    pos = [
        p.upper() for p in re.findall(po_pattern, text, re.IGNORECASE) if len(p) >= 5
    ]
    obls = [o.upper() for o in re.findall(obl_pattern, text, re.IGNORECASE)]
    bookings = [b.upper() for b in re.findall(booking_pattern, text, re.IGNORECASE)]

    # 2. LLM handles ambiguous entities (Locations, Dates, Carriers, and validating regex results)
    system_prompt = """
    Extract logistics entities from the user's question. 
    Return a JSON object with keys: 
    - "container_number": list of container IDs (e.g. SEGU5935510)
    - "po_numbers": list of PO numbers (e.g. 5302997239)
    - "booking_numbers": list of booking numbers (e.g. TH2017996)
    - "obl_nos": list of Ocean Bill of Lading numbers
    - "location": list of cities or ports (e.g. "Los Angeles", "CNSHA")
    - "carrier": list of shipping lines (e.g. "Maersk", "MSC")
    - "date_range": e.g. "Oct", "November 25", "December"
    - "status_keywords": e.g. "on water", "delivered", "delayed", "hot"

    If nothing is found for a key, return an empty list.
    """.strip()

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    llm_extracted = {}
    usage_metadata = state.get("usage_metadata") or {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    try:
        chat = _get_chat_tool()
        import json

        response = chat.chat_completion(messages, temperature=0.0)
        res = response["content"]
        usage = response["usage"]

        # Accumulate usage
        for k in usage:
            usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]

        # Find JSON block in response
        json_match = re.search(r"\{.*\}", res, re.DOTALL)
        if json_match:
            llm_extracted = json.loads(json_match.group(0))
    except Exception as e:
        logger.warning(f"LLM Extraction failed: {e}. Falling back to regex.")

    # Merge results
    # Merge results and normalize to UPPERCASE for ID fields
    merged = {
        "container_number": list(
            set(
                [
                    x.upper()
                    for x in (
                        containers + (llm_extracted.get("container_number") or [])
                    )
                ]
            )
        ),
        "po_numbers": list(
            set([x.upper() for x in (pos + (llm_extracted.get("po_numbers") or []))])
        ),
        "booking_numbers": list(
            set(
                [
                    x.upper()
                    for x in (bookings + (llm_extracted.get("booking_numbers") or []))
                ]
            )
        ),
        "obl_nos": list(
            set([x.upper() for x in (obls + (llm_extracted.get("obl_nos") or []))])
        ),
        "location": llm_extracted.get("location") or [],
        "carrier": llm_extracted.get("carrier") or [],
        "date_range": llm_extracted.get("date_range") or [],
        "status_keywords": llm_extracted.get("status_keywords") or [],
    }
    time_window_days = _extract_time_window_days(text)

    count = sum(len(v) if isinstance(v, list) else 1 for v in merged.values() if v)
    logger.info(
        f"Extracted {count} entities", extra={"extra_data": {"extracted": merged}}
    )

    return {
        "extracted_ids": merged,
        "time_window_days": time_window_days,
        "usage_metadata": usage_metadata,
    }
