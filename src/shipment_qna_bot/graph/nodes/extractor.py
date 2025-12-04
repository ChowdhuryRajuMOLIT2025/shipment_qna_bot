# src/shipment_qna_bot/graph/nodes/extractor.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context

# --- Patterns ---
CONTAINER_RE = re.compile(r"\b([A-Z]{4}\d{7, 20})\b", re.IGNORECASE)

# TODO:(We’ll later refine by intent + field-specific patterns.) alphanumeric with optional hyphen/slash, length >= 6
IDISH_RE = re.compile(r"\b([A-Z0-9][A-Z0-9\-\/]{5,})\b", re.IGNORECASE)

# Time window patterns
NEXT_DAYS_RE = re.compile(r"\b(next|within)\s+(\d{1,3})\s*(day|days)\b", re.IGNORECASE)
IN_DAYS_RE = re.compile(r"\b(in)\s+(\d{1,3})\s*(day|days)\b", re.IGNORECASE)


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _rank(items: List[str], confidences: float) -> List[Tuple[str, float]]:
    return [(x, confidences) for x in _dedupe_preserve_order(items)]


def _parse_time_window_days(q: str) -> Tuple[Optional[int], Optional[str]]:
    """Return (days, notice)
    - notice is used when we default window duration to 7 days
    """
    match = NEXT_DAYS_RE.search(q) or IN_DAYS_RE.search(q)
    if match:
        days = int(match.group(2))
        return days, None

    match = IN_DAYS_RE.search(q)
    if match:
        days = int(match.group(2))
        return days, None

    # heuristic phrase matching
    q_low = q.lower()
    phrases = ["arriving soon", "arrive soon", "soon"]
    if any(phrase in q_low for phrase in phrases):
        return 7, "No duration provided; using default window of 7 days."

    return None, None


def _log_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    """Log summary of extracted entities."""
    return {
        "intent": state.get("intent", "-"),
        "normalized_question": state.get("normalized_question", "-")[:51],
        "consignee_codes": state.get("consignee_codes", []),
        "container_numbers": (state.get("container_numbers") or [])[:5],
        "po_numbers": (state.get("po_numbers") or [])[:5],
        "obl_numbers": (state.get("obl_numbers") or [])[:5],
        "booking_numbers": (state.get("booking_numbers") or [])[:5],
        "time_window_days": state.get("time_window_days"),
        "time_window_notice": state.get("time_window_notice"),
    }


def sync_log_context_from_state(state: Dict[str, Any]) -> None:
    """Sync log context from state."""
    # set_log_context(**state)
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        container_numbers=state.get("container_numbers", []),
        intent=state.get("intent"),
        normalized_question=state.get("normalized_question"),
        time_window_days=state.get("time_window_days"),
        time_window_notice=state.get("time_window_notice"),
    )


def extractor_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Populate identifier fields + time window fields from question.
    Extract entities from question from user text and update state.
    Also push the entities into logging context.
    This node should not call external tools
    """
    sync_log_context_from_state(state)

    with log_node_execution("Extractor", _log_summary(state)):
        q = state.get("normalized_question") or state.get("question_raw") or ""
        q_clean = q.strip()
        # entities = extract_entities(q_clean)

        # ---------container number ---------
        container_nums = [m.group(1).upper() for m in CONTAINER_RE.finditer(q_clean)]
        containers = _dedupe_preserve_order(container_nums)
        # ------------id-ish tokens----------
        # we will use this pool for PO/OBL/Booking inference-keep it as candidates for now
        idish_tokens = [m.group(1).upper() for m in IDISH_RE.finditer(q_clean)]
        idish = _dedupe_preserve_order(idish_tokens)

        # remove containers from idish pool so we don't double-count
        idish = [x for x in idish if x not in set(containers)]

        # best-effort: classify by prefix keywords in question tokens into PO/OBL/Booking
        # later we will do proper regex per field
        po_like: List[str] = []
        obl_like: List[str] = []
        booking_like: List[str] = []

        q_low = q_clean.lower()
        for token in idish:
            # simple hints based classification on question text
            if "po" in q_low and token.isdigit() and len(token) >= 6:
                po_like.append(token)
            elif (
                ("obl" in q_low or "bl" in q_low)
                and token.isdigit()
                and len(token) >= 6
            ):
                obl_like.append(token)
            elif "booking" in q_low and token.isdigit() and len(token) >= 6:
                booking_like.append(token)

        # time window
        days, notice = _parse_time_window_days(q_clean)
        if days is not None:
            state["time_window_days"] = days
        if notice:
            state.setdefault("notices", []).append(notice)

        # save extracted entities
        state["container_numbers"] = _rank(containers, 0.95)
        state["po_numbers"] = _rank(po_like, 0.75)
        state["obl_numbers"] = _rank(obl_like, 0.75)
        state["booking_numbers"] = _rank(booking_like, 0.70)

        logger.info(
            f"Extracted: containers={containers[:5]} po={po_like[:5]} obl={obl_like[:5]} booking={booking_like[:5]} "
            f"time_window_days={state.get('time_window_days')}",
            extra={"step": "NODE:Extractor"},
        )

        return state
