# src/shipment_qna_bot/graph/nodes/planner.py

from __future__ import annotations

from typing import Dict

from shipment_qna_bot.graph.state import GraphState, RetrievalPlan
from shipment_qna_bot.logging_utils import (  # use your existing helpers
    log_node_end, log_node_start)


def planner_node(state: GraphState) -> Dict:
    log_node_start("Planner", state)

    intent = state.get("primary_intent", "generic")
    q = state.get("normalized_question") or state.get("question") or ""

    containers = state.get("container_numbers", [])
    pos = state.get("po_numbers", [])
    obls = state.get("obl_numbers", [])
    bookings = state.get("booking_numbers", [])

    # Build a better-than-naive query text.
    tokens = []
    if containers:
        tokens.append(" ".join(containers))
    if obls:
        tokens.append(" ".join(obls))
    if pos:
        tokens.append(" ".join(pos))
    if bookings:
        tokens.append(" ".join(bookings))

    query_text = " ".join(tokens).strip() or q

    plan: RetrievalPlan = {
        "query_text": query_text,
        "top_k": 5,
        "vector_k": 30,
        "filter": "",  # optional extra filter later (dates, status, etc.)
        "reason": f"intent={intent}; ids={bool(tokens)}",
    }

    log_node_end("Planner", state)
    return {"retrieval_plan": plan}
