from typing import Literal

from shipment_qna_bot.graph.state import GraphState


def route_node(
    state: GraphState,
) -> Literal[
    "retrieval",
    "analytics",
    "static_info",
    "clarification",
    "end",
]:
    """
    Decides the next path based on intent.
    """
    intent = state.get("intent")
    sub_intents = state.get("sub_intents") or []

    if intent == "company_overview":
        return "static_info"
    if intent == "clarification":
        return "clarification"
    if intent == "greeting":
        return "end"
    if intent == "end":
        return "end"
    if intent == "analytics" and (
        state.get("topic_shift_candidate") or state.get("analytics_scope_candidate")
    ):
        return "clarification"
    if intent == "analytics":
        return "analytics"
    if intent in ["retrieval", "status", "eta", "delay"]:
        # We group status/eta/delay under retrieval for backward compatibility if needed,
        # but LLM now primarily outputs 'retrieval' or 'greeting'.
        return "retrieval"
    return "end"
