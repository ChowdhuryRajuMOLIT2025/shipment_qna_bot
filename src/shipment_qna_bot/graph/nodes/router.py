from typing import Literal

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger


def route_node(
    state: GraphState,
) -> Literal["retrieval", "analytics", "static_info", "clarification", "end"]:
    """
    Decides the next path based on intent.
    """
    intent = state.get("intent")

    if intent == "company_overview":
        return "static_info"
    if intent == "analytics" and (
        state.get("topic_shift_candidate") or state.get("analytics_scope_candidate")
    ):
        return "clarification"
    if intent == "analytics":
        return "analytics"
    if intent == "clarification":
        return "clarification"
    elif intent in ["retrieval", "status", "eta", "delay"]:
        # We group status/eta/delay under retrieval for backward compatibility if needed,
        # but LLM now primarily outputs 'retrieval' or 'greeting'.
        return "retrieval"
    elif intent == "greeting":
        return "end"
    else:
        return "end"
