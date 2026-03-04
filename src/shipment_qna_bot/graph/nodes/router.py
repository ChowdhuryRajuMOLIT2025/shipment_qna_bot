from typing import Literal

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger


def route_node(
    state: GraphState,
) -> Literal[
    "retrieval",
    "analytics",
    "weather_impact",
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
    if "weather" in sub_intents:
        logger.info("Routing query to dedicated weather impact path.")
        return "weather_impact"
    if intent == "analytics" and state.get("topic_shift_candidate"):
        return "clarification"
    if intent == "analytics":
        return "analytics"
    if intent in ["retrieval", "status", "eta", "delay"]:
        # We group status/eta/delay under retrieval for backward compatibility if needed,
        # but LLM now primarily outputs 'retrieval' or 'greeting'.
        return "retrieval"
    return "end"
