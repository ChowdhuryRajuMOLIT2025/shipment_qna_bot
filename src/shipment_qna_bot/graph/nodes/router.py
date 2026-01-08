from typing import Literal

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger


def route_node(state: GraphState) -> Literal["retrieval", "analytics", "end"]:
    """
    Decides the next path based on intent.
    """
    intent = state.get("intent")

    if intent == "analytics":
        return "analytics"
    elif intent in ["retrieval", "status", "eta", "delay"]:
        # We group status/eta/delay under retrieval for backward compatibility if needed,
        # but LLM now primarily outputs 'retrieval' or 'greeting'.
        return "retrieval"
    elif intent == "greeting":
        return "end"
    else:
        return "end"
