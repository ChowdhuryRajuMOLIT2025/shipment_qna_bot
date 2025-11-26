# src/shipment_qna_bot/graph/nodes/intent_classifier.py

from typing import Any, Dict

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def classify_intent_from_question(question: str) -> str:
    """
    Placeholder for finding user intention or sentiment behind the user's chat query.
    Latter will replace it with LLM based rule
    """
    qry: str = question.lower()

    if "eta" in qry or "arriving" in qry or "next" in qry:
        return "eta_window"
    if "delay" in qry or "late" in qry:
        return "delay_reason"
    if "route" in qry or "port" in qry:
        return "route"
    if "co2" in qry or "carbon footprint" in qry:
        return "sustainability"
    if "chart" in qry or "graph" in qry or "plot" in qry:
        return "viz_analytics"
    return "status"


def intent_classifier_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    LangGraph node: classify question intent and update state.
    Also push the intent into logging context.
    """
    with log_node_execution("IntentClassifier", state):
        question = state.get("normalized_question") or state.get("question_raw") or ""
        intent = classify_intent_from_question(question)

        # update state
        state["label"] = intent
        state["needs_structured_logic"] = intent in (
            "eta_window",
            "delay_reason",
            "route",
            "sustainability",
            "viz_analytics",
            "status",
        )

        # push intent into logging context so all upstream logs carry the state
        set_log_context(intent=intent)

        # enlist the current state into log
        logger.info(
            f"Classified intent= <{intent}> for question= <{question}>",
            extra={"step": "NODE: IntentClassifier"},
        )
        return state


#######################
# TODO: as below
"""
Later, when build the LangGraph graph, this node will be plugged in,
and all logs after it will show intent=<value> in custom format.
"""
#######################
