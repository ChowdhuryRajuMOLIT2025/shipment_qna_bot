# src/shipment_qna_bot/graph/builder.py

# bind LG with user query -> normalize -> intent -> formatter -> end
from __future__ import annotations

from typing import Any, Dict

from langgraph.graph import END, StateGraph

from shipment_qna_bot.graph.nodes.answer_stub import answer_stub_node
from shipment_qna_bot.graph.nodes.extractor import extractor_node
from shipment_qna_bot.graph.nodes.formatter import \
    formatter_node  # type: ignore
from shipment_qna_bot.graph.nodes.intent_classifier import \
    intent_classifier_node
from shipment_qna_bot.graph.nodes.normalizer import \
    query_normalizer_node  # type: ignore
from shipment_qna_bot.graph.nodes.planner import planner_node
from shipment_qna_bot.graph.nodes.retrieve import retrieve_node
from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def sync_log_context_from_state(state: Dict[str, Any]) -> None:
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )


def _log_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    """Small safe subset of state for logging."""
    return {
        "intent": state.get("intent", "-"),
        "container_numbers": state.get("container_numbers" or []),
        "po_numbers": (state.get("po_numbers") or [])[:3],
        "obl_numbers": (state.get("obl_numbers") or [])[:3],
        "booking_numbers": (state.get("booking_numbers") or [])[:3],
        "time_window_days": state.get("time_window_days"),
        "round": state.get("round", 0),
        "conversation_id": state.get("conversation_id", "-"),
        "normalized_question": state.get("normalized_question", "-"),
        "consignee_codes": state.get("consignee_codes", "-"),
    }


def normalize_node(state: Dict[str, Any]) -> Dict[str, Any]:
    sync_log_context_from_state(state)
    with log_node_execution("QueryNormalizer", _log_summary(state)):
        q = (state.get("question_raw") or "").strip()

        # currently keeping it simple for now; expand later with better normalization rules
        state["normalized_question"] = " ".join(q.split()).lower()

        logger.info(
            f'Normalized question: {state["normalized_question"]}',
            extra={"step": "NODE:QueryNormalizer"},
        )
        return state


def intent_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Minimal rules-based intent classifier.
    - Will replace later with LLM or better rules.
    """
    state["intent"] = "-"
    sync_log_context_from_state(state)
    with log_node_execution("IntentClassifier", _log_summary(state)):
        q = state.get("normalized_question") or ""

        # currently keeping it simple for now; expand later with better rules
        if "chart" in q or "bar chart" in q or "plot" in q or "graph" in q:
            intent = "viz_analytics"
        elif "eta" in q or "arriving" in q or "next " in q:
            intent = "eta_window"
        elif "delay" in q or "late" in q:
            intent = "delay_reason"
        elif "route" in q or "port" in q:
            intent = "route"
        elif "co2" in q or "carbon" in q or "footprint" in q:
            intent = "sustainability"
        else:
            intent = "status"

        state["intent"] = intent

        # Push into logger context so future logs include it
        set_log_context(intent=intent)

        logger.info(
            f'Intent classified as: "{intent}"',
            extra={"step": "NODE:IntentClassifier"},
        )

        return state


def formatter_node(state: GraphState) -> GraphState:
    """
    Minimal formatter that produces a basic raw response.
    Later: Will do citations/evidence mapping.
    """
    sync_log_context_from_state(state)
    with log_node_execution("Formatter", _log_summary(state)):
        # NOTE: This is just to prove graph wiring + logging. No RAG yet.
        if state.get("answer_text") is not None:
            return state
        else:
            state["answer_text"] = (
                f"[DEV] Graph is wired up.\n"
                f"-intent: {state.get('intent')}\n"
                f"-normalized_question: {state.get('normalized_question')}\n"
                f"-consignee_codes received: {state.get('consignee_codes')}\n"
            )

        state.setdefault("notices", []).append("[dev] tools not yet integrated...")
        state.setdefault("evidence", [])

        logger.info(
            "Prepared stub response (graph wired, tools not yet integrated).\n"
            f'Basic Answer: "{state["answer_text"]}"\n',
            extra={"step": "NODE:Formatter"},
        )
        return state


def build_graph():
    """
    Returns a compiled runnable graph.
    State type: GraphState (dataclass)
    """
    try:
        graph = StateGraph(state_schema=GraphState)
    except TypeError:
        graph = StateGraph(state_schema=GraphState)

    graph.add_node("normalize", query_normalizer_node)
    graph.add_node("extract", extractor_node)
    graph.add_node("classify", intent_classifier_node)
    graph.add_node("plan", planner_node)
    graph.add_node("retrieve", retrieve_node)
    graph.add_node("answer", answer_stub_node)
    graph.add_node("format", formatter_node)

    graph.set_entry_point("normalize")
    graph.add_edge("normalize", "extract")
    graph.add_edge("extract", "classify")
    graph.add_edge("classify", "plan")
    graph.add_edge("plan", "retrieve")
    graph.add_edge("retrieve", "answer")
    graph.add_edge("answer", "format")
    graph.add_edge("format", END)

    return graph.compile()


# Cache compiled graph so you don't rebuild it per request
_COMPILED_GRAPH = None


def get_compiled_graph():
    global _COMPILED_GRAPH
    if _COMPILED_GRAPH is None:
        _COMPILED_GRAPH = build_graph()
    return _COMPILED_GRAPH


def run_graph(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience runner for FastAPI route.
    payload must include:
      - question_raw
      - consignee_codes (list[str])
      - conversation_id
    """
    state: Dict[str, Any] = {
        "conversation_id": payload.get("conversation_id", "conv-auto"),
        "question_raw": payload.get("question_raw", ""),
        "consignee_codes": payload.get("consignee_codes", []),
        "round": 0,
        "max_rounds": 2,
    }

    # set logging context at graph entry point (route level but double safe)
    set_log_context(
        conversation_id=state["conversation_id"],
        # question_raw=state.question_raw,
        consignee_codes=state["consignee_codes"],
    )

    app = get_compiled_graph()
    result: Dict[str, Any] = app.invoke(state)
    # return app.run(state)
    return result
