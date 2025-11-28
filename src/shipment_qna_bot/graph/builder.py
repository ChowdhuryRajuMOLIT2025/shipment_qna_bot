# src/shipment_qna_bot/graph/builder.py

# bind LG with user query -> normalize -> intent -> formatter -> end
from __future__ import annotations

from typing import Any, Dict

from langgraph.graph import END, StateGraph

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def normalize_node(state: GraphState) -> GraphState:
    with log_node_execution("QueryNormalizer", state.to_log_dict()):
        q = (state.question_raw or "").strip()

        # currently keeping it simple for now; expand later with better normalization rules
        state.normalized_question = " ".join(q.split()).lower()

        logger.info(
            f'Normalized question: "{state.normalized_question}"',
            extra={"step": "NODE:QueryNormalizer"},
        )
        return state


def intent_node(state: GraphState) -> GraphState:
    """Minimal rules-based intent classifier.
    - Will replace later with LLM or better rules.
    """
    with log_node_execution("IntentClassifier", state.to_log_dict()):
        q = state.normalized_question or ""

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

        state.intent = intent

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
    with log_node_execution("Formatter", state.to_log_dict()):
        # NOTE: This is just to prove graph wiring + logging. No RAG yet.
        state.answer_text = (
            f"[DEV] Graph is wired up.\n"
            f"-intent: {state.intent}\n"
            f"-normalized_question: {state.normalized_question}\n"
            f"-consignee_codes received: {state.consignee_codes}\n"
        )

        logger.info(
            "Prepared stub response (graph wired, tools not yet integrated).\n"
            f'Basic Answer: "{state.answer_text}"\n',
            extra={"step": "NODE:Formatter"},
        )
        return state


def build_graph():
    """
    Returns a compiled runnable graph.
    State type: GraphState (dataclass)
    """

    graph = StateGraph(
        state_type=GraphState,
        nodes=[
            ("normalize", normalize_node),
            ("intent", intent_node),
            ("formatter", formatter_node),
        ],
    )

    graph.add_node("normalize", normalize_node)
    graph.add_node("intent", intent_node)
    graph.add_node("formatter", formatter_node)

    graph.set_entry_point("normalize")
    graph.add_edge("normalize", "intent")
    graph.add_edge("intent", "formatter")
    graph.add_edge("formatter", END)

    return graph.compile()


def run_graph(payload: Dict[str, Any]) -> GraphState:
    """
    Convenience runner for FastAPI route.
    payload must include:
      - question_raw
      - consignee_codes (list[str])
      - conversation_id
    """
    state = GraphState(
        conversation_id=payload.get("conversation_id", "conv-auto"),
        question_raw=payload.get("question_raw", ""),
        consignee_codes=payload.get("consignee_codes", []),
    )

    # set logging context at graph entry point (route level but double safe)
    set_log_context(
        conversation_id=state.conversation_id,
        # question_raw=state.question_raw,
        consignee_codes=state.consignee_codes,
    )

    app = build_graph()
    result: GraphState = app.invoke(state)
    # return app.run(state)
    return result
