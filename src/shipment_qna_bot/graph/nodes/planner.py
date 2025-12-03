# src/shipment_qna_bot/graph/nodes/planner.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def _ids_only(pairs: List[Tuple[str, float]] | None) -> List[str]:
    if not pairs:
        return []
    return [p[0] for p in pairs if p and p[0]]


# find critix
def _sync_ctx(state: Dict[str, Any]) -> None:
    # set_log_context({"step": "NODE:Planner", "state": _summarize_state(state)})
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )


def planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces:
      state["retrieval_plan"] = {
        query_text, top_k, vector_k, extra_filter, reason
      }
    """
    _sync_ctx(state)

    with log_node_execution(
        "Planner",
        {
            "intent": state.get("intent", "-"),
            "normalized_question": (state.get("normalized_question") or "-")[:120],
            "time_window_days": state.get("time_window_days"),
        },
    ):
        intent = state.get("intent", "generic")
        q = (
            state.get("normalized_question") or state.get("question_raw") or ""
        ).strip()

        containers = _ids_only(state.get("container_numbers"))
        pos = _ids_only(state.get("po_numbers"))
        obls = _ids_only(state.get("obl_numbers"))
        bookings = _ids_only(state.get("booking_numbers"))

        # Build a better-than-naive query text: prioritize identifiers if present.
        # tokens = []
        # if containers:
        #     tokens.append(" ".join(containers))
        # if obls:
        #     tokens.append(" ".join(obls))
        # if pos:
        #     tokens.append(" ".join(pos))
        # if bookings:
        #     tokens.append(" ".join(bookings))

        # Build query text: prioritize identifiers if present
        id_tokens = [*containers, *obls, *pos, *bookings]
        query_text = " ".join(id_tokens).strip() or q

        plan: RetrievalPlan = {
            "query_text": query_text,
            "top_k": 5,
            "vector_k": 30,
            "extra_filter": extra_filter,
            "reason": f"intent={intent}; ids={bool(id_tokens)}",
        }

        state["retrieval_plan"] = plan

        logger.info(
            f"Planned retrieval: query_text='{query_text[:80]}' top_k={plan['top_k']} vector_k={plan['vector_k']} "
            f"extra_filter={'yes' if extra_filter else 'no'} reason={plan['reason']}",
            extra={"step": "NODE:Planner"},
        )

        return state
