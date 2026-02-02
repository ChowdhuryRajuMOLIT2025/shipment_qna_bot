from typing import Any, Dict

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def analytics_planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Plans retrieval for analytics queries (e.g., 'how many...', 'status breakdown').
    Sets up a plan to fetch counts and facets instead of just documents.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "AnalyticsPlanner",
        {"intent": state.get("intent", "-")},
    ):
        q = (
            state.get("normalized_question") or state.get("question_raw") or ""
        ).strip()

        # Simple heuristic: if asking for "how many", we just want count.
        # If asking for "status" or "delayed", we might want facets.
        # For now, we'll request generic facets useful for analytics.

        plan: RetrievalPlan = {
            "query_text": "*",  # usually analytics over all or filtered set
            "top_k": 0,  # we care about aggregates, not individual hits usually
            "vector_k": 10,
            "extra_filter": None,  # could add filter based on entities if needed
            "include_total_count": True,
            "reason": "analytics intent",
        }

        # If question contains specific status keywords, maybe filter?
        # For Phase 5 demo, we will just return global counts/facets for the consignee.

        state["retrieval_plan"] = plan

        logger.info(
            f"Planned analytics: query='*' count=True",
            extra={"step": "NODE:AnalyticsPlanner"},
        )

        return state
