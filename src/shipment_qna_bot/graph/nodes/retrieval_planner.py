# src/shipment_qna_bot/graph/nodes/retrieval_planner.py

from typing import Any, Dict

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger


def retrieval_planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch docs from vectorDB for top k, filters, hybrid weights, etc.
    """
    with log_node_execution("RetrievalPlanner", state):
        # TODO: placeholder for fetching/filtering docs condition logic
        state.setdefault("filters", {})
        state.setdefault("hybrid_weights", {"bm": 0.6, "vector": 0.4})
        state.setdefault("k", 5)

        logger.info(
            f"Planner decided k= <{state['k']}> hybrid weights=<{state[hybrid_weights]}>",
            extra={"step", "NODE.RetrievalPlanner"},  # type: ignore
        )

        return state
