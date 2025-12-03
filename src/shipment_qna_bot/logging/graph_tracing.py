# src/shipment_qna_bot/logging/graph_tracing.py

import time
from contextlib import contextmanager
from typing import Any, Dict

from shipment_qna_bot.logging.logger import logger

# from .logger import logger


# summarized current state
def _summarize_state(state: Dict[str, Any]) -> str:
    """
    Light-weight summary of state for logs.
    """
    # keys adjustment based on dataclass of state
    keys_of_interest = [
        "intent",
        "normalized_question",
        "consignee_codes",
    ]
    parts = []
    for key in keys_of_interest:
        if key in state and state[key]:
            parts.append(f"{key}={state[key]}")  # type: ignore
    return ", ".join(parts) if parts else "<no-key-state>"  # type: ignore


@contextmanager  # type: ignore
def log_node_execution(node_name: str, state_snapshot: Dict[str, Any]) -> None:  # type: ignore
    """
    Context manager to wrap each LangGaraph node
    Usage:
    - with log_node_execution("RetrievalPlanner", state.to_dict()):
    ...node logic ...
    """
    step = f"NODE:{node_name}"
    summary = _summarize_state(state_snapshot)

    logger.info(f"Entering node with state: {summary}", extra={"step": step})

    start = time.perf_counter()
    try:
        yield  # type: ignore
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.error(
            f"Node raised exeception after {duration_ms: .1f} ms",
            extra={"step": step},
            exc_info=True,
        )
        raise
    else:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"Exiting node after {duration_ms: .1f} ms", extra={"step": step})


# usage inside a node implementation:
#####################################

# src/shipment_qna_bot/graph/nodes/retrieval_planner.py

# from typing import Dict, Any
# from shipment_qna_bot.logging.graph_tracing import log_node_execution
# from shipment_qna_bot.logging.logger import logger


# def retrieval_planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
#     with log_node_execution("RetrievalPlanner", state):
#         # ... your planner logic ...
#         logger.debug(
#             "Planner decided k=8 with strong container filter",
#             extra={"step": "NODE:RetrievalPlanner"},
#         )
#         return state  # updated
