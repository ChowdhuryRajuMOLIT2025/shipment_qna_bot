from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

from shipment_qna_bot.logging.logger import logger


class GraphTracingCallbackHandler(BaseCallbackHandler):
    """
    Callback handler to log LangGraph/LangChain events.
    """

    def on_chain_start(
        self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any
    ) -> Any:
        logger.info(
            "Graph Node/Chain Started",
            extra={"extra_data": {"inputs": str(inputs)[:500]}},
        )

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> Any:
        logger.info(
            "Graph Node/Chain Ended",
            extra={"extra_data": {"outputs": str(outputs)[:500]}},
        )

    def on_chain_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        logger.error(f"Graph Node/Chain Error: {error}", exc_info=True)

    def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> Any:
        logger.info(
            f"Tool Started: {serialized.get('name')}",
            extra={"extra_data": {"input": input_str}},
        )

    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        logger.info("Tool Ended", extra={"extra_data": {"output": output[:500]}})

    def on_tool_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        logger.error(f"Tool Error: {error}", exc_info=True)

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> Any:
        logger.info(
            "LLM Started", extra={"extra_data": {"prompts": [p[:200] for p in prompts]}}
        )

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> Any:
        logger.info(
            "LLM Ended", extra={"extra_data": {"response": str(response)[:500]}}
        )

    def on_llm_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        logger.error(f"LLM Error: {error}", exc_info=True)


import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional


def _truncate(val: Any, limit: int = 160) -> str:
    if val is None:
        return ""
    s = str(val)
    if len(s) <= limit:
        return s
    return s[:limit] + "..."


def _summarize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return {"state_type": str(type(state))}

    idx_analytics = state.get("idx_analytics") or {}
    table_spec = state.get("table_spec") or {}
    chart_spec = state.get("chart_spec") or {}
    topic_shift_candidate = state.get("topic_shift_candidate") or {}
    pending_topic_shift = state.get("pending_topic_shift") or {}

    return {
        "question_raw": _truncate(state.get("question_raw")),
        "normalized_question": _truncate(state.get("normalized_question")),
        "intent": state.get("intent"),
        "sub_intents": state.get("sub_intents"),
        "retry_count": state.get("retry_count"),
        "max_retries": state.get("max_retries"),
        "is_satisfied": state.get("is_satisfied"),
        "messages_count": len(state.get("messages") or []),
        "hits_count": len(state.get("hits") or []),
        "idx_analytics_count": (
            idx_analytics.get("count") if isinstance(idx_analytics, dict) else None
        ),
        "citations_count": len(state.get("citations") or []),
        "errors_count": len(state.get("errors") or []),
        "notices_count": len(state.get("notices") or []),
        "table_rows": (
            len(table_spec.get("rows") or []) if isinstance(table_spec, dict) else None
        ),
        "chart_kind": chart_spec.get("kind") if isinstance(chart_spec, dict) else None,
        "topic_shift_added": (
            topic_shift_candidate.get("added")
            if isinstance(topic_shift_candidate, dict)
            else None
        ),
        "pending_topic_shift": True if pending_topic_shift else False,
        "analytics_attempt_count": state.get("analytics_attempt_count"),
        "analytics_last_error": _truncate(state.get("analytics_last_error"), limit=120),
    }


def _record_node_latency(
    state_ref: Optional[Dict[str, Any]], node_name: str, elapsed_ms: float
) -> None:
    if not isinstance(state_ref, dict):
        return

    node_latency = state_ref.get("node_latency_ms")
    if not isinstance(node_latency, dict):
        node_latency = {}
        state_ref["node_latency_ms"] = node_latency

    stats = node_latency.get(node_name)
    if not isinstance(stats, dict):
        stats = {"count": 0, "total_ms": 0.0, "avg_ms": 0.0, "last_ms": 0.0}

    stats["count"] = int(stats.get("count") or 0) + 1
    stats["total_ms"] = round(float(stats.get("total_ms") or 0.0) + elapsed_ms, 3)
    stats["last_ms"] = round(elapsed_ms, 3)
    stats["avg_ms"] = round(stats["total_ms"] / stats["count"], 3)
    node_latency[node_name] = stats

    latency_trace = state_ref.get("node_latency_trace")
    if not isinstance(latency_trace, list):
        latency_trace = []
        state_ref["node_latency_trace"] = latency_trace
    latency_trace.append({"node": node_name, "elapsed_ms": round(elapsed_ms, 3)})


@contextmanager
def log_node_execution(
    node_name: str,
    context: Optional[Dict[str, Any]] = None,
    state_ref: Optional[Dict[str, Any]] = None,
) -> Generator[None, None, None]:
    """
    Context manager to log the start and end of a graph node execution.
    """
    context = context or {}
    start = time.perf_counter()
    logger.info(f"Node execution started: {node_name}", extra={"extra_data": context})
    try:
        yield
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        _record_node_latency(state_ref, node_name, elapsed_ms)
        logger.info(
            f"Node execution completed: {node_name}",
            extra={
                "extra_data": {
                    "elapsed_ms": round(elapsed_ms, 3),
                    "node": node_name,
                }
            },
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        _record_node_latency(state_ref, node_name, elapsed_ms)
        logger.error(
            f"Node execution failed: {node_name} - {e}",
            extra={
                "extra_data": {
                    "elapsed_ms": round(elapsed_ms, 3),
                    "node": node_name,
                }
            },
            exc_info=True,
        )
        raise
