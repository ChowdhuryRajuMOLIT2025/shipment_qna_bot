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
    logger.info(f"Node execution started: {node_name}", extra={"extra_data": context})
    try:
        yield
        logger.info(f"Node execution completed: {node_name}")
    except Exception as e:
        logger.error(f"Node execution failed: {node_name} - {e}", exc_info=True)
        raise
