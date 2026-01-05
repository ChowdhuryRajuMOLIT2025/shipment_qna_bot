# src/shipment_qna_bot/graph/nodes/retrieve.py

from __future__ import annotations

from typing import Any, Dict, Optional

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient

_SEARCH: Optional[AzureAISearchTool] = None
_EMBED: Optional[AzureOpenAIEmbeddingsClient] = None


def _sync_ctx(state: Dict[str, Any]) -> None:
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )


def _get_search() -> AzureAISearchTool:
    global _SEARCH
    if _SEARCH is None:
        _SEARCH = AzureAISearchTool()
    return _SEARCH


def _get_embedder() -> AzureOpenAIEmbeddingsClient:
    global _EMBED
    if _EMBED is None:
        _EMBED = AzureOpenAIEmbeddingsClient()
    return _EMBED


def retrieve_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch docs from vectorDB for top k, filters, hybrid weights, etc.
    This node assumes `state['consignee_codes']` is already an *effective*,
    authorized scope. It must NEVER receive raw payload values.
    """
    _sync_ctx(state)

    with log_node_execution(
        "Retrieve",
        {
            "intent": state.get("intent", "-"),
            "consignee_codes": state.get("consignee_codes", []),
            "query_text": ((state.get("retrieval_plan") or {}).get("query_text") or "")[
                :120
            ],
        },
    ):
        plan = state.get("retrieval_plan") or {}
        consignee_codes = state.get("consignee_codes") or []
        query_text = (
            plan.get("query_text") or state.get("normalized_question") or ""
        ).strip()
        extra_filter = (plan.get("extra_filter") or "").strip() or None

        # fail closed on missing consigneescope
        if not consignee_codes:
            state.setdefault("errors", []).append(
                "Missing consignee scope; cannot retrieve."
            )
            state["hits"] = []
            return state

        try:
            embedder = _get_embedder()
            vector = embedder.embed_query(query_text)
        except Exception as e:
            # if embeddings fail, use semantic search with BM25-only.
            logger.warning(
                f"Embedding failed; falling back to keyword-only; err={e}",
                extra={"step": "NODE:Retriever"},
            )
            vector = None

        try:
            tool = _get_search()
            search_response = tool.search(
                query_text=query_text or "*",
                consignee_codes=consignee_codes,
                top_k=int(plan.get("top_k", 8)),
                vector=vector,
                vector_k=int(plan.get("vector_k", 30)),
                extra_filter=extra_filter,
                include_total_count=plan.get("include_total_count", False),
                skip=plan.get("skip"),
                order_by=plan.get("order_by"),
            )
            hits = search_response["hits"]
            state["hits"] = hits
            state["idx_analytics"] = {
                "count": search_response.get("count"),
                "facets": search_response.get("facets"),
            }
            logger.info(
                f"Retrieved {len(hits)} hits for query=<{query_text}>",
                extra={"step": "NODE:Retriever"},
            )
        except Exception as e:
            error_msg = str(e)
            if extra_filter and (
                "Invalid expression" in error_msg or "search.in" in error_msg
            ):
                logger.warning(
                    f"Search failed with invalid filter '{extra_filter}'. Retrying without filter.",
                    extra={"step": "NODE:Retriever"},
                )
                try:
                    search_response = tool.search(
                        query_text=query_text or "*",
                        consignee_codes=consignee_codes,
                        top_k=int(plan.get("top_k", 8)),
                        vector=vector,
                        vector_k=int(plan.get("vector_k", 30)),
                        extra_filter=None,  # Retry without the bad filter
                        include_total_count=plan.get("include_total_count", False),
                        skip=plan.get("skip"),
                        order_by=plan.get("order_by"),
                    )
                    hits = search_response["hits"]
                    state["hits"] = hits
                    state["idx_analytics"] = {
                        "count": search_response.get("count"),
                        "facets": search_response.get("facets"),
                    }
                    logger.info(
                        f"Retrieved {len(hits)} hits (fallback) for query=<{query_text}>",
                        extra={"step": "NODE:Retriever"},
                    )
                    return state
                except Exception as retry_e:
                    logger.error(f"Fallback search also failed: {retry_e}")

            state.setdefault("errors", []).append(
                f"Search failed: {type(e).__name__}: {e}"
            )
            state["hits"] = []
            state.setdefault("notices", []).append(
                f"Note: Search encountered a temporary issue ({type(e).__name__}). Some data might be missing."
            )
            logger.exception(
                f"Search failed completely. err={e}",
                extra={"step": "NODE:Retriever"},
            )

        return state
