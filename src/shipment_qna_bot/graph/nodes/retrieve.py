# src/shipment_qna_bot/graph/nodes/retrieve.py

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from shipment_qna_bot.graph.state import RetrievalPlan  # type: ignore
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient
from shipment_qna_bot.tools.news_tool import NewsTool
from shipment_qna_bot.tools.weather_tool import WeatherTool
from shipment_qna_bot.utils.config import is_news_enabled, is_weather_enabled
from shipment_qna_bot.utils.runtime import is_test_mode

_SEARCH: Optional[AzureAISearchTool] = None
_EMBED: Optional[AzureOpenAIEmbeddingsClient] = None
_WEATHER: Optional[WeatherTool] = None
_NEWS: Optional[NewsTool] = None

_FILTER_FIELDS = {
    "container_number",
    "po_numbers",
    "booking_numbers",
    "obl_nos",
    "shipment_status",
    "hot_container_flag",
    "container_type",
    "destination_service",
    "load_port",
    "final_load_port",
    "discharge_port",
    "last_cy_location",
    "place_of_receipt",
    "place_of_delivery",
    "final_destination",
    "first_vessel_name",
    "final_carrier_name",
    "final_vessel_name",
    "true_carrier_scac_name",
    "etd_lp_date",
    "etd_flp_date",
    "eta_dp_date",
    "eta_fd_date",
    "revised_eta",
    "atd_lp_date",
    "ata_flp_date",
    "atd_flp_date",
    "ata_dp_date",
    "supplier_vendor_name",
    "manufacturer_name",
    "ship_to_party_name",
    "job_type",
    "mcs_hbl",
    "transport_mode",
    "dp_delayed_dur",
    "fd_delayed_dur",
    "delayed_dp",
    "delayed_fd",
}


def _is_filter_safe(filter_str: str) -> bool:
    if not filter_str:
        return True
    # Remove quoted strings to avoid false token hits.
    scrubbed = re.sub(r"'[^']*'", "''", filter_str)
    tokens = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", scrubbed)
    if not tokens:
        return True

    keywords = {
        "and",
        "or",
        "eq",
        "ne",
        "gt",
        "ge",
        "lt",
        "le",
        "any",
        "all",
        "in",
        "true",
        "false",
        "null",
        "contains",
        "search",
    }
    for t in tokens:
        if t in keywords:
            continue
        if len(t) == 1:  # lambda variables like p, b, o
            continue
        if t not in _FILTER_FIELDS:
            return False
    return True


def _normalize_filter_fields(filter_str: str) -> str:
    """
    Rewrite legacy/non-indexed date field aliases in OData filters so they remain valid
    against the Azure Search index schema.
    """
    if not filter_str:
        return filter_str
    normalized = filter_str
    field_aliases = {
        "optimal_ata_dp_date": "ata_dp_date",
        "derived_ata_dp_date": "ata_dp_date",
        "optimal_eta_fd_date": "eta_fd_date",
    }
    for source_field, target_field in field_aliases.items():
        normalized = re.sub(rf"\b{re.escape(source_field)}\b", target_field, normalized)
    return normalized


def _sync_ctx(state: Dict[str, Any]) -> None:
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )


def _get_search() -> AzureAISearchTool:
    global _SEARCH
    if _SEARCH is None:
        _SEARCH = AzureAISearchTool()  # type: ignore
    return _SEARCH


def _get_embedder() -> AzureOpenAIEmbeddingsClient:
    global _EMBED
    if _EMBED is None:
        _EMBED = AzureOpenAIEmbeddingsClient()  # type: ignore
    return _EMBED


def _get_weather_tool() -> WeatherTool:
    global _WEATHER
    if _WEATHER is None:
        _WEATHER = WeatherTool()
    return _WEATHER


def _fetch_weather_alerts(hits: list[Dict[str, Any]], state: Dict[str, Any]) -> None:
    """
    Fetches weather for unique locations in hits and adds to state['notices'].
    """
    if not is_weather_enabled() or "weather" not in (state.get("sub_intents") or []):
        return

    locations = set()
    for h in hits[:10]:
        for field in ["discharge_port", "final_destination", "load_port"]:
            loc = h.get(field)
            if loc and isinstance(loc, str) and len(loc) > 2:
                locations.add(loc.strip().upper())

    if not locations:
        return

    weather_tool = _get_weather_tool()
    success_count = 0
    failure_count = 0
    for loc in sorted(list(locations))[:6]:
        res = weather_tool.get_impact_for_location(loc, forecast_days=3)
        for notice in weather_tool.consume_transport_notices():
            if notice not in state.setdefault("notices", []):
                state["notices"].append(notice)
        if res:
            msg = (
                f"Weather Outlook for {res['location']} ({res.get('country', '')}): "
                f"{res.get('summary', 'No summary available.')}"
            )
            state.setdefault("notices", []).append(msg)
            logger.info(f"Added weather notice for {loc}")
            success_count += 1
        else:
            failure_count += 1

    if failure_count and not success_count:
        state.setdefault("notices", []).append(
            "Live weather enrichment could not retrieve data for the matched shipment locations."
        )


def _get_news_tool() -> NewsTool:
    global _NEWS
    if _NEWS is None:
        _NEWS = NewsTool()
    return _NEWS


def _fetch_news_impact(hits: list[Dict[str, Any]], state: Dict[str, Any]) -> None:
    """
    Fetches logistics news for unique carriers and ports in hits.
    """
    if not is_news_enabled() or "news" not in (state.get("sub_intents") or []):
        return

    keywords = set()
    for h in hits[:5]:
        # Extract meaningful keywords for news search
        for field in ["discharge_port", "true_carrier_scac_name", "final_carrier_name"]:
            val = h.get(field)
            if val and isinstance(val, str) and len(val) > 3:
                # Clean up carrier names for better search
                clean_val = re.sub(
                    r"\b(Inc|Ltd|Corp|Co|Shipping|Line)\b", "", val, flags=re.I
                ).strip()
                if clean_val:
                    keywords.add(clean_val)

    if not keywords:
        return

    news_tool = _get_news_tool()
    # Limit to top 3 keywords to avoid too much noise
    search_terms = sorted(list(keywords))[:3]
    articles = news_tool.fetch_news(search_terms, limit=3)

    for art in articles:
        msg = f"News Impact ({art['source']}): {art['title']} - Potential impact on shipments involving {', '.join(search_terms)}."
        state.setdefault("notices", []).append(msg)
        logger.info(f"Added news notice from {art['source']}")


def retrieve_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch docs from vectorDB for top k, filters, hybrid weights, etc.
    This node assumes `state['consignee_codes']` is already an *effective*,
    authorized scope. It must NEVER receive raw payload values.
    """
    _sync_ctx(state)

    if state.get("intent") == "analytics":
        logger.info(
            "Skipping Azure Search because intent=analytics",
            extra={"step": "NODE:Retriever"},
        )
        state["hits"] = []
        state["idx_analytics"] = {"count": 0, "facets": None}
        return state

    with log_node_execution(
        "Retrieve",
        {
            "intent": state.get("intent", "-"),
            "consignee_codes": state.get("consignee_codes", []),
            "query_text": ((state.get("retrieval_plan") or {}).get("query_text") or "")[  # type: ignore
                :120
            ],
        },
        state_ref=state,
    ):
        plan = state.get("retrieval_plan") or {}  # type: ignore
        consignee_codes = state.get("consignee_codes") or []  # type: ignore
        query_text = (  # type: ignore
            plan.get("query_text") or state.get("normalized_question") or ""  # type: ignore
        ).strip()  # type: ignore
        extra_filter = (plan.get("extra_filter") or "").strip() or None  # type: ignore
        if extra_filter:
            normalized_filter = _normalize_filter_fields(extra_filter)
            if normalized_filter != extra_filter:
                logger.info(
                    f"Normalized filter fields: {extra_filter} -> {normalized_filter}",
                    extra={"step": "NODE:Retriever"},
                )
                extra_filter = normalized_filter
        if extra_filter and not _is_filter_safe(extra_filter):  # type: ignore
            logger.warning(
                f"Dropping unsafe filter: {extra_filter}",
                extra={"step": "NODE:Retriever"},
            )
            extra_filter = None

        # fail closed on missing consigneescope
        if not consignee_codes:
            state.setdefault("errors", []).append(
                "Missing consignee scope; cannot retrieve."
            )
            state["hits"] = []
            return state

        if is_test_mode():
            state["hits"] = []
            state["idx_analytics"] = {"count": 0, "facets": None}
            return state

        try:
            embedder = _get_embedder()
            vector = embedder.embed_query(query_text)  # type: ignore
        except Exception as e:
            # if embeddings fail, use semantic search with BM25-only.
            logger.warning(
                f"Embedding failed; falling back to keyword-only; err={e}",
                extra={"step": "NODE:Retriever"},
            )
            vector = None

        def _parse_dt(val: Any) -> Optional[datetime]:
            if not val or val == "NaT":
                return None
            try:
                s = str(val).replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return None

        def _get_now_utc() -> datetime:
            raw = state.get("now_utc")
            if raw:
                try:
                    s = str(raw).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except Exception:
                    pass
            return datetime.now(timezone.utc)

        now_utc = _get_now_utc()
        start_of_today = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        def _load_metadata(hit: Dict[str, Any]) -> Dict[str, Any]:
            raw = hit.get("metadata_json")
            if isinstance(raw, dict):
                return raw  # type: ignore
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except Exception:
                    return {}
            return {}

        def _hydrate_hit(hit: Dict[str, Any]) -> None:
            meta = _load_metadata(hit)
            for key in [
                "derived_ata_dp_date",
                "optimal_eta_fd_date",
                "dp_delayed_dur",
                "fd_delayed_dur",
                "delayed_dp",
                "delayed_fd",
                "empty_container_return_date",
                "eta_dp_date",
                "eta_fd_date",
                "ata_dp_date",
                "optimal_ata_dp_date",
            ]:
                if key not in hit and key in meta:
                    hit[key] = meta.get(key)

        def _post_filter_hits(
            hits: list[Dict[str, Any]], post_filter: Dict[str, Any]
        ) -> list[Dict[str, Any]]:
            if not post_filter:
                return hits
            filtered = []
            for h in hits:
                _hydrate_hit(h)
                meta = _load_metadata(h)

                def _get_field(name: str) -> Any:
                    if name in h:
                        return h.get(name)
                    return meta.get(name)

                ok = True
                date_window = post_filter.get("date_window")
                if date_window:
                    fields = date_window.get("fields")
                    if isinstance(fields, str):
                        fields = [fields]
                    if not fields:
                        legacy_field = date_window.get("field")
                        fields = [legacy_field] if legacy_field else []
                    days = int(date_window.get("days") or 0)
                    direction = date_window.get("direction", "next")
                    dt_val: Optional[datetime] = None
                    for field in fields:
                        dt_val = _parse_dt(_get_field(field))
                        if dt_val:
                            break
                    if not dt_val:
                        ok = False
                    elif direction == "next":
                        # Calendar-day window: include today, end at start_of_today + N days (exclusive).
                        window_end = start_of_today + timedelta(days=days)
                        ok = dt_val >= start_of_today and dt_val < window_end

                delay_rule = post_filter.get("delay")
                if ok and delay_rule:
                    field = delay_rule.get("field")
                    op = delay_rule.get("op", ">=")
                    try:
                        val = float(_get_field(field) or 0.0)
                    except Exception:
                        val = 0.0
                    threshold = float(delay_rule.get("days") or 0.0)
                    if op == ">":
                        ok = val > threshold
                    else:
                        ok = val >= threshold

                if ok:
                    filtered.append(h)
            return filtered

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
            for h in hits:
                _hydrate_hit(h)
            post_filter = plan.get("post_filter") or {}
            if post_filter:
                hits = _post_filter_hits(hits, post_filter)
            state["hits"] = hits
            state["idx_analytics"] = {
                "count": (
                    search_response.get("count")
                    if plan.get("include_total_count")
                    else len(hits)
                ),
                "facets": search_response.get("facets"),
            }
            logger.info(
                f"Retrieved {len(hits)} hits for query=<{query_text}>",
                extra={"step": "NODE:Retriever"},
            )

            # Weather Enrichment
            _fetch_weather_alerts(hits, state)

            # News Impact Enrichment
            _fetch_news_impact(hits, state)

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
                    for h in hits:
                        _hydrate_hit(h)
                    post_filter = plan.get("post_filter") or {}
                    if post_filter:
                        hits = _post_filter_hits(hits, post_filter)
                    state["hits"] = hits
                    state["idx_analytics"] = {
                        "count": (
                            search_response.get("count")
                            if plan.get("include_total_count")
                            else len(hits)
                        ),
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
