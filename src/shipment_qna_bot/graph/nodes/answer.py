import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, cast

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import get_today_date
from shipment_qna_bot.utils.runtime import is_test_mode

_chat_tool: Optional[AzureOpenAIChatTool] = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


def answer_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synthesizes a natural language answer from retrieved documents using LLM.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "Answer",
        {
            "intent": state.get("intent", "-"),
            "hits_count": len(state.get("hits") or []),
        },
    ):
        hits = cast(List[Dict[str, Any]], state.get("hits") or [])
        analytics = cast(Dict[str, Any], state.get("idx_analytics") or {})
        question = state.get("question_raw") or ""
        extracted = cast(Dict[str, Any], state.get("extracted_ids") or {})

        if is_test_mode():
            if not hits and not (analytics and (analytics.get("count") or 0) > 0):
                state["answer_text"] = (
                    "I couldn't find any information matching your request within your authorized scope."
                )
            else:
                state["answer_text"] = f"Found {len(hits)} shipments."
            return state

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

        def _mentions_final_destination(text: str) -> bool:
            lowered = text.lower()
            if "final destination" in lowered or "final_destination" in lowered:
                return True
            if "distribution center" in lowered or "distribution centre" in lowered:
                return True
            if re.search(r"\bin-?dc\b", lowered):
                return True
            if re.search(r"\bfd\b", lowered):
                return True
            return False

        def _wants_bucket_chart(text: str) -> bool:
            lowered = text.lower()
            bucket_words = ["bucket", "breakdown", "group", "chart", "graph"]
            window_words = ["today", "week", "fortnight", "month"]
            return any(w in lowered for w in bucket_words) and any(
                w in lowered for w in window_words
            )

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

        def _bucket_counts(hits_list: List[Dict[str, Any]]) -> Dict[str, Any]:
            now = _get_now_utc()
            windows = [
                ("today", 1),
                ("this_week", 7),
                ("this_fortnight", 14),
                ("this_month", 30),
            ]
            only_hot = "hot" in question.lower() and "normal" not in question.lower()
            only_normal = "normal" in question.lower() and "hot" not in question.lower()
            categories = ["hot", "normal"]
            if only_hot:
                categories = ["hot"]
            elif only_normal:
                categories = ["normal"]

            def _is_hot(hit: Dict[str, Any]) -> bool:
                val = hit.get("hot_container_flag")
                if val is None and isinstance(hit.get("metadata_json"), str):
                    try:
                        meta = json.loads(str(hit["metadata_json"]))
                        val = meta.get("hot_container_flag")
                    except Exception:
                        val = None
                return bool(val)

            def _arrival_dt(hit: Dict[str, Any]) -> Optional[datetime]:
                if _mentions_final_destination(question):
                    return _parse_dt(
                        hit.get("optimal_eta_fd_date") or hit.get("eta_fd_date")
                    )
                return _parse_dt(
                    hit.get("optimal_ata_dp_date")
                    or hit.get("eta_dp_date")
                    or hit.get("ata_dp_date")
                )

            rows: List[Dict[str, Any]] = []
            chart_rows: List[Dict[str, Any]] = []

            for label, days in windows:
                bucket_end = now + timedelta(days=days)
                for category in categories:
                    count = 0
                    for h in hits_list:
                        dt = _arrival_dt(h)
                        if not dt:
                            continue
                        if not (now <= dt < bucket_end):
                            continue
                        is_hot = _is_hot(h)
                        if category == "hot" and not is_hot:
                            continue
                        if category == "normal" and is_hot:
                            continue
                        count += 1
                    chart_rows.append(
                        {"bucket": label, "category": category, "count": count}
                    )

                totals = {"bucket": label}
                if "hot" in categories:
                    totals["hot_count"] = next(
                        r["count"]
                        for r in chart_rows
                        if r["bucket"] == label and r["category"] == "hot"
                    )
                if "normal" in categories:
                    totals["normal_count"] = next(
                        r["count"]
                        for r in chart_rows
                        if r["bucket"] == label and r["category"] == "normal"
                    )
                totals["total_count"] = sum(
                    r["count"] for r in chart_rows if r["bucket"] == label
                )
                rows.append(totals)

            return {"rows": rows, "chart_rows": chart_rows, "categories": categories}

        def _normalize_id_list(val: Any) -> List[str]:
            if val is None:
                return []
            if isinstance(val, list):
                return [str(v).strip().upper() for v in val if str(v).strip()]
            raw = str(val)
            parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
            return parts

        def _hit_has_ids(hit: Dict[str, Any], ids: Dict[str, List[str]]) -> bool:
            if ids.get("container_number"):
                hit_container = str(hit.get("container_number") or "").upper()
                if hit_container and hit_container in ids["container_number"]:
                    return True
            if ids.get("po_numbers"):
                po_list = _normalize_id_list(hit.get("po_numbers"))
                if set(po_list) & set(ids["po_numbers"]):
                    return True
            if ids.get("booking_numbers"):
                bk_list = _normalize_id_list(hit.get("booking_numbers"))
                if set(bk_list) & set(ids["booking_numbers"]):
                    return True
            if ids.get("obl_nos"):
                obl_list = _normalize_id_list(hit.get("obl_nos"))
                if set(obl_list) & set(ids["obl_nos"]):
                    return True
            return False

        requested_ids = {
            "container_number": _normalize_id_list(extracted.get("container_number")),
            "po_numbers": _normalize_id_list(extracted.get("po_numbers")),
            "booking_numbers": _normalize_id_list(extracted.get("booking_numbers")),
            "obl_nos": _normalize_id_list(extracted.get("obl_nos")),
        }

        if state.get("intent") == "retrieval" and any(requested_ids.values()):
            filtered_hits = [h for h in hits if _hit_has_ids(h, requested_ids)]
            if filtered_hits:
                hits = filtered_hits
                state["hits"] = hits

        total_count = len(hits)
        if analytics and analytics.get("count") is not None:
            try:
                total_count = int(analytics.get("count") or total_count)
            except Exception:
                total_count = len(hits)
        display_count = min(len(hits), 10)

        # Context construction
        context_str = ""

        # 1. Add Analytics Context
        if analytics:
            count = analytics.get("count")
            facets = analytics.get("facets")
            context_str += f"--- Analytics Data ---\nTotal Matches in System: {count}\n"
            if facets:
                # Add human-readable facet summaries
                facet_summary = ""
                for field, values in facets.items():
                    facet_summary += (
                        f"{field}: "
                        + ", ".join([f"{v['value']} ({v['count']})" for v in values])
                        + "\n"
                    )
                context_str += f"Status Breakdown: {facet_summary}\n"

        # 2. Add Documents Context
        if hits:
            # Swap columns based on orientation
            is_fd = _mentions_final_destination(question)

            for i, hit in enumerate(hits[:10]):
                context_str += f"\n--- Document {i+1} ---\n"

                # Prioritize key fields based on intent
                priority_fields = [
                    "container_number",
                    "shipment_status",
                    "po_numbers",
                    "booking_numbers",
                    "true_carrier_scac_name",
                    "final_carrier_name",
                    "first_vessel_name",
                    "final_vessel_name",
                ]

                if is_fd:
                    priority_fields.extend(
                        [
                            "final_destination",
                            "eta_fd_date",
                            "optimal_eta_fd_date",
                            "delayed_fd",
                            "fd_delayed_dur",
                        ]
                    )
                else:
                    priority_fields.extend(
                        [
                            "discharge_port",
                            "eta_dp_date",
                            "ata_dp_date",
                            "optimal_ata_dp_date",
                            "delayed_dp",
                            "dp_delayed_dur",
                        ]
                    )

                # Add numeric details for grounding
                priority_fields.extend(
                    [
                        "cargo_weight_kg",
                        "cargo_measure_cubic_meter",
                        "cargo_count",
                        "cargo_detail_count",
                    ]
                )

                priority_fields.append("hot_container_flag")
                priority_fields.append("empty_container_return_date")

                for f in priority_fields:
                    if f in hit:
                        context_str += f"{f}: {hit[f]}\n"

                # Always include full content for grounding if available
                if "content" in hit:
                    context_str += f"Content: {hit['content']}\n"

                # Add metadata_json content intelligently
                if "metadata_json" in hit:
                    try:
                        m = json.loads(str(hit["metadata_json"]))
                        if "milestones" in m:
                            context_str += (
                                f"Milestones: {json.dumps(m['milestones'])}\n"
                            )
                    except:
                        pass

        # Pagination Hint
        pagination_hint = ""
        top_count = analytics.get("count") or 0
        if analytics and top_count > len(hits):
            pagination_hint = f"There are {top_count} total results matching your query. Ask 'show more' or 'next page' to see more."
            context_str += f"\nNOTE: {pagination_hint}\n"

        # 3. Add Current Date Context
        today_str = state.get("today_date") or get_today_date()
        context_str += (
            f"\n--- System Information ---\nCurrent Date (UTC): {today_str}\n"
        )

        # If no info at all
        if not hits and not (analytics and (analytics.get("count") or 0) > 0):
            state["answer_text"] = (
                "I couldn't find any information matching your request within your authorized scope."
            )
            return state

        # Prompt Construction
        is_fd = _mentions_final_destination(question)
        dest_label = "Final Destination" if is_fd else "Discharge Port"
        date_label = "ETA FD" if is_fd else "Arrival Date (ETA/ATA)"

        system_prompt = f"""
Role:
You are an expert logistics analyst assistant. 

Goal:
Analyze the provided shipment data to answer user questions accurately.

Logistics Concepts:
- Status vs Milestone: "Current Status" is often the 'shipment_status' field.
- Hot PO/Container: Indicated by 'hot_container_flag' being true. THESE ARE PRIORITY.
- ETA DP: Estimated Time of Arrival at Discharge Port.
- ATA DP: Actual Time of Arrival at Discharge Port (use 'ata_dp_date' field).
- ETA FD: Estimated Time of Arrival at Final Destination (use 'eta_fd_date' field).
- Delay DP/FD: Use dp_delayed_dur and fd_delayed_dur.

System Instructions:
1. DATA PRESENTATION (STRICT):
   - If multiple shipments are found, ALWAYS present them in a Markdown Table.
   - TABLE COLUMNS: | Container | PO Numbers | {dest_label} | {date_label} | Status |
   - ARRIVAL DATE: Use 'ata_dp_date' if available, otherwise 'eta_dp_date'. Format as 'dd-mmm-yy'.
   - STATUS: Mention if "Delayed" or "Hot" in the status column if applicable.
   - HIDE: Do not show 'document_id' or 'doc_id' in the answer.

2. NUMERIC & LOGISTICS DETAILS (IMPORTANT):
   - Always report Weight (cargo_weight_kg), Volume (cargo_measure_cubic_meter), and counts if requested.
   - Always report Carrier (true_carrier_scac_name or final_carrier_name) and Vessel (first_vessel_name or final_vessel_name) details if requested.
   - Never say "data not available" if these fields have values in the Document sections.

3. ANALYTICS (CRITICAL):
   - Use "Total Matches in System" for the high-level count. 
   - Use "Status Breakdown" (facets) for accurate aggregate numbers.
   - Mention total counts in your summary.

4. GROUNDING:
   - Use ONLY the provided context. Do not speculate.

5. SUMMARY:
   - Briefly summarize key findings (e.g. "5 containers found, 2 are hot/priority").
""".strip()

        if hits and _wants_bucket_chart(question):
            bucket_spec = _bucket_counts(hits)
            if bucket_spec.get("rows"):
                context_str += (
                    "\n--- Analytics Buckets ---\n"
                    + json.dumps(bucket_spec["rows"], indent=2)
                    + "\n"
                )

                chart_title = "Discharge Port Arrival Buckets (Hot vs Normal)"
                if is_fd:
                    chart_title = "Final Destination Arrival Buckets (Hot vs Normal)"

                state["table_spec"] = {
                    "columns": list(bucket_spec["rows"][0].keys()),
                    "rows": bucket_spec["rows"],
                    "title": "Arrival Buckets",
                }
                state["chart_spec"] = {
                    "kind": "bar",
                    "title": chart_title,
                    "data": bucket_spec["chart_rows"],
                    "encodings": {"x": "bucket", "y": "count", "color": "category"},
                }

        user_prompt = (
            f"Context:\n{context_str}\n\n" f"Question: {question}\n\n" "Answer:"
        )

        from langchain_core.messages import AIMessage, HumanMessage

        llm_messages = [{"role": "system", "content": system_prompt}]
        history = cast(List[Any], state.get("messages") or [])

        for msg in history:
            if isinstance(msg, HumanMessage) and msg.content == question:
                continue
            role = "user" if getattr(msg, "type", "") == "human" else "assistant"
            llm_messages.append({"role": role, "content": str(msg.content)})

        llm_messages.append({"role": "user", "content": user_prompt})

        try:
            chat_tool = _get_chat_tool()
            response = chat_tool.chat_completion(llm_messages)
            response_text = response["content"]
            usage = response["usage"]

            usage_metadata = state.get("usage_metadata") or {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            }
            for k in usage:
                usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]
            state["usage_metadata"] = usage_metadata

            if not response_text or response_text.strip() == "":
                response_text = "I processed the data but couldn't generate a summary. Please try rephrasing your question."

            def _fmt_date(val: Optional[str]) -> str:
                if not val:
                    return "-"
                try:
                    s = str(val).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.strftime("%d-%b-%y")
                except Exception:
                    return str(val)

            def _build_table(rows: List[Dict[str, Any]]) -> str:
                is_fd = _mentions_final_destination(question)
                dest_col = "final_destination" if is_fd else "discharge_port"
                date_col = "eta_fd_date" if is_fd else "eta_dp_date"

                header_dest = "Final Destination" if is_fd else "Discharge Port"
                header_date = "ETA FD" if is_fd else "Arrival (ETA/ATA)"

                lines = [
                    f"| Container | PO Numbers | {header_dest} | {header_date} | Status |",
                    "|---|---|---|---|---|",
                ]
                for h in rows:
                    container = h.get("container_number") or "-"
                    po_raw = h.get("po_numbers") or []
                    if isinstance(po_raw, list):
                        # Deduplicate POs
                        po_numbers = ", ".join(sorted(list(set(map(str, po_raw)))))
                    else:
                        po_numbers = str(po_raw)

                    if not po_numbers or po_numbers == "[]":
                        po_numbers = "-"

                    dest_val = h.get(dest_col) or "-"

                    arrival_val = h.get("ata_dp_date") if not is_fd else None
                    if not arrival_val:
                        arrival_val = h.get(date_col)

                    arrival = _fmt_date(arrival_val)

                    status_parts = []
                    if h.get("hot_container_flag"):
                        status_parts.append("🔥 Hot")
                    ship_stat = h.get("shipment_status")
                    if ship_stat:
                        status_parts.append(ship_stat)

                    status_str = " / ".join(status_parts) if status_parts else "-"

                    lines.append(
                        f"| {container} | {po_numbers} | {dest_val} | {arrival} | {status_str} |"
                    )
                return "\n".join(lines)

            def _build_count_prefix() -> Optional[str]:
                po_list = requested_ids.get("po_numbers") or []
                cont_list = requested_ids.get("container_number") or []

                if not po_list and not cont_list:
                    return None

                if cont_list and not po_list:
                    label = "container" if len(cont_list) == 1 else "containers"
                    nums = ", ".join(cont_list)
                else:
                    label = "PO number" if len(po_list) == 1 else "PO numbers"
                    nums = ", ".join(po_list)

                prefix = f"{total_count} shipments found for {label} {nums}."
                if total_count > display_count and display_count > 0:
                    prefix += f" Showing {display_count} of {total_count} below."
                return prefix

            count_prefix = _build_count_prefix()
            if count_prefix:
                response_text = f"{count_prefix}\n\n{response_text}"

            state["answer_text"] = response_text

            # --- Structured Table Construction ---
            if hits and len(hits) > 0 and not state.get("table_spec"):
                is_fd = _mentions_final_destination(question)

                # Deduplicate hits by container_number to avoid multiple rows for same shipment chunks
                unique_hits = []
                seen_containers = set()
                for h in hits:
                    c_num = h.get("container_number") or h.get("document_id")
                    if c_num not in seen_containers:
                        unique_hits.append(h)
                        seen_containers.add(c_num)

                # If a PO/Booking/OBL was requested, keep only matching rows for display.
                if any(requested_ids.values()):
                    filtered_unique = [
                        h for h in unique_hits if _hit_has_ids(h, requested_ids)
                    ]
                    if filtered_unique:
                        unique_hits = filtered_unique

                cols = [
                    "container_number",
                    "po_numbers",
                    "final_destination" if is_fd else "discharge_port",
                    "eta_fd_date" if is_fd else "eta_dp_date",
                    "shipment_status",
                    "final_carrier_name",
                    "final_vessel_name",
                    "hot_container_flag",
                ]
                table_rows: List[Dict[str, Any]] = []
                for h in unique_hits:
                    row = {}
                    for c in cols:
                        val = h.get(c)
                        # Format list types (like po_numbers)
                        if isinstance(val, list):
                            val = ", ".join(sorted(list(set(map(str, val)))))

                        # Format dates specifically for the table spec
                        if c in [
                            "eta_fd_date",
                            "eta_dp_date",
                            "ata_dp_date",
                            "atd_lp_date",
                        ]:
                            val = _fmt_date(val)

                        # Human-readable boolean mapping
                        if c == "hot_container_flag":
                            val = "🔥 PRIORITY" if val else "Normal"

                        row[c] = val
                    table_rows.append(row)

                state["table_spec"] = {
                    "columns": cols,
                    "rows": table_rows,
                    "title": "Shipment List",
                }

                if "|" not in response_text:
                    response_text = (
                        response_text.rstrip() + "\n\n" + _build_table(unique_hits[:10])
                    )
                    state["answer_text"] = response_text

            # Evidence for response model
            citations: List[Dict[str, Any]] = []
            for h in hits[:5]:
                citations.append(
                    {
                        "doc_id": h.get("doc_id") or h.get("document_id"),
                        "container_number": h.get("container_number"),
                        "field_used": [
                            k
                            for k in [
                                "shipment_status",
                                "eta_dp_date",
                                "ata_dp_date",
                                "eta_fd_date",
                                "discharge_port",
                                "hot_container_flag",
                            ]
                            if h.get(k) is not None
                        ],
                    }
                )
            state["citations"] = citations
            state["messages"] = [AIMessage(content=response_text)]

            logger.info(
                f"Generated answer with dynamic table: {response_text[:100]}...",
                extra={"step": "NODE:Answer"},
            )
        except Exception as e:
            logger.error(f"LLM generation failed: {e}", exc_info=True)
            state["answer_text"] = (
                "I found relevant documents but encountered an error generating the summary. "
                "Please check the evidence logs."
            )
            state.setdefault("errors", []).append(f"LLM Error: {e}")

        return state
