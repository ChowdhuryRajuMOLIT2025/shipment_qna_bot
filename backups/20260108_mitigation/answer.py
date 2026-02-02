import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, cast

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import get_today_date

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

        # Context construction
        context_str = ""

        # 1. Add Analytics Context
        if analytics:
            count = analytics.get("count")
            facets = analytics.get("facets")
            context_str += f"--- Analytics Data ---\nTotal Matches: {count}\n"
            if facets:
                context_str += f"Facets: {facets}\n"

        # 2. Add Documents Context
        if hits:
            for i, hit in enumerate(hits[:10]):
                context_str += f"\n--- Document {i+1} ---\n"

                # Prioritize key fields
                priority_fields = [
                    "container_number",
                    "shipment_status",
                    "po_numbers",
                    "obl_nos",
                    "discharge_port",
                    "eta_dp_date",
                    "ata_dp_date",
                    "optimal_ata_dp_date",
                    "eta_fd_date",
                    "optimal_eta_fd_date",
                    "delayed_dp",
                    "dp_delayed_dur",
                    "delayed_fd",
                    "fd_delayed_dur",
                    "empty_container_return_date",
                ]
                for f in priority_fields:
                    if f in hit:
                        context_str += f"{f}: {hit[f]}\n"

                # Add metadata_json content intelligently
                if "metadata_json" in hit:
                    try:
                        m = json.loads(str(hit["metadata_json"]))
                        # Extract milestones if present
                        if "milestones" in m:
                            context_str += (
                                f"Milestones: {json.dumps(m['milestones'])}\n"
                            )
                        # Add other relevant bits, avoiding huge chunks
                        for k, v in m.items():
                            if (
                                k not in priority_fields
                                and k != "milestones"
                                and k
                                not in [
                                    "consignee_code_ids",
                                    "consignee_codes",
                                    "id",
                                ]  # Filter sensitive fields
                                and len(str(v)) < 200
                            ):
                                context_str += f"{k}: {v}\n"
                    except:
                        pass

        # Pagination Hint
        pagination_hint = ""
        if hits and len(hits) == 10:  # Assuming default top_k=10
            pagination_hint = "There are more results. Ask 'next 10' to see more."
            context_str += f"\nNOTE: {pagination_hint}\n"

        # 3. Add Current Date Context
        today_str = state.get("today_date") or get_today_date()
        context_str += (
            f"\n--- System Information ---\nCurrent Date (UTC): {today_str}\n"
        )

        # If no info at all
        if not hits and not analytics:
            state["answer_text"] = (
                "I couldn't find any information matching your request within your authorized scope."
            )
            return state

        # Prompt Construction
        system_prompt = f"""
Role:
You are an expert logistics analyst assistant. 

Goal:
Analyze the provided shipment data to answer user questions accurately.

Logistics Concepts:
- Status vs Milestone: "Current Status" is often the 'shipment_status' field.
- Hot PO/Container: Indicated by 'hot_container_flag' being true.
- ETA DP: Estimated Time of Arrival at Discharge Port.
- ATA DP: Actual Time of Arrival at Discharge Port (use 'ata_dp_date' field).
- ETA FD: Estimated Time of Arrival at Final Destination (use 'eta_fd_date' field).
- Delay DP/FD: Use dp_delayed_dur and fd_delayed_dur when present.

Result Guidelines:
1. DATA PRESENTATION (STRICT):
   - If multiple shipments are found, ALWAYS present them in a Markdown Table.
   - TABLE COLUMNS: | Container | PO Numbers | Discharge Port | Arrival Date (ETA/ATA) |
   - ARRIVAL DATE: Use 'ata_dp_date' if the shipment has arrived, otherwise 'eta_dp_date'. Use 'dd-mmm-yy' format.
   - DATE FORMAT: Use dd-mmm-yy (e.g., 20-Oct-25).
   - SORTING: The data is provided in descending order of arrival. Maintain this order.
   - HIDE: Do not show 'document_id' in any part of the answer.

2. GROUNDING (CRITICAL):
   - Use ONLY the provided context to answer. 
   - DO NOT include containers, POs, or details NOT present in the context.
   - If the user asks for more than what is visible, refer them to the total match count or suggest clicking "Show more".
   - DO NOT speculate or hallucinate.

3. SUMMARY:
   - Provide a brief summary of how many hot containers were found and any specific filters applied (e.g., "3 days", "Rotterdam").
   - For status questions, include both DP delay and FD delay when available.

4. PAGINATION:
   - If there are more results, include the hint: {pagination_hint}

Output Format:
a. Direct Answer / Summary
b. Data Table (if applicable)
c. Pagination Button (if applicable)
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
                if _mentions_final_destination(question):
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

        # Build message history for OpenAI
        llm_messages = [{"role": "system", "content": system_prompt}]

        # Add history
        # We want to include previous turns, but correctly handle the current turn's context
        history = cast(List[Any], state.get("messages") or [])

        # If this is a retry, the last message in history is the previous (unsatisfactory) AIMessage
        # The one before it is the current HumanMessage
        for msg in history:
            if isinstance(msg, HumanMessage) and msg.content == question:
                # This is the current question, we'll add it later with context
                continue

            role = "user" if getattr(msg, "type", "") == "human" else "assistant"
            llm_messages.append({"role": role, "content": str(msg.content)})

        # Add current user prompt with context
        llm_messages.append({"role": "user", "content": user_prompt})

        try:
            chat_tool = _get_chat_tool()
            response = chat_tool.chat_completion(llm_messages)
            response_text = response["content"]
            usage = response["usage"]

            # Accumulate usage
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
                lines = [
                    "| Container | PO Numbers | Discharge Port | Arrival Date (ETA/ATA) |",
                    "|---|---|---|---|",
                ]
                for h in rows:
                    container = h.get("container_number") or "-"
                    po_numbers = h.get("po_numbers") or "-"
                    if isinstance(po_numbers, list):
                        po_numbers = ", ".join(map(str, po_numbers))
                    discharge_port = h.get("discharge_port") or "-"
                    arrival = _fmt_date(h.get("ata_dp_date") or h.get("eta_dp_date"))
                    lines.append(
                        f"| {container} | {po_numbers} | {discharge_port} | {arrival} |"
                    )
                return "\n".join(lines)

            state["answer_text"] = response_text

            # --- Structured Table Construction ---
            if hits and len(hits) > 1 and not state.get("table_spec"):
                cols = [
                    "container_number",
                    "shipment_status",
                    "po_numbers",
                    "booking_numbers",
                    "eta_dp_date",
                ]
                rows: List[Dict[str, Any]] = []
                for h in hits:
                    row = {}
                    for c in cols:
                        val = h.get(c)
                        if isinstance(val, list):
                            val = ", ".join(map(str, val))
                        row[c] = val
                    rows.append(row)

                state["table_spec"] = {
                    "columns": cols,
                    "rows": rows,
                    "title": "Shipment List",
                }

                if "|" not in response_text:
                    response_text = (
                        response_text.rstrip() + "\n\n" + _build_table(hits[:10])
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
                            ]
                            if h.get(k) is not None
                        ],
                    }
                )
            state["citations"] = citations

            # In LangGraph with add_messages, we return the NEW message to be appended.
            # If we already have history, we might want to avoid bloating it with failed attempts?
            # For now, just append the new one.
            state["messages"] = [AIMessage(content=response_text)]

            logger.info(
                f"Generated answer: {response_text[:100]}...",
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
