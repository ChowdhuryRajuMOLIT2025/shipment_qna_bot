import json
import re
from typing import Any, Dict, List, Optional

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_chat_tool: AzureOpenAIChatTool | None = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


def planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a RetrievalPlan (query text, top_k, filters) using LLM and extracted metadata.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "Planner",
        {
            "intent": state.get("intent", "-"),
            "question": (state.get("normalized_question") or "-")[:120],
        },
    ):
        q = (
            state.get("normalized_question") or state.get("question_raw") or ""
        ).strip()
        extracted = state.get("extracted_ids") or {}
        time_window_days = state.get("time_window_days")

        # Logistics mapping and synonym dictionary for the LLM
        logistics_context = """
        Field Mappings in Index:
        - container_number (String): e.g. SEGU5935510. Use: container_number eq '...' or contains(container_number, '...')
        - po_numbers (Collection): e.g. 5302997239. Use: po_numbers/any(p: p eq '...')
        - booking_numbers (Collection): e.g. TH2017996. Use: booking_numbers/any(b: b eq '...')
        - obl_nos (Collection): e.g. OBL123. Use: obl_nos/any(o: o eq '...')
        - shipment_status (String): DELIVERED, IN_OCEAN, AT_DISCHARGE_PORT, READY_FOR_PICKUP, EMPTY_RETURNED. Use: shipment_status eq '...'
        - hot_container_flag (Boolean): true/false. Use: hot_container_flag eq true
        - discharge_port (String): e.g. "Los Angeles". Use: contains(discharge_port, '...')
        - load_port (String): e.g. "Shanghai". Use: contains(load_port, '...')
        - final_destination (String): e.g. "Dallas". Use: contains(final_destination, '...')
        - first_vessel_name (String): e.g. "MAERSK SERANGOON". Use: contains(first_vessel_name, '...')
        - final_vessel_name (String): e.g. "BASLE EXPRESS". Use: contains(final_vessel_name, '...')
        - optimal_ata_dp_date (DateTime): Use for discharge-port arrival windows.
        - optimal_eta_fd_date (DateTime): Use for final-destination arrival windows.
        - dp_delayed_dur (Float): Delay in days at discharge port.
        - fd_delayed_dur (Float): Delay in days at final destination.
        - delayed_dp / delayed_fd (String): "on_time" or "delay".

        Synonyms & OData Tips:
        - "on water", "sailing" -> shipment_status eq 'IN_OCEAN'
        - "hot" -> hot_container_flag eq true
        - If the user mentions final destination (FD), in-dc, or distribution center, use final_destination.
        - Otherwise, use discharge_port for "arriving at <location>".
        - For ID Collections (PO, Booking, OBL), ALWAYS use 'any(p: p eq '...')' syntax.
        - For descriptive fields (Port, Vessel), 'contains(field, '...')' is more flexible than 'eq'.
    """.strip()

        system_prompt = f"""
        You are a Search Planner for a logistics bot. Given a user question and extracted entities, generate an Azure Search Plan.
        
        {logistics_context}

        Output JSON only:
        {{
            "query_text": "text for hybrid search",
            "top_k": number (default 20, max 100),
            "extra_filter": "OData filter string or null",
            "reason": "short explanation"
        }}
        """

        reflection_feedback = state.get("reflection_feedback")
        retry_count = state.get("retry_count", 0)

        user_content = f"Question: {q}\nExtracted Entities: {json.dumps(extracted)}"

        if reflection_feedback and retry_count > 0:
            user_content += f"\n\n--- PREVIOUS ATTEMPT FEEDBACK ---\nThe previous retrieval did not result in a satisfactory answer. \nFeedback from judge: {reflection_feedback}\nPlease refine the search plan to better address the user's question."

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        plan_data = {}
        usage_metadata = state.get("usage_metadata") or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        try:
            chat = _get_chat_tool()
            response = chat.chat_completion(messages, temperature=0.0)
            res = response["content"]
            usage = response["usage"]

            # Accumulate usage
            for k in usage:
                usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]

            json_match = re.search(r"\{.*\}", res, re.DOTALL)
            if json_match:
                plan_data = json.loads(json_match.group(0))
        except Exception as e:
            logger.warning(f"Planning LLM failed: {e}")

        # Construct final plan
        plan: RetrievalPlan = {
            "query_text": plan_data.get("query_text") or q,
            "top_k": plan_data.get("top_k", 20),
            "vector_k": 30,
            "extra_filter": plan_data.get("extra_filter"),
            "post_filter": None,
            "reason": plan_data.get("reason", "fallback"),
        }

        def _safe(s: str) -> str:
            return s.replace("'", "''")

        def _any_in(field: str, values: List[str]) -> str:
            joined = ",".join(_safe(v) for v in values if v)
            return f"{field}/any(t: search.in(t, '{joined}', ','))"

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

        def _extract_delay_days(text: str) -> Optional[int]:
            lowered = text.lower()
            if "delay" not in lowered and "delayed" not in lowered:
                return None
            match = re.search(r"\b(\d+)\s+days?\b", lowered)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    return None
            return 0

        def _wants_bucket_chart(text: str) -> bool:
            lowered = text.lower()
            bucket_words = ["bucket", "breakdown", "group", "chart", "graph"]
            window_words = ["today", "week", "fortnight", "month"]
            return any(w in lowered for w in bucket_words) and any(
                w in lowered for w in window_words
            )

        containers = extracted.get("container_number") or []
        po_numbers = extracted.get("po_numbers") or []
        booking_numbers = extracted.get("booking_numbers") or []
        obl_nos = extracted.get("obl_nos") or []

        container_set = {c for c in containers if c}
        obl_set = {o for o in obl_nos if o}
        if container_set:
            po_numbers = [p for p in po_numbers if p not in container_set]
            booking_numbers = [b for b in booking_numbers if b not in container_set]
            obl_nos = [o for o in obl_nos if o not in container_set]
        if obl_set:
            po_numbers = [p for p in po_numbers if p not in obl_set]
            booking_numbers = [b for b in booking_numbers if b not in obl_set]

        # Booster: if we have specific IDs, make sure they are in query_text
        all_ids = []
        all_ids.extend(containers)
        all_ids.extend(po_numbers)
        all_ids.extend(booking_numbers)
        all_ids.extend(obl_nos)

        if all_ids:
            plan["query_text"] = " ".join(list(set(all_ids))) + " " + plan["query_text"]

        filter_clauses: List[str] = []
        has_strong_ids = bool(containers or po_numbers or booking_numbers or obl_nos)
        if plan.get("extra_filter") and not has_strong_ids:
            filter_clauses.append(f"({plan['extra_filter']})")

        if containers:
            parts = [f"container_number eq '{_safe(c)}'" for c in containers]
            filter_clauses.append("(" + " or ".join(parts) + ")")

        if po_numbers:
            filter_clauses.append(_any_in("po_numbers", po_numbers))

        if booking_numbers:
            filter_clauses.append(_any_in("booking_numbers", booking_numbers))

        if obl_nos:
            filter_clauses.append(_any_in("obl_nos", obl_nos))

        status_keywords = [s.lower() for s in (extracted.get("status_keywords") or [])]
        status_map = {
            "on water": "IN_OCEAN",
            "sailing": "IN_OCEAN",
            "in ocean": "IN_OCEAN",
            "delivered": "DELIVERED",
            "ready for pickup": "READY_FOR_PICKUP",
            "empty returned": "EMPTY_RETURNED",
            "at discharge port": "AT_DISCHARGE_PORT",
        }
        statuses = {status_map[k] for k in status_keywords if k in status_map}
        if statuses:
            parts = [f"shipment_status eq '{s}'" for s in sorted(statuses)]
            filter_clauses.append("(" + " or ".join(parts) + ")")

        if any(k in status_keywords for k in ["hot", "priority"]):
            filter_clauses.append("hot_container_flag eq true")

        locations = extracted.get("location") or []
        if locations:
            location_field = (
                "final_destination"
                if _mentions_final_destination(q)
                else "discharge_port"
            )
            parts = [f"contains({location_field}, '{_safe(loc)}')" for loc in locations]
            filter_clauses.append("(" + " or ".join(parts) + ")")

        if filter_clauses:
            plan["extra_filter"] = " and ".join(filter_clauses)
            plan["reason"] = plan.get("reason", "") + " (deterministic filters)"

        post_filter: Dict[str, Any] = {}
        if time_window_days:
            date_field = (
                "optimal_eta_fd_date"
                if _mentions_final_destination(q)
                else "optimal_ata_dp_date"
            )
            post_filter["date_window"] = {
                "field": date_field,
                "days": time_window_days,
                "direction": "next",
            }
            if "order_by" not in plan or not plan["order_by"]:
                plan["order_by"] = (
                    "eta_fd_date desc"
                    if date_field == "optimal_eta_fd_date"
                    else "eta_dp_date desc"
                )
            plan["top_k"] = max(plan.get("top_k", 20), 100)

        delay_days = _extract_delay_days(q)
        if delay_days is not None:
            delay_field = (
                "fd_delayed_dur" if _mentions_final_destination(q) else "dp_delayed_dur"
            )
            post_filter["delay"] = {
                "field": delay_field,
                "op": ">=" if delay_days else ">",
                "days": delay_days,
            }

        if post_filter:
            plan["post_filter"] = post_filter

        if _wants_bucket_chart(q):
            plan["include_total_count"] = True
            plan["top_k"] = max(plan.get("top_k", 20), 500)
            if not plan.get("query_text") or plan["query_text"] == q:
                plan["query_text"] = "*"

        state["retrieval_plan"] = plan
        logger.info(f"Planned retrieval: {plan}")

        return state
