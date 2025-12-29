import json
import re
from typing import Any, Dict, List, Optional

from shipment_qna_bot.graph.state import RetrievalPlan
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import (GET_TODAY_DATE_SCHEMA,
                                               get_today_date)

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


from datetime import datetime


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

        # Logistics mapping and synonym dictionary for the LLM
        # Logistics mapping and synonym dictionary for the LLM
        logistics_context = """
        Field Mappings in Index:
        - container_number (String): e.g. ABCD1234567
        - po_numbers (Collection): e.g. 12356789. Filter using: po_numbers/any(p: p eq '12356789')
        - booking_numbers (Collection): e.g. TH2017996. Filter using: booking_numbers/any(b: b eq 'TH2017996')
        - obl_nos (Collection): e.g. MAEU12897654. Filter using: obl_nos/any(o: o eq 'MAEU12897654')
        - shipment_status (String): DELIVERED, IN_OCEAN, AT_DISCHARGE_PORT, READY_FOR_PICKUP, EMPTY_RETURNED
        - discharge_port_name (String): e.g. "Los Angeles"
        - mother_vessel_name (String): e.g. "MAERSK SERANGOON"
        - optimal_eta_fd_date (DateTimeOffset): e.g. "2024-01-01T00:00:00Z"
        - eta_dp_date (DateTimeOffset): e.g. "2024-01-01T00:00:00Z"

        Synonyms:
        - "on water", "sailing" -> shipment_status eq 'IN_OCEAN'
        - "arrived" -> shipment_status eq 'AT_DISCHARGE_PORT'
    """.strip()

        today_str = datetime.now().strftime("%Y-%m-%d")
        # NOTE: We now allow the LLM to call get_today_date if it needs to be sure,
        # but we also provide it here as context.

        system_prompt = f"""
        You are a Search Planner for a logistics bot. Given a user question and extracted entities, generate an Azure Search Plan.
        
        Current Date: {today_str}

        {logistics_context}

        CRITICAL INSTRUCTIONS:
        1. FORBIDDEN FIELDS: NEVER use field names like 'ocean_bl_numbers', 'booking_id', or 'po_id'. ONLY use the field names listed in 'Field Mappings in Index'.
        2. NORMALIZATION: Extracted entities (PO, OBL, Container, Booking) have been normalized to UPPERCASE. ALWAYS use these normalized values from 'Extracted Entities' in your filters. 
        3. COLLECTION FIELDS: 'po_numbers', 'booking_numbers', and 'obl_nos' are collections. Filter using: field/any(x: x eq 'VALUE').
        4. QUERY TEXT (ID BOOSTING): ALWAYS include all extracted identifiers (PO, OBL, Booking, Container) at the BEGINNING of the "query_text" string to boost exact matching in hybrid search.
           - Example: If PO is 2J69300, query_text should be: "2J69300 [original query text]".
        
        Date Filtering (Relative Dates):
        - Use optimal_eta_fd_date for future/past day checks.
        
        Pagination & Sorting:
        - "next 10" -> set "skip".
        - "recent", "latest" -> set "order_by": "optimal_eta_fd_date desc".
        
        Output JSON only:
        {{
            "query_text": "text for hybrid search",
            "extra_filter": "OData filter string or null",
            "skip": 0,
            "order_by": "optimal_eta_fd_date desc" or null,
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

            # First pass: Allow tool usage
            response = chat.chat_completion(
                messages,
                temperature=0.0,
                tools=[GET_TODAY_DATE_SCHEMA],
                tool_choice="auto",
            )

            # Check for tool calls
            if response.get("tool_calls"):
                tool_calls = response["tool_calls"]
                # Append assistant's tool call message
                messages.append({"role": "assistant", "tool_calls": tool_calls})

                for tc in tool_calls:
                    if tc.function.name == "get_today_date":
                        date_result = get_today_date()
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "name": "get_today_date",
                                "content": date_result,
                            }
                        )

                # Second pass: Get final answer with tool result
                # Force the model to generate text (the plan) instead of calling tools again
                response = chat.chat_completion(
                    messages, temperature=0.0, tool_choice="none"
                )

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
            "top_k": 10,  # Default to 10 as requested
            "vector_k": 30,
            "extra_filter": plan_data.get("extra_filter"),
            "skip": plan_data.get("skip"),
            "order_by": plan_data.get("order_by"),
            "reason": plan_data.get("reason", "fallback"),
        }

        # Booster: if we have specific IDs, make sure they are in query_text
        all_ids = []
        for k in ["container_number", "po_numbers", "booking_numbers", "obl_nos"]:
            all_ids.extend(extracted.get(k) or [])

        if all_ids:
            plan["query_text"] = " ".join(list(set(all_ids))) + " " + plan["query_text"]

        state["retrieval_plan"] = plan
        logger.info(f"Planned retrieval: {plan}")

        return state
