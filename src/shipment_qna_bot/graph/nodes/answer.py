import json
from typing import Any, Dict

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


from datetime import datetime


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
        hits = state.get("hits") or []
        analytics = state.get("idx_analytics") or {}
        question = state.get("question_raw") or ""

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
                    "document_id",
                    "container_number",
                    "shipment_status",
                    "po_numbers",
                    "obl_nos",
                ]
                for f in priority_fields:
                    if f in hit:
                        context_str += f"{f}: {hit[f]}\n"

                # Add metadata_json content intelligently
                if "metadata_json" in hit:
                    try:
                        m = json.loads(hit["metadata_json"])
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
                                    "id",
                                ]  # Filter sensitive fields
                                and len(str(v)) < 200
                            ):
                                context_str += f"{k}: {v}\n"
                    except:
                        pass

        # Pagination Hint
        if hits and len(hits) == 10:  # Assuming default top_k=10
            context_str += "\nNOTE: There are more results. The user can ask 'next 10' to see them.\n"

        # 3. Add Current Date Context
        today_str = datetime.now().strftime("%Y-%m-%d")
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
        system_prompt = """
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

4. PAGINATION:
   - If there are more results, include the hint: {pagination_hint}

Output Format:
a. Direct Answer / Summary
b. Data Table
c. Pagination Button (if applicable)
""".strip()

        user_prompt = (
            f"Context:\n{context_str}\n\n" f"Question: {question}\n\n" "Answer:"
        )

        from langchain_core.messages import AIMessage, HumanMessage

        # Build message history for OpenAI
        llm_messages = [{"role": "system", "content": system_prompt}]

        # Add history
        # We want to include previous turns, but correctly handle the current turn's context
        history = state.get("messages") or []

        # If this is a retry, the last message in history is the previous (unsatisfactory) AIMessage
        # The one before it is the current HumanMessage

        current_question_found = False
        for msg in history:
            if isinstance(msg, HumanMessage) and msg.content == question:
                # This is the current question, we'll add it later with context
                current_question_found = True
                continue

            role = "user" if msg.type == "human" else "assistant"
            llm_messages.append({"role": role, "content": msg.content})

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

            state["answer_text"] = response_text

            # --- Structured Table Construction ---
            if hits and len(hits) > 1:
                cols = [
                    "container_number",
                    "shipment_status",
                    "po_numbers",
                    "booking_numbers",
                    "eta_dp_date",
                ]
                rows = []
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
