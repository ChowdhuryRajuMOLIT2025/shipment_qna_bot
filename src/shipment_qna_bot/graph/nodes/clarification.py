import re
from typing import Any, Dict, Optional, cast

from langchain_core.messages import AIMessage, HumanMessage

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_chat_tool: Optional[AzureOpenAIChatTool] = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


def _has_specific_ids(text: str) -> bool:
    lowered = (text or "").lower()
    if re.search(r"\b[a-z]{4}\d{7}\b", lowered):
        return True
    if re.search(r"\b(container|po|booking|obl|bol)\s*(number|no|#)?\s*[:\-]?\s*[a-z0-9]{6,}\b", lowered):
        return True
    return False


def _needs_scope_choice(question: str) -> bool:
    lowered = (question or "").strip().lower()
    if not lowered or _has_specific_ids(lowered):
        return False

    explicit_ambiguity = {
        "show me dates",
        "show dates",
        "list shipments",
        "show shipments",
        "shipment details",
        "show status",
        "check status",
    }
    if lowered in explicit_ambiguity:
        return True

    return bool(
        re.search(
            r"\b(show|list|check|give)\b.*\b(shipments?|status|dates?)\b", lowered
        )
        and "how many" not in lowered
        and "count" not in lowered
    )


def clarification_node(state: GraphState) -> GraphState:
    """
    Generates a clarifying question when the user's intent is ambiguous.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        intent="clarification",
    )

    with log_node_execution(
        "Clarification", {"intent": "clarification"}, state_ref=state
    ):
        question = state.get("question_raw") or ""
        history = state.get("messages") or []

        topic_shift = state.get("topic_shift_candidate")
        if isinstance(topic_shift, dict) and topic_shift:
            raw_q = topic_shift.get("raw") or question
            norm_q = (
                topic_shift.get("normalized")
                or state.get("normalized_question")
                or question
            )
            added = topic_shift.get("added") or []
            reason = ", ".join(added) if added else "prior context"

            clarification_text = (
                "I want to confirm the scope before running analytics.\n\n"
                f"I can interpret your question in two ways ({reason} was added from earlier context):\n"
                f"1) Use previous context: {norm_q}\n"
                f"2) New topic (ignore previous context): {raw_q}\n\n"
                "Reply with 1 or 2. You can also rephrase your question explicitly."
            )

            state["answer_text"] = clarification_text
            state["messages"] = [AIMessage(content=clarification_text)]
            state["is_satisfied"] = True
            state["intent"] = "clarification"
            state["pending_topic_shift"] = {
                "raw": raw_q,
                "normalized": norm_q,
            }
            state["topic_shift_candidate"] = None
            return state

        if _needs_scope_choice(question):
            analytics_choice = f"Show an analytics summary for: {question}"
            retrieval_choice = (
                f"{question} (specific shipment lookup by container/PO/booking/OBL ID)"
            )

            clarification_text = (
                "I can help in two ways for this request:\n\n"
                f"1) Analytics summary across your shipments: {question}\n"
                "2) Specific shipment lookup by IDs (container/PO/booking/OBL)\n\n"
                "Reply with 1 or 2."
            )

            state["answer_text"] = clarification_text
            state["messages"] = [AIMessage(content=clarification_text)]
            state["is_satisfied"] = True
            state["intent"] = "clarification"
            state["pending_topic_shift"] = {
                "raw": retrieval_choice,
                "normalized": analytics_choice,
            }
            state["topic_shift_candidate"] = None
            return state

        # Construct Prompt
        system_prompt = (
            "You are a helpful assistant for a logistics bot.\n"
            "The user's last query was ambiguous or lacked specific details needed to provide an accurate answer.\n"
            "Your goal is to ask a polite, specific question to clarify their intent.\n"
            "Examples:\n"
            "- User: 'Show me dates' -> Bot: 'Would you like to see Estimated Time of Arrival (ETA) at Discharge Port or Final Destination?'\n"
            "- User: 'List shipments' -> Bot: 'Could you please specify which shipments you are interested in? For example, delayed, hot, or from a specific carrier?'\n"
            "Keep it short and professional."
        )

        messages = [{"role": "system", "content": system_prompt}]

        # Add history for context (optional, but good for flow)
        # We limit history to avoid huge context, just last few turns + current question
        for msg in history[-4:]:
            role = "user" if isinstance(msg, HumanMessage) else "assistant"
            messages.append({"role": role, "content": str(msg.content)})

        # Ensure the current question is at the end if not already in history (it should be)
        if not history or history[-1].content != question:
            messages.append({"role": "user", "content": question})

        try:
            chat = _get_chat_tool()
            response = chat.chat_completion(messages, temperature=0.7)
            clarification_text = response["content"]

            state["answer_text"] = clarification_text
            state["messages"] = [AIMessage(content=clarification_text)]
            # We treat this as 'satisfied' because we want to stop execution and wait for user input.
            state["is_satisfied"] = True
            state["intent"] = "clarification"

        except Exception as e:
            logger.error(f"Clarification generation failed: {e}")
            state["answer_text"] = (
                "I'm not sure I understood. Could you please provide more details?"
            )
            state["is_satisfied"] = True
            state["intent"] = "clarification"

    return state
