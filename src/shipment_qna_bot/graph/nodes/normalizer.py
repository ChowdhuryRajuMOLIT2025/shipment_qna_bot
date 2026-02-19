import re
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.messages import BaseMessage, HumanMessage

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.utils.runtime import is_test_mode

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


_ANAPHORA_TOKENS = {
    "it",
    "its",
    "they",
    "them",
    "their",
    "those",
    "these",
    "that",
    "this",
    "same",
    "previous",
    "earlier",
    "above",
    "again",
    "continue",
    "follow up",
}

_CONTROL_REPLIES = {
    "1",
    "2",
    "option 1",
    "option 2",
    "choice 1",
    "choice 2",
    "a",
    "b",
    "use previous",
    "use previous context",
    "use context",
    "previous",
    "same",
    "new",
    "new topic",
    "ignore",
    "ignore previous",
    "ignore previous context",
}

_COMPANY_QUERY_TOKENS = {
    "mcs",
    "mol",
    "mol consolidation",
    "mol consolidation service",
    "starlink",
    "mitsui osk",
}

_COMPANY_INTENT_HINTS = {
    "what is",
    "who is",
    "stand for",
    "full form",
    "mean",
    "meaning",
    "about",
    "history",
    "vision",
    "mission",
    "ceo",
    "office",
    "services",
}


def _has_anaphora(text: str) -> bool:
    lowered = (text or "").lower()
    return any(token in lowered for token in _ANAPHORA_TOKENS)


def _contains_time_window(text: str) -> bool:
    lowered = (text or "").lower()
    patterns = [
        r"\bnext\s+\d+\s+days?\b",
        r"\bin\s+\d+\s+days?\b",
        r"\bnext\s+week\b",
        r"\bnext\s+month\b",
        r"\bthis\s+week\b",
        r"\bthis\s+month\b",
        r"\btoday\b",
        r"\btomorrow\b",
        r"\byesterday\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}-[a-z]{3}-\d{2,4}\b",
    ]
    return any(re.search(p, lowered) for p in patterns)


def _contains_ids(text: str) -> bool:
    lowered = (text or "").lower()
    if re.search(r"\b[a-z]{4}\d{7}\b", lowered):
        return True
    if re.search(r"\b\d{6,}\b", lowered):
        return True
    return False


def _strip_new_topic_prefix(text: str) -> Tuple[str, bool]:
    lowered = (text or "").strip().lower()
    prefixes = [
        "new topic:",
        "new topic -",
        "ignore previous context:",
        "ignore previous:",
        "fresh question:",
    ]
    for p in prefixes:
        if lowered.startswith(p):
            return text[len(p) :].strip(), True
    return text, False


def _parse_topic_shift_choice(text: str) -> Optional[str]:
    lowered = (text or "").strip().lower()
    if lowered in {
        "1",
        "a",
        "option 1",
        "choice 1",
        "use previous",
        "use previous context",
        "use context",
    }:
        return "use_previous"
    if lowered in {
        "2",
        "b",
        "option 2",
        "choice 2",
        "new",
        "new topic",
        "ignore",
        "ignore previous",
    }:
        return "new_topic"
    return None


def _is_control_reply(text: str) -> bool:
    lowered = (text or "").strip().lower()
    return lowered in _CONTROL_REPLIES


def _looks_like_specific_lookup(text: str) -> bool:
    lowered = (text or "").lower()
    if re.search(r"\b[a-z]{4}\d{7}\b", lowered):
        return True
    if re.search(r"\b\d{6,}\b", lowered) and re.search(
        r"\b(container|po|booking|obl|bol)\b", lowered
    ):
        return True
    return False


def _looks_like_company_fact_question(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered or _looks_like_specific_lookup(lowered):
        return False
    if not any(token in lowered for token in _COMPANY_QUERY_TOKENS):
        return False
    return any(hint in lowered for hint in _COMPANY_INTENT_HINTS)


def _topic_shift_candidate(
    raw_question: str, normalized_question: str
) -> Optional[Dict[str, Any]]:
    if not raw_question or not normalized_question:
        return None

    raw = raw_question.strip().lower()
    norm = normalized_question.strip().lower()
    if raw == norm:
        return None

    if _has_anaphora(raw):
        return None

    added: List[str] = []
    if _contains_time_window(norm) and not _contains_time_window(raw):
        added.append("time_window")
    if _contains_ids(norm) and not _contains_ids(raw):
        added.append("ids")

    if not added:
        return None

    return {"raw": raw_question, "normalized": normalized_question, "added": added}


def normalize_node(state: GraphState) -> Dict[str, Any]:
    """
    Normalizes the user's question and resolves co-references using conversation history.
    """
    with log_node_execution(
        "Normalizer",
        {"question": (state.get("question_raw") or "")[:120]},
        state_ref=state,
    ):
        question = (state.get("question_raw") or "").strip()
        question, forced_new_topic = _strip_new_topic_prefix(question)
        if forced_new_topic:
            state["question_raw"] = question
        history: List[BaseMessage] = state.get("messages", [])

        pending = state.get("pending_topic_shift")
        if pending:
            choice = _parse_topic_shift_choice(question)
            if choice:
                chosen = (
                    pending.get("normalized")
                    if choice == "use_previous"
                    else pending.get("raw")
                )
                chosen = (chosen or question).strip()
                state["question_raw"] = chosen
                state["normalized_question"] = chosen.lower()
                state["pending_topic_shift"] = None
                state["topic_shift_candidate"] = None
                state.setdefault("messages", []).append(HumanMessage(content=chosen))
                logger.info(
                    "Applied topic-shift choice: %s",
                    choice,
                    extra={"extra_data": {"chosen": chosen[:120]}},
                )
                return state

            # If the user responded with a new question (not a control token),
            # clear the pending choice and continue normal processing.
            if not _is_control_reply(question):
                state["pending_topic_shift"] = None

        if is_test_mode() or forced_new_topic:
            state["normalized_question"] = question.lower()
            state["topic_shift_candidate"] = None
            return state

        if _looks_like_company_fact_question(question):
            state["normalized_question"] = question.lower()
            state["topic_shift_candidate"] = None
            return state

        # If there is no history or only one message (the current one), just return the lowercase question
        if len(history) <= 1:
            state["normalized_question"] = question.lower()
            state["topic_shift_candidate"] = None
            return state

        # Prompt for co-reference resolution
        system_prompt = """
Role:
You are an expert at resolving co-references in conversations for a logistics chatbot.

Task:
Given a conversation history and a final follow-up question, rewrite the follow-up question to be a standalone question that includes all necessary context (like container numbers, PO numbers, etc.) mentioned previously.

Guidelines:
- If the question is already standalone or starts a new topic, return it as is.
- If the question uses pronouns like "it", "they", "that shipment", replace them with the specific identifiers from the history.
- **TOPIC SHIFT:** If the user asks a broad question (e.g., "How many total shipments?") after specific questions about a container, do NOT inject the specific container ID into the broad question unless explicitly linked (e.g., "What about its weight?").
- **INDEPENDENCE:** Treat general analytics queries (counts, sums) as independent unless they clearly refer to the previous results.
- Maintain the original intent of the question.
- Return ONLY the rewritten question text.
""".strip()

        llm_messages = [{"role": "system", "content": system_prompt}]

        # Add history to prompt
        # history includes current question as the last item (if it was added in run_graph)
        # Actually builder.py adds it just before invoke.

        for msg in history[:-1]:
            content = str(getattr(msg, "content", "")).strip()
            if _is_control_reply(content):
                continue
            role = "user" if msg.type == "human" else "assistant"
            llm_messages.append({"role": role, "content": content})

        llm_messages.append(
            {"role": "user", "content": f"Follow-up Question: {question}"}
        )

        try:
            chat_tool = _get_chat_tool()
            response = chat_tool.chat_completion(llm_messages, temperature=0.0)
            standalone_question = response["content"].strip()
            usage = response["usage"]

            # Accumulate usage
            usage_metadata = state.get("usage_metadata") or {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            }
            for k in usage:
                usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]

            normalized = standalone_question.lower()

            logger.info(
                f"Resolved standalone question: {normalized}",
                extra={"extra_data": {"original": question}},
            )
        except Exception as e:
            logger.warning(f"Co-reference resolution failed: {e}")
            normalized = question.lower()
            usage_metadata = state.get("usage_metadata")

        candidate = _topic_shift_candidate(question, normalized)
        state["normalized_question"] = normalized
        state["usage_metadata"] = usage_metadata
        state["topic_shift_candidate"] = candidate
        return state
