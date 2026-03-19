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
    "previous result",
    "previous list",
    "above list",
    "session scope",
    "all shipments",
    "full scope",
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

_PREVIOUS_RESULT_SCOPE_HINTS = {
    "from above",
    "from the above",
    "above list",
    "above results",
    "from above list",
    "from previous result",
    "from previous results",
    "from previous list",
    "among those",
    "among them",
    "among above",
    "from that list",
    "from those",
    "from these",
    "of the above",
}

_SESSION_SCOPE_HINTS = {
    "all shipments",
    "all my shipments",
    "overall",
    "across all shipments",
    "full scope",
    "session scope",
    "whole dataset",
    "entire dataset",
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


def _parse_analytics_scope_choice(text: str) -> Optional[str]:
    lowered = (text or "").strip().lower()
    if lowered in {
        "1",
        "a",
        "option 1",
        "choice 1",
        "previous",
        "previous result",
        "previous list",
        "above list",
        "use previous",
        "use previous result",
        "use above list",
    }:
        return "previous_result"
    if lowered in {
        "2",
        "b",
        "option 2",
        "choice 2",
        "session",
        "session scope",
        "all",
        "all shipments",
        "full scope",
        "use session scope",
        "use all shipments",
    }:
        return "session"
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


def _has_previous_analytics_subset(state: GraphState) -> bool:
    selector = state.get("last_analytics_result_selector")
    if not isinstance(selector, dict):
        return False
    ids = selector.get("ids")
    if isinstance(ids, dict):
        for values in ids.values():
            if isinstance(values, list) and values:
                return True
    count = selector.get("row_count")
    return isinstance(count, int) and count > 0


def _mentions_previous_result_scope(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False
    if any(h in lowered for h in _PREVIOUS_RESULT_SCOPE_HINTS):
        return True
    return bool(re.search(r"\b(above|previous)\s+(list|result|results)\b", lowered))


def _mentions_session_scope(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False
    return any(h in lowered for h in _SESSION_SCOPE_HINTS)


def _looks_like_ambiguous_analytics_followup(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False
    if _looks_like_specific_lookup(lowered):
        return False
    if _contains_time_window(lowered):
        return False
    if _mentions_previous_result_scope(lowered) or _mentions_session_scope(lowered):
        return False

    keywords = {
        "hot",
        "priority",
        "delay",
        "delayed",
        "status",
        "eta",
        "ata",
        "carrier",
        "vessel",
        "port",
        "shipment",
        "shipments",
    }
    if not any(k in lowered for k in keywords):
        return False

    if _has_anaphora(lowered):
        return True

    # Short follow-ups like "which are hot?" are ambiguous after a list/table result.
    words = re.findall(r"\b\w+\b", lowered)
    starters = {"which", "what", "show", "list", "count", "give", "how"}
    if words and words[0] in starters and len(words) <= 8:
        return True
    return False


def _build_analytics_scope_candidate(
    raw_question: str, normalized_question: str, state: GraphState
) -> Optional[Dict[str, Any]]:
    if not _has_previous_analytics_subset(state):
        return None

    raw_q = (raw_question or "").strip()
    norm_q = (normalized_question or "").strip()
    if not raw_q and not norm_q:
        return None

    prev_count = state.get("last_analytics_result_count")

    if _mentions_previous_result_scope(raw_q) or _mentions_previous_result_scope(
        norm_q
    ):
        return None

    if _mentions_session_scope(raw_q) or _mentions_session_scope(norm_q):
        return None

    if not _looks_like_ambiguous_analytics_followup(raw_q):
        return None

    return {
        "raw_question": raw_q or norm_q,
        "normalized_question": norm_q or raw_q.lower(),
        "previous_result_count": prev_count,
    }


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

        def _apply_analytics_scope_flags(raw_q: str, normalized_q: str) -> None:
            state["analytics_scope_candidate"] = None
            state["analytics_context_mode"] = None
            if _mentions_previous_result_scope(
                raw_q
            ) or _mentions_previous_result_scope(normalized_q):
                if _has_previous_analytics_subset(state):
                    state["analytics_context_mode"] = "previous_result"
                return
            if _mentions_session_scope(raw_q) or _mentions_session_scope(normalized_q):
                state["analytics_context_mode"] = "session"
                return
            analytics_scope_candidate = _build_analytics_scope_candidate(
                raw_q, normalized_q, state
            )
            if analytics_scope_candidate:
                state["analytics_context_mode"] = "previous_result"
                return
            state["analytics_scope_candidate"] = None

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

        pending_analytics = state.get("pending_analytics_scope")
        if pending_analytics:
            choice = _parse_analytics_scope_choice(question)
            if choice:
                chosen_raw = (pending_analytics.get("question_raw") or question).strip()
                chosen_normalized = (
                    pending_analytics.get("normalized_question") or chosen_raw.lower()
                ).strip()
                state["question_raw"] = chosen_raw
                state["normalized_question"] = chosen_normalized
                state["pending_analytics_scope"] = None
                state["analytics_scope_candidate"] = None
                state["analytics_context_mode"] = choice
                state.setdefault("messages", []).append(
                    HumanMessage(content=chosen_raw)
                )
                logger.info(
                    "Applied analytics scope choice: %s",
                    choice,
                    extra={"extra_data": {"chosen": chosen_raw[:120]}},
                )
                return state

            if not _is_control_reply(question):
                state["pending_analytics_scope"] = None

        # Praise/Feedback Guardrail (Issue A)
        praise_patterns = [
            r"^(thank you|thanks|great|good job|well done|nice|cool|awesome|perfect|exactly|no corrections?|you are (doing )?good|keep it up)[\s\d!.]*$",
            r"^(no|nothing|that's it|all set|i'm good|no thanks)[\s.]*$",
        ]
        if any(re.search(p, question.lower()) for p in praise_patterns):
            logger.info(
                "Normalizer: Bypassing LLM rewrite for praise/acknowledgment message."
            )
            state["normalized_question"] = question.lower()
            state["topic_shift_candidate"] = None
            return state

        if is_test_mode() or forced_new_topic:
            normalized_q = question.lower()
            state["normalized_question"] = normalized_q
            state["topic_shift_candidate"] = None
            _apply_analytics_scope_flags(question, normalized_q)
            return state

        if _looks_like_company_fact_question(question):
            normalized_q = question.lower()
            state["normalized_question"] = normalized_q
            state["topic_shift_candidate"] = None
            _apply_analytics_scope_flags(question, normalized_q)
            return state

        # If there is no history or only one message (the current one), just return the lowercase question
        if len(history) <= 1:
            normalized_q = question.lower()
            state["normalized_question"] = normalized_q
            state["topic_shift_candidate"] = None
            _apply_analytics_scope_flags(question, normalized_q)
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
        _apply_analytics_scope_flags(question, normalized)
        return state
