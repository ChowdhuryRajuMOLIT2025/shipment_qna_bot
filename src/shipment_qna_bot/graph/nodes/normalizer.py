from typing import Any, Dict, List

from langchain_core.messages import BaseMessage

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.utils.runtime import is_test_mode

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def normalize_node(state: GraphState) -> Dict[str, Any]:
    """
    Normalizes the user's question and resolves co-references using conversation history.
    """
    question = state.get("question_raw", "").strip()
    history: List[BaseMessage] = state.get("messages", [])

    if is_test_mode():
        return {"normalized_question": question.lower()}

    # If there is no history or only one message (the current one), just return the lowercase question
    if len(history) <= 1:
        return {"normalized_question": question.lower()}

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
        role = "user" if msg.type == "human" else "assistant"
        llm_messages.append({"role": role, "content": msg.content})

    llm_messages.append({"role": "user", "content": f"Follow-up Question: {question}"})

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

    return {"normalized_question": normalized, "usage_metadata": usage_metadata}
