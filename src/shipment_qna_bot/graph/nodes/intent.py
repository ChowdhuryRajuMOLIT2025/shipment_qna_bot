from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def intent_node(state: GraphState) -> GraphState:
    """
    Classifies the user's intent using LLM.
    """
    text = state.get("normalized_question", "")
    if not text:
        return {"intent": "end"}

    import json
    import re

    system_prompt = (
        "You are an intent classifier for a Shipment Q&A Bot.\n"
        "Analyze the user's input and extract:\n"
        "1. Primary Intent: One of ['retrieval', 'analytics', 'greeting', 'end'].\n"
        "2. All Intents: A list of all applicable intents (e.g. ['retrieval', 'complaint']).\n"
        "3. Sentiment: One of ['positive', 'neutral', 'negative'].\n\n"
        "Output JSON ONLY:\n"
        "{\n"
        '  "primary_intent": "retrieval",\n'
        '  "intents": ["retrieval"],\n'
        '  "sentiment": "neutral"\n'
        "}"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text},
    ]

    try:
        chat_tool = _get_chat_tool()
        response = chat_tool.chat_completion(messages, temperature=0.0)
        content = response["content"].strip()
        usage = response["usage"]

        # Accumulate usage
        usage_metadata = state.get("usage_metadata") or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        for k in usage:
            usage_metadata[k] = usage_metadata.get(k, 0) + usage[k]

        # Parse JSON
        try:
            # simple cleanup for markdown code blocks if LLM adds them
            clean_content = re.sub(r"```json|```", "", content).strip()
            data = json.loads(clean_content)
            intent = data.get("primary_intent", "retrieval").lower()
            sub_intents = data.get("intents", [])
            sentiment = data.get("sentiment", "neutral").lower()
        except json.JSONDecodeError:
            logger.warning(f"Intent classification JSON parse failed. Raw: {content}")
            intent = "retrieval"
            sub_intents = ["retrieval"]
            sentiment = "neutral"

        # Valid intents check
        valid_intents = ["retrieval", "analytics", "greeting", "end"]
        if intent not in valid_intents:
            intent = "retrieval"

    except Exception as e:
        logger.error(f"Intent classification failed: {e}")
        intent = "retrieval"
        sub_intents = ["retrieval"]
        sentiment = "neutral"
        usage_metadata = state.get("usage_metadata")

    return {
        "intent": intent,
        "sub_intents": sub_intents,
        "sentiment": sentiment,
        "usage_metadata": usage_metadata,
    }

    logger.info(
        f"Classified intent: {intent}",
        extra={"extra_data": {"text_snippet": text[:50]}},
    )

    return {"intent": intent}
