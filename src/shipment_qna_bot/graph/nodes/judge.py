import json
from typing import Any, Dict

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import get_today_date

_CHAT_TOOL = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def judge_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates if the generated answer is grounded in retrieved documents and answers the question.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "Judge",
        {
            "intent": state.get("intent", "-"),
            "retry_count": state.get("retry_count", 0),
        },
    ):
        question = state.get("question_raw") or ""
        answer = state.get("answer_text") or ""
        hits = state.get("hits") or []

        if not hits:
            # If no hits, we can't really judge grounding, but we can judge if the "no info" answer is acceptable.
            state["is_satisfied"] = True
            state["reflection_feedback"] = None
            return state

        # Evaluation Prompt
        system_prompt = """
Role:
You are a quality assurance judge for a logistics chatbot.

Goal:
Evaluate the drafted answer based on the provided retrieved documents and the user's question.

Retrieved Documents:
{context}

User Question:
{question}

Today's UTC Date:
{today}

Drafted Answer:
{answer}

Task:
1. Grounding: Is the answer strictly based on the provided documents? (Yes/No)
2. Completeness: Does the answer address all parts of the user question? (Yes/No)
3. Accuracy: Are there any hallucinations or incorrect details? (List them if any)

Decision:
If the answer is grounded, complete, and accurate, set decision="satisfied".
Otherwise, set decision="retry" and provide specific "feedback" on what needs to be improved in the next retrieval or planning step.

Output MUST be a JSON object:
{{
  "decision": "satisfied" | "retry",
  "feedback": "string or null"
}}
""".strip()

        today_str = state.get("today_date") or get_today_date()
        context_str = ""
        for i, hit in enumerate(hits[:10]):
            context_str += f"\n--- Doc {i+1} ---\n{json.dumps(hit, indent=2)}\n"

        user_prompt = "Judge the answer now."

        llm_messages = [
            {
                "role": "system",
                "content": system_prompt.format(
                    context=context_str,
                    question=question,
                    answer=answer,
                    today=today_str,
                ),
            },
            {"role": "user", "content": user_prompt},
        ]

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

            # Extract JSON
            try:
                # Find the first { and last }
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start != -1 and end != -1:
                    result = json.loads(response_text[start:end])
                else:
                    result = {"decision": "satisfied", "feedback": None}
            except:
                logger.warning(f"Failed to parse judge JSON: {response_text}")
                result = {"decision": "satisfied", "feedback": None}

            state["is_satisfied"] = result.get("decision") == "satisfied"
            state["reflection_feedback"] = result.get("feedback")

            if not state["is_satisfied"]:
                state["retry_count"] = state.get("retry_count", 0) + 1
                logger.info(f"Judge requested retry: {state['reflection_feedback']}")
            else:
                logger.info("Judge satisfied with answer.")

        except Exception as e:
            logger.error(f"Judge node failed: {e}", exc_info=True)
            state["is_satisfied"] = True  # Default to satisfied on error to avoid loops
            state["reflection_feedback"] = None

        return state
