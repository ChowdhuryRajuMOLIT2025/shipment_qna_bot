import re

from langchain_core.messages import AIMessage

from shipment_qna_bot.graph.nodes.static_greet_info_handler import \
    should_handle_overview
from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.utils.runtime import is_test_mode

_chat_tool: AzureOpenAIChatTool | None = None


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


def _has_extracted_ids(state: GraphState) -> bool:
    extracted = state.get("extracted_ids") or {}
    if not isinstance(extracted, dict):
        return False
    for key in ("container_number", "po_numbers", "booking_numbers", "obl_nos"):
        vals = extracted.get(key)
        if isinstance(vals, list) and any(str(v).strip() for v in vals):
            return True
    return False


def _looks_like_association_analytics_query(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False

    analytics_markers = {"analyze", "analyse", "analysis", "analytics"}
    assoc_markers = {
        "associated",
        "association",
        "related",
        "linked",
        "mapping",
        "mapped",
        "corresponding",
    }

    has_analytics_marker = any(m in lowered for m in analytics_markers)
    has_assoc_marker = any(m in lowered for m in assoc_markers)
    has_lookup_object = bool(
        re.search(r"\b(container|containers|po|po number|booking|obl|bol)\b", lowered)
    )
    return has_analytics_marker and has_assoc_marker and has_lookup_object


def _contains_keyword(text: str, keyword: str) -> bool:
    lowered = (text or "").strip().lower()
    term = (keyword or "").strip().lower()
    if not lowered or not term:
        return False
    return bool(re.search(r"\b" + re.escape(term) + r"\b", lowered))


def intent_node(state: GraphState) -> GraphState:
    """
    Classifies the user's intent using LLM.
    """
    with log_node_execution(
        "Intent",
        {"question": (state.get("normalized_question") or "")[:120]},
        state_ref=state,
    ):
        text = (state.get("normalized_question") or "").strip()
        raw_text = (state.get("question_raw") or "").strip()
        if not text:
            state["intent"] = "end"
            return state

        overview_source = None
        if raw_text and should_handle_overview(raw_text):
            overview_source = "raw"
        elif should_handle_overview(text):
            overview_source = "normalized"

        if overview_source:
            logger.info(
                "Intent forced to company_overview by overview gate",
                extra={
                    "extra_data": {
                        "source": overview_source,
                        "text_snippet": (
                            raw_text if overview_source == "raw" else text
                        )[:80],
                    }
                },
            )
            usage_metadata = state.get("usage_metadata") or {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            }
            state.update(
                {
                    "intent": "company_overview",
                    "sub_intents": ["company_overview"],
                    "sentiment": "neutral",
                    "usage_metadata": usage_metadata,
                }
            )
            return state

        usage_metadata = state.get("usage_metadata") or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

        analytics_scope_mode = state.get("analytics_context_mode")
        if analytics_scope_mode in {"session", "previous_result"} or state.get(
            "analytics_scope_candidate"
        ):
            logger.info(
                "Intent forced to analytics by analytics follow-up scope context",
                extra={
                    "extra_data": {
                        "analytics_context_mode": analytics_scope_mode,
                        "has_scope_candidate": bool(
                            state.get("analytics_scope_candidate")
                        ),
                    }
                },
            )
            state.update(
                {
                    "intent": "analytics",
                    "sub_intents": ["analytics"],
                    "sentiment": "neutral",
                    "usage_metadata": usage_metadata,
                }
            )
            return state

        if _looks_like_association_analytics_query(text) and _has_extracted_ids(state):
            logger.info(
                "Intent forced to analytics by association-analysis rule",
                extra={"extra_data": {"text_snippet": text[:120]}},
            )
            state.update(
                {
                    "intent": "analytics",
                    "sub_intents": ["analytics", "association_lookup"],
                    "sentiment": "neutral",
                    "usage_metadata": usage_metadata,
                }
            )
            return state

        if is_test_mode():
            lowered = text.lower()
            greeting_words = {"hi", "hello", "hey", "good morning", "good afternoon"}
            analytics_words = {"chart", "graph", "analytics", "breakdown", "bucket"}
            exit_words = {
                "bye",
                "goodbye",
                "quit",
                "exit",
                "end chat",
                "close session",
            }

            intent = "retrieval"
            if any(_contains_keyword(lowered, w) for w in greeting_words):
                intent = "greeting"
            elif any(_contains_keyword(lowered, w) for w in exit_words):
                intent = "end"
            elif any(_contains_keyword(lowered, w) for w in analytics_words):
                intent = "analytics"

            sub_intents = [intent]
            if "eta" in lowered:
                sub_intents.append("eta")
            if "delay" in lowered or "delayed" in lowered:
                sub_intents.append("delay")
            if "status" in lowered:
                sub_intents.append("status")

            # Deduplicate while preserving order
            seen = set()
            sub_intents = [s for s in sub_intents if not (s in seen or seen.add(s))]

            state.update(
                {
                    "intent": intent,
                    "sub_intents": sub_intents,
                    "sentiment": "neutral",
                    "usage_metadata": state.get("usage_metadata")
                    or {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                }
            )

            if intent == "greeting":
                greeting_text = (
                    "Hello! I can help with shipment status, ETA, delays, or analytics. "
                    "What would you like to check?"
                )
                state["answer_text"] = greeting_text
                state["messages"] = [AIMessage(content=greeting_text)]
                state["is_satisfied"] = True

            if intent == "end":
                exit_text = "Thank you for using the Shipment Q&A Bot. Your session has been closed. Goodbye!"
                state["answer_text"] = exit_text
                state["messages"] = [AIMessage(content=exit_text)]
                state["is_satisfied"] = True

            return state

        import json

        system_prompt = (
            "You are an intent classifier for a Logistics Shipment Q&A Bot.\n"
            "Analyze the user's input and extract:\n"
            "1. Primary Intent: One of ['retrieval', 'analytics', 'greeting', 'company_overview', 'clarification', 'end'].\n"
            "   - 'analytics': Use for general aggregating queries, summaries, counts, or listing distinct values. Examples: 'How many...', 'Total weight...', 'Which carriers...', 'List all suppliers', 'Show delay statistics', 'Check FD dates'.\n"
            "   - 'retrieval': Use for specific single-shipment lookup where an ID is provided (Container, PO, Booking, OBL) or asking for status of a specific subset. If the user asks for a 'list' or 'count' without specific IDs, prefer 'analytics'.\n"
            "   - 'clarification': Use IF AND ONLY IF the user's query is too vague, ambiguous, or lacks necessary context to decide between analytics/retrieval or to perform the action. Examples: 'Show me dates' (Which dates?), 'List shipments' (All of them? Too generic).\n"
            "   - 'greeting': Use for 'hi', 'hello', etc.\n"
            "   - 'company_overview': Use for questions about the company itself.\n"
            "   - 'end': Use ONLY for explicit farewells or requests to close the session (e.g., 'bye', 'goodbye', 'end chat', 'quit'). Do NOT use for simple 'thank you' or praise if the user might have follow-up questions.\n"
            "2. All Intents: A list of all applicable intents (include sub-intents like ['status', 'delay', 'eta_window', 'hot', 'fd', 'in-cd']).\n"
            "3. Sentiment: One of ['positive', 'neutral', 'negative'].\n\n"
            "Output JSON ONLY:\n"
            "{\n"
            '  "primary_intent": "analytics",\n'
            '  "intents": ["analytics", "weight"],\n'
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
                logger.warning(
                    f"Intent classification JSON parse failed. Raw: {content}"
                )
                intent = "retrieval"
                sub_intents = ["retrieval"]
                sentiment = "neutral"

            # Valid intents check
            valid_intents = [
                "retrieval",
                "analytics",
                "greeting",
                "company_overview",
                "clarification",
                "end",
            ]
            if intent not in valid_intents:
                intent = "retrieval"

        except Exception as e:
            logger.error(f"Intent classification failed: {e}")
            intent = "retrieval"
            sub_intents = ["retrieval"]
            sentiment = "neutral"
            usage_metadata = state.get("usage_metadata")

        logger.info(
            f"Classified intent: {intent}",
            extra={"extra_data": {"text_snippet": text[:50]}},
        )

        state.update(
            {
                "intent": intent,
                "sub_intents": sub_intents,
                "sentiment": sentiment,
                "usage_metadata": usage_metadata,
            }
        )

        if intent == "greeting":
            greeting_text = (
                "Hello! I can help with shipment status, ETA, delays, or analytics. "
                "What would you like to check?"
            )
            state["answer_text"] = greeting_text
            state["messages"] = [AIMessage(content=greeting_text)]
            state["is_satisfied"] = True
        elif intent == "end":
            exit_text = "Thank you for using the Shipment Q&A Bot. Your session has been closed. Goodbye!"
            state["answer_text"] = exit_text
            state["messages"] = [AIMessage(content=exit_text)]
            state["is_satisfied"] = True

        return state
