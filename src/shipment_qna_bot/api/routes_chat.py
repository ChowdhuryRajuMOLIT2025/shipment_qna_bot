# src/shipment_qna_bot/api/routes_chat.py

import uuid
from typing import List  # type: ignore

from fastapi import APIRouter, Request

from shipment_qna_bot.graph.builder import run_graph
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.models.schemas import (ChartSpec, ChatAnswer,
                                             ChatRequest, EvidenceItem,
                                             TableSpec)
from shipment_qna_bot.security.scope import resolve_allowed_scope

router = APIRouter(tags=["chat"], prefix="/api")


@router.get("/session")
async def get_session(request: Request):
    """
    I'll return the current session state so the frontend stays in sync after refreshes.
    """
    return {
        "consignee_codes": request.session.get("consignee_codes", []),
        "conversation_id": request.session.get("conversation_id"),
    }


@router.post("/chat", response_model=ChatAnswer)
async def chat_endpoint(payload: ChatRequest, request: Request) -> ChatAnswer:
    """
    I handle all incoming chat requests for shipment queries here.
    What I do:
    - I make sure there's always a valid conversation ID.
    - I derive the effective consignee scope for security.
    - I set up the logging context so I can trace every request.
    - I run the LangGraph and return the final answer, including any charts or tables.
    """

    # I prioritize existing session IDs to prevent cross-user collisions.
    session_id = request.session.get("conversation_id")
    conversation_id = session_id or payload.conversation_id or str(uuid.uuid4())

    # Store in session for future requests
    request.session["conversation_id"] = conversation_id

    # I'll store the ID in the request state so I can use it in response logs later.
    request.state.conversation_id = conversation_id

    # I'm deriving the effective consignee scope here for row-level security.
    raw_consignee_codes: List[str] = payload.consignee_codes

    # I'll treat the user identity as optional for now, but I've left the hook for real auth.
    user_identity = request.headers.get("X-User-Identity")

    allowed_consignee_codes = resolve_allowed_scope(
        user_identity=user_identity,
        payload_codes=raw_consignee_codes,
    )

    # If payload codes were provided, persist them in the session
    if raw_consignee_codes:
        request.session["consignee_codes"] = allowed_consignee_codes

    # I'm making the effective scope visible to the logger.
    request.state.consignee_codes = allowed_consignee_codes

    logger.info(
        "Deriving effective consignee scope from %d payload codes -> allowed_scope count=%d",
        len(raw_consignee_codes),
        len(allowed_consignee_codes),
        extra={"step": "API:/chat"},
    )

    # I'll set the logging context early, though I'll update the intent later.
    set_log_context(
        conversation_id=conversation_id,
        consignee_codes=allowed_consignee_codes,
        # intent will be set by the intent classifier once graph classifies it
    )

    logger.info(
        "Received chat request: question_len=%d consignees_count=%d",
        len(payload.question),
        len(allowed_consignee_codes),
        extra={"step": "API:/chat"},
    )

    # I'm passing only the validated consignee codes to the graph to keep things secure.

    # TODO: call LangGraph execution here.
    # For now, stub response to verify logs pipeline.
    # Placeholder logic for processing the chat request
    # In a production implementation, this would involve NLP processing, database queries, etc.

    import time
    from fastapi.concurrency import run_in_threadpool

    start_time = time.time()

    # run graph in a separate thread so concurrent users don't block the FastAPI event loop
    result = await run_in_threadpool(
        run_graph,
        {
            "conversation_id": conversation_id,
            "question_raw": payload.question,
            "consignee_codes": allowed_consignee_codes,
            "usage_metadata": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
    )

    end_time = time.time()
    latency_ms = int((end_time - start_time) * 1000)

    # Calculate costs
    usage = result.get("usage_metadata") or {}
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    total_tokens = usage.get("total_tokens", 0)

    # GPT-4o pricing (approximate)
    cost_usd = (prompt_tokens * 0.000005) + (completion_tokens * 0.000015)

    # I'm syncing the classified intent back into the logs.
    final_intent = result.get("intent", "-")
    request.state.intent = final_intent
    set_log_context(intent=final_intent)

    # If I see an 'end' intent, I'll clear the session to reset the conversation.
    if final_intent == "end":
        request.session.clear()
        logger.info("Session cleared due to 'end' intent.", extra={"step": "API:/chat"})

    answer_text = result.get("answer_text")
    if not answer_text:
        errors = result.get("errors", [])
        if errors:
            answer_text = f"I encountered issues processing your request: {'; '.join(map(str, errors))}"
        else:
            answer_text = "I'm sorry, I couldn't generate an answer for your request."

    logger.info(
        "Graph execution completed: intent=%s, answer_preview='%s...'",
        final_intent,
        # answer_text.replace("\n", " ")[:120],
        f"Responding with answer: {answer_text[:100]}... | Tokens: {total_tokens} | Cost: ${cost_usd:.4f} | Latency: {latency_ms}ms",
        extra={"step": "API:/chat"},
    )

    # I'm building the evidence items from the citations I received.
    raw_citations = result.get("citations", []) or []
    evidence_items: List[EvidenceItem] = []
    # evidence_items = []
    for ev in raw_citations:
        if not isinstance(ev, dict):
            continue
        try:
            evidence_items.append(EvidenceItem(**ev))
        except Exception:
            # keep response stable even if one evidence item fails to parse
            continue

    # I'll optionally include charts and tables if they're in the result.
    chart_model: ChartSpec | None = None
    raw_chart_spec = result.get("chart_spec")
    if isinstance(raw_chart_spec, dict) and raw_chart_spec:
        try:
            chart_model = ChartSpec(**raw_chart_spec)
        except Exception:
            # keep response stable even if chart spec fails to parse
            pass
            logger.warning(
                "Failed to parse chart spec: %s",
                raw_chart_spec,
                extra={"step": "API:/chat"},
            )
            chart_model = None

    table_model: TableSpec | None = None
    raw_table_spec = result.get("table_spec")
    if isinstance(raw_table_spec, dict) and raw_table_spec:
        try:
            table_model = TableSpec(**raw_table_spec)
        except Exception:
            # keep response stable even if table spec fails to parse
            pass
            logger.warning(
                "Failed to parse table spec: %s",
                raw_table_spec,
                extra={"step": "API:/chat"},
            )
            table_model = None

    # I'll bundle everything into the final response now.
    from shipment_qna_bot.models.schemas import ResponseMetadata

    response = ChatAnswer(
        conversation_id=conversation_id,
        intent=final_intent if final_intent != "-" else None,
        answer=answer_text,
        notices=result.get("notices", []),
        evidence=evidence_items,
        chart=chart_model,
        table=table_model,
        metadata=ResponseMetadata(
            tokens=total_tokens,
            cost_usd=round(cost_usd, 6),
            latency_ms=latency_ms,
        ),
    )

    return response
