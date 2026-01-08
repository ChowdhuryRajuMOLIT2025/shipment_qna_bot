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


@router.post("/chat", response_model=ChatAnswer)
async def chat_endpoint(payload: ChatRequest, request: Request) -> ChatAnswer:
    """
    Main `chat` endpoint to handle chat requests related to shipment queries.

    Responsibilities (current stage):
    - Ensure we always have a conversation_id (for session/memory).
    - Normalize and log consignee codes coming from the payload.
    - Derive an *effective* consignee scope via `resolve_allowed_scope` (RLS plumbing hook).
    - Set structured logging context (conversation_id, consignee scope, intent).
    - Call the LangGraph runner with a clean initial state.
    - Map graph result into the public `ChatAnswer` schema, including
      evidence items and (optionally, in future) chart/table data.
    """

    # 1) Conversation/session handling
    # ensure payload always have convesation_id and its always has a value associated with it
    # server generates conversation id if missing, generate a new one.
    conversation_id = payload.conversation_id or str(uuid.uuid4())

    # store in request.state so middleware can use it for RESPONSE logs
    request.state.conversation_id = conversation_id

    # 2) Derive effective consignee scope (RLS plumbing)
    # Raw codes from payload are already normalized by ChatRequest validator.
    raw_consignee_codes: List[str] = payload.consignee_codes

    # In a real deployment, this would come from auth/token/headers.
    # For now, we treat it as optional and let `resolve_allowed_scope`
    # behave as a pure normalizer, but wiring is in place for future RLS.
    user_identity = request.headers.get("X-User-Identity")

    allowed_consignee_codes = resolve_allowed_scope(
        user_identity=user_identity,
        payload_codes=raw_consignee_codes,
    )

    # Make the effective scope visible to middleware logging.
    # This is what we actually use for tools/RLS, not the raw payload.
    # Use effective scope for logging/middleware (never raw payload codes).
    request.state.consignee_codes = allowed_consignee_codes

    logger.info(
        "Normalized consignee_codes from payload=%s -> allowed_scope=%s",
        raw_consignee_codes,
        allowed_consignee_codes,
        extra={"step": "API:/chat"},
    )

    # set and update logging context for each request early for the route
    # inten will be set later once graph classifies it
    set_log_context(
        conversation_id=conversation_id,
        consignee_codes=allowed_consignee_codes,
        # intent will be set by the intent classifier once graph classifies it
    )

    logger.info(
        "Received chat request: question='%s...' consignees=%s",
        payload.question,
        allowed_consignee_codes,
        extra={"step": "API:/chat"},
    )

    # 4) Run the langGraph with a clean initial state
    # important we pass *effective* consignee codes only, the graph/tooling
    # must never see unvalidated raw payload values.

    # TODO: call LangGraph execution here.
    # For now, stub response to verify logs pipeline.
    # Placeholder logic for processing the chat request
    # In a production implementation, this would involve NLP processing, database queries, etc.

    import time

    start_time = time.time()

    # run graph
    result = run_graph(
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

    # 5) Sync intent into logs + middleware response
    final_intent = result.get("intent", "-")
    request.state.intent = final_intent
    set_log_context(intent=final_intent)

    answer_text = result.get("answer_text", "-") or "-"

    logger.info(
        "Graph execution completed: intent=%s, answer_preview='%s...'",
        final_intent,
        # answer_text.replace("\n", " ")[:120],
        f"Responding with answer: {answer_text[:100]}... | Tokens: {total_tokens} | Cost: ${cost_usd:.4f} | Latency: {latency_ms}ms",
        extra={"step": "API:/chat"},
    )

    # 6) Build evidence items list from citations- convert evidence
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

    # 7) Optional analytics: chart and table
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

    # 8) Build final response
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
