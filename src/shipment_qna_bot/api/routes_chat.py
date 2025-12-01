# src/shipment_qna_bot/api/routes_chat.py

import uuid
from typing import List  # type: ignore

from fastapi import APIRouter, Request

from shipment_qna_bot.graph.builder import run_graph
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.models.schemas import (ChatAnswer, ChatRequest,
                                             EvidenceItem)

router = APIRouter(tags=["chat"], prefix="/api")


@router.post("/chat", response_model=ChatAnswer)
async def chat_endpoint(payload: ChatRequest, request: Request) -> ChatAnswer:
    """
    Main `chat` endpoint to handle chat requests related to shipment queries.
    For now:
        - sets logging context (conversation_id, consignee_codes)
        - logs basic request info
        - returns stub response
    Later we can switch or add new context:
        - will call LangGraph runner with this payload
    """

    # ensure payload always have convesation_id and its always has a value associated with it
    # server generates conversation id if missing
    conversation_id = payload.conversation_id or str(uuid.uuid4())

    # store in request.state so middleware can use it for RESPONSE logs
    request.state.conversation_id = conversation_id
    request.state.consignee_codes = payload.consignee_codes
    logger.info(
        f"Normalized consignee_codes(type={type(payload.consignee_codes)}): {payload.consignee_codes}",
        extra={"step": "API:/chat"},
    )

    # set and update logging context for each request early for the route
    set_log_context(
        conversation_id=conversation_id,
        consignee_codes=payload.consignee_codes,
        # intent will be set by the intent classifier ode later
    )

    logger.info(
        f"Received chat request: question= '{payload.question}...'"
        f"consignees = {payload.consignee_codes}",
        extra={"step": "API:/chat"},
    )

    # TODO: call LangGraph execution here.
    # For now, stub response to verify logs pipeline.
    # Placeholder logic for processing the chat request
    # In a production implementation, this would involve NLP processing, database queries, etc.

    # run graph
    result = run_graph(
        {
            "conversation_id": conversation_id,
            "question_raw": payload.question,
            "consignee_codes": payload.consignee_codes,
        }
    )

    # sync intent into logs + middleware response
    # update log context with the determined intent so subsequent logs (like the response log) show it
    # set_log_context(intent=result.get("intent", "-"))
    final_intent = result.get("intent", "-")
    request.state.intent = final_intent
    set_log_context(intent=final_intent)

    answer_text = result.get("answer_text", "-")
    # request.state.answer_text = answer_text
    # set_log_context(answer_text=answer_text)

    logger.info(
        f"Responding with answer: {answer_text}",
        extra={"step": "API:/chat"},
    )

    # convert evidence if we have it; placeholder empty list for now
    evidence_items = []
    for ev in result.get("evidence", []) or []:
        try:
            evidence_items.append(EvidenceItem(**ev))
        except Exception:
            # keep response stable even if evidence item malformed during early dev
            continue

    response = ChatAnswer(
        conversation_id=conversation_id,
        intent=final_intent if final_intent != "-" else None,
        answer=answer_text,
        notices=result.get("notices", []),
        evidence=evidence_items,
    )

    return response
