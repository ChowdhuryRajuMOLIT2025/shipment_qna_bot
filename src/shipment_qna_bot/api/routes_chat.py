# src/shipment_qna_bot/api/routes_chat.py

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

    # ensure payload always have convesation_id and its always ahas a value associated with it
    conversation_id = payload.conversation_id or "conv-auto"
    request.state.conversation_id = conversation_id
    request.state.consignee_codes = payload.consignee_codes

    # set and update logging context for each request
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

    result = run_graph(
        {
            "conversation_id": conversation_id,
            "question_raw": payload.question,
            "consignee_codes": payload.consignee_codes,
        }
    )

    logger.info(
        f"Responding with answer: {result['answer_text']}",
        extra={"step": "API:/chat"},
    )

    response = ChatAnswer(
        intent=result["intent"],
        answer=result["answer_text"],
        notices=result["notices"],
        # evidence=result['evidence'],
    )

    logger.info(f"Responding with answer: {response.answer}")

    return response
