# src/shipment_qna_bot/models/schemas.py

from typing import List, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., description="User's query in natural language")
    consignee_codes: List[str] = Field(  # type: ignore
        ...,
        description="Consignee hierarchy, e.g. [PARENT, CHILD1, CHILD2]",
        min_items=1,
    )
    conversation_id: Optional[str] = Field(
        None,
        description="Conversation/session identifier (UUID or similar)",
    )


class EvidenceItem(BaseModel):
    doc_id: str
    container_number: Optional[str] = None
    field_used: Optional[List[str]] = None


class ChatAnswer(BaseModel):
    intent: Optional[str] = None
    answer: str
    notices: Optional[List[str]] = None
    evidence: Optional[List[EvidenceItem]] = None
