# src/shipment_qna_bot/models/schemas.py

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator


def _split_codes(s: str) -> List[str]:
    return [p.strip() for p in s.split(",") if p.strip()]


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class ChatRequest(BaseModel):
    question: str = Field(
        ..., description="User's query in natural language", min_length=1
    )

    consignee_codes: List[str] = Field(
        ...,
        description="Consignee hierarchy, e.g. [PARENT, CHILD1, CHILD2]",
        min_length=1,
    )
    conversation_id: Optional[str] = Field(
        None,
        description="Conversation/session identifier (UUID or similar)",
    )

    @field_validator("question", mode="before")
    @classmethod
    def normalize_question(cls, v: Any) -> str:
        q = "" if v is None else str(v).strip()
        if not q:
            raise ValueError("question must not be empty")
        return q

    @field_validator("consignee_codes", mode="before")
    @classmethod
    def normalize_consignee_codes(cls, v: Any) -> List[str]:
        if v is None:
            raise ValueError("consignee_codes is required")

        codes: List[str] = []

        # accept single comma-string
        if isinstance(v, str):
            codes = _split_codes(v)

        # accept list input, including comma-packed list item
        elif isinstance(v, list):
            for item in v:
                if item is None:
                    continue
                s = str(item).strip()
                if not s:
                    continue
                codes.extend(_split_codes(s))
        else:
            codes = _split_codes(str(v))

        codes = _dedupe_preserve_order(codes)
        if not codes:
            raise ValueError("consignee_codes contain at least 1 codes")
        return codes


class EvidenceItem(BaseModel):
    doc_id: str
    container_number: Optional[str] = None
    field_used: Optional[List[str]] = None


class ChatAnswer(BaseModel):
    conversation_id: str
    intent: Optional[str] = None
    answer: str
    notices: Optional[List[str]] = None
    evidence: Optional[List[EvidenceItem]] = None
