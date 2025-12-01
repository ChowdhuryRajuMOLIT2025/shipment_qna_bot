# src/shipment_qna_bot/graph/state.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, field_validator
from typing_extensions import NotRequired, TypedDict


###################### consignee_codes validation ######################
# performing right way to handle parent-child relation while posting request using pydantic model
def _split_codes(s: str) -> List[str]:
    # Split by comma, strip whitespace, drop empties
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


################################## consignee_codes validation end ###########################

############################## Chat Request Payload Validation ##############################


class ChatRequest(BaseModel):
    question: str = Field(
        ...,
        description="User's natural language question",
        min_length=1,
    )
    consignee_codes: str = Field(
        # consignee_codes: Union[List[str], str] = Field(
        ...,
        description="Consignee hierarchy (parent first), list[str] preferred; comma-string also accepted",
    )
    conversation_id: Optional[str] = Field(
        None,
        description="Conversation/session identifier. Optional; server generates if missing.",
    )

    @field_validator("question", mode="before")
    @classmethod
    def normalize_question(cls, v: Any) -> str:
        if v is None:
            raise ValueError("question is required")
        q = str(v).strip()
        if not q:
            raise ValueError("question cannot be empty")
        return q

    @field_validator("conversation_id", mode="before")
    @classmethod
    def normalize_conversation_id(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @field_validator("consignee_codes", mode="before")
    @classmethod
    def normalize_consignee_codes(cls, v: Any) -> List[str]:
        """Normalizes consignee_codes into list[str], preserving order (parent first).
        Handles:
          - str: "0025833, 0001665"
          - list[str]: ["0025833","0001665"]
          - list[str] but comma-packed: ["0025833, 0001665"]
          - mixed list: ["0025833", "0001665, 0003717"]
        """
        if v is None:
            raise ValueError("consignee_codes is required")
        codes: List[str] = []

        if isinstance(v, str):
            codes = _split_codes(v)
        elif isinstance(v, list):
            for item in v:
                if item is None:
                    continue
                s = str(item).strip()
                if not s:
                    continue
                # split each element because many clients send comma-packed strings in list
                codes.extend(_split_codes(s))
        else:
            # anything else -> attempt string split
            codes = _split_codes(str(v))

        codes = _dedupe_preserve_order(codes)

        if not codes:
            raise ValueError("consignee_codes cannot be empty")
        return codes


############################### Chat Request Payload Validation End ###########################

############################## Evidence Model Validation #####################################


class EvidenceItem(BaseModel):
    doc_id: str
    container_number: Optional[str] = None
    fields_used: Optional[List[str]] = None


############################# Evidence Model Validation End ##################################

####################### Chat Answer Model Validation #####################################


class ChatAnswer(BaseModel):
    # IMPORTANT: return conversation_id so client UI can reuse it
    conversation_id: str

    intent: Optional[str] = None
    answer: str
    notices: Optional[List[str]] = None
    evidence: Optional[List[EvidenceItem]] = None


####################### Chat Answer Model Validation End #####################################


############################## Graph State #####################################
class GraphState(TypedDict):
    # holding user request context
    conversation_id: str
    question_raw: str
    consignee_codes: List[str]

    # derived working fields from raw input
    normalized_question: NotRequired[str] = ""
    intent: NotRequired[str] = ""  # status/eta_window/delay_reason/route...etc

    # ranking identifiers :[score, confidence]
    container_numbers: NotRequired[List[Tuple[str, float]]]
    po_numbers: NotRequired[List[Tuple[str, float]]]
    obl_numbers: NotRequired[List[Tuple[str, float]]]
    booking_numbers: NotRequired[List[Tuple[str, float]]]

    time_window_days: NotRequired[Optional[int]]

    # planning for retrieval of relevent docs from the vector store
    retrieval_plan: NotRequired[Dict[str, Any]]
    hits: NotRequired[
        List[Dict[str, Any]]
    ]  # e.g. {doc_id, store, any field related to jsonl metadata}
    claims: NotRequired[
        List[Dict[str, Any]]
    ]  # e.g. use for evidences {field, value, evidence_doc_id}

    # critical fields for final response and looping
    round: NotRequired[int]
    max_rounds: NotRequired[int]
    ungrounded: NotRequired[bool]
    leakage_attempt: NotRequired[bool]
    missing_field: NotRequired[List[str]]

    # final bot response fields
    answer_text: NotRequired[str]
    notices: NotRequired[List[str]]
    evidences: NotRequired[List[Dict[str, Any]]]

    def to_log_dict(self) -> Dict[str, Any]:
        """Small safe subset for logs (avoid dumping everything) fetched from doc metadata."""
        return {
            "conversation_id": self.conversation_id,
            "question_raw": self.question_raw,
            "consignee_codes": self.consignee_codes,
            "normalized_question": self.normalized_question,
            "intent": self.intent,
            "container_numbers": self.container_numbers,
            "po_numbers": self.po_numbers,
            "obl_numbers": self.obl_numbers,
            "booking_numbers": self.booking_numbers,
            "time_window_days": self.time_window_days,
            "retrieval_plan": self.retrieval_plan,
            "hits": self.hits,
            "claims": self.claims,
            "round": self.round,
            "max_rounds": self.max_rounds,
            "ungrounded": self.ungrounded,
            "leakage_attempt": self.leakage_attempt,
            "missing_field": self.missing_field,
            "answer_text": self.answer_text,
            "notices": self.notices,
            "evidences": self.evidences,
        }


############################## Graph State End #####################################
