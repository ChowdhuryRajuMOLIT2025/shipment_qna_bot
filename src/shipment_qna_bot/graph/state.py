# src/shipment_qna_bot/graph/state.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from typing_extensions import NotRequired, TypedDict


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
