# src/shipment_qna_bot/models/schemas.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

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
    """
    Request payload for the /api/chat endpoint.

    Notes
    -----
    - `question` is the raw natural-language query from the user.
    - `consignee_codes` are **what the caller claims** as their consignee
      hierarchy (e.g. ["PARENT", "CHILD1", "CHILD2"]). These are **not**
      automatically trusted as the effective RLS scope; the backend is
      expected to run them through a scope resolver to derive the actual
      allowed scope.
    - `conversation_id` identifies a logical chat session. Reusing the same
      conversation_id across requests allows the backend/graph to maintain
      short-term memory (window of turns). Sending a new conversation_id
      starts a fresh session.
    """

    question: str = Field(
        ..., description="User's query in natural language", min_length=1
    )

    consignee_codes: List[str] = Field(
        ...,
        description=(
            "Consignee hierarchy as provided by the caller, e.g. "
            "[PARENT, CHILD1, CHILD2]. This is payload-only and must be "
            "validated/filtered by the backend before being used for RLS."
        ),
        min_length=1,
    )

    conversation_id: Optional[str] = Field(
        None,
        description=(
            "Conversation/session identifier (UUID or similar). "
            "If omitted, the server generates a new ID. Reuse the same "
            "ID to continue the same conversation with shared short-term memory."
        ),
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
        """
        Normalize consignee_codes so that the model always sees:

        - A non-empty list of strings.
        - No duplicates.
        - Whitespace trimmed.
        - Supports:
          * Single comma-separated string (e.g. '0000866,234567')
          * List of strings, where each element may itself be comma-separated. (e.g. '0000866','234567')
        """
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
    """
    Reference to a specific underlying shipment document/record used
    to support the answer.
    """

    doc_id: str = Field(
        ...,
        description="Stable document identifier (e.g. carr_eqp_uid or document_id).",
    )
    container_number: Optional[str] = Field(
        None,
        description="Container number associated with this evidence, if applicable.",
    )
    field_used: Optional[List[str]] = Field(
        None,
        description=(
            "List of metadata fields from the document that were used to "
            "support the answer (e.g. ['eta_dp_date', 'delivery_to_consignee_date'])."
        ),
    )


class ChartSpec(BaseModel):
    """
    Optional chart specification for analytics-style questions.

    This is intentionally generic so that different frontends (Streamlit, .NET, JS) can interpret it as needed:
    - `kind` describes the chart type (e.g. 'bar', 'line', 'pie').
    - `data` is a list of row dicts.
    - `encodings` describes which keys map to axes/series (e.g. x='bucket', y='count').
    """

    kind: str = Field(
        ...,
        description="Chart type, e.g. 'bar', 'line', 'pie'.",
    )
    title: Optional[str] = Field(
        None,
        description="Human-readable chart title.",
    )
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Chart data points; each item is a row of dimension/measure values.",
    )
    encodings: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Encoding configuration, e.g. {'x': 'bucket', 'y': 'count', "
            "'color': 'status'} for consumption by the frontend."
        ),
    )


class TableSpec(BaseModel):
    """
    Optional table specification for analytics-style questions.
    """

    columns: List[str] = Field(
        ...,
        description="List of column names/headers.",
    )
    rows: List[Dict[str, Any]] = Field(
        ...,
        description="List of data rows; each item is a dict matching columns.",
    )
    title: Optional[str] = Field(
        None,
        description="Optional title for the table.",
    )


class ResponseMetadata(BaseModel):
    tokens: int
    cost_usd: float
    latency_ms: int


# old chat response model
# class ChatAnswer(BaseModel):
# conversation_id: str
# intent: Optional[str] = None
# answer: str
# notices: Optional[List[str]] = None
# evidence: Optional[List[EvidenceItem]] = None
# metadata: Optional[ResponseMetadata] = None


# new chat response model
class ChatAnswer(BaseModel):
    """
    Standard response envelope for the /api/chat endpoint.

    - `answer` is the natural-language response.
    - `intent` is the (possibly coarse) intent label chosen by the graph.
    - `notices` carries any important warnings or clarifications (e.g. "
      "default date window used, or partial data availability).
    - `evidence` lists the underlying documents/containers used to support
      factual claims.
    - `chart` and `table` are optional structured artifacts primarily for
      analytics-style questions, allowing frontends to render visualizations
      and tables without guessing from free text.
    """

    conversation_id: str = Field(
        ...,
        description="Conversation/session ID associated with this answer.",
    )
    intent: Optional[str] = Field(
        None,
        description="High-level intent label resolved by the graph (e.g. 'status', 'eta_window', 'analytics').",
    )
    answer: str = Field(
        ...,
        description="Natural-language answer text.",
    )
    notices: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of notices or clarifications added by the system "
            "(e.g. 'No explicit days provided; using default 7-day window')."
        ),
    )
    evidence: Optional[List[EvidenceItem]] = Field(
        default=None,
        description="Optional list of evidence items grounding the answer.",
    )

    # Analytics / visualization support
    chart: Optional[ChartSpec] = Field(
        default=None,
        description=(
            "Optional chart specification if the question triggered an analytics "
            "path (e.g. hot vs normal containers, delay buckets)."
        ),
    )
    table: Optional[TableSpec] = Field(
        default=None,
        description=(
            "Optional tabular data used to support the answer "
            "or drive visualizations."
        ),
    )
    metadata: Optional[ResponseMetadata] = None
