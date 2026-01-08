from operator import add
from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class RetrievalPlan(TypedDict):
    query_text: str
    top_k: int
    vector_k: int
    extra_filter: Optional[str]
    post_filter: Optional[Dict[str, Any]]
    include_total_count: Optional[bool]
    skip: Optional[int]
    order_by: Optional[str]
    reason: str
    hybrid_weights: Optional[Dict[str, float]]


class GraphState(TypedDict):
    """
    Represents the state of the conversation graph.
    """

    # --- Input ---
    question_raw: str  # Original question
    normalized_question: Optional[str]
    messages: Annotated[List[BaseMessage], add_messages]

    # --- Context ---
    conversation_id: str
    trace_id: str
    consignee_codes: List[str]  # aligned with nodes
    today_date: Optional[str]
    now_utc: Optional[str]

    # --- Extraction ---
    # We use 'add' reducer to accumulate entities if multiple nodes find them (though usually just one extractor)
    # For now, simple overwrite is fine, but 'add' is safer for lists items.
    extracted_ids: Dict[
        str, List[str]
    ]  # e.g. {'container': ['ABCD123'], 'po': [], 'obl': []}
    time_window_days: Optional[int]

    # --- Intent ---
    intent: Optional[str]
    sub_intents: List[str]
    sentiment: Optional[str]  # e.g., "positive", "neutral", "negative"

    # --- Retrieval ---
    retrieval_plan: Optional[RetrievalPlan]
    hits: List[Dict[str, Any]]  # aligned with nodes
    idx_analytics: Optional[Dict[str, Any]]  # e.g. {count: 10, facets: ...}

    # --- Output ---
    answer_text: Optional[str]  # aligned with nodes
    citations: List[Dict[str, Any]]
    chart_spec: Optional[Dict[str, Any]]
    table_spec: Optional[Dict[str, Any]]

    # --- Control Flow ---
    retry_count: int
    max_retries: int
    is_satisfied: bool
    reflection_feedback: Optional[str]

    # --- Metrics ---
    usage_metadata: Dict[
        str, Any
    ]  # {prompt_tokens: int, completion_tokens: int, cost_usd: float}

    # --- Errors/Notices ---
    errors: Annotated[List[str], add]
    notices: Annotated[List[str], add]
