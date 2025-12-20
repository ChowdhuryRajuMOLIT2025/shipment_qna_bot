from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class RetrievalPlan(TypedDict):
    query_text: str
    top_k: int
    vector_k: int
    extra_filter: Optional[str]
    include_total_count: Optional[bool]
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

    # --- Extraction ---
    # We use 'add' reducer to accumulate entities if multiple nodes find them (though usually just one extractor)
    # For now, simple overwrite is fine, but 'add' is safer for lists.
    extracted_ids: Dict[str, List[str]]  # e.g. {'container': ['ABCD123'], 'po': []}
    time_window_days: Optional[int]

    # --- Intent ---
    intent: Optional[str]
    sub_intents: List[str]

    # --- Retrieval ---
    retrieval_plan: Optional[RetrievalPlan]
    hits: List[Dict[str, Any]]  # aligned with nodes
    idx_analytics: Optional[Dict[str, Any]]  # e.g. {count: 10, facets: ...}

    # --- Output ---
    answer_text: Optional[str]  # aligned with nodes
    citations: List[Dict[str, Any]]
    chart_spec: Optional[Dict[str, Any]]

    # --- Errors/Notices ---
    errors: Annotated[List[str], add]
    notices: Annotated[List[str], add]
