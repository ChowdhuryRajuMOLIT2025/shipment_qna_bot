# src/shipment_qna_bot/tools/azure_ai_search.py

from __future__ import annotations

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from typing import Any, Dict, List, Optional

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient

# from openai import AzureOpenAI

try:
    from azure.search.documents.models import VectorizedQuery
except Exception as err:
    VectorizedQuery = None


class AzureAISearchTool:
    """
    Hybrid search = BM25 keyword(semantic search) + vector query.
    ALWAYS applies consignee filter (RLS).
    """

    def __init__(self) -> None:
        endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        api_key = os.getenv("AZURE_SEARCH_API_KEY")
        index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")

        if not endpoint or not api_key or not index_name:
            raise RuntimeError(
                "Missing Azure Search env vars. "
                "Need AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_API_KEY, AZURE_SEARCH_INDEX_NAME."
            )
        cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()

        self._client = SearchClient(
            endpoint=endpoint,
            credential=cred,
            index_name=index_name,
        )

        # configured field names in az-index
        self._id_field = os.getenv("AZURE_SEARCH_ID_FIELD", "chunk_id")
        self._content_field = os.getenv("AZURE_SEARCH_CONTENT_FIELD", "chunk")
        self._container_field = os.getenv(
            "AZURE_SEARCH_CONTAINER_FIELD", "container_number"
        )

        # code-only field for consignee filter- RLS
        self._consignee_field = os.getenv(
            "AZURE_SEARCH_CONSIGNEE_FIELD", "consignee_codes"
        )
        self._consignee_field_collection = os.getenv(
            "AZURE_SEARCH_CONSIGNEE_FIELD", "consignee_codes"
        )

        # IMPORTANT: should be code-only, ideally a collection field in the index
        self._consignee_field = os.getenv(
            "AZURE_SEARCH_CONSIGNEE_FIELD", "consignee_codes"
        )

        # vector field
        self._vector_field = os.getenv("AZURE_SEARCH_VECTOR_FIELD", "text_vector")

    def _consignee_filter(self, codes: List[str]) -> str:
        # Uses search.in for matching against a list.
        # For a simple STRING field: search.in(field, 'a,b', ',')
        # For a COLLECTION field, best practice is to store it as collection and filter with any().
        # We support both via env switch.
        if not codes:
            # No scope? We fail closed.
            return "false"

        joined = ",".join([c.strip() for c in codes if c and c.strip()])

        # Collection field:
        # consignee_code_ids/any(c: search.in(c, '0000866,234567', ','))
        if self._consignee_is_collection:
            return f"{self._consignee_field}/any(c: search.in(c, '{joined}', ','))"

        # Plain string field:
        # search.in(consignee_codes, '0000866,234567', ',')
        return f"search.in({self._consignee_field}, '{joined}', ',')"

    def search(
        self,
        *,
        query_text: str,
        consignee_codes: List[str],
        top_k: int = 10,
        vector: Optional[List[float]] = None,
        vector_k: int = 30,
        extra_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        base_filter = self._consignee_filter(consignee_codes)
        final_filter = (
            base_filter if not extra_filter else f"({base_filter}) AND ({extra_filter})"
        )
        select = [
            self._id_field,
            self._content_field,
            self._container_field,
            self._consignee_field,
        ]

        kwargs: Dict[str, Any] = {
            "search_text": query_text or "*",
            "top": top_k,
            "filter": final_filter,
            "select": select,
        }

        if vector is not None and vector:
            if VectorizedQuery is None:
                raise RuntimeError(
                    "VectorizedQuery not available in your azure-search-documents version."
                )
            kwargs["vector_queries"] = [
                VectorizedQuery(
                    vector=vector,
                    k_nearest_neighbors=vector_k,
                    fields=self._vector_field,
                )
            ]

        results = self._client.search(**kwargs)

        out: List[Dict[str, Any]] = []
        for r in results:
            doc = dict(r)
            out.append(
                {
                    "doc_id": doc.get(self._id_field),
                    "container_number": doc.get(self._container_field),
                    "content": doc.get(self._content_field),
                    "score": doc.get("@search.score"),
                    "reranker_score": doc.get("@search.reranker_score"),
                }
            )
        return out
