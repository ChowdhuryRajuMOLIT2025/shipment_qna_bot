# src/shipment_qna_bot/tools/azure_ai_search.py

from __future__ import annotations

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from typing import Any, Dict, List, Optional

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient

from shipment_qna_bot.security.rls import build_search_filter

# from openai import AzureOpenAI

try:
    from azure.search.documents.models import VectorizedQuery
except Exception as err:
    VectorizedQuery = None


class AzureAISearchTool:
    """
    Hybrid search = BM25 keyword(semantic search) + vector query.
    ALWAYS applies consignee filter (RLS).
    NEVER show consignee_code_ids in the response.
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
        self._id_field = os.getenv("AZURE_SEARCH_ID_FIELD", "document_id")
        self._content_field = os.getenv("AZURE_SEARCH_CONTENT_FIELD", "chunk")
        self._container_field = os.getenv(
            "AZURE_SEARCH_CONTAINER_FIELD", "container_number"
        )
        self._metadata_field = os.getenv("AZURE_SEARCH_METADATA_FIELD", "metadata_json")

        # code-only field for consignee filter- RLS
        self._consignee_field = os.getenv(
            "AZURE_SEARCH_CONSIGNEE_FIELD", "consignee_code_ids"
        )
        self._consignee_is_collection = (
            os.getenv("AZURE_SEARCH_CONSIGNEE_IS_COLLECTION", "true").lower() == "true"
        )

        # vector field
        self._vector_field = os.getenv("AZURE_SEARCH_VECTOR_FIELD", "content_vector")

    def _consignee_filter(self, codes: List[str]) -> str:
        # Uses search.in for matching against a list.
        # For a simple STRING field: search.in(field, 'a,b', ',')
        # For a COLLECTION field, best practice is to store it as collection and filter with any().
        # We support both via env switch.
        if not codes:
            # No scope? We fail closed.
            return "false"

        clean_codes = [c.strip() for c in codes if c and c.strip()]
        if not clean_codes:
            return "false"

        # Collection field:
        # consignee_code_ids/any(c: search.in(c, '0000866,234567', ','))
        if self._consignee_is_collection:
            return build_search_filter(
                allowed_codes=clean_codes, field_name=self._consignee_field
            )

        # Legacy: plain string field (e.g., `consignee_codes` as a single string)
        # Escaping single quotes to keep OData happy
        safe_codes = [c.replace("'", "''") for c in clean_codes]
        joined = ",".join(safe_codes)
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
        include_total_count: bool = False,
        facets: Optional[List[str]] = None,
        skip: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Hybrid search entry point.

        NOTE:
        - `consignee_codes` MUST be the already-authorized scope (effective scope).
          Never pass raw payload values here. The API layer is responsible for using
          `resolve_allowed_scope` and only forwarding the allowed list.
        """
        base_filter = self._consignee_filter(consignee_codes)
        final_filter = (
            base_filter if not extra_filter else f"({base_filter}) and ({extra_filter})"
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
            "select": None,  # Retrieve all retrievable fields
            "skip": skip,
            "order_by": order_by,
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

        hits: List[Dict[str, Any]] = []
        for r in results:
            doc = dict(r)

            # Extract key fields using configured names
            container_number = doc.get(self._container_field)
            if not container_number:
                # Fallback check inside metadata_json if top-level missing
                raw_meta = doc.get(self._metadata_field)
                if isinstance(raw_meta, str):
                    try:
                        import json

                        meta_dict = json.loads(raw_meta)
                        container_number = meta_dict.get("container_number")
                    except:
                        pass
                elif isinstance(raw_meta, dict):
                    container_number = raw_meta.get("container_number")

            hit = {
                "doc_id": doc.get(self._id_field),
                "container_number": container_number,
                "content": doc.get(self._content_field),
                "score": doc.get("@search.score"),
                "reranker_score": doc.get("@search.reranker_score"),
            }
            # Include all other fields except vectors to avoid bloat
            for k, v in doc.items():
                if k not in hit and k != self._vector_field:
                    hit[k] = v

            hits.append(hit)

        return {
            "hits": hits,
            "count": results.get_count() if include_total_count else None,
            "facets": results.get_facets() if facets else None,
        }

    def upload_documents(self, documents: List[Dict[str, Any]]) -> None:
        """
        Uploads a batch of documents to the Azure Search index.
        """
        try:
            results = self._client.upload_documents(documents=documents)
            failed = [r for r in results if not r.succeeded]
            if failed:
                raise RuntimeError(
                    f"Failed to upload {len(failed)} documents. "
                    f"First error: {failed[0].error_message}"
                )
        except Exception as e:
            raise RuntimeError(f"Error uploading documents: {str(e)}")

    def clear_index(self) -> None:
        """
        Deletes ALL documents from the index. Use with caution.
        """
        try:
            # Azure Search doesn't have a simple "delete all", so we fetch all keys and delete.
            # However, for RAG scenarios, sometimes it's easier to just delete and recreate the index,
            # but here we'll try to delete docs by key if they exist.
            # A more efficient way for large indexes is checking the count and batching.
            results = self._client.search(
                search_text="*", select=[self._id_field], top=1000
            )
            keys_to_delete = [
                {"@search.action": "delete", self._id_field: r[self._id_field]}
                for r in results
            ]

            if keys_to_delete:
                self._client.upload_documents(documents=keys_to_delete)
                print(f"Deleted {len(keys_to_delete)} documents from index.")
            else:
                print("Index already empty.")
        except Exception as e:
            print(f"Warning during clear_index: {e}")
