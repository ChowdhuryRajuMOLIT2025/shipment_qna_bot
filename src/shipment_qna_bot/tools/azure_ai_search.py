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
from shipment_qna_bot.utils.runtime import is_test_mode

try:
    from azure.search.documents.models import VectorizedQuery
except Exception as err:
    VectorizedQuery = None


class AzureAISearchTool:
    """
    Hybrid search = BM25 keyword(semantic search) + vector query.
    ALWAYS applies consignee filter (RLS).
    NEVER show consignee_code_ids, document_id in the response.
    """

    def __init__(self) -> None:
        self._test_mode = is_test_mode()
        if self._test_mode:
            self._client = None
            self._id_field = "document_id"
            self._content_field = "chunk"
            self._container_field = "container_number"
            self._metadata_field = "metadata_json"
            self._consignee_field = "consignee_code_ids"
            self._consignee_is_collection = True
            self._vector_field = "content_vector"
            return

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

        self._id_field = os.getenv("AZURE_SEARCH_ID_FIELD", "document_id")
        self._content_field = os.getenv("AZURE_SEARCH_CONTENT_FIELD", "chunk")
        self._container_field = os.getenv(
            "AZURE_SEARCH_CONTAINER_FIELD", "container_number"
        )
        self._metadata_field = os.getenv("AZURE_SEARCH_METADATA_FIELD", "metadata_json")

        self._consignee_field = os.getenv(
            "AZURE_SEARCH_CONSIGNEE_FIELD", "consignee_code_ids"
        )
        self._consignee_is_collection = (
            os.getenv("AZURE_SEARCH_CONSIGNEE_IS_COLLECTION", "true").lower() == "true"
        )

        self._vector_field = os.getenv("AZURE_SEARCH_VECTOR_FIELD", "content_vector")

    def _consignee_filter(self, codes: List[str]) -> str:
        # Uses search.in for matching against a list.
        # For a simple STRING field: search.in(field, 'a,b', ',')
        # For a COLLECTION field, store it as collection and filter with any().
        if not codes:
            return "false"

        clean_codes = [c.strip() for c in codes if c and c.strip()]
        if not clean_codes:
            return "false"

        # Collection field:
        # consignee_code_ids/any(c: search.in(c, '0000123,7234567', ','))
        if self._consignee_is_collection:
            return build_search_filter(
                allowed_codes=clean_codes, field_name=self._consignee_field
            )

        # Legacy: plain string field (e.g., `consignee_codes` as a single string)
        # Escaping single quotes to keep OData clean
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
        if self._test_mode:
            return {
                "hits": [],
                "count": 0 if include_total_count else None,
                "facets": None,
            }

        base_filter = self._consignee_filter(consignee_codes)
        final_filter = (
            base_filter if not extra_filter else f"({base_filter}) and ({extra_filter})"
        )
        select = [  # type: ignore
            self._id_field,
            self._content_field,
            self._container_field,
        ]

        kwargs: Dict[str, Any] = {
            "search_text": query_text or "*",
            "top": top_k,
            "filter": final_filter,
            "select": None,  # Retrieve all retrievable fields
            "skip": skip,
            "order_by": order_by,
        }

        if query_text and query_text != "*":
            kwargs["query_type"] = "semantic"
            # kwargs["semantic_configuration_name"] = "vec-sem-ana-semantic"
            kwargs["semantic_configuration_name"] = "default"

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

        results = self._client.search(**kwargs)  # type: ignore

        hits: List[Dict[str, Any]] = []
        for r in results:  # type: ignore
            doc = dict(r)  # type: ignore

            # Extract key fields using configured names
            container_number = doc.get(self._container_field)  # type: ignore
            if not container_number:
                # Fallback check inside metadata_json if top-level missing
                raw_meta = doc.get(self._metadata_field)  # type: ignore
                if isinstance(raw_meta, str):
                    try:
                        import json

                        meta_dict = json.loads(raw_meta)
                        container_number = meta_dict.get("container_number")
                    except:
                        pass
                elif isinstance(raw_meta, dict):
                    container_number = raw_meta.get("container_number")  # type: ignore

            hit = {  # type: ignore
                "doc_id": doc.get(self._id_field),  # type: ignore
                "container_number": container_number,
                "content": doc.get(self._content_field),  # type: ignore
                "score": doc.get("@search.score"),  # type: ignore
                "reranker_score": doc.get("@search.reranker_score"),  # type: ignore
            }
            # Include all other fields except vectors to avoid puffed
            for k, v in doc.items():  # type: ignore
                if k not in hit and k not in {
                    self._vector_field,
                    self._consignee_field,
                }:
                    hit[k] = v

            hits.append(hit)  # type: ignore

        return {
            "hits": hits,
            "count": results.get_count() if include_total_count else None,
            "facets": results.get_facets() if facets else None,  # type: ignore
        }

    def upload_documents(self, documents: List[Dict[str, Any]]) -> None:
        """
        Uploads a batch of documents to the Azure Search index.
        """
        try:
            results = self._client.upload_documents(documents=documents)  # type: ignore
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
            results = self._client.search(  # type: ignore
                search_text="*", select=[self._id_field], top=1000
            )
            keys_to_delete = [  # type: ignore
                {"@search.action": "delete", self._id_field: r[self._id_field]}
                for r in results  # type: ignore
            ]

            if keys_to_delete:
                self._client.upload_documents(documents=keys_to_delete)  # type: ignore
                print(f"Deleted {len(keys_to_delete)} documents from index.")  # type: ignore
            else:
                print("Index already empty.")
        except Exception as e:
            print(f"Warning during clear_index: {e}")
