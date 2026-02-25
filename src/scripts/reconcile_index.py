import argparse
import glob
import json
import os
import sys
import time
from typing import Dict, Iterable, List, Tuple

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from ingest_all import (compute_doc_hash, load_manifest, save_manifest,
                        write_deadletter)
from reindex_data import flatten_document, load_data

from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient


def _get_search_client() -> Tuple[SearchClient, str]:
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")
    if not endpoint or not api_key or not index_name:
        raise RuntimeError(
            "Missing Azure Search env vars. "
            "Need AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_API_KEY, AZURE_SEARCH_INDEX_NAME."
        )
    cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()
    return (
        SearchClient(endpoint=endpoint, credential=cred, index_name=index_name),
        index_name,
    )


def _fetch_index_ids(client: SearchClient, id_field: str, batch_size: int) -> List[str]:
    results = client.search(
        search_text="*",
        select=[id_field],
        top=batch_size,
        include_total_count=True,
    )
    total = results.get_count() or 0
    ids = [str(r[id_field]) for r in results]
    for skip in range(batch_size, total, batch_size):
        page = client.search(
            search_text="*",
            select=[id_field],
            top=batch_size,
            skip=skip,
        )
        ids.extend([str(r[id_field]) for r in page])
    return ids


def _load_jsonl_docs(data_dir: str) -> Dict[str, dict]:
    files = sorted(glob.glob(os.path.join(data_dir, "*.jsonl")))
    if not files:
        raise RuntimeError(f"No .jsonl files found in {data_dir}")

    docs_by_id: Dict[str, dict] = {}
    duplicates = 0
    for path in files:
        for doc in load_data(path):
            doc_id = doc.get("document_id")
            if doc_id is None:
                continue
            doc_id_str = str(doc_id)
            if doc_id_str in docs_by_id:
                duplicates += 1
                continue
            docs_by_id[doc_id_str] = doc
    if duplicates:
        print(f"WARNING: {duplicates} duplicate document_id rows ignored.")
    return docs_by_id


def _write_id_list(path: str, ids: Iterable[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for doc_id in sorted(ids):
            f.write(f"{doc_id}\n")


def _robust_upload(
    tool: AzureAISearchTool,
    docs: List[dict],
    batch_size: int,
    max_retries: int,
) -> None:
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        success = False
        retries = 0
        while not success and retries < max_retries:
            try:
                tool.upload_documents(batch)
                success = True
                print(
                    f"  Uploaded batch {i // batch_size + 1}/{(len(docs) - 1) // batch_size + 1}"
                )
            except Exception as e:
                retries += 1
                wait = retries * 5
                print(
                    f"  Batch upload failed (attempt {retries}): {e}. Retrying in {wait}s..."
                )
                time.sleep(wait)
        if not success:
            raise RuntimeError(
                f"Failed to upload batch starting at index {i} after {max_retries} attempts."
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile Azure Search index vs local JSONL and repair missing docs."
    )
    parser.add_argument(
        "--data-dir",
        default=os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "processed"
        ),
        help="Directory containing processed JSONL files.",
    )
    parser.add_argument(
        "--report-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "reports"),
        help="Directory to write reconciliation reports.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for fetching index IDs.",
    )
    parser.add_argument(
        "--upload-batch-size",
        type=int,
        default=100,
        help="Batch size for uploading missing documents.",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Only report missing docs; do not upload.",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Upload what succeeds even if some docs fail; writes dead-letter.",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip post-upload verification against the index.",
    )
    parser.add_argument(
        "--no-update-manifest",
        action="store_true",
        help="Skip manifest rebuild after successful reconciliation.",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    report_dir = os.path.abspath(args.report_dir)
    os.makedirs(report_dir, exist_ok=True)

    docs_by_id = _load_jsonl_docs(data_dir)
    jsonl_ids = set(docs_by_id.keys())
    current_hash_by_id = {
        doc_id: compute_doc_hash(doc) for doc_id, doc in docs_by_id.items()
    }

    manifest = load_manifest(data_dir)
    manifest_ids = set(manifest.keys())

    id_field = os.getenv("AZURE_SEARCH_ID_FIELD", "document_id")
    client, index_name = _get_search_client()
    index_ids = set(_fetch_index_ids(client, id_field, args.batch_size))

    missing_in_index = sorted(jsonl_ids - index_ids)
    extra_in_index = sorted(index_ids - jsonl_ids)
    missing_in_manifest = sorted(jsonl_ids - manifest_ids)
    extra_in_manifest = sorted(manifest_ids - jsonl_ids)
    changed_by_cdc = sorted(
        doc_id
        for doc_id in jsonl_ids
        if manifest.get(doc_id) != current_hash_by_id[doc_id]
    )
    upload_candidates = sorted(set(missing_in_index) | set(changed_by_cdc))

    report = {
        "index_name": index_name,
        "jsonl_count": len(jsonl_ids),
        "index_count": len(index_ids),
        "manifest_count": len(manifest_ids),
        "missing_in_index": len(missing_in_index),
        "extra_in_index": len(extra_in_index),
        "missing_in_manifest": len(missing_in_manifest),
        "extra_in_manifest": len(extra_in_manifest),
        "changed_by_cdc": len(changed_by_cdc),
        "upload_candidates": len(upload_candidates),
    }
    report_path = os.path.join(report_dir, "reconcile_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    missing_index_path = os.path.join(report_dir, "missing_in_index.txt")
    extra_index_path = os.path.join(report_dir, "extra_in_index.txt")
    missing_manifest_path = os.path.join(report_dir, "missing_in_manifest.txt")
    extra_manifest_path = os.path.join(report_dir, "extra_in_manifest.txt")
    changed_cdc_path = os.path.join(report_dir, "changed_by_cdc.txt")
    upload_candidates_path = os.path.join(report_dir, "upload_candidates.txt")
    _write_id_list(missing_index_path, missing_in_index)
    _write_id_list(extra_index_path, extra_in_index)
    _write_id_list(missing_manifest_path, missing_in_manifest)
    _write_id_list(extra_manifest_path, extra_in_manifest)
    _write_id_list(changed_cdc_path, changed_by_cdc)
    _write_id_list(upload_candidates_path, upload_candidates)

    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"Missing IDs list: {missing_index_path}")
    print(f"Extra IDs list: {extra_index_path}")

    if args.no_upload or not upload_candidates:
        return

    print(
        f"Re-embedding and upserting {len(upload_candidates)} docs "
        f"(missing={len(missing_in_index)}, changed={len(changed_by_cdc)})."
    )
    embedder = AzureOpenAIEmbeddingsClient()
    tool = AzureAISearchTool()
    processed_docs: List[dict] = []
    errors: List[dict] = []

    for i, doc_id in enumerate(upload_candidates, start=1):
        doc = docs_by_id[doc_id]
        try:
            processed_docs.append(flatten_document(doc, embedder))
            if i % 50 == 0:
                print(f"Processed {i}/{len(upload_candidates)} upsert docs...")
        except Exception as e:
            errors.append(
                {
                    "document_id": doc_id,
                    "error": str(e),
                    "document": doc,
                }
            )

    if errors:
        base_data_dir = (
            os.path.dirname(data_dir)
            if os.path.basename(data_dir) == "processed"
            else data_dir
        )
        deadletter = write_deadletter(base_data_dir, "reconcile_missing.jsonl", errors)
        print(f"ERROR: {len(errors)} docs failed. Wrote dead-letter to {deadletter}.")
        if not args.allow_partial:
            return

    if processed_docs:
        _robust_upload(
            tool,
            processed_docs,
            batch_size=args.upload_batch_size,
            max_retries=3,
        )

    if args.no_verify:
        return

    index_ids_after = set(_fetch_index_ids(client, id_field, args.batch_size))
    missing_after = sorted(jsonl_ids - index_ids_after)
    if missing_after:
        missing_after_path = os.path.join(report_dir, "missing_after_upload.txt")
        _write_id_list(missing_after_path, missing_after)
        print(
            f"ERROR: {len(missing_after)} docs still missing after upload. "
            f"See {missing_after_path}."
        )
        return

    if args.no_update_manifest:
        return

    new_manifest = dict(manifest)
    for doc_id in upload_candidates:
        new_manifest[doc_id] = current_hash_by_id[doc_id]
    save_manifest(data_dir, new_manifest)
    print(f"Manifest updated with {len(new_manifest)} entries.")


if __name__ == "__main__":
    main()
