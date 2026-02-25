# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/reindex_data.py shipment_dec25.jsonl

# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/ingest_all.py


import glob
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import timedelta

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from reindex_data import flatten_document, load_data

from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import \
    AzureOpenAIEmbeddingsClient


def robust_upload(tool, docs, batch_size=100, max_retries=3):
    """Uploads documents in batches with retry logic."""
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


def _manifest_path(processed_dir: str) -> str:
    return os.path.join(processed_dir, "manifest.json")


def load_manifest(processed_dir: str) -> dict:
    path = _manifest_path(processed_dir)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to read manifest: {e}. Starting fresh.")
        return {}


def save_manifest(processed_dir: str, manifest: dict) -> None:
    path = _manifest_path(processed_dir)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def compute_doc_hash(doc: dict) -> str:
    payload = {
        "content": doc.get("content", ""),
        "metadata": doc.get("metadata") or {},
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _deadletter_path(data_dir: str, file_name: str) -> str:
    failed_dir = os.path.join(data_dir, "failed")
    os.makedirs(failed_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(file_name))[0]
    return os.path.join(failed_dir, f"{base}.failed.jsonl")


def write_deadletter(data_dir: str, file_name: str, errors: list[dict]) -> str:
    path = _deadletter_path(data_dir, file_name)
    with open(path, "w", encoding="utf-8") as f:
        for err in errors:
            f.write(json.dumps(err, ensure_ascii=True) + "\n")
    return path


def ingest_all(*, allow_partial: bool = False) -> None:
    start_time = time.perf_counter()
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )
    processed_dir = os.path.join(data_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    manifest = load_manifest(processed_dir)

    jsonl_files = glob.glob(os.path.join(data_dir, "*.jsonl"))

    if not jsonl_files:
        print(f"No .jsonl files found in {data_dir}")
        return

    print(f"Found {len(jsonl_files)} files to ingest.")
    jsonl_files.sort()

    embedder = AzureOpenAIEmbeddingsClient()
    tool = AzureAISearchTool()

    for file_path in jsonl_files:
        file_name = os.path.basename(file_path)
        print(f"\n--- Starting ingestion for {file_name} ---")

        try:
            raw_docs = load_data(file_path)
        except Exception as e:
            print(f"Failed to read {file_name}: {e}. Skipping.")
            continue

        print(f"Loaded {len(raw_docs)} documents.")

        processed_docs = []
        pending_manifest = dict(manifest)
        skipped = 0
        errors: list[dict] = []
        print(f"Processing and embedding...")
        for i, d in enumerate(raw_docs):
            try:
                doc_id = d.get("document_id")
                if not doc_id:
                    raise ValueError("Missing document_id")
                content_hash = compute_doc_hash(d)
                if pending_manifest.get(str(doc_id)) == content_hash:
                    skipped += 1
                    continue
                processed_docs.append(flatten_document(d, embedder))
                pending_manifest[str(doc_id)] = content_hash
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1}/{len(raw_docs)} docs...")
            except Exception as e:
                errors.append(
                    {
                        "document_id": d.get("document_id"),
                        "error": str(e),
                        "document": d,
                    }
                )
                print(f"Failed to process doc {d.get('document_id')}: {e}")

        total_expected = len(raw_docs)
        total_handled = skipped + len(processed_docs)
        if errors:
            deadletter = write_deadletter(data_dir, file_name, errors)
            print(
                f"ERROR: {len(errors)} docs failed. Wrote dead-letter to {deadletter}."
            )
            if not allow_partial:
                raise RuntimeError(
                    f"Aborting ingestion for {file_name}; fix errors and retry."
                )

        if processed_docs:
            print(f"Uploading {len(processed_docs)} docs in batches...")
            try:
                robust_upload(tool, processed_docs)
                print(f"Finished ingestion for {file_name}")

                if total_handled == total_expected and not errors:
                    # Move to processed folder
                    dest_path = os.path.join(processed_dir, file_name)
                    shutil.move(file_path, dest_path)
                    print(f"Moved {file_name} to 'processed' folder.")
                    manifest = pending_manifest
                    save_manifest(processed_dir, manifest)
                else:
                    print(
                        f"WARNING: {file_name} not moved to processed; counts mismatch or errors present."
                    )
            except Exception as e:
                print(f"ERROR: Complete ingestion failed for {file_name}: {e}")
                print(f"File {file_name} remains in data folder for retry.")
        else:
            print(f"No documents to upload for {file_name} (skipped {skipped}).")
            try:
                if total_handled == total_expected and not errors:
                    dest_path = os.path.join(processed_dir, file_name)
                    shutil.move(file_path, dest_path)
                    print(f"Moved {file_name} to 'processed' folder.")
                    manifest = pending_manifest
                    save_manifest(processed_dir, manifest)
                else:
                    print(
                        f"WARNING: {file_name} not moved to processed; counts mismatch or errors present."
                    )
            except Exception as e:
                print(f"Failed to move {file_name} to processed: {e}")

    end_time = time.perf_counter()
    delta = end_time - start_time

    print(f"Total time taken: {timedelta(seconds=round(delta))}")
    print("\n--- All ingestions complete! ---")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest all JSONL files under data/ into the Azure Search index."
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Upload docs that processed successfully even if some failed. "
        "Files with errors stay in data/ for retry.",
    )
    args = parser.parse_args()

    ingest_all(allow_partial=args.allow_partial)
