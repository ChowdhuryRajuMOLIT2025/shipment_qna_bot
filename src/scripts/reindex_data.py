# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/reindex_data.py shipment_dec25.jsonl

import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool
from shipment_qna_bot.tools.azure_openai_embeddings import AzureOpenAIEmbeddingsClient


def load_data(file_path: str) -> List[Dict[str, Any]]:
    documents = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                documents.append(record)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid line: {e}")
    return documents


def flatten_document(
    doc: Dict[str, Any], embedder: AzureOpenAIEmbeddingsClient
) -> Dict[str, Any]:
    metadata = doc.get("metadata", {})
    content = doc.get("content", "")
    doc_id = doc.get("document_id")

    if not doc_id or not content or not isinstance(metadata, dict):
        raise ValueError(
            "Invalid JSONL schema. Require document_id, content, and metadata dict."
        )

    # Consignee codes (RLS)
    consignee_codes = metadata.get("consignee_codes", [])
    if not consignee_codes:
        raw = doc.get("consignee_code")
        if raw:
            try:
                consignee_codes = json.loads(raw.replace("'", '"'))
            except:
                consignee_codes = [raw]
    if not consignee_codes:
        raise ValueError("Missing consignee_codes for RLS.")

    # Geenerate embedding
    print(f"Generating embedding for doc {doc_id}...")
    vector = embedder.embed_query(content)

    def to_list(val):
        if val is None:
            return []
        if isinstance(val, list):
            # Flatten any nested lists and ensure all elements are strings
            flat = []
            for item in val:
                if isinstance(item, str) and "," in item:
                    flat.extend([s.strip() for s in item.split(",") if s.strip()])
                else:
                    flat.append(str(item))
            return list(set(flat))
        if isinstance(val, str):
            if "," in val:
                return [s.strip() for s in val.split(",") if s.strip()]
            return [val.strip()]
        return [str(val)]

    def _meta(key: str) -> Any:
        return metadata.get(key)

    def _normalize_dt(val: Any) -> Any:
        if val is None:
            return None
        try:
            if str(val).strip().lower() in {"nat", "nan", "none", ""}:
                return None
        except Exception:
            pass
        if isinstance(val, str):
            s = val.strip()
            if not s:
                return None
            if s.lower() in {"nat", "nan", "none"}:
                return None
            if s.endswith("Z") or re.search(r"[+-]\d\d:\d\d$", s):
                return s
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
                    return s + "T00:00:00Z"
                if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", s):
                    return s + "Z"
                return s
        return val

    ata_dp_date = (
        _meta("derived_ata_dp_date")
        or _meta("ata_dp_date")
        or _meta("optimal_ata_dp_date")
    )
    best_eta_dp_date = (
        _meta("best_eta_dp_date")
        or _meta("optimal_ata_dp_date")
        or _meta("derived_ata_dp_date")
        or _meta("ata_dp_date")
    )
    eta_fd_date = _meta("optimal_eta_fd_date") or _meta("eta_fd_date")
    best_eta_fd_date = (
        _meta("best_eta_fd_date")
        or _meta("optimal_eta_fd_date")
        or _meta("revised_eta_fd_date")
        or _meta("eta_fd_date")
    )

    flattened = {
        "document_id": str(doc_id),
        "content": content,
        "content_vector": vector,
        "consignee_code_ids": to_list(consignee_codes),
        "container_number": metadata.get("container_number"),
        "po_numbers": to_list(metadata.get("po_numbers", [])),
        "obl_nos": to_list(
            metadata.get("obl_nos", metadata.get("ocean_bl_numbers", []))
        ),
        "booking_numbers": to_list(metadata.get("booking_numbers", [])),
        "hot_container_flag": bool(
            metadata.get("hot_container", metadata.get("hot_container_flag", False))
        ),
        "container_type": _meta("container_type"),
        "destination_service": _meta("destination_service"),
        "load_port": _meta("load_port"),
        "final_load_port": _meta("final_load_port"),
        "discharge_port": _meta("discharge_port"),
        "last_cy_location": _meta("last_cy_location"),
        "place_of_receipt": _meta("place_of_receipt"),
        "place_of_delivery": _meta("place_of_delivery"),
        "final_destination": _meta("final_destination"),
        "first_vessel_name": _meta("first_vessel_name"),
        "final_carrier_name": _meta("final_carrier_name"),
        "final_vessel_name": _meta("final_vessel_name"),
        "shipment_status": _meta("shipment_status"),
        "true_carrier_scac_name": _meta("true_carrier_scac_name"),
        "etd_lp_date": _normalize_dt(_meta("etd_lp_date")),
        "etd_flp_date": _normalize_dt(_meta("etd_flp_date")),
        "eta_dp_date": _normalize_dt(_meta("eta_dp_date")),
        "eta_fd_date": _normalize_dt(eta_fd_date),
        "best_eta_dp_date": _normalize_dt(best_eta_dp_date),
        "best_eta_fd_date": _normalize_dt(best_eta_fd_date),
        "atd_lp_date": _normalize_dt(_meta("atd_lp_date")),
        "ata_flp_date": _normalize_dt(_meta("ata_flp_date")),
        "atd_flp_date": _normalize_dt(_meta("atd_flp_date")),
        "ata_dp_date": _normalize_dt(ata_dp_date),
        "supplier_vendor_name": _meta("supplier_vendor_name"),
        "manufacturer_name": _meta("manufacturer_name"),
        "ship_to_party_name": _meta("ship_to_party_name"),
        "job_type": _meta("job_type"),
        "mcs_hbl": _meta("mcs_hbl"),
        "transport_mode": _meta("transport_mode"),
        # Metadata JSON blob
        "metadata_json": json.dumps(metadata),
    }

    # Handle dates if present
    # ... (simplifying for now, can add more later if needed)

    return flattened


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


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest JSONL data into Azure Search index."
    )
    parser.add_argument(
        "file_name",
        nargs="?",
        default="shipment_dec25.jsonl",
        help="Name of the file in data/ directory",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Upload docs that processed successfully even if some failed. "
        "Writes dead-letter for failures.",
    )
    args = parser.parse_args()

    data_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", args.file_name
    )
    if not os.path.exists(data_path):
        print(f"Data file not found: {data_path}")
        return

    raw_docs = load_data(data_path)
    print(f"Loaded {len(raw_docs)} documents from {args.file_name}.")

    embedder = AzureOpenAIEmbeddingsClient()

    processed_docs = []
    errors: list[dict] = []
    print(
        f"Starting processing and embedding (this may take a few minutes for {len(raw_docs)} docs)..."
    )

    for i, d in enumerate(raw_docs):
        try:
            processed_docs.append(flatten_document(d, embedder))
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

    if errors:
        deadletter = write_deadletter(
            os.path.dirname(data_path), args.file_name, errors
        )
        print(f"ERROR: {len(errors)} docs failed. Wrote dead-letter to {deadletter}.")
        if not args.allow_partial:
            return

    print(f"Uploading {len(processed_docs)} docs to the index...")
    tool = AzureAISearchTool()
    try:
        # Use batching for upload if possible, tool.upload_documents usually handles this
        tool.upload_documents(processed_docs)
        print("Full re-indexing complete!")
    except Exception as e:
        print(f"Upload failed: {e}")


if __name__ == "__main__":
    main()
