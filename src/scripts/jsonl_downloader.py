import argparse
import os
import sys
from pathlib import Path

from dotenv import find_dotenv, load_dotenv  # type: ignore

try:
    from azure.storage.blob import BlobServiceClient
except Exception as exc:
    raise SystemExit(
        "Missing dependency: azure-storage-blob. Install requirements.txt."
    ) from exc


def download_jsonl(
    *,
    conn_str: str,
    container_name: str,
    dest_dir: Path,
    prefix: str | None,
    overwrite: bool,
) -> int:
    dest_dir.mkdir(parents=True, exist_ok=True)
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)

    count = 0
    for blob in container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if not name or not name.lower().endswith(".jsonl"):
            continue
        out_path = dest_dir / Path(name).name
        if out_path.exists() and not overwrite:
            print(f"Skip existing: {out_path}")
            continue
        blob_client = container.get_blob_client(name)
        print(f"Downloading {name} -> {out_path}")
        with open(out_path, "wb") as f:
            stream = blob_client.download_blob(max_concurrency=4)
            stream.readinto(f)
        count += 1
    return count


def main() -> None:
    load_dotenv(find_dotenv(), override=True)

    parser = argparse.ArgumentParser(
        description="Download JSONL files from Azure Blob to local data/ directory."
    )
    parser.add_argument(
        "--container",
        default=os.getenv("AZURE_STORAGE_CONTAINER_UPLD", "shipment-csv-data"),
        help="Azure Blob container name.",
    )
    parser.add_argument(
        "--conn-str",
        default=os.getenv("AZURE_STORAGE_CONN_STR")
        or os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        help="Azure Storage connection string (env AZURE_STORAGE_CONN_STR or AZURE_STORAGE_CONNECTION_STRING).",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional blob prefix to filter downloads.",
    )
    parser.add_argument(
        "--dest",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "data"),
        help="Destination directory for downloaded JSONL files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files if present.",
    )
    args = parser.parse_args()

    if not args.conn_str:
        raise SystemExit(
            "Missing AZURE_STORAGE_CONN_STR or AZURE_STORAGE_CONNECTION_STRING (env or --conn-str)."
        )

    dest_dir = Path(args.dest).resolve()
    downloaded = download_jsonl(
        conn_str=args.conn_str,
        container_name=args.container,
        dest_dir=dest_dir,
        prefix=args.prefix,
        overwrite=args.overwrite,
    )
    print(f"Downloaded {downloaded} file(s) to {dest_dir}")

    scripts_dir = os.path.dirname(__file__)
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)

    from ingest_all import ingest_all  # type: ignore

    ingest_all()


if __name__ == "__main__":
    main()
