import glob
import os
from datetime import datetime
from typing import List, Optional

import pandas as pd
from azure.storage.blob import BlobClient
from dotenv import find_dotenv, load_dotenv

from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.analytics_metadata import ANALYTICS_METADATA
from shipment_qna_bot.tools.date_tools import get_today_date
from shipment_qna_bot.utils.runtime import is_test_mode

load_dotenv(find_dotenv(), override=True)


class BlobAnalyticsManager:
    """
    Manages the lifecycle of the local Master Cache for analytics.
    Downloads the full dataset from Azure Blob Storage once per day
    and provides filtered DataFrames to the application.
    """

    def __init__(self, cache_dir: str = "data_cache"):
        self.cache_dir = cache_dir
        self._test_mode = is_test_mode()

        # Ensure cache directory exists
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        self.conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_UPLD")
        self.blob_name = os.getenv("AZURE_STORAGE_BLOB_NAME", "master_ds.parquet")

    def _get_today_str(self) -> str:
        # Use the unified date tool format if possible, or simple YYYY-MMM-DD
        # date_tools returns YYYY-MMM-DD (e.g. 2026-Jan-30).
        # We'll normalize to YYYY-MM-DD for simpler filenames
        return get_today_date()

    def _get_cache_path(self, date_str: str) -> str:
        return os.path.join(self.cache_dir, f"master_{date_str}.parquet")

    def _cleanup_old_cache(self, current_date_str: str):
        """
        Removes any master_*.parquet files that do not match the current date.
        """
        pattern = os.path.join(self.cache_dir, "master_*.parquet")
        for fpath in glob.glob(pattern):
            fname = os.path.basename(fpath)
            if fname != f"master_{current_date_str}.parquet":
                try:
                    os.remove(fpath)
                    logger.info(f"Cleaned up old cache file: {fpath}")
                except OSError as e:
                    logger.warning(f"Failed to remove old cache file {fpath}: {e}")

    def download_master_data(self) -> str:
        """
        Ensures the master dataset for today is present locally.
        Returns the absolute path to the local parquet file.
        """
        today = self._get_today_str()
        target_path = self._get_cache_path(today)

        # 1. Cleanup old cache
        self._cleanup_old_cache(today)

        # 2. Check if exists
        if os.path.exists(target_path):
            logger.info(f"Master cache found for {today} at {target_path}")
            return target_path

        if self._test_mode:
            # Mock file creation for test mode
            logger.info("TEST MODE: Creating dummy master parquet.")
            df = pd.DataFrame(
                [
                    {"id": "1", "consignee_code": "TEST", "status": "DELIVERED"},
                    {"id": "2", "consignee_code": "OTHER", "status": "IN_OCEAN"},
                ]
            )
            df.to_parquet(target_path)
            return target_path

        # 3. Download
        if not self.conn_str or not self.container_name:
            raise RuntimeError(
                "Missing Azure Blob env vars (CONNECTION_STRING or CONTAINER_NAME)."
            )

        logger.info(f"Downloading {self.blob_name} to {target_path}...")
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.conn_str,
                container_name=self.container_name,
                blob_name=self.blob_name,
            )

            with open(target_path, "wb") as my_blob:
                blob_data = blob_client.download_blob()
                blob_data.readinto(my_blob)

            logger.info("Download complete.")
            return target_path
        except Exception as e:
            # Clean up partial download if failed
            if os.path.exists(target_path):
                os.remove(target_path)
            raise RuntimeError(f"Blob download failed: {e}")

    def load_filtered_data(self, consignee_codes: List[str]) -> pd.DataFrame:
        """
        Loads the master dataset and returns a DataFrame filtered for the given consignee_ids.
        """
        if not consignee_codes:
            logger.warning(
                "No consignee codes provided for filtering. Returning empty DF."
            )
            return pd.DataFrame()

        file_path = self.download_master_data()

        try:
            # We assume the parquet has a column 'consignee_code' (singular) or similar.
            # Adjust column name as per schema. Assuming 'consignee_code' based on context.
            # Using pyarrow pushdown predicate if possible.
            # Note: 'consignee_code' must be a column in the parquet.

            # Using pandas read_parquet with filters (requires pyarrow)
            # filters=[('col', 'in', ['a', 'b'])]

            # Note: Filter format for read_parquet is [ (col, op, val), ... ] which are ANDed.
            # For "IN" logic with multiple codes, DNF (lists of lists) or specific 'in' operator support
            # depends on engine.
            # Pyarrow 'filters' parameter supports DNF: [[('col', '==', 'a'), ('col', '==', 'b')]] is (A or B).
            # But let's verify if 'in' operator is supported in recent pyarrow.
            # Safe bet: Load columns, then filter in pandas if dataset fits in memory.
            # Given requirement "high-throughput Data Loader", trying pushdown is better.

            # Let's try standard filtering in pandas after load for robustness first,
            # as pushdown requires partitioned datasets often for strict row skipping.

            df = pd.read_parquet(file_path)

            # Filter
            # The parquet column is 'consignee_codes' (list of strings).
            target_col = "consignee_codes"
            if target_col not in df.columns:
                # fallback or check if we have data at all
                logger.warning(
                    f"Column '{target_col}' not found in dataset. Columns: {df.columns}"
                )
                return pd.DataFrame()  # fail safe

            # Apply filter: overlap between row's [codes] and user's [codes]
            # Efficient method: Explode -> IsIn -> Index Unique -> Slice
            # This handles if a shipment belongs to multiple consignees (rare but possible with shared IDs).

            # Ensure column is treated as list (in case it loaded as string representation)
            # If it's pure parquet list, pandas reads as array/list.

            # Optimization: If the dataset is huge, this might be memory intensive.
            # But for <100k rows, it's fine.

            # Check first row type to be safe
            if not df.empty and isinstance(df[target_col].iloc[0], str):
                # If it looks like "['A', 'B']", validation needed.
                # But assuming standard parquet list support.
                pass

            exploded = df.explode(target_col)
            mask = exploded[target_col].isin(consignee_codes)
            valid_indices = exploded[mask].index.unique()

            filtered_df = df.loc[valid_indices].copy()

            # Automatic Type Casting based on Metadata
            for col, meta in ANALYTICS_METADATA.items():
                if col in filtered_df.columns:
                    col_type = meta.get("type")
                    if col_type == "numeric":
                        filtered_df[col] = pd.to_numeric(
                            filtered_df[col], errors="coerce"
                        )
                    elif col_type == "datetime":
                        filtered_df[col] = pd.to_datetime(
                            filtered_df[col], errors="coerce"
                        )

            logger.info(
                f"Loaded {len(filtered_df)} rows for codes {consignee_codes[:3]}..."
            )
            return filtered_df

        except Exception as e:
            logger.error(f"Failed to load/filter parquet: {e}")
            raise e
