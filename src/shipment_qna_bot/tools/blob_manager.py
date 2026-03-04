# src/shipment_qna_bot/tools/blob_manager.py

import glob
import os
from datetime import datetime, timedelta  # type: ignore
from typing import Dict, List, Optional

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
    Manages the lifecycle of the local cache for analytics.
    provides filtered DataFrames to the application.
    """

    _MASTER_DF_CACHE: Optional[pd.DataFrame] = None
    _FILTERED_CACHE: Dict[str, pd.DataFrame] = {}
    _LAST_LOAD_DATE: Optional[str] = None

    def __init__(self, cache_dir: str = "data_cache"):
        self.cache_dir = cache_dir
        self._test_mode = is_test_mode()

        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        self.conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_UPLD")
        self.blob_name = os.getenv("AZURE_STORAGE_BLOB_NAME", "master_ds.parquet")

    def _get_today_str(self) -> str:
        return get_today_date()

    def _get_cache_path(self, date_str: str) -> str:
        suffix = ".test" if self._test_mode else ""
        return os.path.join(self.cache_dir, f"master_{date_str}{suffix}.parquet")

    def _cleanup_old_cache(self, current_date_str: str):
        """
        Removes any files that do not match the current date and mode.
        """
        target_fname = os.path.basename(self._get_cache_path(current_date_str))
        pattern = os.path.join(self.cache_dir, "master_*.parquet")
        for fpath in glob.glob(pattern):
            fname = os.path.basename(fpath)
            if fname != target_fname:
                try:
                    os.remove(fpath)
                    logger.info(f"Cleaned up old cache file: {fpath}")
                except OSError as e:
                    logger.warning(f"Failed to remove old cache file {fpath}: {e}")

    def download_master_data(self) -> str:
        """
        Ensures dataset for today is present.
        Returns the absolute path of file.
        """
        today = self._get_today_str()
        target_path = self._get_cache_path(today)

        self._cleanup_old_cache(today)

        if os.path.exists(target_path):
            return target_path

        if self._test_mode:
            logger.info("TEST MODE: Creating dummy master file.")
            # Create a mock DF for tests
            data = {  # type: ignore
                "consignee_codes": [["TEST"], ["OTHER"]],
                "shipment_status": ["DELIVERED", "IN_OCEAN"],
                "container_number": ["CONT123", "CONT456"],
            }
            # Ensure columns are present
            from shipment_qna_bot.tools.analytics_metadata import \
                ANALYTICS_METADATA

            for col in ANALYTICS_METADATA:
                if col not in data:
                    data[col] = [None, None]

            df = pd.DataFrame(data)  # type: ignore
            df.to_parquet(target_path)
            return target_path

        if not self.conn_str or not self.container_name:
            raise RuntimeError(
                "Missing Azure Blob env vars (CONNECTION_STRING or CONTAINER_NAME)."
            )

        logger.info(f"Reading! {self.blob_name} to {target_path}...")
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.conn_str,
                container_name=self.container_name,
                blob_name=self.blob_name,
            )

            with open(target_path, "wb") as my_blob:
                blob_data = blob_client.download_blob()
                blob_data.readinto(my_blob)

            logger.info("Memorized!")
            return target_path
        except Exception as e:
            if os.path.exists(target_path):
                os.remove(target_path)
            raise RuntimeError(f"Unable to read: {e}")

    def get_local_path(self) -> str:
        """
        Returns the absolute local path of today's df.
        Raed it if not present.
        """
        return self.download_master_data()

    def load_filtered_data(self, consignee_codes: List[str]) -> pd.DataFrame:
        """
        Loads the df and returns a filtered df for the given consignee_ids.
        Uses in-memory caching to avoid redundant I/O and CPU-heavy filtering.
        """
        if not consignee_codes:
            return pd.DataFrame()

        today = self._get_today_str()
        cache_key = "|".join(sorted(consignee_codes))

        if (
            BlobAnalyticsManager._LAST_LOAD_DATE == today
            and cache_key in BlobAnalyticsManager._FILTERED_CACHE
        ):
            logger.info("Consignee cache hit for codes: %s", consignee_codes[:2])
            return BlobAnalyticsManager._FILTERED_CACHE[cache_key]

        if (
            BlobAnalyticsManager._LAST_LOAD_DATE != today
            or BlobAnalyticsManager._MASTER_DF_CACHE is None
        ):
            logger.info("Cache miss or date rollover. Reading fresh...")
            file_path = self.download_master_data()

            requested_cols = list(ANALYTICS_METADATA.keys()) + [
                "consignee_codes",
                "document_id",
                "carr_eqp_uid",
            ]

            try:

                import pyarrow.parquet as pq  # type: ignore

                schema = pq.read_schema(file_path)  # type: ignore
                available_cols = set(schema.names)  # type: ignore
                actual_load_cols = [c for c in requested_cols if c in available_cols]

                logger.info(
                    "Pruning: Requesting %d/%d on top of available columns.",
                    len(actual_load_cols),
                    len(available_cols),  # type: ignore
                )

                full_df = pd.read_parquet(file_path, columns=actual_load_cols)

                for col, meta in ANALYTICS_METADATA.items():
                    if col in full_df.columns:
                        col_type = meta.get("type")
                        if col_type == "numeric":
                            full_df[col] = pd.to_numeric(full_df[col], errors="coerce")
                        elif col_type == "datetime":
                            full_df[col] = pd.to_datetime(full_df[col], errors="coerce")

                BlobAnalyticsManager._MASTER_DF_CACHE = full_df
                BlobAnalyticsManager._LAST_LOAD_DATE = today
                BlobAnalyticsManager._FILTERED_CACHE = (
                    {}
                )  # Invalidate all filtered slices
            except Exception as e:
                logger.error(f"Failed to read df: {e}")
                raise e

        df = BlobAnalyticsManager._MASTER_DF_CACHE
        target_col = "consignee_codes"

        try:
            exploded = df.explode(target_col)
            mask = exploded[target_col].isin(consignee_codes)
            valid_indices = exploded[mask].index.unique()

            filtered_df = df.loc[valid_indices].copy()

            BlobAnalyticsManager._FILTERED_CACHE[cache_key] = filtered_df

            logger.info(
                f"Filtered {len(filtered_df)} records for consignee {consignee_codes[:3]}..."
            )
            return filtered_df

        except Exception as e:
            logger.error(f"Filtering records failed: {e}")
            raise e
