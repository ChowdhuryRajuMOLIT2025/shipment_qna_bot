# src/shipment_qna_bot/tools/duckdb_engine.py

import json  # type: ignore
import os  # type: ignore
import re
from typing import Any, Dict, List, Optional  # type: ignore

import numpy as np
import pandas as pd

from shipment_qna_bot.logging.logger import logger


class DuckDBAnalyticsEngine:
    """
    Executes SQL queries on Parquet files using DuckDB.
    Maintains compatibility with the existing Pandas engine's response structure.
    """

    def __init__(self, db_path: str = ":memory:"):
        import duckdb

        self.db_path = db_path
        self.con = duckdb.connect(self.db_path)

    @staticmethod
    def _strip_code_fences(code: str) -> str:
        cleaned = (code or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(
                r"^```(?:sql|python)?\s*", "", cleaned, flags=re.IGNORECASE
            )
            cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    @staticmethod
    def _to_json_safe_value(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (pd.Timestamp, pd.Timedelta)):
            return str(v)
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if isinstance(v, dict):
            return {
                str(k): DuckDBAnalyticsEngine._to_json_safe_value(val)  # type: ignore
                for k, val in v.items()  # type: ignore
            }
        if isinstance(v, (list, tuple, set)):
            return [DuckDBAnalyticsEngine._to_json_safe_value(x) for x in v]  # type: ignore
        if hasattr(v, "item"):  # numpy types
            try:
                return v.item()
            except Exception:
                return str(v)
        return v

    def execute_query(
        self, parquet_path: str, sql: str, consignee_codes: List[str]
    ) -> Dict[str, Any]:
        """
        Executes a SQL query against ds.
        Applies RLS automatically.
        """
        sql = self._strip_code_fences(sql)
        logger.info(f"DDB Engine running: {parquet_path}")
        logger.info(f"QRY:\n{sql}")

        try:
            import duckdb

            # Create an isolated connection for this query execution solely.
            con = duckdb.connect(self.db_path)

            safe_codes = [c.replace("'", "''") for c in consignee_codes]
            codes_str = ", ".join([f"'{c}'" for c in safe_codes])

            rls_query = f"""
                CREATE OR REPLACE VIEW df AS 
                SELECT * FROM read_parquet('{parquet_path}')
                WHERE list_has_any(consignee_codes, [{codes_str}]::VARCHAR[])
            """
            con.execute(rls_query)

            rel = con.sql(sql)

            if rel is None:  # type: ignore
                con.close()
                return {
                    "success": True,
                    "output": "Query executed successfully (no result set).",
                    "result": "",
                    "final_answer": "Success",
                }

            # 3. Convert to Pandas for compatibility
            df_result = rel.df()

            # 4. Check for effectively empty results
            # Note: A count(*) query on an empty view returns 1 row with value 0.
            # We want to detect if the underlying data was empty for better UI responses.
            is_scalar_count = (
                len(df_result) == 1
                and len(df_result.columns) == 1
                and any(
                    c.lower() in df_result.columns[0].lower()
                    for c in ["count", "total", "sum"]
                )
            )

            # Calculate underlying row count if not already known
            underlying_count = con.sql("SELECT count(*) FROM df").fetchone()[0]  # type: ignore

            if underlying_count == 0 and not is_scalar_count:
                con.close()
                return {
                    "success": True,
                    "output": "",
                    "result": "",
                    "final_answer": "No rows matched your filters.",
                    "filtered_rows": 0,
                    "result_columns": [str(c) for c in df_result.columns.tolist()],
                    "result_rows": [],
                    "result_value": [],
                }

            result_columns = [str(c) for c in df_result.columns.tolist()]
            table_df = df_result.copy()
            table_df = table_df.replace({np.nan: None})

            result_rows = [
                {str(k): self._to_json_safe_value(v) for k, v in row.items()}
                for row in table_df.to_dict(orient="records")
            ]

            result_export = table_df.to_markdown(index=False)

            return {
                "success": True,
                "output": "",
                "result": result_export,
                "final_answer": result_export.strip(),
                "result_type": "DataFrame",
                "filtered_rows": underlying_count,
                "result_columns": result_columns,
                "result_rows": result_rows,
                "result_value": result_rows,
            }

            con.close()
            return result

        except Exception as e:
            logger.error(f"QRY execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": "",
            }
