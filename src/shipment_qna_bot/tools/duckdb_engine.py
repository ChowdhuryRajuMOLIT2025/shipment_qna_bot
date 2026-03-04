# src/shipment_qna_bot/tools/duckdb_engine.py

import re
from typing import Any, Dict, List, Optional  # type: ignore

import duckdb
import numpy as np
import pandas as pd

from shipment_qna_bot.logging.logger import logger


class DuckDBAnalyticsEngine:
    """
    Executes SQL queries on Parquet files using DuckDB.
    Returns a stable result structure that the rest of the app can consume.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.con = duckdb.connect(self.db_path)

    @staticmethod
    def _sql_quote(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

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

    @classmethod
    def _build_rls_filter(cls, consignee_codes: List[str]) -> str:
        safe_codes = [str(c).replace("'", "''") for c in consignee_codes if str(c)]
        codes_str = ", ".join([f"'{c}'" for c in safe_codes])
        return f"list_has_any(consignee_codes, [{codes_str}]::VARCHAR[])"

    @classmethod
    def _normalize_selector_values(cls, raw_values: Any) -> List[str]:
        if raw_values is None:
            return []
        if isinstance(raw_values, list):
            values = raw_values
        else:
            values = [raw_values]

        normalized: List[str] = []
        for value in values:
            if value is None:
                continue
            text = str(value).strip().upper()
            if text:
                normalized.append(text)
        return list(dict.fromkeys(normalized))

    @classmethod
    def _build_selector_filter(
        cls, selector: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        if not isinstance(selector, dict):
            return None

        raw_ids = selector.get("ids")
        if not isinstance(raw_ids, dict):
            return None

        clauses: List[str] = []

        container_ids = cls._normalize_selector_values(raw_ids.get("container_number"))
        if container_ids:
            quoted = ", ".join(cls._sql_quote(value) for value in container_ids)
            clauses.append(f"upper(CAST(container_number AS VARCHAR)) IN ({quoted})")

        for field in ("po_numbers", "booking_numbers", "obl_nos"):
            field_ids = cls._normalize_selector_values(raw_ids.get(field))
            if not field_ids:
                continue
            quoted = ", ".join(cls._sql_quote(value) for value in field_ids)
            clauses.append(
                f"({field} IS NOT NULL AND EXISTS ("
                f"SELECT 1 FROM UNNEST({field}) AS t(val) "
                f"WHERE upper(CAST(val AS VARCHAR)) IN ({quoted})"
                f"))"
            )

        if not clauses:
            return None

        return "(" + " OR ".join(clauses) + ")"

    def prepare_view(
        self,
        parquet_path: str,
        consignee_codes: List[str],
        selector: Optional[Dict[str, Any]] = None,
    ) -> None:
        parquet_sql = parquet_path.replace("'", "''")
        where_clauses = [self._build_rls_filter(consignee_codes)]

        selector_filter = self._build_selector_filter(selector)
        if selector_filter:
            where_clauses.append(selector_filter)

        where_sql = " AND ".join(f"({clause})" for clause in where_clauses if clause)
        if not where_sql:
            where_sql = "TRUE"

        rls_query = f"""
            CREATE OR REPLACE VIEW df AS
            SELECT * FROM read_parquet('{parquet_sql}')
            WHERE {where_sql}
        """
        self.con.execute(rls_query)

    def execute_query(
        self,
        parquet_path: str,
        sql: str,
        consignee_codes: List[str],
        selector: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes a SQL query against ds.
        Applies RLS automatically.
        """
        sql = self._strip_code_fences(sql)
        logger.info(f"DDB Engine running: {parquet_path}")
        logger.info(f"QRY:\n{sql}")

        try:
            self.prepare_view(parquet_path, consignee_codes, selector=selector)

            rel = self.con.sql(sql)

            if rel is None:  # type: ignore
                return {
                    "success": True,
                    "output": "Query executed successfully (no result set).",
                    "result": "",
                    "final_answer": "Success",
                }

            # 3. Convert to a tabular result shape the app already understands.
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
            underlying_count = self.con.sql("SELECT count(*) FROM df").fetchone()[0]  # type: ignore

            if underlying_count == 0 and not is_scalar_count:
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

        except Exception as e:
            logger.error(f"QRY execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": "",
            }
