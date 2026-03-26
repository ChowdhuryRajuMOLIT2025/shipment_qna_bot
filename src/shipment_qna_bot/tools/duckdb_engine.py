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
    def _quote_ident(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

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
    def _normalize_sql_dialect(sql: str) -> str:
        """
        Convert common non-DuckDB SQL function names emitted by the LLM.
        """
        normalized = (sql or "").strip()

        # SQLite-style date function frequently appears in generated SQL.
        # DuckDB equivalent is `julian(...)`.
        normalized = re.sub(
            r"\bjulianday\s*\(", "julian(", normalized, flags=re.IGNORECASE
        )

        # MySQL-style DATE_ADD(unit, amount, date_expr) appears in repaired SQL.
        # Convert to DuckDB interval arithmetic: date_expr + INTERVAL amount UNIT.
        normalized = re.sub(
            r"\bdate_add\s*\(\s*'(?P<unit>day|days|month|months|year|years)'\s*,\s*(?P<amount>[+-]?\d+)\s*,\s*(?P<expr>[^\)]+?)\s*\)",
            lambda m: (
                f"({m.group('expr').strip()} + INTERVAL {m.group('amount')} {m.group('unit').upper().rstrip('S')})"
            ),
            normalized,
            flags=re.IGNORECASE,
        )

        return normalized

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
    def _build_rls_filter(
        cls, consignee_codes: List[str], schema: Dict[str, str]
    ) -> str:
        safe_codes = [
            str(c).strip().replace("'", "''") for c in consignee_codes if str(c).strip()
        ]
        if not safe_codes:
            return "FALSE"

        if "consignee_codes" in schema:
            codes_str = ", ".join([f"'{c}'" for c in safe_codes])
            return f"list_has_any(consignee_codes, [{codes_str}]::VARCHAR[])"

        if "consignee_code_multiple" in schema:
            # Example source values: "WILSON SPORTING GOODS, CO.(0000866)"
            clauses = [
                "regexp_matches("
                "upper(CAST(consignee_code_multiple AS VARCHAR)), "
                f"'(^|[^0-9A-Z]){code.upper()}([^0-9A-Z]|$)')"
                for code in safe_codes
            ]
            return "(" + " OR ".join(clauses) + ")"

        return "FALSE"

    def _get_parquet_schema(self, parquet_path: str) -> Dict[str, str]:
        parquet_sql = parquet_path.replace("'", "''")
        rows = self.con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_sql}')"
        ).fetchall()
        return {str(r[0]): str(r[1]) for r in rows}

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
        cls, selector: Optional[Dict[str, Any]], schema: Dict[str, str]
    ) -> Optional[str]:
        if not isinstance(selector, dict):
            return None

        raw_ids = selector.get("ids")
        if not isinstance(raw_ids, dict):
            return None

        clauses: List[str] = []

        container_ids = cls._normalize_selector_values(raw_ids.get("container_number"))
        if container_ids and "container_number" in schema:
            quoted = ", ".join(cls._sql_quote(value) for value in container_ids)
            clauses.append(f"upper(CAST(container_number AS VARCHAR)) IN ({quoted})")

        field_candidates = {
            "po_numbers": ["po_numbers", "po_number_multiple"],
            "booking_numbers": ["booking_numbers", "booking_number_multiple"],
            "obl_nos": ["obl_nos", "obl_no_multiple", "obl_number_multiple"],
        }

        for field, candidates in field_candidates.items():
            field_ids = cls._normalize_selector_values(raw_ids.get(field))
            if not field_ids:
                continue

            actual_field = next((c for c in candidates if c in schema), None)
            if not actual_field:
                continue

            quoted = ", ".join(cls._sql_quote(value) for value in field_ids)
            field_type = schema.get(actual_field, "").upper()
            if "[]" in field_type or field_type.startswith("LIST"):
                clauses.append(
                    f"({actual_field} IS NOT NULL AND EXISTS ("
                    f"SELECT 1 FROM UNNEST({actual_field}) AS t(val) "
                    f"WHERE upper(CAST(val AS VARCHAR)) IN ({quoted})"
                    f"))"
                )
            else:
                token_matches = [
                    "regexp_matches("
                    f"upper(CAST({actual_field} AS VARCHAR)), "
                    f"'(^|[^0-9A-Z]){v.upper()}([^0-9A-Z]|$)')"
                    for v in field_ids
                ]
                clauses.append("(" + " OR ".join(token_matches) + ")")

        if not clauses:
            return None

        return "(" + " OR ".join(clauses) + ")"

    @classmethod
    def _build_projection_sql(cls, schema: Dict[str, str]) -> str:
        # Common date fields in this dataset can appear as text like 10-NOV-2024.
        # Normalize in the view so planner-generated CAST(... AS DATE) remains safe.
        date_like = {
            "etd_lp",
            "etd_flp",
            "eta_dp",
            "eta_dp_date",
            "best_eta_dp_date",
            "eta_fd",
            "eta_fd_date",
            "best_eta_fd_date",
            "ata_dp",
            "ata_dp_date",
            "ata_flp",
            "atd_lp",
            "atd_flp",
            "derived_ata_dp",
            "derived_ata_dp_date",
            "delivery_date_to_consignee",
            "delivery_to_consignee_date",
            "empty_container_return_date",
            "rail_load_dp_date",
            "rail_departure_dp_date",
            "rail_arrival_destination_date",
            "predictive_eta",
        }

        def _date_expr(qcol: str) -> str:
            return (
                "COALESCE("
                f"TRY_CAST({qcol} AS DATE), "
                f"CAST(TRY_STRPTIME(CAST({qcol} AS VARCHAR), '%d-%b-%Y') AS DATE), "
                f"CAST(TRY_STRPTIME(CAST({qcol} AS VARCHAR), '%d-%B-%Y') AS DATE), "
                f"CAST(TRY_STRPTIME(CAST({qcol} AS VARCHAR), '%Y-%m-%d') AS DATE)"
                f")"
            )

        pieces: List[str] = []
        for col in schema.keys():
            qcol = cls._quote_ident(col)
            if col in date_like:
                pieces.append(f"{_date_expr(qcol)} AS {qcol}")
            else:
                pieces.append(qcol)

        # Compatibility aliases for planner/LLM date names that may not exist in raw parquet.
        # These aliases keep generated SQL resilient without widening data access.
        alias_map = {
            "ata_dp_date": "ata_dp",
            "eta_dp_date": "eta_dp",
            "best_eta_dp_date": "eta_dp",
            "derived_ata_dp_date": "derived_ata_dp",
            "eta_fd_date": "eta_fd",
            "best_eta_fd_date": "eta_fd",
            "delivery_to_consignee_date": "delivery_date_to_consignee",
        }
        for alias_col, source_col in alias_map.items():
            if alias_col in schema or source_col not in schema:
                continue
            src_qcol = cls._quote_ident(source_col)
            alias_qcol = cls._quote_ident(alias_col)
            pieces.append(f"{_date_expr(src_qcol)} AS {alias_qcol}")

        return ",\n                ".join(pieces)

    def prepare_view(
        self,
        parquet_path: str,
        consignee_codes: List[str],
        selector: Optional[Dict[str, Any]] = None,
    ) -> None:
        parquet_sql = parquet_path.replace("'", "''")
        schema = self._get_parquet_schema(parquet_path)
        where_clauses = [self._build_rls_filter(consignee_codes, schema)]

        selector_filter = self._build_selector_filter(selector, schema)
        if selector_filter:
            where_clauses.append(selector_filter)

        where_sql = " AND ".join(f"({clause})" for clause in where_clauses if clause)
        if not where_sql:
            where_sql = "TRUE"

        projection_sql = self._build_projection_sql(schema)

        rls_query = f"""
            CREATE OR REPLACE VIEW df AS
            SELECT
                {projection_sql}
            FROM read_parquet('{parquet_sql}')
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
        sql = self._normalize_sql_dialect(self._strip_code_fences(sql))
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
