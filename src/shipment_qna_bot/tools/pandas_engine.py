import contextlib
import io
import json
import re
import sys  # type: ignore
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from shipment_qna_bot.logging.logger import logger


class PandasAnalyticsEngine:
    """
    Safely executes Python/Pandas code on a provided DataFrame.
    Use this to perform aggregations, filtering, and detailed analysis that
    vector search cannot handle (e.g., "average weight", "count delays by port", "delay to port").
    """

    def __init__(self):
        # Allow specific safe modules to be used in the exec environment
        self.allowed_modules = {
            "pd": pd,
            "pandas": pd,
            "np": np,
            "numpy": np,
            "json": json,
        }
        self.allowed_import_roots = {"pandas", "numpy", "json"}
        self.max_preview_rows = 25
        self.max_preview_cols = 16
        self.max_result_rows = 200
        self.max_result_cols = 20
        self.max_cell_chars = 120

    @staticmethod
    def _strip_code_fences(code: str) -> str:
        cleaned = (code or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:python)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    def _preflight_validate_code(self, code: str) -> Optional[str]:
        import_matches = re.findall(
            r"^\s*(?:from|import)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
            code,
            flags=re.MULTILINE,
        )
        for module_name in import_matches:
            root = module_name.split(".")[0].lower()
            if root not in self.allowed_import_roots:
                return (
                    f"Import '{module_name}' is not allowed in analytics execution. "
                    "Use only pandas/numpy/json."
                )

        date_literals = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", code)
        for token in date_literals:
            try:
                pd.Timestamp(token)
            except Exception:
                return (
                    f"Invalid date literal '{token}'. "
                    "Please use a valid calendar date."
                )

        for var in re.findall(r"\bif\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*:", code):
            if var in {"df", "df_filtered", "result"} or var.endswith("_df"):
                return (
                    f"Ambiguous truth-value check on '{var}'. "
                    "Use explicit checks like `.empty`, `.any()`, or `.all()`."
                )
        return None

    @staticmethod
    def _extract_str_columns(code: str) -> List[str]:
        cols = re.findall(r"\[['\"]([^'\"]+)['\"]\]\.str\.", code)
        return list(dict.fromkeys(cols))

    @staticmethod
    def _sort_df_latest_first(df_in: pd.DataFrame) -> pd.DataFrame:
        if df_in.empty:
            return df_in
        date_priority = [
            "best_eta_dp_date",
            "best_eta_fd_date",
            "ata_dp_date",
            # "derived_ata_dp_date",
            "eta_dp_date",
            "eta_fd_date",
        ]
        for col in date_priority:
            if col not in df_in.columns:
                continue
            s = df_in[col]
            if pd.api.types.is_datetime64_any_dtype(s):
                return df_in.sort_values(by=col, ascending=False, kind="stable")
            parsed = pd.to_datetime(s, errors="coerce", utc=True)
            if parsed.notna().sum() > 0:
                # sorted_df = df_in.copy()
                sorted_df = df_in
                sorted_df["_sort_dt_tmp"] = parsed
                sorted_df = sorted_df.sort_values(
                    by="_sort_dt_tmp", ascending=False, kind="stable"
                )
                return sorted_df.drop(columns=["_sort_dt_tmp"])
        return df_in

    def _truncate_cell(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        if isinstance(value, (list, tuple, dict)):
            try:
                text = json.dumps(value, default=str)
            except Exception:
                text = str(value)  # type: ignore
        else:
            text = str(value)
        text = text.replace("\r", " ").replace("\n", " ").strip()
        if len(text) > self.max_cell_chars:
            return text[: self.max_cell_chars - 3] + "..."
        return text

    def _to_markdown_limited(
        self,
        value: pd.DataFrame | pd.Series,
        *,
        max_rows: int,
        max_cols: int,
        index: bool = True,
    ) -> str:
        notes: List[str] = []

        if isinstance(value, pd.Series):
            row_count = int(len(value))
            series_view = value.head(max_rows).copy()
            series_view = series_view.map(self._truncate_cell)
            if row_count > max_rows:
                notes.append(f"rows truncated to {max_rows} of {row_count}")
            md = series_view.to_markdown()
        else:
            row_count = int(len(value))
            cols = list(value.columns)
            selected_cols = cols[:max_cols]
            df_view = value[selected_cols].head(max_rows).copy().astype("object")
            for col in df_view.columns:
                df_view.loc[:, col] = df_view[col].map(self._truncate_cell)
            if row_count > max_rows:
                notes.append(f"rows truncated to {max_rows} of {row_count}")
            if len(cols) > max_cols:
                notes.append(f"columns truncated to {max_cols} of {len(cols)}")
            md = df_view.to_markdown(index=index)

        if notes:
            md += "\n\n[truncated: " + "; ".join(notes) + "]"
        return md

    def execute_code(self, df: pd.DataFrame, code: str) -> Dict[str, Any]:
        """
        Executes the provided Python code with the DataFrame `df` in context.
        The user code should print the result or assign it to a variable named `result`.

        Returns:
            Dict containing:
            - 'output': Captured stdout (print statements)
            - 'result': Value of 'result' variable if defined
            - 'error': Error message if failed
            - 'success': Bool
        """
        code = self._strip_code_fences(code)
        logger.info(f"Pandas Engine executing code on DF with shape {df.shape}")
        logger.info(f"Pandas Code:\n{code}")

        preflight_error = self._preflight_validate_code(code)
        if preflight_error:
            logger.warning("Pandas preflight validation failed: %s", preflight_error)
            return {
                "success": False,
                "error": preflight_error,
                "output": "",
            }

        # Trap stdout
        output_buffer = io.StringIO()

        # Shallow copy dramatically reduces peak memory vs deep copy on wide tables.
        # We only deep-copy if we actually need to coerce one or more columns for `.str` ops.
        working_df = df.copy(deep=False)
        deep_copy_made = False
        for col in self._extract_str_columns(code):
            if col not in working_df.columns:
                continue
            series = working_df[col]
            if not pd.api.types.is_string_dtype(series):
                if not deep_copy_made:
                    working_df = working_df.copy()
                    deep_copy_made = True
                working_df.loc[:, col] = working_df[col].astype("string")

        # Execution context
        local_scope = {  # type: ignore
            "df": working_df,
            "pd": pd,
            "np": np,
            "json": json,
            "result": None,  # User code can assign to this
        }

        try:
            with contextlib.redirect_stdout(output_buffer):
                exec(code, {}, local_scope)  # type: ignore

            output = output_buffer.getvalue()
            result_val = local_scope.get("result")  # type: ignore
            result_type = (
                type(result_val).__name__ if result_val is not None else "None"  # type: ignore
            )

            filtered_rows = None
            filtered_preview = ""
            filtered_dataframe: Optional[pd.DataFrame] = None
            df_filtered = local_scope.get("df_filtered")  # type: ignore
            if isinstance(df_filtered, pd.DataFrame):
                df_filtered = self._sort_df_latest_first(df_filtered)
                local_scope["df_filtered"] = df_filtered
                filtered_dataframe = df_filtered
                filtered_rows = len(df_filtered)
                if filtered_rows > 0:
                    preferred_cols = [
                        "container_number",
                        "po_numbers",
                        "load_port",
                        "discharge_port",
                        "eta_dp_date",
                        "best_eta_dp_date",
                        "ata_dp_date",
                        # "derived_ata_dp_date",
                        "final_destination",
                        "eta_fd_date",
                        "best_eta_fd_date",
                    ]
                    cols = [c for c in preferred_cols if c in df_filtered.columns]
                    preview_df = (
                        df_filtered[cols].head(self.max_preview_rows)
                        if cols
                        else df_filtered.head(self.max_preview_rows)
                    )
                    filtered_preview = self._to_markdown_limited(
                        preview_df,
                        max_rows=self.max_preview_rows,
                        max_cols=self.max_preview_cols,
                        index=False,
                    )

            # If result is a dataframe or series, convert to something json-serializable/string
            # for the agent to consume easily
            result_dataframe: Optional[pd.DataFrame] = None
            if isinstance(result_val, (pd.DataFrame, pd.Series)):
                if result_val.empty:
                    return {
                        "success": True,
                        "output": output_buffer.getvalue(),
                        "result": "",
                        "final_answer": "No rows matched your filters.",
                        "filtered_rows": filtered_rows,
                        "filtered_preview": filtered_preview,
                        "filtered_dataframe": filtered_dataframe,
                        "result_dataframe": (
                            result_val if isinstance(result_val, pd.DataFrame) else None
                        ),
                    }
                if isinstance(result_val, pd.DataFrame):
                    result_val = self._sort_df_latest_first(result_val)
                    result_dataframe = result_val
                result_export = self._to_markdown_limited(
                    result_val,  # type: ignore
                    max_rows=self.max_result_rows,
                    max_cols=self.max_result_cols,
                )
            else:
                result_export = str(result_val) if result_val is not None else ""  # type: ignore

            # If no result variable, rely on print output
            final_answer = result_export if result_export else output

            return {
                "success": True,
                "output": output,
                "result": result_export,
                "final_answer": final_answer.strip(),
                "result_type": result_type,
                "filtered_rows": filtered_rows,
                "filtered_preview": filtered_preview,
                "filtered_dataframe": filtered_dataframe,
                "result_dataframe": result_dataframe,
            }

        except Exception as e:
            logger.error(f"Pandas execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": output_buffer.getvalue(),
            }
