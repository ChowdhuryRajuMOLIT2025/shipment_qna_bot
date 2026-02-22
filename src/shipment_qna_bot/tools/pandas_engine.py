import ast
import contextlib
import io
import json
import re
import sys
import threading
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
        self.allowed_import_roots = set()  # NO IMPORTS ALLOWED AT RUNTIME
        self.forbidden_attrs = {
            "__import__",
            "__builtins__",
            "eval",
            "exec",
            "open",
            "system",
            "subprocess",
            "globals",
            "locals",
        }

    @staticmethod
    def _strip_code_fences(code: str) -> str:
        cleaned = (code or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:python)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    def _preflight_validate_code(self, code: str) -> Optional[str]:
        """
        Hardened validation using AST analysis.
        """
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return f"Syntax Error in generated code: {e}"

        class SecurityVisitor(ast.NodeVisitor):
            def __init__(self, forbidden_attrs):
                self.forbidden_attrs = forbidden_attrs
                self.error = None

            def visit_Import(self, node):
                self.error = "Import statements are strictly prohibited."

            def visit_ImportFrom(self, node):
                self.error = "Import statements are strictly prohibited."

            def visit_Attribute(self, node):
                if node.attr in self.forbidden_attrs or node.attr.startswith("__"):
                    self.error = f"Access to attribute '{node.attr}' is forbidden."
                self.generic_visit(node)

            def visit_Name(self, node):
                if node.id in self.forbidden_attrs:
                    self.error = f"Use of '{node.id}' is forbidden."
                self.generic_visit(node)

            def visit_Call(self, node):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.forbidden_attrs:
                        self.error = f"Function call '{node.func.id}' is forbidden."
                self.generic_visit(node)

        visitor = SecurityVisitor(self.forbidden_attrs)
        visitor.visit(tree)
        if visitor.error:
            return visitor.error

        # Additional regex checks for things AST might miss in strings or edge cases
        if re.search(r"(__class__|__mro__|__subclasses__|__init__)", code):
            return "Detected suspicious attribute access (dunder methods)."

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
                sorted_df = df_in.copy()
                sorted_df["_sort_dt_tmp"] = parsed
                sorted_df = sorted_df.sort_values(
                    by="_sort_dt_tmp", ascending=False, kind="stable"
                )
                return sorted_df.drop(columns=["_sort_dt_tmp"])
        return df_in

    @staticmethod
    def _to_json_safe_value(v: Any) -> Any:
        if isinstance(v, (pd.Timestamp, pd.Timedelta)):
            return str(v)
        if isinstance(v, dict):
            return {
                str(k): PandasAnalyticsEngine._to_json_safe_value(val)
                for k, val in v.items()
            }
        if isinstance(v, (list, tuple, set)):
            return [PandasAnalyticsEngine._to_json_safe_value(x) for x in v]
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                return str(v)
        return v

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

        working_df = df.copy()
        for col in self._extract_str_columns(code):
            if col not in working_df.columns:
                continue
            series = working_df[col]
            if not pd.api.types.is_string_dtype(series):
                working_df[col] = series.astype("string")

        # Execution context
        local_scope = {
            "df": working_df,
            "pd": pd,
            "np": np,
            "json": json,
            "result": None,  # User code can assign to this
        }

        try:
            # Create a restricted globals bridge
            # We explicitly pass our allowed modules into the local scope too
            safe_globals = {
                "__builtins__": {
                    "print": print,
                    "range": range,
                    "len": len,
                    "sum": sum,
                    "min": min,
                    "max": max,
                    "abs": abs,
                    "round": round,
                    "list": list,
                    "dict": dict,
                    "set": set,
                    "tuple": tuple,
                    "str": str,
                    "int": int,
                    "float": float,
                    "bool": bool,
                    "enumerate": enumerate,
                    "zip": zip,
                    "any": any,
                    "all": all,
                    "sorted": sorted,
                    "getattr": getattr,
                }
            }

            with contextlib.redirect_stdout(output_buffer):
                # exec with timeout (simulated as we can't easily kill thread in pure python without signals)
                # For now, we use a simple try/except and reliance on AST + limited builtins.
                exec(code, safe_globals, local_scope)

            output = output_buffer.getvalue()
            result_val = local_scope.get("result")
            result_type = (
                type(result_val).__name__ if result_val is not None else "None"
            )

            filtered_rows = None
            filtered_preview = ""
            df_filtered = local_scope.get("df_filtered")
            if isinstance(df_filtered, pd.DataFrame):
                df_filtered = self._sort_df_latest_first(df_filtered)
                local_scope["df_filtered"] = df_filtered
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
                        df_filtered[cols].head(50) if cols else df_filtered.head(50)
                    )
                    filtered_preview = preview_df.to_markdown(index=False)

            # If result is a dataframe or series, convert to something json-serializable/string
            # for the agent to consume easily
            result_rows: Optional[List[Dict[str, Any]]] = None
            result_columns: Optional[List[str]] = None
            result_value: Any = None

            if isinstance(result_val, (pd.DataFrame, pd.Series)):
                if result_val.empty:
                    return {
                        "success": True,
                        "output": output_buffer.getvalue(),
                        "result": "",
                        "final_answer": "No rows matched your filters.",
                    }
                if isinstance(result_val, pd.Series):
                    series_name = str(result_val.name or "value")
                    index_name = str(result_val.index.name or "category")
                    table_df = result_val.reset_index(name=series_name)
                    table_df.columns = [index_name, series_name]
                else:
                    table_df = self._sort_df_latest_first(result_val)

                table_df = table_df.copy()
                table_df = table_df.replace({np.nan: None})

                result_columns = [str(c) for c in table_df.columns.tolist()]
                result_rows = [
                    {str(k): self._to_json_safe_value(v) for k, v in row.items()}
                    for row in table_df.to_dict(orient="records")
                ]
                result_value = result_rows
                result_export = table_df.to_markdown(index=False)
            elif isinstance(result_val, dict):
                result_value = self._to_json_safe_value(result_val)
                result_export = str(result_val)
            elif isinstance(result_val, (list, tuple, set)):
                result_value = self._to_json_safe_value(result_val)
                result_export = str(result_val)
            else:
                result_value = self._to_json_safe_value(result_val)
                result_export = str(result_val) if result_val is not None else ""

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
                "result_columns": result_columns,
                "result_rows": result_rows,
                "result_value": result_value,
            }

        except Exception as e:
            logger.error(f"Pandas execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": output_buffer.getvalue(),
            }
