import contextlib
import io
import json
import sys
from typing import Any, Dict, Optional

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
        logger.info(f"Pandas Engine executing code on DF with shape {df.shape}")
        logger.info(f"Pandas Code:\n{code}")

        # Trap stdout
        output_buffer = io.StringIO()

        # Execution context
        local_scope = {
            "df": df,
            "pd": pd,
            "np": np,
            "json": json,
            "result": None,  # User code can assign to this
        }

        try:
            with contextlib.redirect_stdout(output_buffer):
                exec(code, {}, local_scope)

            output = output_buffer.getvalue()
            result_val = local_scope.get("result")
            result_type = (
                type(result_val).__name__ if result_val is not None else "None"
            )

            filtered_rows = None
            filtered_preview = ""
            df_filtered = local_scope.get("df_filtered")
            if isinstance(df_filtered, pd.DataFrame):
                filtered_rows = len(df_filtered)
                if filtered_rows > 0:
                    preferred_cols = [
                        "container_number",
                        "po_numbers",
                        "optimal_ata_dp_date",
                        "eta_dp_date",
                        "eta_fd_date",
                        "load_port",
                        "discharge_port",
                        "final_destination",
                    ]
                    cols = [c for c in preferred_cols if c in df_filtered.columns]
                    preview_df = (
                        df_filtered[cols].head(50) if cols else df_filtered.head(50)
                    )
                    filtered_preview = preview_df.to_markdown(index=False)

            table_spec = None
            table_total_rows = None
            table_truncated = False

            def _to_py(val: Any) -> Any:
                if pd.isna(val):
                    return None
                if isinstance(val, (pd.Timestamp,)):
                    return val.isoformat()
                if isinstance(val, (np.integer, np.floating)):
                    return val.item()
                if isinstance(val, (np.ndarray,)):
                    return val.tolist()
                return val

            # If result is a dataframe or series, convert to something json-serializable/string
            # for the agent to consume easily
            if isinstance(result_val, (pd.DataFrame, pd.Series)):
                if result_val.empty:
                    return {
                        "success": True,
                        "output": output_buffer.getvalue(),
                        "result": "",
                        "final_answer": "No rows matched your filters.",
                    }

                if isinstance(result_val, pd.Series):
                    name = result_val.name or "value"
                    result_df = result_val.reset_index()
                    result_df.columns = ["key", name]
                else:
                    result_df = result_val

                table_total_rows = len(result_df)
                max_rows = 200
                table_truncated = table_total_rows > max_rows
                table_df = result_df.head(max_rows)

                rows = []
                for row in table_df.to_dict(orient="records"):
                    rows.append({k: _to_py(v) for k, v in row.items()})

                table_spec = {
                    "columns": list(table_df.columns),
                    "rows": rows,
                }

                result_export = result_df.to_markdown()
            else:
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
                "table_spec": table_spec,
                "table_total_rows": table_total_rows,
                "table_truncated": table_truncated,
            }

        except Exception as e:
            logger.error(f"Pandas execution failed: {e}")
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "output": output_buffer.getvalue(),
            }
