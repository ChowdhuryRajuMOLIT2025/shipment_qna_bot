import json  # type: ignore
import re
from typing import Any, Dict, List, Optional  # type: ignore

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.analytics_metadata import (ANALYTICS_METADATA,
                                                       INTERNAL_COLUMNS)
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.blob_manager import BlobAnalyticsManager
from shipment_qna_bot.tools.pandas_engine import PandasAnalyticsEngine
from shipment_qna_bot.utils.runtime import is_test_mode

_CHAT_TOOL: Optional[AzureOpenAIChatTool] = None
_BLOB_MGR: Optional[BlobAnalyticsManager] = None
_PANDAS_ENG: Optional[PandasAnalyticsEngine] = None


def _get_chat() -> AzureOpenAIChatTool:
    global _CHAT_TOOL
    if _CHAT_TOOL is None:
        _CHAT_TOOL = AzureOpenAIChatTool()  # type: ignore
    return _CHAT_TOOL


def _get_blob_manager() -> BlobAnalyticsManager:
    global _BLOB_MGR
    if _BLOB_MGR is None:
        _BLOB_MGR = BlobAnalyticsManager()  # type: ignore
    return _BLOB_MGR


def _get_pandas_engine() -> PandasAnalyticsEngine:
    global _PANDAS_ENG
    if _PANDAS_ENG is None:
        _PANDAS_ENG = PandasAnalyticsEngine()  # type: ignore
    return _PANDAS_ENG


def _extract_python_code(content: str) -> str:
    if not content:
        return ""
    match = re.search(r"```python\s*(.*?)```", content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return content.strip()


def _merge_usage(state: Dict[str, Any], usage: Optional[Dict[str, Any]]) -> None:
    if not isinstance(usage, dict):
        return
    usage_metadata = state.get("usage_metadata") or {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    for k, v in usage.items():
        if isinstance(v, (int, float)):
            usage_metadata[k] = usage_metadata.get(k, 0) + v
    state["usage_metadata"] = usage_metadata


def _repair_generated_code(
    question: str,
    code: str,
    error_msg: str,
    columns: List[str],
    sample_markdown: str,
) -> tuple[str, Dict[str, Any]]:
    if is_test_mode():
        return "", {}

    repair_prompt = f"""
You are fixing Python/Pandas code that failed to run.
Return ONLY corrected code in a ```python``` block.

Rules:
- Use existing DataFrame `df`.
- Do not import external libraries (especially matplotlib/seaborn).
- Avoid ambiguous truth checks on DataFrames/Series (`if df:` is invalid).
- If using `.str`, ensure string-safe operations.
- Keep output in variable `result`.

Question:
{question}

Columns:
{columns}

Sample rows:
{sample_markdown}

Previous code:
```python
{code}
```

Error:
{error_msg}
""".strip()

    chat = _get_chat()
    resp = chat.chat_completion(
        [{"role": "user", "content": repair_prompt}],
        temperature=0.0,
    )
    fixed = _extract_python_code(resp.get("content", ""))
    return fixed, resp.get("usage", {}) or {}


def _wants_chart(question: str) -> bool:
    lowered = (question or "").lower()
    chart_terms = [
        "chart",
        "graph",
        "plot",
        "bar",
        "line",
        "pie",
        "trend",
        "visualize",
        "visualise",
        "distribution",
        "breakdown",
        "by carrier",
        "by port",
        "top 5",
        "top 10",
    ]
    return any(term in lowered for term in chart_terms)


def _chart_kind(question: str) -> str:
    lowered = (question or "").lower()
    if any(t in lowered for t in ["pie", "donut", "doughnut"]):
        return "pie"
    if any(t in lowered for t in ["line", "trend", "timeline", "over time"]):
        return "line"
    return "bar"


def _as_float(val: Any) -> Optional[float]:
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if val is None:
        return None
    try:
        raw = str(val).strip().replace(",", "")
        if raw.endswith("%"):
            raw = raw[:-1]
        if raw == "":
            return None
        return float(raw)
    except Exception:
        return None


def _build_table_spec_from_exec(
    exec_result: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    columns = exec_result.get("result_columns")
    rows = exec_result.get("result_rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        columns = None
        rows = None

    if isinstance(columns, list) and isinstance(rows, list) and columns and rows:
        safe_columns = [str(c) for c in columns]
        safe_rows: List[Dict[str, Any]] = []
        for row in rows[:500]:
            if not isinstance(row, dict):
                continue
            safe_row = {col: row.get(col) for col in safe_columns}
            safe_rows.append(safe_row)

        if safe_rows:
            return {
                "columns": safe_columns,
                "rows": safe_rows,
                "title": "Analytics Result",
            }

    result_value = exec_result.get("result_value")
    if isinstance(result_value, dict) and result_value:
        dict_rows = [{"label": str(k), "value": v} for k, v in result_value.items()]
        return {
            "columns": ["label", "value"],
            "rows": dict_rows[:500],
            "title": "Analytics Result",
        }

    if isinstance(result_value, list) and result_value:
        first = result_value[0]
        if isinstance(first, dict):
            cols: List[str] = []
            for item in result_value:
                if not isinstance(item, dict):
                    continue
                for key in item.keys():
                    k = str(key)
                    if k not in cols:
                        cols.append(k)
            if cols:
                list_rows: List[Dict[str, Any]] = []
                for item in result_value[:500]:
                    if not isinstance(item, dict):
                        continue
                    list_rows.append({c: item.get(c) for c in cols})
                if list_rows:
                    return {
                        "columns": cols,
                        "rows": list_rows,
                        "title": "Analytics Result",
                    }
        else:
            scalar_rows = [{"value": item} for item in result_value[:500]]
            return {
                "columns": ["value"],
                "rows": scalar_rows,
                "title": "Analytics Result",
            }

    return None


def _build_chart_spec_from_table(
    question: str, table_spec: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not _wants_chart(question):
        return None
    if not isinstance(table_spec, dict):
        return None

    columns = table_spec.get("columns") or []
    rows = table_spec.get("rows") or []
    if not isinstance(columns, list) or not isinstance(rows, list):
        return None
    if len(columns) < 2 or len(rows) == 0:
        return None

    sample_rows = [r for r in rows[:80] if isinstance(r, dict)]
    if not sample_rows:
        return None

    numeric_cols: List[str] = []
    categorical_cols: List[str] = []
    for col in columns:
        numeric_hits = 0
        for row in sample_rows:
            if _as_float(row.get(col)) is not None:
                numeric_hits += 1
        if numeric_hits > 0:
            numeric_cols.append(str(col))
        else:
            categorical_cols.append(str(col))

    if not numeric_cols:
        return None

    kind = _chart_kind(question)

    if kind == "pie":
        label_col = categorical_cols[0] if categorical_cols else str(columns[0])
        value_col = (
            next((c for c in numeric_cols if c != label_col), None) or numeric_cols[0]
        )
        chart_data: List[Dict[str, Any]] = []
        for row in sample_rows[:50]:
            value = _as_float(row.get(value_col))
            if value is None:
                continue
            label = row.get(label_col)
            chart_data.append(
                {label_col: str(label) if label is not None else "-", value_col: value}
            )
        if not chart_data:
            return None
        return {
            "kind": "pie",
            "title": table_spec.get("title") or "Analytics Pie Chart",
            "data": chart_data,
            "encodings": {"label": label_col, "value": value_col},
        }

    x_col = categorical_cols[0] if categorical_cols else str(columns[0])
    y_col = next((c for c in numeric_cols if c != x_col), None) or numeric_cols[0]

    chart_data = []
    for row in sample_rows[:80]:
        y_val = _as_float(row.get(y_col))
        if y_val is None:
            continue
        point: Dict[str, Any] = {
            x_col: row.get(x_col),
            y_col: y_val,
        }
        chart_data.append(point)

    # Heuristic for grouping: If x_col is a date, ensure it's sorted
    if "date" in x_col.lower() or "eta" in x_col.lower() or "ata" in x_col.lower():
        # Sort data points for line charts
        try:
            chart_data.sort(key=lambda x: str(x.get(x_col)))
        except:
            pass

    if not chart_data:
        return None

    return {
        "kind": kind,
        "title": f"{kind.capitalize()} of {y_col} by {x_col}",
        "data": chart_data,
        "encodings": {"x": x_col, "y": y_col},
    }


def analytics_planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pandas Analyst Agent Node.
    1. Downloads/Loads the full dataset (Master Cache).
    2. Filters for the current user (Consignee Scope).
    3. Generates Pandas code using LLM.
    4. Executes code to answer the question.
    """
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "AnalyticsPlanner", {"intent": state.get("intent")}, state_ref=state
    ):
        q = (
            state.get("normalized_question") or state.get("question_raw") or ""
        ).strip()
        consignee_codes = state.get("consignee_codes") or []  # type: ignore

        # 0. Safety Check
        if not consignee_codes:
            state["answer_text"] = (
                "No consignee codes provided. Please select at least one code to view data."
            )
            state["is_satisfied"] = True
            return state

        # 1. Load Data
        try:
            blob_mgr = _get_blob_manager()
            df = blob_mgr.load_filtered_data(consignee_codes)  # type: ignore

            if df.empty:
                state["answer_text"] = (
                    "I found no data available for your account (Master Dataset empty or filtered out)."
                )
                state["is_satisfied"] = True
                return state

        except Exception as e:
            logger.error(f"Analytics Data Load Failed: {e}")
            state.setdefault("errors", []).append(f"Data Load Error: {e}")
            state["answer_text"] = (
                "I couldn't load the analytics dataset right now. "
                "Please try again in a moment."
            )
            state["is_satisfied"] = True
            return state

        # 2. Prepare Context for LLM
        columns = list(df.columns)
        # Head sample (first 5 rows) to help LLM understand values
        head_sample = df.head(5).to_markdown(index=False)
        shape_info = f"Rows: {df.shape[0]}, Columns: {df.shape[1]}"

        # Dynamic Column Reference
        # Load Ready Reference if available
        ready_ref_content = ""
        try:
            # Assuming docs is at the root of the project, relative to this file path
            # This file is in src/shipment_qna_bot/graph/nodes/
            # docs is in docs/
            # So we need to go up 4 levels: .../src/shipment_qna_bot/graph/nodes/../../../../docs/ready_ref.md
            # Better to use a relative path from the CWD if we assume running from root
            import os

            ready_ref_path = "docs/ready_ref.md"
            if os.path.exists(ready_ref_path):
                with open(ready_ref_path, "r") as f:
                    ready_ref_content = f.read()
            else:
                # Fallback: try absolute path based on file location
                base_dir = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "../../../../")
                )
                ready_ref_path = os.path.join(base_dir, "docs", "ready_ref.md")
                if os.path.exists(ready_ref_path):
                    with open(ready_ref_path, "r") as f:
                        ready_ref_content = f.read()
        except Exception as e:
            logger.warning(f"Could not load ready_ref.md: {e}")

        col_ref = ""
        # We have ready_ref, we might not need the auto-generated list,
        # but let's keep the auto-generated one for now as a fallback or concise list if ready_ref is missing columns.
        # Actually, the ready_ref to be THE source for LLM understanding.
        # Now, let's append the ready ref to the context.

        for k, v in ANALYTICS_METADATA.items():
            if k in columns:
                col_ref += f"- `{k}`: {v['desc']} (Type: {v['type']})\n"

        system_prompt = f"""
You are a Pandas Data Analyst. You have access to a DataFrame `df` containing shipment data.
Your goal is to write Python code to answer the user's question using `df`.

## Context
Today's Date: {state.get('today_date')}

## Key Column Reference
{col_ref}

## Operational Reference (Ready Ref)
{ready_ref_content}

## Dataset Schema
Columns: {columns}
Shape: {shape_info}
Sample Data:
{head_sample}

## Instructions
1. Write valid Python/Pandas code.
2. Assign the final answer (string, number, list, or dataframe) to the variable `result`.
3. For "How many" or "Total" questions, `result` should be a single number.
4. For "List" or "Which" questions, `result` should be a unique list or a DataFrame.
5. **STRICT RULE:** Never include internal technical columns like {INTERNAL_COLUMNS} in the final `result`.
6. **RELEVANCE:** When returning a DataFrame/table, select only the columns relevant to the user's question.
7. **DATE FORMATTING:** Whenever displaying or returning a datetime column in a result, ALWAYS use `.dt.strftime('%d-%b-%Y')` to ensure a clean, user-friendly format (e.g., '22-Jul-2025').
8. **COLUMN SELECTION:**
   - For discharge-port ETA/arrival windows and overdue checks, use `best_eta_dp_date` (fallback: `eta_dp_date`).
   - For actual DP-arrival checks, use `ata_dp_date` (fallback: `derived_ata_dp_date` if needed).
   - If user asks "not yet arrived at DP": filter `ata_dp_date.isna()`.
   - If user asks "failed/missed ETA at DP": filter `(ata_dp_date.isna()) & (best_eta_dp_date <= today)`.
   - For final destination ETA logic, use `best_eta_fd_date` (fallback: `eta_fd_date`).
9. Use `str.contains(..., na=False, case=False, regex=True)` for flexible text filtering.
10. **SORTING RULE:** For DataFrame/list outputs containing date columns, sort by latest date first (descending) BEFORE date formatting. Prefer date columns in this order: `best_eta_dp_date`, `best_eta_fd_date`, `ata_dp_date`, `derived_ata_dp_date`, `eta_dp_date`, `eta_fd_date`.
11. Do NOT import plotting libraries or call charting code (`matplotlib`, `seaborn`, `plotly`).
12. Avoid ambiguous DataFrame truth checks (`if df:`). Use explicit checks such as `if not df.empty:`.
13. Return ONLY the code inside a ```python``` block. Explain your logic briefly outside the block.

## Examples:
User: "How many delivered shipments?"
Code:
```python
result = df[df['shipment_status'] == 'DELIVERED'].shape[0]
```

User: "What is the total weight of my shipments?"
Code:
```python
result = df['cargo_weight_kg'].sum()
```

User: "Which carriers are involved?"
Code:
```python
result = df['final_carrier_name'].dropna().unique().tolist()
```

User: "Show me shipments with more than 5 days delay."
Code:
```python
# Select only relevant columns and format dates
cols = ['container_number', 'po_numbers', 'eta_dp_date', 'best_eta_dp_date', 'dp_delayed_dur', 'discharge_port']
df_filtered = df[df['dp_delayed_dur'] > 5].copy()
# Sort latest first prior to formatting
df_filtered = df_filtered.sort_values('best_eta_dp_date', ascending=False)
# Apply date formatting
df_filtered['eta_dp_date'] = df_filtered['eta_dp_date'].dt.strftime('%d-%b-%Y')
df_filtered['best_eta_dp_date'] = df_filtered['best_eta_dp_date'].dt.strftime('%d-%b-%Y')
result = df_filtered[cols]
```

User: "List shipments departing next week."
Code:
```python
# Use etd_lp_date for estimated departures
cols = ['container_number', 'po_numbers', 'etd_lp_date', 'load_port']
df_filtered = df[df['etd_lp_date'].dt.isocalendar().week == (today_week + 1)].copy()
df_filtered['etd_lp_date'] = df_filtered['etd_lp_date'].dt.strftime('%d-%b-%Y')
result = df_filtered[cols]
```
"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Question: {q}"},
        ]

        # 3. Generate Code
        generated_code = ""
        try:
            if is_test_mode():
                # Mock generation for tests
                generated_code = "result = 'Mock Answer'"
            else:
                chat = _get_chat()
                resp = chat.chat_completion(messages, temperature=0.0)
                _merge_usage(state, resp.get("usage"))
                content = resp.get("content", "")
                generated_code = _extract_python_code(content)

        except Exception as e:
            logger.error(f"LLM Code Gen Failed: {e}")
            state.setdefault("errors", []).append(f"Code Gen Error: {e}")
            state["answer_text"] = (
                "I couldn't generate the analytics query in time. "
                "Please narrow the request or try again."
            )
            state["is_satisfied"] = False
            state["reflection_feedback"] = (
                "Code generation failed; retry analytics with a simpler plan."
            )
            return state

        # 4. Execute Code
        if not generated_code:
            state.setdefault("errors", []).append("LLM produced no code.")
            state["answer_text"] = (
                "I couldn't generate a valid analytics query for that question. "
                "Please rephrase or add more detail."
            )
            state["is_satisfied"] = False
            state["reflection_feedback"] = (
                "No executable code was generated; retry with stricter code-only output."
            )
            return state

        engine = _get_pandas_engine()
        exec_attempts = 1
        exec_result = engine.execute_code(df, generated_code)

        if not exec_result.get("success"):
            error_msg = str(exec_result.get("error") or "")
            logger.warning(
                "Initial analytics execution failed, attempting one repair: %s",
                error_msg,
            )
            try:
                repaired_code, repair_usage = _repair_generated_code(
                    question=q,
                    code=generated_code,
                    error_msg=error_msg,
                    columns=columns,
                    sample_markdown=head_sample,
                )
                _merge_usage(state, repair_usage)
                if repaired_code and repaired_code != generated_code:
                    generated_code = repaired_code
                    exec_attempts += 1
                    exec_result = engine.execute_code(df, generated_code)
            except Exception as repair_exc:
                logger.warning("Analytics repair pass failed: %s", repair_exc)

        if exec_result["success"]:
            result_type = exec_result.get("result_type")
            filtered_rows = exec_result.get("filtered_rows")
            filtered_preview = exec_result.get("filtered_preview") or ""

            logger.info(
                "Analytics result rows=%s type=%s",
                filtered_rows,
                result_type,
                extra={"step": "NODE:AnalyticsPlanner"},
            )

            final_ans = exec_result.get("final_answer", "")

            if result_type == "bool":
                if filtered_rows and filtered_rows > 0 and filtered_preview:
                    final_ans = (
                        f"Found {filtered_rows} matching shipments.\n\n"
                        f"{filtered_preview}"
                    )
                elif filtered_rows == 0:
                    final_ans = "No shipments matched your filters."

            # Basic formatting if it's just a raw value
            state["answer_text"] = f"Here is what I found:\n{final_ans}"
            state["is_satisfied"] = True
            state["analytics_last_error"] = None
            state["analytics_attempt_count"] = exec_attempts

            table_spec = _build_table_spec_from_exec(exec_result)
            if table_spec:
                state["table_spec"] = table_spec

            chart_spec = _build_chart_spec_from_table(q, table_spec)
            if chart_spec:
                state["chart_spec"] = chart_spec

            logger.info(
                "Analytics artifacts generated: table=%s chart=%s",
                bool(table_spec),
                chart_spec.get("kind") if isinstance(chart_spec, dict) else None,
                extra={"step": "NODE:AnalyticsPlanner"},
            )
        else:
            error_msg = exec_result.get("error")
            logger.warning(f"Pandas Execution Error: {error_msg}")
            state.setdefault("errors", []).append(f"Analysis Failed: {error_msg}")
            state["answer_text"] = (
                "I couldn't run that analytics query successfully. "
                "Please try narrowing the request or rephrasing."
            )
            state["is_satisfied"] = False
            state["reflection_feedback"] = (
                "Analytics execution failed. Regenerate safer pandas code "
                "without unsupported imports and with valid date/string handling."
            )
            state["analytics_last_error"] = str(error_msg or "")
            state["analytics_attempt_count"] = exec_attempts

    return state
