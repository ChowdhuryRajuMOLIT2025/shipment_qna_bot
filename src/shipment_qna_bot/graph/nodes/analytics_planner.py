import ast
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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
        _CHAT_TOOL = AzureOpenAIChatTool()
    return _CHAT_TOOL


def _get_blob_manager() -> BlobAnalyticsManager:
    global _BLOB_MGR
    if _BLOB_MGR is None:
        _BLOB_MGR = BlobAnalyticsManager()
    return _BLOB_MGR


def _get_pandas_engine() -> PandasAnalyticsEngine:
    global _PANDAS_ENG
    if _PANDAS_ENG is None:
        _PANDAS_ENG = PandasAnalyticsEngine()
    return _PANDAS_ENG


def _mentions_final_destination(text: str) -> bool:
    lowered = text.lower()
    if "final destination" in lowered or "final_destination" in lowered:
        return True
    if "distribution center" in lowered or "distribution centre" in lowered:
        return True
    if re.search(r"\bin-?dc\b", lowered):
        return True
    if re.search(r"\bfd\b", lowered):
        return True
    return False


def _extract_time_windows(text: str) -> List[str]:
    lowered = text.lower()
    windows: List[str] = []

    def _add(label: str) -> None:
        if label not in windows:
            windows.append(label)

    if "today" in lowered:
        _add("today")
    if re.search(r"\bweek\b", lowered) or "next week" in lowered:
        _add("this_week")
    if (
        "fortnight" in lowered
        or re.search(r"\b14\b", lowered)
        or re.search(r"\b15\b", lowered)
        or "two weeks" in lowered
        or "next 2 weeks" in lowered
        or "next two weeks" in lowered
    ):
        _add("this_fortnight")
    if "month" in lowered:
        _add("this_month")

    return windows


def _get_now_ts(state: Dict[str, Any]) -> datetime:
    raw = state.get("now_utc") or state.get("today_date")
    if raw:
        try:
            s = str(raw).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return dt
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _build_arrival_bucket_chart(
    df, question: str, state: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    lowered = question.lower()
    chart_words = ["chart", "graph", "plot", "visualize", "bar"]
    bucket_words = ["bucket", "breakdown", "group"]
    windows = _extract_time_windows(lowered)

    bucket_requested = any(w in lowered for w in bucket_words)
    wants_chart = any(w in lowered for w in chart_words) or bucket_requested
    has_hot_normal = "hot" in lowered or "normal" in lowered

    if not windows and bucket_requested:
        windows = ["today", "this_week", "this_fortnight", "this_month"]

    if not windows or not (wants_chart or has_hot_normal):
        return None

    try:
        import pandas as pd
    except Exception:
        return None

    use_fd = _mentions_final_destination(lowered)
    arrival = pd.Series(pd.NaT, index=df.index)

    if use_fd:
        if "optimal_eta_fd_date" in df.columns:
            arrival = df["optimal_eta_fd_date"].copy()
        elif "eta_fd_date" in df.columns:
            arrival = df["eta_fd_date"].copy()
        else:
            return None
        if "eta_fd_date" in df.columns:
            arrival = arrival.fillna(df["eta_fd_date"])
    else:
        if "optimal_ata_dp_date" in df.columns:
            arrival = df["optimal_ata_dp_date"].copy()
        elif "eta_dp_date" in df.columns:
            arrival = df["eta_dp_date"].copy()
        elif "ata_dp_date" in df.columns:
            arrival = df["ata_dp_date"].copy()
        else:
            return None
        if "eta_dp_date" in df.columns:
            arrival = arrival.fillna(df["eta_dp_date"])
        if "ata_dp_date" in df.columns:
            arrival = arrival.fillna(df["ata_dp_date"])

    hot_flag = (
        df["hot_container_flag"].fillna(False).astype(bool)
        if "hot_container_flag" in df.columns
        else pd.Series(False, index=df.index)
    )

    hot_normal_phrases = [
        "hot vs normal",
        "normal vs hot",
        "hot/normal",
        "hot and normal",
        "normal and hot",
        "hot flag",
    ]

    if any(p in lowered for p in hot_normal_phrases) or (
        "hot" in lowered and "normal" in lowered
    ):
        categories = ["hot", "normal"]
    elif "hot" in lowered:
        categories = ["hot"]
    elif "normal" in lowered:
        categories = ["normal"]
    else:
        categories = ["total"]

    bucket_days = {
        "today": 1,
        "this_week": 7,
        "this_fortnight": 14,
        "this_month": 30,
    }
    now = _get_now_ts(state)

    chart_rows: List[Dict[str, Any]] = []
    table_rows: List[Dict[str, Any]] = []

    for bucket in windows:
        days = bucket_days.get(bucket)
        if not days:
            continue
        end = now + timedelta(days=days)
        mask = arrival.notna() & (arrival >= now) & (arrival < end)
        row: Dict[str, Any] = {"bucket": bucket}

        if categories == ["total"]:
            total_count = int(mask.sum())
            row["total_count"] = total_count
            chart_rows.append(
                {"bucket": bucket, "category": "total", "count": total_count}
            )
        else:
            total_count = int(mask.sum())
            if "hot" in categories:
                hot_count = int((mask & hot_flag).sum())
                row["hot_count"] = hot_count
                chart_rows.append(
                    {"bucket": bucket, "category": "hot", "count": hot_count}
                )
            if "normal" in categories:
                normal_count = int((mask & ~hot_flag).sum())
                row["normal_count"] = normal_count
                chart_rows.append(
                    {
                        "bucket": bucket,
                        "category": "normal",
                        "count": normal_count,
                    }
                )
            row["total_count"] = total_count

        table_rows.append(row)

    if not chart_rows or not table_rows:
        return None

    loc_label = "Final Destination" if use_fd else "Discharge Port"
    if categories == ["total"]:
        title = f"{loc_label} Arrival Buckets"
        encodings = {"x": "bucket", "y": "count"}
    elif len(windows) == 1:
        title = f"{loc_label} Arrivals (Hot vs Normal)"
        encodings = {"x": "category", "y": "count"}
    else:
        title = f"{loc_label} Arrival Buckets (Hot vs Normal)"
        encodings = {"x": "bucket", "y": "count", "color": "category"}

    chart_spec = {
        "kind": "bar",
        "title": title,
        "data": chart_rows,
        "encodings": encodings,
    }
    table_spec = {
        "columns": list(table_rows[0].keys()),
        "rows": table_rows,
        "title": f"{loc_label} Arrival Buckets",
    }

    summary_lines = []
    for row in table_rows:
        if "hot_count" in row or "normal_count" in row:
            summary_lines.append(
                f"{row['bucket']}: hot={row.get('hot_count', 0)}, normal={row.get('normal_count', 0)}, total={row.get('total_count', 0)}"
            )
        else:
            summary_lines.append(f"{row['bucket']}: total={row.get('total_count', 0)}")
    answer_text = "Analysis Result:\n" + "\n".join(summary_lines)

    return {
        "answer_text": answer_text,
        "chart_spec": chart_spec,
        "table_spec": table_spec,
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
        consignee_codes = state.get("consignee_codes") or []

        # 0. Safety Check
        if not consignee_codes:
            state.setdefault("errors", []).append(
                "No authorized consignee codes for analytics."
            )
            return state

        # 1. Load Data
        try:
            blob_mgr = _get_blob_manager()
            df = blob_mgr.load_filtered_data(consignee_codes)

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

        bucket_payload = _build_arrival_bucket_chart(df, q, state)
        if bucket_payload:
            state["answer_text"] = bucket_payload["answer_text"]
            state["chart_spec"] = bucket_payload["chart_spec"]
            state["table_spec"] = bucket_payload["table_spec"]
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
            # file is in src/shipment_qna_bot/graph/nodes/
            # docs is in docs/
            # need to go up 4 levels: .../src/shipment_qna_bot/graph/nodes/../../../../docs/ready_ref.md
            # use a relative path from the CWD, assume running from root
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
        # We have ready_ref, we do not need the auto-generated list,
        # but let's keep the auto-generated one for now as a fallback or concise list if ready_ref is missing columns and testing.
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
   - DEFAULT to using `optimal_ata_dp_date` for arrival/delay calculations (unless value is null, then fall back to `eta_dp_date`).
   - ONLY use `optimal_eta_fd_date` (or `optimal_eta_fd_date`) if the user explicitly asks for "Final Destination" (FD) or "In-CD".
9. Use `str.contains(..., na=False, case=False, regex=True)` for flexible text filtering.
10. Return ONLY the code inside a ```python``` block. Explain your logic briefly outside the block.
11. When filtering based on date, show the coolumn name and its value
12. **CHARTS:** If the user asks for a chart/graph/plot/bucket/breakdown, do NOT use matplotlib or seaborn. Instead, return a Python dict assigned to `result` with:
    - `answer_text`: short summary
    - `chart_spec`: {{kind, title, data (list of row dicts), encodings}}
    - `table_spec` (optional): {{columns, rows, title}}
    The `data` rows must be JSON-serializable (no DataFrames/Series in dicts).
13. **AVOID:** Do not return DataFrames inside a dict; return a DataFrame directly or use `table_spec`.

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
cols = ['container_number', 'po_numbers', 'eta_dp_date', 'optimal_ata_dp_date', 'dp_delayed_dur', 'discharge_port']
df_filtered = df[df['dp_delayed_dur'] > 5].copy()
# Apply date formatting
df_filtered['eta_dp_date'] = df_filtered['eta_dp_date'].dt.strftime('%d-%b-%Y')
df_filtered['optimal_ata_dp_date'] = df_filtered['optimal_ata_dp_date'].dt.strftime('%d-%b-%Y')
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

User: "Show a chart of hot vs normal containers arriving this month."
Code:
```python
# Filter arrivals this month and count hot vs normal
today = pd.Timestamp.utcnow()
df_filtered = df[df['optimal_ata_dp_date'].notna()].copy()
df_filtered = df_filtered[
    (df_filtered['optimal_ata_dp_date'].dt.month == today.month)
    & (df_filtered['optimal_ata_dp_date'].dt.year == today.year)
]
counts = df_filtered.groupby('hot_container_flag')['container_number'].count()
data = [
    {{"category": "normal", "count": int(counts.get(False, 0))}},
    {{"category": "hot", "count": int(counts.get(True, 0))}},
]
result = {{
    "answer_text": "Hot vs normal arrivals for this month.",
    "chart_spec": {{
        "kind": "bar",
        "title": "Hot vs Normal Arrivals (This Month)",
        "data": data,
        "encodings": {{"x": "category", "y": "count"}},
    }},
    "table_spec": {{
        "columns": ["category", "count"],
        "rows": data,
        "title": "Hot vs Normal Arrivals",
    }},
}}
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
                content = resp.get("content", "")

                # Extract code block
                match = re.search(r"```python\s*(.*?)```", content, re.DOTALL)
                if match:
                    generated_code = match.group(1).strip()
                else:
                    generated_code = content.strip()  # Fallback

        except Exception as e:
            logger.error(f"LLM Code Gen Failed: {e}")
            state.setdefault("errors", []).append(f"Code Gen Error: {e}")
            state["answer_text"] = (
                "I couldn't generate the analytics query in time. "
                "Please narrow the request or try again."
            )
            state["is_satisfied"] = True
            return state

        # 4. Execute Code
        if not generated_code:
            state.setdefault("errors", []).append("LLM produced no code.")
            state["answer_text"] = (
                "I couldn't generate a valid analytics query for that question. "
                "Please rephrase or add more detail."
            )
            state["is_satisfied"] = True
            return state

        engine = _get_pandas_engine()
        exec_result = engine.execute_code(df, generated_code)

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

            table_spec = exec_result.get("table_spec")
            table_total_rows = exec_result.get("table_total_rows")
            table_truncated = exec_result.get("table_truncated")

            if isinstance(table_spec, dict) and table_spec.get("rows"):
                state["table_spec"] = table_spec
                if table_total_rows is not None:
                    if table_truncated:
                        final_ans = (
                            f"Showing the first {len(table_spec.get('rows') or [])} "
                            f"of {table_total_rows} rows."
                        )
                    else:
                        final_ans = f"Found {table_total_rows} rows."
                else:
                    final_ans = "Here are the results."

            # Basic formatting if it's just a raw value
            state["answer_text"] = f"Analysis Result:\n{final_ans}"
            state["is_satisfied"] = True

            def _maybe_parse_dict(val: Any) -> Optional[Dict[str, Any]]:
                if isinstance(val, dict):
                    return val
                if isinstance(val, str):
                    s = val.strip()
                    if s.startswith("{") and s.endswith("}"):
                        try:
                            return json.loads(s)
                        except Exception:
                            try:
                                return ast.literal_eval(s)
                            except Exception:
                                return None
                return None

            payload = _maybe_parse_dict(exec_result.get("result"))
            if payload is None:
                payload = _maybe_parse_dict(exec_result.get("final_answer"))

            if isinstance(payload, dict):
                chart_payload = _maybe_parse_dict(
                    payload.get("chart_spec") or payload.get("chart")
                )
                table_payload = _maybe_parse_dict(
                    payload.get("table_spec") or payload.get("table")
                )
                if chart_payload:
                    state["chart_spec"] = chart_payload
                if table_payload:
                    state["table_spec"] = table_payload

                answer_payload = (
                    payload.get("answer_text")
                    or payload.get("answer")
                    or payload.get("text")
                    or payload.get("result")
                    or payload.get("value")
                )
                if answer_payload is not None and str(answer_payload).strip():
                    final_ans = str(answer_payload)
                    state["answer_text"] = f"Analysis Result:\n{final_ans}"
        else:
            error_msg = exec_result.get("error")
            logger.warning(f"Pandas Execution Error: {error_msg}")
            # We can allow the Judge to see this or retry.
            # For now, let's treat it as a failure to satisfy.
            state.setdefault("errors", []).append(f"Analysis Failed: {error_msg}")
            state["answer_text"] = (
                "I couldn't run that analytics query successfully. "
                "Please try narrowing the request or rephrasing."
            )
            state["is_satisfied"] = True

    return state
