# src/shipment_qna_bot/graph/nodes/analytics_planner.py

import json  # type: ignore
import os
import re
from typing import Any, Dict, List, Optional  # type: ignore

from dotenv import find_dotenv, load_dotenv
from langchain_core.messages import AIMessage

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.analytics_metadata import (
    INTERNAL_COLUMNS, format_analytics_column_reference)
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.blob_manager import BlobAnalyticsManager
from shipment_qna_bot.tools.duckdb_engine import DuckDBAnalyticsEngine
from shipment_qna_bot.tools.ready_ref import load_ready_ref
from shipment_qna_bot.utils.runtime import is_test_mode

# Load environment overrides from .env (if present)
load_dotenv(find_dotenv(), override=True)

# Configurable analytics defaults. Non-positive values disable the default behavior.
DEFAULT_FUTURE_WINDOW_DAYS = int(os.getenv("ANALYTICS_DEFAULT_FUTURE_WINDOW_DAYS", "0"))
DEFAULT_PAST_WINDOW_DAYS = int(os.getenv("ANALYTICS_DEFAULT_PAST_WINDOW_DAYS", "0"))
DEFAULT_DELAY_THRESHOLD_DAYS = int(
    os.getenv("ANALYTICS_DEFAULT_DELAY_THRESHOLD_DAYS", "0")
)

_CHAT_TOOL: Optional[AzureOpenAIChatTool] = None
_BLOB_MGR: Optional[BlobAnalyticsManager] = None
_DUCKDB_ENG: Optional[DuckDBAnalyticsEngine] = None


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


def _get_duckdb_engine() -> DuckDBAnalyticsEngine:
    global _DUCKDB_ENG
    if _DUCKDB_ENG is None:
        _DUCKDB_ENG = DuckDBAnalyticsEngine()  # type: ignore
    return _DUCKDB_ENG


def _extract_sql_code(content: str) -> str:
    if not content:
        return ""
    match = re.search(r"```sql\s*(.*?)```", content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```python\s*(.*?)```", content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return content.strip()


def _contains_explicit_time_window(question: str) -> bool:
    lowered = (question or "").lower()

    explicit_patterns = [
        r"\b\d+\s*(day|days|week|weeks|month|months|year|years)\b",
        r"\b(today|tomorrow|yesterday)\b",
        r"\b(last|next|previous)\b",
        r"\bbetween\b",
        r"\bfrom\b.*\bto\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
    ]
    return any(re.search(pattern, lowered) for pattern in explicit_patterns)


def _is_future_event_question(question: str) -> bool:
    lowered = (question or "").lower()
    if any(m in lowered for m in ["arrived", "received", "delivered"]):
        return False
    future_markers = [
        "will",
        "upcoming",
        "coming",
        "to arrive",
        "to be delivered",
        "arriving",
        "delivering",
        "expected",
    ]
    event_markers = [
        "arrive",
        "arrival",
        "arriving",
        "deliver",
        "delivery",
        "delivering",
    ]
    return any(m in lowered for m in future_markers) and any(
        m in lowered for m in event_markers
    )


def _is_past_event_question(question: str) -> bool:
    lowered = (question or "").lower()
    if "will " in lowered:
        return False
    past_markers = ["arrived", "received", "delivered"]
    return any(m in lowered for m in past_markers)


def _is_dp_context(question: str) -> bool:
    lowered = (question or "").lower()
    return any(
        marker in lowered
        for marker in [" dp", "discharge", "port", "seaport", "harbor"]
    )


def _is_fd_context(question: str) -> bool:
    lowered = (question or "").lower()
    return any(
        marker in lowered
        for marker in [" fd", "final destination", "destination", "consignee", "place"]
    )


def _sql_has_dp_context(sql: str) -> bool:
    return bool(
        re.search(
            r"\b(discharge_port|best_eta_dp_date|eta_dp_date|ata_dp_date|derived_ata_dp_date)\b",
            sql or "",
            re.I,
        )
    )


def _sql_has_fd_context(sql: str) -> bool:
    return bool(
        re.search(
            r"\b(final_destination|best_eta_fd_date|eta_fd_date|delivery_to_consignee_date|empty_container_return_date)\b",
            sql or "",
            re.I,
        )
    )


def _has_existing_column_constraint(sql: str, col_name: str) -> bool:
    return bool(
        re.search(
            rf"\b{re.escape(col_name)}\b\s*(<=|>=|=|<|>|between)\b",
            sql,
            re.I,
        )
    )


def _has_existing_null_constraint(sql: str, col_name: str) -> bool:
    return bool(
        re.search(
            rf"\b{re.escape(col_name)}\b\s+is\s+(not\s+)?null\b",
            sql,
            re.I,
        )
    )


def _inject_filter_condition(sql: str, condition: str) -> tuple[str, bool]:
    base_sql = (sql or "").strip().rstrip(";")
    if not base_sql or not re.match(r"(?is)^\s*select\b", base_sql):
        return sql, False

    split_match = re.search(r"\b(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)\b", base_sql, re.I)
    if split_match:
        pre = base_sql[: split_match.start()].rstrip()
        post = base_sql[split_match.start() :].lstrip()
    else:
        pre = base_sql
        post = ""

    if re.search(r"\bWHERE\b", pre, re.I):
        updated_pre = f"{pre} AND ({condition})"
    else:
        updated_pre = f"{pre} WHERE ({condition})"

    updated_sql = updated_pre if not post else f"{updated_pre} {post}"
    return updated_sql, True


def _apply_default_time_cap(question: str, sql: str) -> tuple[str, Optional[str]]:
    if _contains_explicit_time_window(question):
        return sql, None

    lowered = (question or "").lower()
    if not any(
        k in lowered
        for k in [
            "arrive",
            "arrival",
            "arriving",
            "arrived",
            "deliver",
            "delivering",
            "delivered",
            "received",
        ]
    ):
        return sql, None

    is_future = _is_future_event_question(question)
    is_past = _is_past_event_question(question)
    if not (is_future or is_past):
        return sql, None

    fd_context = _is_fd_context(question) or _sql_has_fd_context(sql)
    dp_context = _is_dp_context(question) or _sql_has_dp_context(sql)
    if not (fd_context or dp_context):
        return sql, None

    if is_future:
        date_col = (
            "best_eta_fd_date" if fd_context and not dp_context else "best_eta_dp_date"
        )
        if _has_existing_column_constraint(sql, date_col):
            return sql, None
        # honor configured default; skip applying if non-positive (disabled)
        if DEFAULT_FUTURE_WINDOW_DAYS <= 0:
            return sql, None
        if fd_context and not dp_context:
            if (
                _has_existing_column_constraint(sql, "delivery_to_consignee_date")
                or _has_existing_null_constraint(sql, "delivery_to_consignee_date")
                or _has_existing_column_constraint(sql, "empty_container_return_date")
                or _has_existing_null_constraint(sql, "empty_container_return_date")
            ):
                return sql, None
            condition = (
                "delivery_to_consignee_date IS NULL "
                "AND empty_container_return_date IS NULL "
                f"AND {date_col} >= CURRENT_DATE "
                f"AND {date_col} <= CURRENT_DATE + INTERVAL {DEFAULT_FUTURE_WINDOW_DAYS} DAY"
            )
        else:
            if _has_existing_column_constraint(
                sql, "ata_dp_date"
            ) or _has_existing_null_constraint(sql, "ata_dp_date"):
                return sql, None
            condition = (
                "ata_dp_date IS NULL "
                f"AND {date_col} >= CURRENT_DATE "
                f"AND {date_col} <= CURRENT_DATE + INTERVAL {DEFAULT_FUTURE_WINDOW_DAYS} DAY"
            )
        updated_sql, changed = _inject_filter_condition(sql, condition)
        if changed:
            note = f"Applied default future window: CURRENT_DATE to CURRENT_DATE + {DEFAULT_FUTURE_WINDOW_DAYS} days."
            return updated_sql, note
        return sql, None

    # past event cap
    if fd_context and not dp_context:
        date_expr = "COALESCE(delivery_to_consignee_date, empty_container_return_date)"
        if re.search(
            r"\b(delivery_to_consignee_date|empty_container_return_date)\b\s*(<=|>=|=|<|>|between)\b",
            sql,
            re.I,
        ):
            return sql, None
    else:
        date_expr = "COALESCE(ata_dp_date, best_eta_dp_date)"
        if re.search(
            r"\b(ata_dp_date|best_eta_dp_date)\b\s*(<=|>=|=|<|>|between)\b",
            sql,
            re.I,
        ):
            return sql, None

    condition = (
        f"{date_expr} >= CURRENT_DATE - INTERVAL {DEFAULT_PAST_WINDOW_DAYS} DAY "
        f"AND {date_expr} <= CURRENT_DATE"
    )
    updated_sql, changed = _inject_filter_condition(sql, condition)
    if changed:
        note = f"Applied default past window: CURRENT_DATE - {DEFAULT_PAST_WINDOW_DAYS} days to CURRENT_DATE."
        return updated_sql, note
    return sql, None


def _apply_default_delay_cap(question: str, sql: str) -> tuple[str, Optional[str]]:
    lowered = (question or "").lower()
    if _contains_explicit_time_window(question):
        return sql, None

    mentions_delayed = any(k in lowered for k in ["delay", "delayed", "late"])
    mentions_early = "early" in lowered
    if not (mentions_delayed or mentions_early):
        return sql, None

    delay_col = (
        "fd_delayed_dur"
        if _is_fd_context(question) and not _is_dp_context(question)
        else "dp_delayed_dur"
    )
    if _has_existing_column_constraint(sql, delay_col):
        return sql, None

    # honor configured default threshold; skip applying if non-positive (disabled)
    if DEFAULT_DELAY_THRESHOLD_DAYS <= 0:
        return sql, None

    if mentions_delayed and not mentions_early:
        condition = f"{delay_col} >= {DEFAULT_DELAY_THRESHOLD_DAYS}"
    elif mentions_early and not mentions_delayed:
        condition = f"{delay_col} > -{DEFAULT_DELAY_THRESHOLD_DAYS}"
    else:
        condition = f"ABS({delay_col}) <= {DEFAULT_DELAY_THRESHOLD_DAYS}"

    updated_sql, changed = _inject_filter_condition(sql, condition)
    if changed:
        note = f"Applied default delay/early threshold: {DEFAULT_DELAY_THRESHOLD_DAYS} days."
        return updated_sql, note
    return sql, None


def _apply_default_query_caps(question: str, sql: str) -> tuple[str, List[str]]:
    notes: List[str] = []
    updated_sql = sql

    updated_sql, time_note = _apply_default_time_cap(question, updated_sql)
    if time_note:
        notes.append(time_note)

    updated_sql, delay_note = _apply_default_delay_cap(question, updated_sql)
    if delay_note:
        notes.append(delay_note)

    # Deduplicate in case a repair path re-applies logic.
    notes = list(dict.fromkeys(notes))
    return updated_sql, notes


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
            usage_metadata[k] = usage_metadata.get(k, 0) + v  # type: ignore
    state["usage_metadata"] = usage_metadata


def _repair_generated_sql(
    question: str,
    sql: str,
    error_msg: str,
    columns: List[str],
    sample_markdown: str,
) -> tuple[str, Dict[str, Any]]:
    if is_test_mode():
        return "", {}

    repair_prompt = f"""
You are fixing DuckDB SQL code that failed to run.
Return ONLY corrected SQL in a ```sql``` block.

Rules:
- Query against the view `df`.
- Ensure date operations use DuckDB syntax (e.g., strftime).
- Be careful with list columns (e.g. use list_has_any or related functions).

Question:
{question}

Columns:
{columns}

Sample rows:
{sample_markdown}

Previous SQL:
```sql
{sql}
```

Error:
{error_msg}
""".strip()

    chat = _get_chat()
    resp = chat.chat_completion(
        [{"role": "user", "content": repair_prompt}],
        temperature=0.0,
    )
    fixed = _extract_sql_code(resp.get("content", ""))
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
        safe_columns = [str(c) for c in columns]  # type: ignore
        safe_rows: List[Dict[str, Any]] = []
        for row in rows[:500]:  # type: ignore
            if not isinstance(row, dict):
                continue
            safe_row = {col: row.get(col) for col in safe_columns}  # type: ignore
            safe_rows.append(safe_row)  # type: ignore

        if safe_rows:
            return {
                "columns": safe_columns,
                "rows": safe_rows,
                "title": "Analytics Result",
            }

    result_value = exec_result.get("result_value")
    if isinstance(result_value, dict) and result_value:
        dict_rows = [{"label": str(k), "value": v} for k, v in result_value.items()]  # type: ignore
        return {
            "columns": ["label", "value"],
            "rows": dict_rows[:500],
            "title": "Analytics Result",
        }

    if isinstance(result_value, list) and result_value:
        first = result_value[0]  # type: ignore
        if isinstance(first, dict):
            cols: List[str] = []
            for item in result_value:  # type: ignore
                if not isinstance(item, dict):
                    continue
                for key in item.keys():  # type: ignore
                    k = str(key)  # type: ignore
                    if k not in cols:
                        cols.append(k)
            if cols:
                list_rows: List[Dict[str, Any]] = []
                for item in result_value[:500]:  # type: ignore
                    if not isinstance(item, dict):
                        continue
                    list_rows.append({c: item.get(c) for c in cols})  # type: ignore
                if list_rows:
                    return {
                        "columns": cols,
                        "rows": list_rows,
                        "title": "Analytics Result",
                    }
        else:
            scalar_rows = [{"value": item} for item in result_value[:500]]  # type: ignore
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

    columns = table_spec.get("columns") or []  # type: ignore
    rows = table_spec.get("rows") or []  # type: ignore
    if not isinstance(columns, list) or not isinstance(rows, list):
        return None
    if len(columns) < 2 or len(rows) == 0:  # type: ignore
        return None

    sample_rows = [r for r in rows[:80] if isinstance(r, dict)]  # type: ignore
    if not sample_rows:
        return None

    numeric_cols: List[str] = []
    categorical_cols: List[str] = []
    for col in columns:  # type: ignore
        numeric_hits = 0
        for row in sample_rows:  # type: ignore
            if _as_float(row.get(col)) is not None:  # type: ignore
                numeric_hits += 1
        if numeric_hits > 0:
            numeric_cols.append(str(col))  # type: ignore
        else:
            categorical_cols.append(str(col))  # type: ignore

    if not numeric_cols:
        return None

    kind = _chart_kind(question)

    if kind == "pie":
        label_col = categorical_cols[0] if categorical_cols else str(columns[0])  # type: ignore
        value_col = (
            next((c for c in numeric_cols if c != label_col), None) or numeric_cols[0]
        )
        chart_data: List[Dict[str, Any]] = []
        for row in sample_rows[:50]:  # type: ignore
            value = _as_float(row.get(value_col))  # type: ignore
            if value is None:
                continue
            label = row.get(label_col)  # type: ignore
            chart_data.append(
                {label_col: str(label) if label is not None else "-", value_col: value}  # type: ignore
            )
        if not chart_data:
            return None
        return {
            "kind": "pie",
            "title": table_spec.get("title") or "Analytics Pie Chart",
            "data": chart_data,
            "encodings": {"label": label_col, "value": value_col},
        }

    x_col = categorical_cols[0] if categorical_cols else str(columns[0])  # type: ignore
    y_col = next((c for c in numeric_cols if c != x_col), None) or numeric_cols[0]

    chart_data = []
    for row in sample_rows[:80]:  # type: ignore
        y_val = _as_float(row.get(y_col))  # type: ignore
        if y_val is None:
            continue
        point: Dict[str, Any] = {
            x_col: row.get(x_col),  # type: ignore
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


def _normalize_identifier_values(raw_value: Any) -> List[str]:
    if raw_value is None:
        return []

    if isinstance(raw_value, list):
        values = raw_value  # type: ignore
    else:
        values = [raw_value]

    normalized: List[str] = []
    for value in values:  # type: ignore
        if value is None:
            continue
        if isinstance(value, str) and "," in value:
            parts = [part.strip() for part in value.split(",")]
        else:
            parts = [str(value).strip()]  # type: ignore
        for part in parts:
            cleaned = part.strip().upper()
            if cleaned:
                normalized.append(cleaned)
    return list(dict.fromkeys(normalized))


def _selector_has_ids(selector: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(selector, dict):
        return False
    raw_ids = selector.get("ids")
    if not isinstance(raw_ids, dict):
        return False
    for field in ("container_number", "po_numbers", "booking_numbers", "obl_nos"):
        if _normalize_identifier_values(raw_ids.get(field)):  # type: ignore
            return True
    return False


def _build_result_selector(exec_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    result_rows = exec_result.get("result_rows")
    if not isinstance(result_rows, list) or not result_rows:
        return None

    ids: Dict[str, List[str]] = {
        "container_number": [],
        "po_numbers": [],
        "booking_numbers": [],
        "obl_nos": [],
    }

    for row in result_rows:  # type: ignore
        if not isinstance(row, dict):
            continue
        for field in ids.keys():
            values = _normalize_identifier_values(row.get(field))  # type: ignore
            if values:
                ids[field].extend(values)

    normalized_ids = {
        field: list(dict.fromkeys(values)) for field, values in ids.items() if values
    }

    if not normalized_ids:
        return None

    return {
        "kind": "id_sets",
        "ids": normalized_ids,
        "row_count": len([row for row in result_rows if isinstance(row, dict)]),  # type: ignore
    }


def analytics_planner_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    DuckDB analytics node.
    1. Resolves the analytics parquet path.
    2. Creates a scoped DuckDB view for the authorized user.
    3. Generates SQL with the LLM.
    4. Executes the SQL and returns structured table/chart artifacts.
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
        analytics_context_mode = str(
            state.get("analytics_context_mode") or "session"
        ).strip()

        # 0. Safety Check
        if not consignee_codes:
            state["answer_text"] = (
                "No consignee codes provided. Please select at least one code to view data."
            )
            state["is_satisfied"] = True
            state["messages"] = [AIMessage(content=state["answer_text"])]
            return state

        # 1. Load Data/Path
        try:
            blob_mgr = _get_blob_manager()
            parquet_path = blob_mgr.get_local_path()

            engine = _get_duckdb_engine()
            selector: Optional[Dict[str, Any]] = None
            scope_label = "authorized session scope"

            if analytics_context_mode == "previous_result":
                selector_candidate = (
                    state.get("last_analytics_result_selector")
                    if isinstance(state.get("last_analytics_result_selector"), dict)
                    else None
                )
                if not _selector_has_ids(selector_candidate):
                    state["answer_text"] = (
                        "I couldn't reuse the previous analytics result list for this follow-up. "
                        "Please rerun the previous list query or choose the full scope."
                    )
                    state["is_satisfied"] = True
                    state.setdefault("notices", []).append(
                        "Previous analytics result subset could not be reconstructed."
                    )
                    state["messages"] = [AIMessage(content=state["answer_text"])]
                    return state

                selector = selector_candidate
                selector_count = selector.get("row_count")  # type: ignore
                scope_label = "previous analytics result scope"
                if isinstance(selector_count, int) and selector_count >= 0:
                    scope_label = (
                        f"previous analytics result scope ({selector_count} rows)"
                    )

            engine.prepare_view(parquet_path, consignee_codes, selector=selector)  # type: ignore

            scope_row_count = engine.con.sql("SELECT count(*) AS row_count FROM df").fetchone()[0]  # type: ignore
            if int(scope_row_count or 0) <= 0:  # type: ignore
                if analytics_context_mode == "previous_result":
                    state["answer_text"] = (
                        "The previous analytics result list no longer matches any rows in the current session data. "
                        "Please rerun the previous query."
                    )
                    state.setdefault("notices", []).append(
                        "Previous analytics result subset resolved to 0 rows."
                    )
                else:
                    state["answer_text"] = (
                        "I found no data available in the current authorized analytics scope."
                    )
                state["is_satisfied"] = True
                state["messages"] = [AIMessage(content=state["answer_text"])]
                return state

            if analytics_context_mode == "previous_result":
                state.setdefault("notices", []).append(
                    f"Applied previous analytics result scope ({int(scope_row_count)} rows)."  # type: ignore
                )

            sample_rel = engine.con.sql("SELECT * FROM df LIMIT 5")
            df_head = sample_rel.df()
            columns = df_head.columns.tolist()

            if df_head.empty:
                state["answer_text"] = (
                    "I found no data available in the master dataset."
                )
                state["is_satisfied"] = True
                state["messages"] = [AIMessage(content=state["answer_text"])]
                return state

        except Exception as e:
            logger.error(f"Analytics Data Path Resolution Failed: {e}")
            state.setdefault("errors", []).append(f"Data Path Error: {e}")
            state["answer_text"] = (
                "I couldn't access the analytics dataset right now. "
                "Please try again in a moment."
            )
            state["is_satisfied"] = True
            state["messages"] = [AIMessage(content=state["answer_text"])]
            return state

        # 2. Prepare Context
        head_sample = df_head.to_markdown(index=False)
        shape_info = f"Rows in scope: {int(scope_row_count)}; Columns: {len(columns)}"  # type: ignore

        ready_ref_content = load_ready_ref()
        col_ref = format_analytics_column_reference(columns)

        system_prompt = f"""
You are a SQL Data Analyst powered by DuckDB. You have access to a view `df` containing shipment data.
Your goal is to write SQL queries to answer the user's question using `df`.

## Context
Today's Date: {state.get('today_date')}
Execution Scope: {scope_label}

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
1. Write valid DuckDB SQL queries. 
2. Query against the view `df`. `df` is already filtered for the current user's authorized scope.
3. For "How many" or "Total" questions, use `COUNT(*)` or `SUM()`.
4. **TYPE CASTING RULE:** If you need to perform math (SUM, AVG, comparisons) on columns specified as 'number' in metadata but stored as strings in the schema (e.g., `cargo_weight_kg`, `teus`), ALWAYS explicitly cast them: `column::DOUBLE`.
5. **STRICT RULE:** Never include internal technical columns like {INTERNAL_COLUMNS} in the final output.
6. **RELEVANCE:** When returning tables, select only the columns relevant to the user's question.
7. **DATE FORMATTING:** Whenever displaying or returning a date column, ALWAYS use `strftime(column, '%d-%b-%Y')` to ensure a clean, user-friendly format (e.g., '22-Jul-2025').
8. **COLUMN SELECTION:**
   - For discharge-port ETA/arrival windows and overdue checks, use `best_eta_dp_date` (fallback: `eta_dp_date`).
   - For actual DP-arrival checks, use `ata_dp_date` (fallback: `derived_ata_dp_date` if needed).
   - If user asks "not yet arrived at DP": filter `ata_dp_date IS NULL`.
   - If user asks "failed/missed ETA at DP": filter `(ata_dp_date IS NULL) AND (best_eta_dp_date <= CURRENT_DATE)`.
   - For final destination ETA logic, use `best_eta_fd_date` (fallback: `eta_fd_date`).
9. Use `ILIKE '%pattern%'` for flexible case-insensitive text filtering.
10. **SORTING RULE:** For results containing date columns, sort by latest date first (DESC) BEFORE formatting if possible, or ensure logical sorting.
11. Return ONLY the SQL inside a ```sql``` block. Explain your logic briefly outside the block.

## Examples:
User: "How many delivered shipments?"
SQL:
```sql
SELECT count(*) as total FROM df WHERE shipment_status = 'DELIVERED';
```

User: "What is the total weight of my shipments?"
SQL:
```sql
SELECT sum(cargo_weight_kg::DOUBLE) as total_weight FROM df;
```

User: "Which carriers are involved?"
SQL:
```sql
SELECT DISTINCT final_carrier_name FROM df WHERE final_carrier_name IS NOT NULL;
```

User: "Show me shipments with more than 5 days delay."
SQL:
```sql
SELECT container_number, po_numbers, strftime(eta_dp_date, '%d-%b-%Y') as eta_dp, strftime(best_eta_dp_date, '%d-%b-%Y') as best_eta_dp, dp_delayed_dur, discharge_port 
FROM df 
WHERE dp_delayed_dur > 5 
ORDER BY best_eta_dp_date DESC;
```
"""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Question: {q}"},
        ]

        generated_sql = ""
        applied_default_caps: List[str] = []
        try:
            if is_test_mode():

                generated_sql = "SELECT count(*) as total FROM df"
            else:
                chat = _get_chat()
                resp = chat.chat_completion(messages, temperature=0.0)
                _merge_usage(state, resp.get("usage"))
                content = resp.get("content", "")
                generated_sql = _extract_sql_code(content)
                generated_sql, applied_default_caps = _apply_default_query_caps(
                    q, generated_sql
                )

        except Exception as e:
            logger.error(f"LLM SQL Gen Failed: {e}")
            state.setdefault("errors", []).append(f"SQL Gen Error: {e}")
            state["answer_text"] = (
                "I couldn't generate the analytics query in time. "
                "Please narrow the request or try again."
            )
            state["is_satisfied"] = False
            state["reflection_feedback"] = (
                "SQL generation failed; retry analytics with a simpler plan."
            )
            return state

        # if not generated_code:
        if not generated_sql:
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

        exec_attempts = 1
        exec_result = engine.execute_query(
            parquet_path,
            generated_sql,
            consignee_codes,  # type: ignore
            selector=selector,
        )

        if not exec_result.get("success"):
            error_msg = str(exec_result.get("error") or "")
            logger.warning(
                "Initial SQL execution failed, attempting one repair: %s",
                error_msg,
            )
            try:
                repaired_sql, repair_usage = _repair_generated_sql(
                    question=q,
                    sql=generated_sql,
                    error_msg=error_msg,
                    columns=columns,
                    sample_markdown=head_sample,
                )
                _merge_usage(state, repair_usage)
                if repaired_sql and repaired_sql != generated_sql:
                    generated_sql = repaired_sql
                    generated_sql, reapplied_caps = _apply_default_query_caps(
                        q, generated_sql
                    )
                    if reapplied_caps:
                        applied_default_caps = list(
                            dict.fromkeys(applied_default_caps + reapplied_caps)
                        )
                    exec_attempts += 1
                    exec_result = engine.execute_query(
                        parquet_path,
                        generated_sql,
                        consignee_codes,  # type: ignore
                        selector=selector,
                    )
            except Exception as repair_exc:
                logger.warning("Analytics SQL repair pass failed: %s", repair_exc)

        if exec_result["success"]:
            result_rows = exec_result.get("result_rows")
            if isinstance(result_rows, list) and len(result_rows) == 0:  # type: ignore
                if analytics_context_mode == "previous_result":
                    state["answer_text"] = (
                        "I couldn't find any records in the previous analytics result "
                        "scope that match this filter."
                    )
                else:
                    state["answer_text"] = (
                        "I couldn't find any records in your current authorized "
                        "analytics scope that match this filter."
                    )

                state["is_satisfied"] = True
                state["analytics_last_error"] = None
                state["analytics_attempt_count"] = exec_attempts
                state["table_spec"] = None
                state["chart_spec"] = None
                state["last_analytics_result_selector"] = None
                state["last_analytics_result_count"] = 0
                state["last_analytics_question"] = q
                if applied_default_caps:
                    state.setdefault("notices", []).extend(applied_default_caps)
                    state["answer_text"] = (
                        "Note: "
                        + " ".join(applied_default_caps)
                        + "\n\n"
                        + state["answer_text"]
                    )
                state["messages"] = [AIMessage(content=state["answer_text"])]
                return state

            final_answer = str(
                exec_result.get("result") or exec_result.get("final_answer") or ""
            ).strip()
            if final_answer:
                state["answer_text"] = (
                    f"Here is what I found - {len(result_rows)} item:\n{final_answer}"
                )
            else:
                state["answer_text"] = "I ran the analytics query successfully."
            if applied_default_caps:
                state.setdefault("notices", []).extend(applied_default_caps)
                state["answer_text"] = (
                    "Note: "
                    + " ".join(applied_default_caps)
                    + "\n\n"
                    + state["answer_text"]
                )
            state["is_satisfied"] = True
            state["analytics_last_error"] = None
            state["analytics_attempt_count"] = exec_attempts

            table_spec = _build_table_spec_from_exec(exec_result)
            state["table_spec"] = table_spec

            chart_spec = _build_chart_spec_from_table(q, table_spec)
            state["chart_spec"] = chart_spec

            selector_metadata = _build_result_selector(exec_result)
            state["last_analytics_result_selector"] = selector_metadata
            state["last_analytics_result_count"] = (
                selector_metadata.get("row_count")
                if isinstance(selector_metadata, dict)
                else None
            )
            state["last_analytics_question"] = q
            state["messages"] = [AIMessage(content=state["answer_text"])]
        else:
            state["answer_text"] = "I couldn't run that analytics query successfully."
            state["is_satisfied"] = False
            state["analytics_last_error"] = str(exec_result.get("error") or "")
            state["analytics_attempt_count"] = exec_attempts
            state["table_spec"] = None
            state["chart_spec"] = None
            state["messages"] = [AIMessage(content=state["answer_text"])]

    return state
