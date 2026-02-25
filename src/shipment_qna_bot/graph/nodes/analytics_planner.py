import json  # type: ignore
import os
import re
from typing import Any, Dict, List, Optional  # type: ignore

import pandas as pd

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
_READY_REF_SNIPPET_CACHE: Optional[str] = None

_PROMPT_SAMPLE_ROWS = 3
_PROMPT_SAMPLE_COLS = 18
_PROMPT_SAMPLE_CELL_CHARS = 80
_READY_REF_MAX_CHARS = 3600
_DEFAULT_ANALYTICS_WINDOW_DAYS = 90
_DEFAULT_DELAY_SEVERITY_CAP_DAYS = 7


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


def _truncate_prompt_text(text: str, limit: int) -> str:
    text = (text or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _prompt_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, (list, tuple, dict)):
        try:
            rendered = json.dumps(value, default=str)
        except Exception:
            rendered = str(value)  # type: ignore
    else:
        rendered = str(value)
    return _truncate_prompt_text(rendered, _PROMPT_SAMPLE_CELL_CHARS)


def _build_compact_df_sample(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no rows)"

    preferred_cols = [
        "container_number",
        "po_numbers",
        "shipment_status",
        "discharge_port",
        "final_destination",
        "best_eta_dp_date",
        "best_eta_fd_date",
        "eta_dp_date",
        "eta_fd_date",
        "ata_dp_date",
        "dp_delayed_dur",
        "fd_delayed_dur",
        "cargo_weight_kg",
        "final_carrier_name",
        "final_vessel_name",
    ]
    selected_cols: List[str] = [c for c in preferred_cols if c in df.columns]
    for c in df.columns:
        if c in selected_cols:
            continue
        selected_cols.append(c)
        if len(selected_cols) >= _PROMPT_SAMPLE_COLS:
            break

    sample_rows = min(_PROMPT_SAMPLE_ROWS, len(df))
    sample_df = df[selected_cols].head(sample_rows).copy().astype("object")
    for col in sample_df.columns:
        sample_df.loc[:, col] = sample_df[col].map(_prompt_cell)

    notes: List[str] = []
    if len(df) > sample_rows:
        notes.append(f"rows truncated to {sample_rows} of {len(df)}")
    if len(df.columns) > len(selected_cols):
        notes.append(f"columns truncated to {len(selected_cols)} of {len(df.columns)}")

    sample_md = sample_df.to_markdown(index=False)
    if notes:
        sample_md += "\n\n[compact sample: " + "; ".join(notes) + "]"
    return sample_md


def _load_ready_ref_snippet() -> str:
    global _READY_REF_SNIPPET_CACHE
    if _READY_REF_SNIPPET_CACHE is not None:
        return _READY_REF_SNIPPET_CACHE

    ready_ref_content = ""
    try:
        ready_ref_path = "docs/ready_ref.md"
        if not os.path.exists(ready_ref_path):
            base_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "../../../../")
            )
            ready_ref_path = os.path.join(base_dir, "docs", "ready_ref.md")

        if os.path.exists(ready_ref_path):
            with open(ready_ref_path, "r", encoding="utf-8") as f:
                ready_ref_content = f.read()
    except Exception as e:
        logger.warning(f"Could not load ready_ref.md: {e}")

    if ready_ref_content and len(ready_ref_content) > _READY_REF_MAX_CHARS:
        ready_ref_content = (
            ready_ref_content[:_READY_REF_MAX_CHARS].rstrip()
            + "\n\n[Ready Ref truncated for token budget.]"
        )

    _READY_REF_SNIPPET_CACHE = ready_ref_content  # type: ignore
    return _READY_REF_SNIPPET_CACHE


def _question_has_explicit_date_window(state: Dict[str, Any], question: str) -> bool:
    if state.get("time_window_days"):
        return True

    extracted = state.get("extracted_ids") or {}
    if isinstance(extracted, dict):
        date_range_val = extracted.get("date_range")
        if isinstance(date_range_val, list) and any(
            str(v).strip() for v in date_range_val
        ):
            return True
        if isinstance(date_range_val, str) and date_range_val.strip():
            return True

    q = (question or "").lower()
    month_re = (
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
        r"nov(?:ember)?|dec(?:ember)?)\b"
    )
    explicit_patterns = [
        month_re,
        r"\b\d{4}\b",
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        r"\b(?:today|yesterday|tomorrow)\b",
        r"\b(?:this|last|next)\s+(?:week|month|quarter|year)\b",
        r"\b(?:within|last|past|previous)\s+\d+\s+days?\b",
        r"\b(?:between|from)\b.*\b(?:to|and)\b",
    ]
    return any(re.search(pat, q) for pat in explicit_patterns)


def _mentions_fd_scope(text: str) -> bool:
    lowered = (text or "").lower()
    if "final destination" in lowered or "final_destination" in lowered:
        return True
    if "distribution center" in lowered or "distribution centre" in lowered:
        return True
    if re.search(r"\bin-?dc\b", lowered):
        return True
    if re.search(r"\bfd\b", lowered):
        return True
    return False


def _infer_default_window_cap(question: str) -> Optional[Dict[str, str]]:
    q = (question or "").lower()

    cargo_received = "cargo received" in q
    future_like = any(
        token in q
        for token in (
            "will receive",
            "incoming",
            "coming in",
            "arriving",
            "expected",
            "eta",
        )
    )
    received_like = any(
        token in q for token in (" received", "received ", "arrived", "arrival")
    )
    delay_like = "delay" in q or "delayed" in q or "late" in q
    early_like = "early" in q

    if cargo_received:
        return {
            "column": "cargo_receiveds_date",
            "direction": "past",
            "reason": "explicit cargo-received query",
        }

    if "received" in q and "not delivered" in q:
        return {
            "column": "ata_dp_date",
            "direction": "past",
            "reason": "received-but-not-delivered query",
        }

    if received_like and not future_like:
        return {
            "column": "ata_dp_date",
            "direction": "past",
            "reason": "received/arrived query",
        }

    if future_like:
        return {
            "column": (
                "best_eta_fd_date" if _mentions_fd_scope(q) else "best_eta_dp_date"
            ),
            "direction": "future",
            "reason": "incoming/future-arrival query",
        }

    if delay_like or early_like:
        return {
            "column": (
                "best_eta_fd_date" if _mentions_fd_scope(q) else "best_eta_dp_date"
            ),
            "direction": "past",
            "reason": "delay/early query without explicit date window",
        }

    return None


def _default_window_today(state: Dict[str, Any]) -> pd.Timestamp:
    raw = state.get("now_utc") or state.get("today_date")
    if raw:
        try:
            ts = pd.Timestamp(raw)
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.tz_convert(None)
            return ts.normalize()
        except Exception:
            pass
    # Fallback to current UTC date normalized, cast to naive for dataframe comparisons.
    return pd.Timestamp.utcnow().tz_localize(None).normalize()


def _apply_default_analytics_time_cap(
    df: pd.DataFrame, state: Dict[str, Any], question: str
) -> tuple[pd.DataFrame, Optional[str]]:
    if df.empty:
        return df, None

    if _question_has_explicit_date_window(state, question):
        return df, None

    cap_spec = _infer_default_window_cap(question)
    if not cap_spec:
        return df, None

    anchor_col = cap_spec["column"]
    if anchor_col not in df.columns:
        return df, None

    date_series = df[anchor_col]
    if not pd.api.types.is_datetime64_any_dtype(date_series):
        date_series = pd.to_datetime(date_series, errors="coerce")

    today = _default_window_today(state)
    days = _DEFAULT_ANALYTICS_WINDOW_DAYS
    if cap_spec["direction"] == "future":
        start_dt = today
        end_dt = today + pd.Timedelta(days=days)
    else:
        start_dt = today - pd.Timedelta(days=days)
        end_dt = today

    mask = date_series.notna() & (date_series >= start_dt) & (date_series <= end_dt)
    capped_df = df.loc[mask].copy()
    if len(capped_df) >= len(df):
        return df, None

    notice = (
        f"Applied default {days}-day "
        f"{'future' if cap_spec['direction'] == 'future' else 'lookback'} "
        f"window on `{anchor_col}` ({cap_spec['reason']}) because no date range was specified."
    )
    return capped_df, notice


def _question_has_explicit_delay_or_early_threshold(question: str) -> bool:
    q = (question or "").lower()
    if not any(token in q for token in ("delay", "delayed", "late", "early")):
        return False

    explicit_threshold_patterns = [
        r"\b(?:more than|greater than|less than|under|over|at least|at most)\s+\d+(?:\.\d+)?\s*days?\b",
        r"\bbetween\s+\d+(?:\.\d+)?\s*(?:and|to)\s*\d+(?:\.\d+)?\s*days?\b",
        r"\b\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*days?\b",
        r"\b(?:delay|delayed|late|early)\s+by\s+(?:more than\s+|less than\s+|over\s+|under\s+)?\d+(?:\.\d+)?\s*days?\b",
        r"\b(?:delay|delayed|late|early)\s+of\s+\d+(?:\.\d+)?\s*days?\b",
        r"\b\d+(?:\.\d+)?\s*days?\s+(?:delay|delayed|late|early)\b",
        r"[<>]=?\s*\d+(?:\.\d+)?\s*days?\b",
    ]
    return any(re.search(pat, q) for pat in explicit_threshold_patterns)


def _infer_default_delay_or_early_severity_cap(
    question: str,
) -> Optional[Dict[str, Any]]:
    q = (question or "").lower()
    delay_like = any(token in q for token in ("delay", "delayed", "late"))
    early_like = "early" in q

    if not delay_like and not early_like:
        return None
    if delay_like and early_like:
        return None
    if _question_has_explicit_delay_or_early_threshold(question):
        return None

    metric_col = "fd_delayed_dur" if _mentions_fd_scope(q) else "dp_delayed_dur"
    scope_label = "FD" if metric_col == "fd_delayed_dur" else "DP"
    days = _DEFAULT_DELAY_SEVERITY_CAP_DAYS

    if early_like:
        return {
            "column": metric_col,
            "kind": "early",
            "days": days,
            "notice": (
                f"No early threshold was specified, so I used the default {scope_label} early range: "
                f"1 to {days} days early ({metric_col} from -{days} to -1)."
            ),
        }

    return {
        "column": metric_col,
        "kind": "delay",
        "days": days,
        "notice": (
            f"No delay threshold was specified, so I used the default {scope_label} delay range: "
            f"1 to {days} days."
        ),
    }


def _apply_default_delay_or_early_severity_cap(
    df: pd.DataFrame, question: str
) -> tuple[pd.DataFrame, Optional[str]]:
    if df.empty:
        return df, None

    cap_spec = _infer_default_delay_or_early_severity_cap(question)
    if not cap_spec:
        return df, None

    metric_col = str(cap_spec["column"])
    if metric_col not in df.columns:
        return df, None

    metric = pd.to_numeric(df[metric_col], errors="coerce")
    days = float(cap_spec["days"])

    if cap_spec["kind"] == "early":
        mask = metric.notna() & (metric < 0) & (metric >= -days)
    else:
        mask = metric.notna() & (metric > 0) & (metric <= days)

    capped_df = df.loc[mask].copy()
    if len(capped_df) >= len(df):
        return df, None

    return capped_df, str(cap_spec["notice"])


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
            usage_metadata[k] = usage_metadata.get(k, 0) + v  # type: ignore
    state["usage_metadata"] = usage_metadata


def _normalize_selector_tokens(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)  # type: ignore
    else:
        raw_items = [value]

    out: List[str] = []
    for item in raw_items:  # type: ignore
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        for p in parsed:  # type: ignore
                            token = str(p).strip().upper()  # type: ignore
                            if token:
                                out.append(token)
                        continue
                except Exception:
                    pass
            if "," in text:
                for part in text.split(","):
                    token = part.strip().upper()
                    if token:
                        out.append(token)
                continue
            out.append(text.upper())
            continue
        token = str(item).strip().upper()  # type: ignore
        if token:
            out.append(token)

    # Deduplicate preserving order
    seen = set()  # type: ignore
    return [x for x in out if not (x in seen or seen.add(x))]  # type: ignore


def _extract_followup_selector(exec_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    candidate_df = exec_result.get("filtered_dataframe")
    if not isinstance(candidate_df, pd.DataFrame):
        alt_df = exec_result.get("result_dataframe")
        candidate_df = alt_df if isinstance(alt_df, pd.DataFrame) else None

    if candidate_df is None or candidate_df.empty:
        return None

    ids: Dict[str, List[str]] = {}
    for col in [
        "document_id",
        "doc_id",
        "container_number",
        "po_numbers",
        "booking_numbers",
        "obl_nos",
    ]:
        if col not in candidate_df.columns:
            continue
        values: List[str] = []
        for val in candidate_df[col].tolist():
            values.extend(_normalize_selector_tokens(val))
        if values:
            # Deduplicate preserving order
            seen = set()  # type: ignore
            ids[col] = [x for x in values if not (x in seen or seen.add(x))]  # type: ignore

    if not ids:
        return None

    return {
        "kind": "id_sets",
        "ids": ids,
        "row_count": int(len(candidate_df)),
    }


def _apply_followup_selector(
    df: pd.DataFrame, selector: Optional[Dict[str, Any]]
) -> Optional[pd.DataFrame]:
    if selector is None or not isinstance(selector, dict):  # type: ignore
        return None
    if df.empty:
        return df.copy()

    selector_ids = selector.get("ids")
    if not isinstance(selector_ids, dict):
        return None

    mask = pd.Series(False, index=df.index)
    used_rule = False

    for col in ["document_id", "doc_id", "container_number"]:
        values = selector_ids.get(col)  # type: ignore
        if col not in df.columns or not isinstance(values, list) or not values:
            continue
        allowed = {str(v).strip().upper() for v in values if str(v).strip()}  # type: ignore
        if not allowed:
            continue
        col_series = df[col].astype("string").str.upper()
        mask = mask | col_series.isin(allowed)
        used_rule = True

    for col in ["po_numbers", "booking_numbers", "obl_nos"]:
        values = selector_ids.get(col)  # type: ignore
        if col not in df.columns or not isinstance(values, list) or not values:
            continue
        allowed = {str(v).strip().upper() for v in values if str(v).strip()}  # type: ignore
        if not allowed:
            continue

        def _row_match(val: Any) -> bool:
            tokens = _normalize_selector_tokens(val)
            return bool(set(tokens) & allowed)

        mask = mask | df[col].apply(_row_match)
        used_rule = True

    if not used_rule:
        return None

    return df.loc[mask].copy()


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
        analytics_context_mode = str(
            state.get("analytics_context_mode") or "session"
        ).strip()

        # 0. Safety Check
        if not consignee_codes:
            state.setdefault("errors", []).append(
                "No authorized consignee codes for analytics."
            )
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

            if analytics_context_mode == "previous_result":
                scoped_df = _apply_followup_selector(
                    df,
                    (
                        state.get("last_analytics_result_selector")
                        if isinstance(state.get("last_analytics_result_selector"), dict)
                        else None
                    ),
                )
                if scoped_df is None:
                    state["answer_text"] = (
                        "I couldn't reuse the previous analytics result list for this follow-up. "
                        "Please rerun the previous list query or choose the full session scope."
                    )
                    state["is_satisfied"] = True
                    state.setdefault("notices", []).append(
                        "Previous analytics result subset could not be reconstructed."
                    )
                    return state
                if scoped_df.empty:
                    state["answer_text"] = (
                        "The previous analytics result list no longer matches any rows in the current session data. "
                        "Please rerun the previous query."
                    )
                    state["is_satisfied"] = True
                    state.setdefault("notices", []).append(
                        "Previous analytics result subset resolved to 0 rows."
                    )
                    return state
                df = scoped_df
                state.setdefault("notices", []).append(
                    f"Applied previous analytics result scope ({len(df)} rows)."
                )

            rows_before_default_cap = len(df)
            df, default_cap_notice = _apply_default_analytics_time_cap(df, state, q)
            if default_cap_notice:
                state.setdefault("notices", []).append(default_cap_notice)
                logger.info(
                    "Applied default analytics time cap rows_before=%s rows_after=%s",
                    rows_before_default_cap,
                    len(df),
                    extra={"step": "NODE:AnalyticsPlanner"},
                )
            rows_before_severity_cap = len(df)
            df, severity_cap_notice = _apply_default_delay_or_early_severity_cap(df, q)
            if severity_cap_notice:
                state.setdefault("notices", []).append(severity_cap_notice)
                logger.info(
                    "Applied default delay/early severity cap rows_before=%s rows_after=%s",
                    rows_before_severity_cap,
                    len(df),
                    extra={"step": "NODE:AnalyticsPlanner"},
                )

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
        # Use a compact bounded sample to avoid oversized prompts on wide datasets.
        head_sample = _build_compact_df_sample(df)
        scope_label = (
            "previous analytics result subset"
            if analytics_context_mode == "previous_result"
            else "authorized session scope"
        )
        shape_info = f"Rows: {df.shape[0]}, Columns: {df.shape[1]}, Execution Scope: {scope_label}"

        # Dynamic Column Reference
        ready_ref_content = _load_ready_ref_snippet()

        col_ref = ""
        # We have ready_ref, we might not need the auto-generated list,
        # but let's keep the auto-generated one for now as a fallback or concise list if ready_ref is missing columns.
        # Actually, the ready_ref to be THE source for LLM understanding.
        # Now, let's append the ready ref to the context.

        for k, v in ANALYTICS_METADATA.items():
            if k in columns:
                col_ref += f"- `{k}`: {v['desc']} (Type: {v['type']})\n"

        logger.info(
            "Analytics prompt context prepared: rows=%s cols=%s sample_chars=%s ready_ref_chars=%s",
            df.shape[0],
            df.shape[1],
            len(head_sample),
            len(ready_ref_content),
            extra={"step": "NODE:AnalyticsPlanner"},
        )

        system_prompt = f"""
You are a Pandas Data Analyst. You have access to a DataFrame `df` containing shipment data.
Your goal is to write Python code to answer the user's question using `df`.

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
            followup_selector = _extract_followup_selector(exec_result)

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
            state["pending_analytics_scope"] = None
            state["analytics_scope_candidate"] = None
            state["last_analytics_question"] = q

            if followup_selector:
                state["last_analytics_result_selector"] = followup_selector
                state["last_analytics_result_count"] = int(
                    followup_selector.get("row_count") or 0
                )
                logger.info(
                    "Stored analytics follow-up selector rows=%s keys=%s",
                    state["last_analytics_result_count"],
                    list((followup_selector.get("ids") or {}).keys()),  # type: ignore
                    extra={"step": "NODE:AnalyticsPlanner"},
                )
            else:
                state["last_analytics_result_selector"] = None
                state["last_analytics_result_count"] = None

            # TODO: If we want to pass chart specs, we'd parse that here.
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
