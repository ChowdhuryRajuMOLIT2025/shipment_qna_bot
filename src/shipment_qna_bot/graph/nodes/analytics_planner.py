import json
import re
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

    with log_node_execution("AnalyticsPlanner", {"intent": state.get("intent")}):
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
            return state

        # 2. Prepare Context for LLM
        columns = list(df.columns)
        # Head sample (first 3 rows) to help LLM understand values
        head_sample = df.head(3).to_markdown(index=False)
        shape_info = f"Rows: {df.shape[0]}, Columns: {df.shape[1]}"

        # Dynamic Column Reference
        col_ref = ""
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
8. Use `str.contains(..., na=False, case=False)` for flexible text filtering.
9. Return ONLY the code inside a ```python``` block. Explain your logic briefly outside the block.

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

User: "Show me shipments with more than 5 days delay at discharge port."
Code:
```python
# Select only relevant columns and format dates
cols = ['container_number', 'eta_dp_date', 'optimal_ata_dp_date', 'dp_delayed_dur', 'discharge_port']
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
cols = ['container_number', 'etd_lp_date', 'load_port']
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
            return state

        # 4. Execute Code
        if not generated_code:
            state.setdefault("errors", []).append("LLM produced no code.")
            return state

        engine = _get_pandas_engine()
        exec_result = engine.execute_code(df, generated_code)

        if exec_result["success"]:
            final_ans = exec_result.get("final_answer", "")
            # Basic formatting if it's just a raw value
            state["answer_text"] = f"Analysis Result:\n{final_ans}"
            state["is_satisfied"] = True

            # TODO: If we want to pass chart specs, we'd parse that here.
        else:
            error_msg = exec_result.get("error")
            logger.warning(f"Pandas Execution Error: {error_msg}")
            # We can allow the Judge to see this or retry.
            # For now, let's treat it as a failure to satisfy.
            state.setdefault("errors", []).append(f"Analysis Failed: {error_msg}")
            state["is_satisfied"] = (
                False  # Logic might trigger retry if Judge sees this.
            )

    return state
