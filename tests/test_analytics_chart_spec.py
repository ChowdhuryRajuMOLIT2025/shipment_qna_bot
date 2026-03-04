import pandas as pd

from shipment_qna_bot.graph.nodes import analytics_planner as analytics_module


class _StubBlobManager:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def load_filtered_data(self, consignee_codes):
        return self._df.copy()


class _StubChatTool:
    def __init__(self, code: str):
        self._code = code

    def chat_completion(self, messages, temperature=0.0):
        return {
            "content": f"```python\n{self._code}\n```",
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }


class _StubEngine:
    def __init__(self, exec_result):
        self._exec_result = exec_result

    def execute_code(self, df, code):
        return dict(self._exec_result)


def _base_state(question: str):
    return {
        "question_raw": question,
        "normalized_question": question,
        "consignee_codes": ["0000866"],
        "intent": "analytics",
        "conversation_id": "chart-spec-test",
        "errors": [],
        "notices": [],
        "usage_metadata": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }


def _exec_result():
    return {
        "success": True,
        "output": "",
        "result": "| discharge_port | count |\n|---|---|\n| LOS ANGELES | 12 |",
        "final_answer": "| discharge_port | count |\n|---|---|\n| LOS ANGELES | 12 |",
        "result_type": "DataFrame",
        "filtered_rows": None,
        "filtered_preview": "",
        "result_columns": ["discharge_port", "count"],
        "result_rows": [
            {"discharge_port": "LOS ANGELES", "count": 12},
            {"discharge_port": "NEW YORK", "count": 7},
            {"discharge_port": "HOUSTON", "count": 4},
        ],
    }


def _exec_result_dict():
    return {
        "success": True,
        "output": "",
        "result": "{'November 2025': 139, 'December 2025': 160}",
        "final_answer": "{'November 2025': 139, 'December 2025': 160}",
        "result_type": "dict",
        "filtered_rows": None,
        "filtered_preview": "",
        "result_columns": None,
        "result_rows": None,
        "result_value": {"November 2025": 139, "December 2025": 160},
    }


def test_analytics_generates_bar_chart_spec(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_pandas_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show bar chart of shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state["table_spec"]["columns"] == ["discharge_port", "count"]
    assert len(new_state["table_spec"]["rows"]) == 3

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "bar"
    assert new_state["chart_spec"]["encodings"]["x"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["y"] == "count"


def test_analytics_generates_pie_chart_spec(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_pandas_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show pie chart of shipment count by discharge_port")
    )

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "pie"
    assert new_state["chart_spec"]["encodings"]["label"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["value"] == "count"


def test_analytics_keeps_table_without_chart_when_not_requested(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module, "_get_chat", lambda: _StubChatTool("result = df")
    )
    monkeypatch.setattr(
        analytics_module, "_get_pandas_engine", lambda: _StubEngine(_exec_result())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("list shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state.get("chart_spec") is None


def test_analytics_dict_result_generates_pie_chart(monkeypatch):
    df = pd.DataFrame(
        {"discharge_port": ["LOS ANGELES"], "shipment_status": ["IN_OCEAN"]}
    )

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool("result = {'November 2025': 139, 'December 2025': 160}"),
    )
    monkeypatch.setattr(
        analytics_module, "_get_pandas_engine", lambda: _StubEngine(_exec_result_dict())
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state(
            "show pie chart for containers received november 2025 vs december 2025"
        )
    )

    assert new_state["table_spec"] is not None
    assert new_state["table_spec"]["columns"] == ["label", "value"]
    assert len(new_state["table_spec"]["rows"]) == 2

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "pie"
    assert new_state["chart_spec"]["encodings"]["label"] == "label"
    assert new_state["chart_spec"]["encodings"]["value"] == "value"
