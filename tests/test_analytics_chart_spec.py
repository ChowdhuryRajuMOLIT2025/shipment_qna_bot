import pandas as pd

from shipment_qna_bot.graph.nodes import analytics_planner as analytics_module
from shipment_qna_bot.tools.duckdb_engine import DuckDBAnalyticsEngine


class _StubBlobManager:
    def __init__(self, parquet_path: str):
        self._parquet_path = parquet_path

    def get_local_path(self):
        return self._parquet_path


class _StubChatTool:
    def __init__(self, sql: str):
        self._sql = sql

    def chat_completion(self, messages, temperature=0.0):
        return {
            "content": f"```sql\n{self._sql}\n```",
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }


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
        "today_date": "2026-03-04",
    }


def _write_parquet(tmp_path):
    path = tmp_path / "analytics.parquet"
    df = pd.DataFrame(
        {
            "container_number": ["CONT1", "CONT2", "CONT3"],
            "consignee_codes": [["0000866"], ["0000866"], ["0000866"]],
            "discharge_port": ["LOS ANGELES", "NEW YORK", "LOS ANGELES"],
            "shipment_status": ["IN_OCEAN", "DELIVERED", "IN_OCEAN"],
            "po_numbers": [["PO1"], ["PO2"], ["PO3"]],
            "booking_numbers": [["BK1"], ["BK2"], ["BK3"]],
            "obl_nos": [["OBL1"], ["OBL2"], ["OBL3"]],
        }
    )
    df.to_parquet(path)
    return str(path)


def test_analytics_generates_bar_chart_spec(monkeypatch, tmp_path):
    parquet_path = _write_parquet(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT discharge_port, count(*) AS count "
            "FROM df GROUP BY discharge_port ORDER BY count DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show bar chart of shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state["table_spec"]["columns"] == ["discharge_port", "count"]
    assert new_state["table_spec"]["rows"][0]["discharge_port"] == "LOS ANGELES"
    assert new_state["table_spec"]["rows"][0]["count"] == 2

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "bar"
    assert new_state["chart_spec"]["encodings"]["x"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["y"] == "count"


def test_analytics_generates_pie_chart_spec(monkeypatch, tmp_path):
    parquet_path = _write_parquet(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT discharge_port, count(*) AS count "
            "FROM df GROUP BY discharge_port ORDER BY count DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show pie chart of shipment count by discharge_port")
    )

    assert new_state["chart_spec"] is not None
    assert new_state["chart_spec"]["kind"] == "pie"
    assert new_state["chart_spec"]["encodings"]["label"] == "discharge_port"
    assert new_state["chart_spec"]["encodings"]["value"] == "count"


def test_analytics_keeps_table_without_chart_when_not_requested(monkeypatch, tmp_path):
    parquet_path = _write_parquet(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT discharge_port, count(*) AS count "
            "FROM df GROUP BY discharge_port ORDER BY count DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("list shipment count by discharge_port")
    )

    assert new_state["table_spec"] is not None
    assert new_state.get("chart_spec") is None


def test_analytics_previous_result_scope_filters_view(monkeypatch, tmp_path):
    parquet_path = _write_parquet(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number, shipment_status FROM df ORDER BY container_number"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    state = _base_state("which shipments from above list are delivered?")
    state["analytics_context_mode"] = "previous_result"
    state["last_analytics_result_selector"] = {
        "kind": "id_sets",
        "ids": {"container_number": ["CONT2", "CONT3"]},
        "row_count": 2,
    }

    new_state = analytics_module.analytics_planner_node(state)

    assert new_state["table_spec"] is not None
    assert [row["container_number"] for row in new_state["table_spec"]["rows"]] == [
        "CONT2",
        "CONT3",
    ]
    assert "Applied previous analytics result scope (2 rows)." in (
        new_state.get("notices") or []
    )
    assert new_state["last_analytics_result_selector"]["ids"]["container_number"] == [
        "CONT2",
        "CONT3",
    ]
    assert new_state["last_analytics_result_count"] == 2
