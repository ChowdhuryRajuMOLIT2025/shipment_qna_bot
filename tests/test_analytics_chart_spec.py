from datetime import date, timedelta

import pandas as pd

from shipment_qna_bot.graph.nodes import analytics_planner as analytics_module
from shipment_qna_bot.tools.duckdb_engine import DuckDBAnalyticsEngine


class _StubBlobManager:
    def __init__(self, parquet_path: str):
        self._parquet_path = parquet_path

    def get_local_path(self):
        return self._parquet_path

    def get_local_path(self):
        # We need a dummy path for duckdb to execute against a view. The actual test engine mock ignores it anyway.
        return "dummy.parquet"


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


<<<<<<< HEAD
=======
class _StubCon:
    def __init__(self):
        self.df = lambda: pd.DataFrame(
            {"discharge_port": ["LOS ANGELES"], "count": [12]}
        )

    def sql(self, *args, **kwargs):
        # returns self so `sample_rel.df()` can be called in the analytics code
        return self


class _StubEngine:
    def __init__(self, exec_result):
        self._exec_result = exec_result
        self.con = _StubCon()

    def execute_query(self, parquet_path, sql, consignee_codes):
        return dict(self._exec_result)


>>>>>>> old_main_dec25
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


def _write_parquet_with_time_windows(tmp_path):
    path = tmp_path / "analytics_time_windows.parquet"
    today = date.today()
    df = pd.DataFrame(
        {
            "container_number": ["CONT_IN", "CONT_OUT", "CONT_PAST", "CONT_DELAY"],
            "consignee_codes": [["0000866"], ["0000866"], ["0000866"], ["0000866"]],
            "discharge_port": [
                "LOS ANGELES",
                "LOS ANGELES",
                "LOS ANGELES",
                "LOS ANGELES",
            ],
            "shipment_status": ["IN_OCEAN", "IN_OCEAN", "DELIVERED", "AT_DP"],
            "po_numbers": [["PO1"], ["PO2"], ["PO3"], ["PO4"]],
            "booking_numbers": [["BK1"], ["BK2"], ["BK3"], ["BK4"]],
            "obl_nos": [["OBL1"], ["OBL2"], ["OBL3"], ["OBL4"]],
            "best_eta_dp_date": pd.to_datetime(
                [
                    today + timedelta(days=5),
                    today + timedelta(days=45),
                    today - timedelta(days=3),
                    today + timedelta(days=2),
                ]
            ),
            "ata_dp_date": pd.to_datetime(
                [None, None, today - timedelta(days=40), today - timedelta(days=10)]
            ),
            "derived_ata_dp_date": pd.to_datetime(
                [None, None, today - timedelta(days=40), today - timedelta(days=10)]
            ),
            "dp_delayed_dur": [2, 5, 1, 8],
            "fd_delayed_dur": [0, 0, 0, 0],
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
<<<<<<< HEAD
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
=======
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
>>>>>>> old_main_dec25
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
<<<<<<< HEAD
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
=======
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
>>>>>>> old_main_dec25
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
<<<<<<< HEAD
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
=======
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result())
>>>>>>> old_main_dec25
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
<<<<<<< HEAD
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


def test_analytics_empty_result_set_returns_clear_message(monkeypatch, tmp_path):
    parquet_path = _write_parquet(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number FROM df WHERE shipment_status = 'CANCELLED'"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
=======
        analytics_module, "_get_duckdb_engine", lambda: _StubEngine(_exec_result_dict())
>>>>>>> old_main_dec25
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show cancelled shipments")
    )

    assert new_state["is_satisfied"] is True
    assert "couldn't find any records" in (new_state.get("answer_text") or "").lower()
    assert "here is what i found" not in (new_state.get("answer_text") or "").lower()
    assert new_state.get("table_spec") is None
    assert new_state.get("chart_spec") is None


def test_analytics_applies_default_future_window_cap(monkeypatch, tmp_path):
    parquet_path = _write_parquet_with_time_windows(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number, best_eta_dp_date FROM df "
            "WHERE best_eta_dp_date IS NOT NULL ORDER BY best_eta_dp_date"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("what are the shipment will arrive in dp/port")
    )

    assert "default future window" in (new_state.get("answer_text") or "").lower()
    assert new_state["table_spec"] is not None
    rows = new_state["table_spec"]["rows"]
    assert len(rows) == 1
    assert {row["container_number"] for row in rows} == {"CONT_IN"}


def test_analytics_arriving_phrase_applies_future_and_not_arrived_cap(
    monkeypatch, tmp_path
):
    parquet_path = _write_parquet_with_time_windows(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number, po_numbers, discharge_port, best_eta_dp_date "
            "FROM df WHERE discharge_port ILIKE '%los angeles%' "
            "ORDER BY best_eta_dp_date DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("What are the POs arriving in Los Angeles?")
    )

    assert "default future window" in (new_state.get("answer_text") or "").lower()
    assert new_state["table_spec"] is not None
    rows = new_state["table_spec"]["rows"]
    assert len(rows) == 1
    assert rows[0]["container_number"] == "CONT_IN"


def test_analytics_applies_default_past_window_cap(monkeypatch, tmp_path):
    parquet_path = _write_parquet_with_time_windows(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number, ata_dp_date FROM df "
            "WHERE ata_dp_date IS NOT NULL ORDER BY ata_dp_date DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("what shipment arrived in dp/port")
    )

    assert "default past window" in (new_state.get("answer_text") or "").lower()
    assert new_state["table_spec"] is not None
    rows = new_state["table_spec"]["rows"]
    assert len(rows) == 1
    assert rows[0]["container_number"] == "CONT_DELAY"


def test_analytics_applies_default_delay_threshold_cap(monkeypatch, tmp_path):
    parquet_path = _write_parquet_with_time_windows(tmp_path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(parquet_path)
    )
    monkeypatch.setattr(
        analytics_module,
        "_get_chat",
        lambda: _StubChatTool(
            "SELECT container_number, dp_delayed_dur FROM df ORDER BY dp_delayed_dur DESC"
        ),
    )
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", lambda: DuckDBAnalyticsEngine()
    )

    new_state = analytics_module.analytics_planner_node(
        _base_state("show me delayed containers in dp/port")
    )

    assert (
        "default delay/early threshold: 7 days"
        in (new_state.get("answer_text") or "").lower()
    )
    assert new_state["table_spec"] is not None
    rows = new_state["table_spec"]["rows"]
    assert len(rows) == 1
    assert rows[0]["container_number"] == "CONT_DELAY"
