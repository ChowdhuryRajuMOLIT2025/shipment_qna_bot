import uuid

import pandas as pd

from shipment_qna_bot.graph import builder as builder_module
from shipment_qna_bot.graph.nodes import analytics_planner as analytics_module


class _StubBlobManager:
    def __init__(self, parquet_path: str):
        self._parquet_path = parquet_path

    def get_local_path(self):
        return self._parquet_path


class _StubChatTool:
    def chat_completion(self, messages, temperature=0.0):
        question = ""
        for msg in reversed(messages):
            content = str(msg.get("content") or "")
            if "Question:" in content:
                question = content
                break
        lowered = question.lower()
        if "which are hot" in lowered:
            sql = (
                "SELECT container_number, po_numbers, discharge_port, hot_container_flag "
                "FROM df WHERE hot_container_flag = TRUE ORDER BY container_number"
            )
        else:
            sql = (
                "SELECT container_number, po_numbers, discharge_port, hot_container_flag "
                "FROM df ORDER BY container_number"
            )
        return {
            "content": f"```sql\n{sql}\n```",
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
            },
        }


def test_graph_preserves_analytics_followup_scope(monkeypatch, tmp_path):
    path = tmp_path / "continuity.parquet"
    df = pd.DataFrame(
        {
            "container_number": ["CONT1", "CONT2", "CONT3"],
            "consignee_codes": [["0000866"], ["0000866"], ["0000866"]],
            "po_numbers": [["PO1"], ["PO2"], ["PO3"]],
            "booking_numbers": [["BK1"], ["BK2"], ["BK3"]],
            "obl_nos": [["OBL1"], ["OBL2"], ["OBL3"]],
            "discharge_port": ["LOS ANGELES", "NEW YORK", "LOS ANGELES"],
            "hot_container_flag": [True, False, True],
            "shipment_status": ["IN_OCEAN", "DELIVERED", "IN_OCEAN"],
        }
    )
    df.to_parquet(path)

    monkeypatch.setattr(analytics_module, "is_test_mode", lambda: False)
    monkeypatch.setattr(
        analytics_module, "_get_blob_manager", lambda: _StubBlobManager(str(path))
    )
    monkeypatch.setattr(analytics_module, "_get_chat", lambda: _StubChatTool())
    monkeypatch.setattr(
        analytics_module, "_get_duckdb_engine", analytics_module.DuckDBAnalyticsEngine
    )

    conversation_id = f"analytics-continuity-{uuid.uuid4()}"

    first_turn = builder_module.run_graph(
        {
            "conversation_id": conversation_id,
            "question_raw": "show me a breakdown of shipments by discharge port",
            "consignee_codes": ["0000866"],
            "usage_metadata": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
    )

    assert first_turn["intent"] == "analytics"
    assert first_turn["last_analytics_result_selector"]["ids"]["container_number"] == [
        "CONT1",
        "CONT2",
        "CONT3",
    ]

    second_turn = builder_module.run_graph(
        {
            "conversation_id": conversation_id,
            "question_raw": "which are hot?",
            "consignee_codes": ["0000866"],
            "usage_metadata": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
    )

    assert second_turn["intent"] == "clarification"
    assert "Reply with 1 or 2." in (second_turn.get("answer_text") or "")
    assert second_turn.get("pending_analytics_scope") is not None

    third_turn = builder_module.run_graph(
        {
            "conversation_id": conversation_id,
            "question_raw": "1",
            "consignee_codes": ["0000866"],
            "usage_metadata": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
    )

    assert third_turn["intent"] == "analytics"
    assert "Applied previous analytics result scope (3 rows)." in (
        third_turn.get("notices") or []
    )
    assert third_turn["table_spec"] is not None
    assert [row["container_number"] for row in third_turn["table_spec"]["rows"]] == [
        "CONT1",
        "CONT3",
    ]
