import pandas as pd

from shipment_qna_bot.graph.nodes.clarification import clarification_node
from shipment_qna_bot.graph.nodes.judge import judge_node
from shipment_qna_bot.graph.nodes.static_greet_info_handler import \
    should_handle_overview
from shipment_qna_bot.tools.pandas_engine import PandasAnalyticsEngine


def _write_overview(tmp_path):
    content = """**Keywords:** MCS, MOL

**Company Overview**
MCS is a logistics provider.

**History**
- 2003 Founded
"""
    path = tmp_path / "overview_info.md"
    path.write_text(content, encoding="utf-8")
    return path


def test_overview_handles_mcs_definition_with_shipment_wording(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    assert should_handle_overview("What does MCS stand for in shipment data?")


def test_overview_rejects_specific_lookup_even_with_company_term(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    assert not should_handle_overview("What is ETA for MCS container ABCD1234567?")


def test_clarification_adds_two_scope_options():
    state = {
        "question_raw": "show me dates",
        "normalized_question": "show me dates",
        "messages": [],
        "intent": "clarification",
        "topic_shift_candidate": None,
        "pending_topic_shift": None,
    }

    new_state = clarification_node(state)

    assert new_state["intent"] == "clarification"
    assert new_state["is_satisfied"] is True
    assert "1)" in (new_state.get("answer_text") or "")
    assert "2)" in (new_state.get("answer_text") or "")
    assert isinstance(new_state.get("pending_topic_shift"), dict)


def test_judge_retries_analytics_failure_without_hits():
    state = {
        "question_raw": "How many delayed shipments?",
        "answer_text": "I couldn't run that analytics query successfully. Please try narrowing the request or rephrasing.",
        "hits": [],
        "intent": "analytics",
        "retry_count": 0,
        "errors": ["Analysis Failed: ValueError: test"],
    }

    new_state = judge_node(state)

    assert new_state["is_satisfied"] is False
    assert new_state["retry_count"] == 1
    assert "retry" in (new_state.get("reflection_feedback") or "").lower()


def test_pandas_preflight_rejects_disallowed_import():
    df = pd.DataFrame({"x": [1, 2, 3]})
    engine = PandasAnalyticsEngine()

    result = engine.execute_code(df, "import matplotlib.pyplot as plt\nresult = df['x'].sum()")

    assert result["success"] is False
    assert "not allowed" in (result.get("error") or "").lower()


def test_pandas_engine_coerces_string_ops():
    df = pd.DataFrame({"po_numbers": [5303012825, 5303012826]})
    engine = PandasAnalyticsEngine()

    code = (
        "df_filtered = df[df['po_numbers'].str.contains('530301', na=False)]\n"
        "result = df_filtered[['po_numbers']]"
    )
    result = engine.execute_code(df, code)

    assert result["success"] is True
    assert "5303012825" in (result.get("final_answer") or "")
