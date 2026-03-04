from shipment_qna_bot.graph.nodes.clarification import clarification_node
from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.judge import judge_node
from shipment_qna_bot.graph.nodes.normalizer import normalize_node
from shipment_qna_bot.graph.nodes.static_greet_info_handler import \
    should_handle_overview


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


def test_clarification_adds_analytics_scope_options():
    state = {
        "question_raw": "which are hot?",
        "normalized_question": "which are hot?",
        "messages": [],
        "intent": "analytics",
        "topic_shift_candidate": None,
        "analytics_scope_candidate": {
            "raw_question": "which are hot?",
            "normalized_question": "which are hot?",
            "previous_result_count": 12,
        },
    }

    new_state = clarification_node(state)

    assert new_state["intent"] == "clarification"
    assert new_state["is_satisfied"] is True
    assert "previous analytics result" in (new_state.get("answer_text") or "").lower()
    assert isinstance(new_state.get("pending_analytics_scope"), dict)


def test_normalizer_uses_previous_result_scope_hint():
    state = {
        "question_raw": "which are hot from above list",
        "messages": [],
        "last_analytics_result_selector": {
            "kind": "id_sets",
            "ids": {"container_number": ["SEGU5935510"]},
            "row_count": 1,
        },
        "last_analytics_result_count": 1,
    }

    new_state = normalize_node(state)

    assert new_state.get("analytics_context_mode") == "previous_result"
    assert new_state.get("analytics_scope_candidate") is None


def test_normalizer_applies_pending_analytics_scope_choice():
    state = {
        "question_raw": "1",
        "messages": [],
        "pending_analytics_scope": {
            "question_raw": "which are hot?",
            "normalized_question": "which are hot?",
        },
    }

    new_state = normalize_node(state)

    assert new_state.get("question_raw") == "which are hot?"
    assert new_state.get("normalized_question") == "which are hot?"
    assert new_state.get("analytics_context_mode") == "previous_result"
    assert new_state.get("pending_analytics_scope") is None


def test_intent_forces_analytics_for_association_queries():
    state = {
        "question_raw": "analyse to show container associated with ABC123",
        "normalized_question": "analyse to show container associated with abc123",
        "extracted_ids": {
            "container_number": [],
            "po_numbers": ["ABC123"],
            "booking_numbers": [],
            "obl_nos": [],
        },
    }

    new_state = intent_node(state)

    assert new_state["intent"] == "analytics"
    assert "association_lookup" in (new_state.get("sub_intents") or [])


def test_intent_test_mode_does_not_treat_shipments_as_greeting():
    state = {
        "question_raw": "show me a breakdown of shipments by discharge port",
        "normalized_question": "show me a breakdown of shipments by discharge port",
    }

    new_state = intent_node(state)

    assert new_state["intent"] == "analytics"


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
