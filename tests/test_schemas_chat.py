# tests/test_schemas_chat.py

from shipment_qna_bot.models.schemas import (ChartSpec, ChatAnswer,
                                             ChatRequest, EvidenceItem,
                                             TableSpec)


def test_chat_request_normalizes_question_and_consignee_codes():
    payload = {
        "question": "  Show me status for my containers  ",
        "consignee_codes": ["0000866, 234567", "0000866"],  # dup + comma-packed
        "conversation_id": None,
    }

    req = ChatRequest(**payload)

    assert req.question == "Show me status for my containers"
    # Order preserved, duplicates removed
    assert req.consignee_codes == ["0000866", "234567"]


def test_chat_request_rejects_empty_question():
    try:
        ChatRequest(question="   ", consignee_codes=["0000866"])
    except ValueError as e:
        assert "question must not be empty" in str(e)
    else:
        raise AssertionError("Empty question should raise ValueError")


def test_chat_request_rejects_empty_consignee_codes():
    try:
        ChatRequest(question="ok", consignee_codes=[])
    except ValueError as e:
        assert "consignee_codes" in str(e).lower()
    else:
        raise AssertionError("Empty consignee_codes should raise ValueError")


def test_chat_answer_with_evidence_and_chart_and_table():
    evidence = [
        EvidenceItem(
            doc_id="320001078211",
            container_number="OOCU8898279",
            field_used=["eta_dp_date", "delivery_to_consignee_date"],
        )
    ]

    chart = ChartSpec(
        kind="bar",
        title="Delayed vs On-time containers",
        data=[
            {"status": "DELAYED", "count": 5},
            {"status": "ON_TIME", "count": 12},
        ],
        encodings={"x": "status", "y": "count"},
    )

    table = TableSpec(
        columns=["container_number", "is_delayed_fd", "delayed_fd"],
        rows=[
            {
                "container_number": "OOCU8898279",
                "is_delayed_fd": "EARLY",
                "delayed_fd": -2,
            },
            {
                "container_number": "TCLU2937251",
                "is_delayed_fd": "DELAYED",
                "delayed_fd": 4,
            },
        ],
    )

    answer = ChatAnswer(
        conversation_id="test-conv",
        intent="analytics",
        answer="Here is the breakdown of delayed vs on-time shipments.",
        notices=["Using test data only."],
        evidence=evidence,
        chart=chart,
        table=table,
    )

    assert answer.conversation_id == "test-conv"
    assert answer.intent == "analytics"
    assert len(answer.evidence or []) == 1
    assert answer.chart is not None
    assert answer.chart.kind == "bar"
    assert answer.table is not None
    assert len(answer.table.rows) == 2
