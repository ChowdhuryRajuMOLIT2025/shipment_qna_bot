# tests/test_static_overview.py

from shipment_qna_bot.graph.builder import run_graph
from shipment_qna_bot.graph.nodes.static_greet_info_handler import \
    build_static_overview_answer


def _write_overview(tmp_path):
    content = """**Keywords:** MCS, MOL

**Company Overview**
MCS is a logistics provider.
StarLink provides visibility and tracking.

**History**
- 2003 Founded

**Vision Statement**
Deliver value.

**CEO Message**
**MCS America**
Cary Lin
CEO of MCS America

**MCS Hong Kong**
Yumi Fukunaga
Chief Executive Officer

**Office Directory list**
## **India Subcontinent**
- **Bangladesh, Chattogram**
  MOL Consolidation Service Ltd / INTASL Logistic Ltd.
  Tel: +88-09-6061 15115
- **Bangladesh, Dhaka**
  MOL Consolidation Service Ltd / INTASL Logistic Ltd.
  Tel: +88-09-6061 15115, Ext 501

**Services Details:**
- Ocean Consolidation

**MOL Official Website**
web address https://example.com

**MOL Official Social Media**
YouTube https://example.com/y
LinkedIn https://example.com/in
"""
    path = tmp_path / "overview_info.md"
    path.write_text(content, encoding="utf-8")
    return path


def test_static_overview_flow(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    result = run_graph(
        {
            "conversation_id": "test-overview",
            "question_raw": "What is MCS?",
            "consignee_codes": ["TEST"],
        }
    )

    assert result["intent"] == "company_overview"
    assert "Company Overview" in (result.get("answer_text") or "")
    assert result.get("is_satisfied") is True


def test_static_overview_section_selection(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    answer = build_static_overview_answer(
        "How many offices are in Bangladesh?", ["Bangladesh"]
    )
    assert "2 offices" in answer
    assert "Dhaka" in answer


def test_static_overview_ceo_selection(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    answer = build_static_overview_answer("Who is the CEO in America?")
    assert "MCS America" in answer
    assert "MCS Hong Kong" not in answer


def test_static_overview_office_summary(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    answer = build_static_overview_answer("Show office summary")
    assert "Office summary by region" in answer
    assert "India Subcontinent" in answer


def test_static_overview_starlink_snippet(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    answer = build_static_overview_answer("What is StarLink?")
    assert "StarLink" in answer


def test_static_overview_social_channel(tmp_path, monkeypatch):
    path = _write_overview(tmp_path)
    monkeypatch.setenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH", str(path))

    answer = build_static_overview_answer("Share the LinkedIn details")
    assert "LinkedIn" in answer
    assert "YouTube" not in answer
