import json

import pytest

from shipment_qna_bot.security import scope as scope_module
from shipment_qna_bot.security.rls import build_search_filter
from shipment_qna_bot.security.scope import resolve_allowed_scope


@pytest.fixture(autouse=True)
def _mock_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "CONSIGNEE_SCOPE_REGISTRY_JSON",
        json.dumps({"user1": ["A", "B", "C"]}),
    )
    scope_module._REGISTRY_CACHE = None


def test_resolve_allowed_scope_empty():
    assert resolve_allowed_scope("user1", None) == []
    assert resolve_allowed_scope("user1", "") == []
    assert resolve_allowed_scope("user1", []) == []


def test_resolve_allowed_scope_string():
    assert resolve_allowed_scope("user1", "A,B, C") == ["A", "B", "C"]


def test_resolve_allowed_scope_list():
    assert resolve_allowed_scope("user1", ["A", "B"]) == ["A", "B"]


def test_resolve_allowed_scope_missing_identity_denies_payload():
    """Missing identity must fail closed (Deny-by-Default)."""
    assert resolve_allowed_scope(None, ["A", "B"]) == []


def test_build_search_filter_empty():
    assert build_search_filter([]) == "false"


def test_build_search_filter_single():
    # Expected: consignee_code_ids/any(t: search.in(t, 'A', ','))
    f = build_search_filter(["A"])
    assert "search.in(t, 'A', ',')" in f


def test_build_search_filter_multiple():
    # Expected: consignee_code_ids/any(t: search.in(t, 'A,B', ','))
    f = build_search_filter(["A", "B"])
    assert "search.in(t, 'A,B', ',')" in f
