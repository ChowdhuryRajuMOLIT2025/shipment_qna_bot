from shipment_qna_bot.tools import ready_ref


def test_load_ready_ref_reads_existing_file(monkeypatch, tmp_path):
    ref = tmp_path / "ready_ref.md"
    ref.write_text("hello ref", encoding="utf-8")

    monkeypatch.setattr(ready_ref, "_resolve_ready_ref_path", lambda: str(ref))
    ready_ref.load_ready_ref.cache_clear()

    assert ready_ref.load_ready_ref() == "hello ref"


def test_load_ready_ref_returns_empty_when_missing(monkeypatch, tmp_path):
    missing = tmp_path / "missing.md"

    monkeypatch.setattr(ready_ref, "_resolve_ready_ref_path", lambda: str(missing))
    ready_ref.load_ready_ref.cache_clear()

    assert ready_ref.load_ready_ref() == ""
