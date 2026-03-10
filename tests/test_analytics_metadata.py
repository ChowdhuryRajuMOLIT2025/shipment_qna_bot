from shipment_qna_bot.tools.analytics_metadata import (
    ANALYTICS_METADATA, format_analytics_column_reference)


def test_analytics_metadata_has_synonyms_key_for_all_columns():
    for col, meta in ANALYTICS_METADATA.items():
        assert "desc" in meta, f"{col} missing desc"
        assert "type" in meta, f"{col} missing type"
        assert "synonyms" in meta, f"{col} missing synonyms"
        assert isinstance(meta["synonyms"], list), f"{col} synonyms must be list"


def test_format_column_reference_includes_synonyms_when_available():
    rendered = format_analytics_column_reference(
        ["container_number", "shipment_status"]
    )
    assert "`container_number`" in rendered
    assert "Synonyms:" in rendered
    assert "`container`" in rendered
