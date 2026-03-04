import os

import duckdb
import pandas as pd
import pytest

from shipment_qna_bot.tools.duckdb_engine import DuckDBAnalyticsEngine


@pytest.fixture
def sample_parquet(tmp_path):
    path = tmp_path / "test.parquet"
    df = pd.DataFrame(
        {
            "container_number": ["CONT1", "CONT2", "CONT3"],
            "consignee_codes": [["A", "B"], ["B"], ["C"]],
            "cargo_weight_kg": [100, 200, 300],
            "shipment_status": ["DELIVERED", "IN_OCEAN", "DELIVERED"],
        }
    )
    df.to_parquet(path)
    return str(path)


def test_execute_query_basic(sample_parquet):
    engine = DuckDBAnalyticsEngine()
    consignee_codes = ["A"]
    sql = "SELECT count(*) as total FROM df"
    result = engine.execute_query(sample_parquet, sql, consignee_codes)

    assert result["success"] is True
    assert result["filtered_rows"] == 1
    assert result["result_rows"][0]["total"] == 1


def test_execute_query_rls_multiple(sample_parquet):
    engine = DuckDBAnalyticsEngine()
    # Code 'B' matches CONT1 and CONT2
    consignee_codes = ["B"]
    sql = "SELECT count(*) as total FROM df"
    result = engine.execute_query(sample_parquet, sql, consignee_codes)

    assert result["success"] is True
    assert result["filtered_rows"] == 2
    assert result["result_rows"][0]["total"] == 2


def test_execute_query_no_match(sample_parquet):
    engine = DuckDBAnalyticsEngine()
    consignee_codes = ["D"]
    sql = "SELECT count(*) as total FROM df"
    result = engine.execute_query(sample_parquet, sql, consignee_codes)

    # DuckDB returns a row with 0 for count(*) even on empty data
    assert result["success"] is True
    assert result["filtered_rows"] == 0
    assert result["result_rows"][0]["total"] == 0
    assert "total" in result["result"]


def test_execute_query_sql_complex(sample_parquet):
    engine = DuckDBAnalyticsEngine()
    consignee_codes = ["A", "B", "C"]
    sql = "SELECT shipment_status, sum(cargo_weight_kg) as total_weight FROM df GROUP BY shipment_status ORDER BY total_weight DESC"
    result = engine.execute_query(sample_parquet, sql, consignee_codes)

    assert result["success"] is True
    assert len(result["result_rows"]) == 2
    assert result["result_rows"][0]["shipment_status"] == "DELIVERED"
    assert result["result_rows"][0]["total_weight"] == 400  # 100 + 300


def test_execute_query_previous_result_selector(sample_parquet):
    engine = DuckDBAnalyticsEngine()
    selector = {
        "kind": "id_sets",
        "ids": {"container_number": ["CONT2", "CONT3"]},
        "row_count": 2,
    }
    sql = "SELECT count(*) as total FROM df"
    result = engine.execute_query(
        sample_parquet, sql, ["A", "B", "C"], selector=selector
    )

    assert result["success"] is True
    assert result["filtered_rows"] == 2
    assert result["result_rows"][0]["total"] == 2
