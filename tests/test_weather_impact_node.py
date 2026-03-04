import pandas as pd

from shipment_qna_bot.graph.nodes import weather_impact as weather_module


class _StubBlobManager:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def load_filtered_data(self, consignee_codes):
        assert consignee_codes == ["0000866"]
        return self._df.copy()


class _StubWeatherTool:
    def __init__(self):
        self._pending_notices = []

    def get_impact_for_location(self, location_name: str, forecast_days: int = 3):
        assert forecast_days == 3
        upper_name = location_name.upper()
        if "SHANGHAI" in upper_name:
            self._pending_notices = [
                "Weather service TLS verification failed; used insecure fallback for the live lookup."
            ]
            return {
                "location": "Shanghai",
                "country": "China",
                "condition": "Thunderstorm: slight or moderate",
                "impact_level": "high",
                "impact_score": 3,
                "impact_reasons": ["thunderstorm risk", "gusts up to 68 km/h"],
                "forecast_window_hours": 72,
                "peak_wind_kph": 45.0,
                "peak_gust_kph": 68.0,
                "precipitation_total_mm": 26.0,
                "resolution_confidence": "medium",
            }
        if "LOS ANGELES" in upper_name:
            self._pending_notices = []
            return {
                "location": "Los Angeles",
                "country": "United States",
                "condition": "Partly cloudy",
                "impact_level": "low",
                "impact_score": 1,
                "impact_reasons": ["breezy conditions up to 30 km/h"],
                "forecast_window_hours": 72,
                "peak_wind_kph": 30.0,
                "peak_gust_kph": 40.0,
                "precipitation_total_mm": 1.0,
                "resolution_confidence": "high",
            }
        self._pending_notices = []
        return None

    def consume_transport_notices(self):
        notices = list(self._pending_notices)
        self._pending_notices = []
        return notices


def test_weather_impact_node_returns_answer_and_structured_artifacts(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "document_id": "DOC-1",
                "container_number": "OOLU1234567",
                "po_numbers": ["PO-1"],
                "booking_numbers": ["BK-1"],
                "obl_nos": ["OBL-1"],
                "shipment_status": "IN_OCEAN",
                "discharge_port": "Shanghai (CNSHA)",
                "hot_container_flag": True,
                "best_eta_dp_date": "2026-03-06T00:00:00+00:00",
            },
            {
                "document_id": "DOC-2",
                "container_number": "OOLU7654321",
                "po_numbers": ["PO-2"],
                "booking_numbers": ["BK-2"],
                "obl_nos": ["OBL-2"],
                "shipment_status": "ARRIVING",
                "discharge_port": "Shanghai (CNSHA)",
                "hot_container_flag": False,
                "best_eta_dp_date": "2026-03-07T00:00:00+00:00",
            },
            {
                "document_id": "DOC-3",
                "container_number": "MSCU1111111",
                "po_numbers": ["PO-3"],
                "booking_numbers": ["BK-3"],
                "obl_nos": ["OBL-3"],
                "shipment_status": "IN_OCEAN",
                "discharge_port": "Los Angeles (USLAX)",
                "hot_container_flag": False,
                "best_eta_dp_date": "2026-03-08T00:00:00+00:00",
            },
            {
                "document_id": "DOC-4",
                "container_number": "MSCU2222222",
                "po_numbers": ["PO-4"],
                "booking_numbers": ["BK-4"],
                "obl_nos": ["OBL-4"],
                "shipment_status": "DELIVERED",
                "discharge_port": "Rotterdam (NLRTM)",
                "hot_container_flag": False,
                "best_eta_dp_date": "2026-02-15T00:00:00+00:00",
            },
        ]
    )

    monkeypatch.setattr(
        weather_module, "_get_blob_manager", lambda: _StubBlobManager(df)
    )
    monkeypatch.setattr(weather_module, "_get_weather_tool", lambda: _StubWeatherTool())

    state = {
        "question_raw": "Is weather affecting each discharge port?",
        "normalized_question": "is weather affecting each discharge port?",
        "conversation_id": "weather-impact-test",
        "consignee_codes": ["0000866"],
        "intent": "retrieval",
        "sub_intents": ["retrieval", "weather"],
        "now_utc": "2026-03-04T00:00:00+00:00",
        "messages": [],
        "errors": [],
        "notices": [],
        "usage_metadata": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "extracted_ids": {
            "container_number": [],
            "po_numbers": [],
            "booking_numbers": [],
            "obl_nos": [],
            "location": [],
        },
    }

    result = weather_module.weather_impact_node(state)

    assert result["is_satisfied"] is True
    assert "Shanghai" in result["answer_text"]
    assert result["table_spec"] is not None
    assert result["table_spec"]["title"] == "Weather Impact Outlook"
    assert len(result["table_spec"]["rows"]) == 2
    assert result["table_spec"]["rows"][0]["location"] == "Shanghai"
    assert result["table_spec"]["rows"][0]["impact_level"] == "high"
    assert result["chart_spec"] is not None
    assert result["chart_spec"]["encodings"]["y"] == "impact_score"
    assert result["citations"][0]["doc_id"] == "DOC-1"
    assert any("insecure fallback" in notice.lower() for notice in result["notices"])
    assert any(
        "city-level weather proxy" in notice.lower() for notice in result["notices"]
    )
