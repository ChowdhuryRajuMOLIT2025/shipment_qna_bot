from requests.exceptions import SSLError

from shipment_qna_bot.tools.weather_tool import WeatherTool


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.headers = {}

    def mount(self, *_args, **_kwargs):
        return None

    def get(self, url, params=None, timeout=None, verify=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "timeout": timeout,
                "verify": verify,
            }
        )
        next_response = self._responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response


def test_weather_tool_retries_with_insecure_fallback_after_ssl_failure():
    session = _FakeSession(
        [
            SSLError("certificate verify failed"),
            _FakeResponse(
                {
                    "results": [
                        {
                            "name": "Shanghai",
                            "country": "China",
                            "country_code": "CN",
                            "feature_code": "PRT",
                            "latitude": 31.23,
                            "longitude": 121.47,
                        }
                    ]
                }
            ),
        ]
    )
    tool = WeatherTool(
        session=session,
        verify="C:/certs/custom.pem",
        allow_insecure_fallback=True,
    )

    coords = tool.get_coordinates("Shanghai (CNSHA)")

    assert coords is not None
    assert coords["country_code"] == "CN"
    assert session.calls[0]["verify"] == "C:/certs/custom.pem"
    assert session.calls[1]["verify"] is False
    notices = tool.consume_transport_notices()
    assert any("insecure fallback" in notice.lower() for notice in notices)


def test_weather_tool_scores_operational_risk_from_forecast_payload():
    tool = WeatherTool(session=_FakeSession([]), allow_insecure_fallback=False)
    impact = tool._build_impact_snapshot(
        {
            "latitude": 31.23,
            "longitude": 121.47,
            "name": "Shanghai",
            "country": "China",
            "country_code": "CN",
            "resolution_confidence": "high",
            "resolution_note": "Port geocode resolved directly.",
        },
        {
            "current": {
                "temperature_2m": 22.0,
                "precipitation": 1.2,
                "weather_code": 95,
                "wind_speed_10m": 41.0,
                "wind_gusts_10m": 68.0,
                "is_day": 1,
            },
            "hourly": {
                "weather_code": [95, 82, 3],
                "precipitation": [12.5, 4.0, 0.0],
                "wind_speed_10m": [42.0, 38.0, 15.0],
                "wind_gusts_10m": [68.0, 54.0, 20.0],
            },
            "daily": {
                "weather_code": [95, 63, 3],
                "temperature_2m_max": [24.0, 23.0, 21.0],
                "temperature_2m_min": [19.0, 18.0, 17.0],
                "precipitation_sum": [24.0, 10.0, 2.0],
                "wind_speed_10m_max": [45.0, 34.0, 18.0],
                "wind_gusts_10m_max": [70.0, 55.0, 25.0],
            },
        },
        forecast_days=3,
    )

    assert impact["impact_level"] == "high"
    assert impact["impact_score"] == 3
    assert impact["peak_gust_kph"] == 70.0
    assert impact["precipitation_total_mm"] == 36.0
    assert any("thunderstorm" in reason for reason in impact["impact_reasons"])
