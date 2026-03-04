import os
import re
from typing import Any, Dict, List, Optional

import certifi
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, SSLError
from urllib3.util.retry import Retry

from shipment_qna_bot.logging.logger import logger


class WeatherTool:
    """
    Fetches port-area weather using Open-Meteo geocoding + forecast data.
    """

    GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

    WMO_CODES = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        56: "Light freezing drizzle",
        57: "Dense freezing drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        66: "Light freezing rain",
        67: "Heavy freezing rain",
        71: "Slight snow fall",
        73: "Moderate snow fall",
        75: "Heavy snow fall",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm: slight or moderate",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail",
    }

    _COUNTRY_HINTS = {
        "AUSTRALIA": "AU",
        "BELGIUM": "BE",
        "BRAZIL": "BR",
        "CANADA": "CA",
        "CHINA": "CN",
        "FRANCE": "FR",
        "GERMANY": "DE",
        "HONG KONG": "HK",
        "INDIA": "IN",
        "INDONESIA": "ID",
        "ITALY": "IT",
        "JAPAN": "JP",
        "KOREA": "KR",
        "MALAYSIA": "MY",
        "MEXICO": "MX",
        "NETHERLANDS": "NL",
        "SAUDI ARABIA": "SA",
        "SINGAPORE": "SG",
        "SOUTH AFRICA": "ZA",
        "SPAIN": "ES",
        "THAILAND": "TH",
        "TURKEY": "TR",
        "UAE": "AE",
        "UK": "GB",
        "UNITED ARAB EMIRATES": "AE",
        "UNITED KINGDOM": "GB",
        "UNITED STATES": "US",
        "USA": "US",
        "VIETNAM": "VN",
    }

    _THUNDER_CODES = {95, 96, 99}
    _HEAVY_PRECIP_CODES = {65, 67, 75, 82, 86}

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        verify: Optional[str | bool] = None,
        allow_insecure_fallback: Optional[bool] = None,
    ):
        self._session = session or requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.headers.update({"User-Agent": "shipment-qna-bot/weather"})

        self._verify = verify if verify is not None else self._resolve_verify_path()
        self._allow_insecure_fallback = (
            allow_insecure_fallback
            if allow_insecure_fallback is not None
            else self._env_flag("WEATHER_SSL_ALLOW_INSECURE_FALLBACK", default=True)
        )
        self._geo_cache: Dict[str, Dict[str, Any]] = {}
        self._forecast_cache: Dict[str, Dict[str, Any]] = {}
        self._transport_notices: List[str] = []

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _resolve_verify_path() -> str | bool:
        for env_name in ("WEATHER_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
            path = os.getenv(env_name)
            if path:
                return path
        return certifi.where()

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (value or "").lower())

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _push_notice(self, message: str) -> None:
        if message and message not in self._transport_notices:
            self._transport_notices.append(message)

    def consume_transport_notices(self) -> List[str]:
        notices = list(self._transport_notices)
        self._transport_notices = []
        return notices

    def _request_json(
        self, url: str, params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        try:
            response = self._session.get(
                url,
                params=params,
                timeout=12,
                verify=self._verify,
            )
            response.raise_for_status()
            return response.json()
        except SSLError as exc:
            logger.warning("Weather HTTPS verification failed for %s: %s", url, exc)
            if self._allow_insecure_fallback:
                self._push_notice(
                    "Weather service TLS verification failed; used insecure fallback for the live lookup."
                )
                try:
                    response = self._session.get(
                        url,
                        params=params,
                        timeout=12,
                        verify=False,
                    )
                    response.raise_for_status()
                    return response.json()
                except RequestException as insecure_exc:
                    logger.error(
                        "Weather request failed even after insecure fallback for %s: %s",
                        url,
                        insecure_exc,
                    )
                    self._push_notice(
                        f"Live weather lookup still failed after TLS fallback ({type(insecure_exc).__name__})."
                    )
                    return None
            self._push_notice(
                "Live weather lookup failed because TLS certificate validation did not succeed."
            )
            return None
        except RequestException as exc:
            logger.error("Weather request failed for %s: %s", url, exc)
            self._push_notice(f"Live weather lookup failed ({type(exc).__name__}).")
            return None
        except Exception as exc:
            logger.error("Weather response parsing failed for %s: %s", url, exc)
            self._push_notice(
                f"Live weather lookup failed while reading the response ({type(exc).__name__})."
            )
            return None

    def _parse_location_query(self, location_name: str) -> Dict[str, Any]:
        locode_match = re.search(r"\(([A-Z]{5})\)", location_name.upper())
        locode = locode_match.group(1) if locode_match else ""

        cleaned = re.sub(r"\(.*?\)", "", location_name).strip()
        parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        primary = parts[0] if parts else cleaned
        primary = re.sub(
            r"^(PORT OF|PORT|HARBOUR|HARBOR|TERMINAL)\s+",
            "",
            primary,
            flags=re.IGNORECASE,
        ).strip()

        country_hint = locode[:2] if locode else ""
        if not country_hint and len(parts) > 1:
            last_part = parts[-1].upper()
            if len(last_part) == 2 and last_part.isalpha():
                country_hint = last_part
            else:
                country_hint = self._COUNTRY_HINTS.get(last_part, "")

        return {
            "query_name": primary or cleaned,
            "country_hint": country_hint,
            "locode": locode,
            "raw_cleaned": cleaned,
        }

    def _score_candidate(
        self,
        candidate: Dict[str, Any],
        query_name: str,
        country_hint: str,
        raw_cleaned: str,
    ) -> int:
        score = 0
        query_norm = self._normalize_text(query_name)
        raw_norm = self._normalize_text(raw_cleaned)
        name = str(candidate.get("name") or "")
        name_norm = self._normalize_text(name)
        feature_code = str(candidate.get("feature_code") or "").upper()
        country_code = str(candidate.get("country_code") or "").upper()

        if name_norm == query_norm:
            score += 180
        elif query_norm and query_norm in name_norm:
            score += 120
        elif name_norm and name_norm in query_norm:
            score += 90

        if raw_norm and raw_norm == name_norm:
            score += 40

        if country_hint and country_code == country_hint:
            score += 90

        if feature_code in {"PRT", "HBR", "SEA"}:
            score += 70
        elif feature_code.startswith("PPL"):
            score += 30

        for key in ("admin1", "admin2", "country", "timezone"):
            value = str(candidate.get(key) or "")
            value_norm = self._normalize_text(value)
            if value_norm and value_norm in raw_norm:
                score += 15

        return score

    def get_coordinates(self, location_name: str) -> Optional[Dict[str, Any]]:
        """
        Translate a port/city label into coordinates, preferring exact and port-like matches.
        """
        if not location_name:
            return None

        cache_key = location_name.strip().upper()
        if cache_key in self._geo_cache:
            return dict(self._geo_cache[cache_key])

        query = self._parse_location_query(location_name)
        if not query["query_name"]:
            return None

        params = {
            "name": query["query_name"],
            "count": 10,
            "language": "en",
            "format": "json",
        }
        data = self._request_json(self.GEO_URL, params=params)
        if not data:
            return None

        results = data.get("results") or []
        if not results:
            logger.warning("No geocoding results for %s", location_name)
            self._push_notice(
                f"No weather geocode match was found for '{location_name}'."
            )
            return None

        ranked = sorted(
            results,
            key=lambda item: self._score_candidate(
                item,
                query["query_name"],
                query["country_hint"],
                query["raw_cleaned"],
            ),
            reverse=True,
        )
        best = ranked[0]
        best_score = self._score_candidate(
            best,
            query["query_name"],
            query["country_hint"],
            query["raw_cleaned"],
        )
        feature_code = str(best.get("feature_code") or "").upper()
        resolution_confidence = "high"
        resolution_note = "Port geocode resolved directly."
        if best_score < 180:
            resolution_confidence = "low"
            resolution_note = "Using a nearby city or port-area weather proxy."
        elif feature_code not in {"PRT", "HBR", "SEA"}:
            resolution_confidence = "medium"
            resolution_note = "Using a city-level weather proxy for the port area."

        coords = {
            "latitude": best["latitude"],
            "longitude": best["longitude"],
            "name": best.get("name", query["query_name"]),
            "country": best.get("country", ""),
            "country_code": best.get("country_code", ""),
            "feature_code": feature_code,
            "resolution_confidence": resolution_confidence,
            "resolution_note": resolution_note,
            "locode": query["locode"],
        }
        self._geo_cache[cache_key] = coords
        return dict(coords)

    def _fetch_forecast_payload(
        self, lat: float, lon: float, forecast_days: int = 3
    ) -> Optional[Dict[str, Any]]:
        cache_key = f"{lat:.4f}:{lon:.4f}:{forecast_days}"
        if cache_key in self._forecast_cache:
            return dict(self._forecast_cache[cache_key])

        params = {
            "latitude": lat,
            "longitude": lon,
            "current": ",".join(
                [
                    "temperature_2m",
                    "precipitation",
                    "weather_code",
                    "wind_speed_10m",
                    "wind_gusts_10m",
                    "is_day",
                ]
            ),
            "hourly": ",".join(
                [
                    "weather_code",
                    "precipitation",
                    "wind_speed_10m",
                    "wind_gusts_10m",
                ]
            ),
            "daily": ",".join(
                [
                    "weather_code",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "wind_speed_10m_max",
                    "wind_gusts_10m_max",
                ]
            ),
            "forecast_days": forecast_days,
            "timezone": "auto",
        }
        data = self._request_json(self.FORECAST_URL, params=params)
        if data is None:
            return None
        self._forecast_cache[cache_key] = data
        return dict(data)

    def get_weather(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Fetch the current weather snapshot for given coordinates.
        """
        payload = self._fetch_forecast_payload(lat, lon, forecast_days=3)
        if not payload:
            return None

        current = payload.get("current") or payload.get("current_weather") or {}
        code = current.get("weather_code")
        if code is None:
            code = current.get("weathercode", 0)
        windspeed = current.get("wind_speed_10m")
        if windspeed is None:
            windspeed = current.get("windspeed")
        return {
            "temp": current.get("temperature_2m", current.get("temperature")),
            "windspeed": windspeed,
            "wind_gust": current.get("wind_gusts_10m"),
            "condition": self.WMO_CODES.get(int(code or 0), "Unknown"),
            "is_day": current.get("is_day"),
            "precipitation": current.get("precipitation"),
        }

    def _build_impact_snapshot(
        self,
        coords: Dict[str, Any],
        payload: Dict[str, Any],
        forecast_days: int,
    ) -> Dict[str, Any]:
        current = payload.get("current") or payload.get("current_weather") or {}
        hourly = payload.get("hourly") or {}
        daily = payload.get("daily") or {}

        hourly_codes = [
            int(c) for c in (hourly.get("weather_code") or []) if c is not None
        ]
        hourly_precip = [
            self._to_float(v) or 0.0 for v in (hourly.get("precipitation") or [])
        ]
        hourly_wind = [
            self._to_float(v) or 0.0 for v in (hourly.get("wind_speed_10m") or [])
        ]
        hourly_gust = [
            self._to_float(v) or 0.0 for v in (hourly.get("wind_gusts_10m") or [])
        ]
        daily_precip = [
            self._to_float(v) or 0.0 for v in (daily.get("precipitation_sum") or [])
        ]
        daily_wind = [
            self._to_float(v) or 0.0 for v in (daily.get("wind_speed_10m_max") or [])
        ]
        daily_gust = [
            self._to_float(v) or 0.0 for v in (daily.get("wind_gusts_10m_max") or [])
        ]
        daily_tmax = [
            self._to_float(v) for v in (daily.get("temperature_2m_max") or [])
        ]
        daily_tmin = [
            self._to_float(v) for v in (daily.get("temperature_2m_min") or [])
        ]

        peak_hourly_precip = max(hourly_precip) if hourly_precip else 0.0
        precip_total = sum(daily_precip) if daily_precip else 0.0
        peak_wind = (
            max(hourly_wind + daily_wind) if (hourly_wind or daily_wind) else 0.0
        )
        peak_gust = (
            max(hourly_gust + daily_gust) if (hourly_gust or daily_gust) else 0.0
        )
        temp_min = min((v for v in daily_tmin if v is not None), default=None)
        temp_max = max((v for v in daily_tmax if v is not None), default=None)

        impact_score = 0
        impact_reasons: List[str] = []

        def _raise(level: int, reason: str) -> None:
            nonlocal impact_score
            impact_score = max(impact_score, level)
            if reason not in impact_reasons:
                impact_reasons.append(reason)

        if any(code in self._THUNDER_CODES for code in hourly_codes):
            _raise(3, "thunderstorm risk")
        elif any(code in self._HEAVY_PRECIP_CODES for code in hourly_codes):
            _raise(2, "heavy precipitation signal")

        if peak_gust >= 70 or peak_wind >= 55:
            _raise(3, f"gusts up to {peak_gust:.0f} km/h")
        elif peak_gust >= 55 or peak_wind >= 40:
            _raise(2, f"winds up to {peak_wind:.0f} km/h")
        elif peak_gust >= 40 or peak_wind >= 28:
            _raise(1, f"breezy conditions up to {peak_wind:.0f} km/h")

        if peak_hourly_precip >= 10 or precip_total >= 30:
            _raise(3, f"rainfall around {precip_total:.1f} mm")
        elif peak_hourly_precip >= 4 or precip_total >= 10:
            _raise(2, f"rainfall around {precip_total:.1f} mm")
        elif precip_total >= 2:
            _raise(1, f"light rainfall around {precip_total:.1f} mm")

        current_code = current.get("weather_code")
        if current_code is None:
            current_code = current.get("weathercode", 0)
        current_condition = self.WMO_CODES.get(int(current_code or 0), "Unknown")
        current_wind = current.get("wind_speed_10m")
        if current_wind is None:
            current_wind = current.get("windspeed")
        if impact_score == 0 and int(current_code or 0) in {45, 48, 51, 53, 55, 61, 63}:
            _raise(1, current_condition.lower())

        impact_level = {0: "none", 1: "low", 2: "medium", 3: "high"}[impact_score]
        if impact_reasons:
            summary = (
                f"{impact_level.capitalize()} operational weather risk in the next "
                f"{forecast_days * 24} hours due to "
                f"{', '.join(impact_reasons[:3])}."
            )
        else:
            summary = f"No material weather disruption signals in the next {forecast_days * 24} hours."

        weather = {
            "temp": current.get("temperature_2m", current.get("temperature")),
            "windspeed": current_wind,
            "wind_gust": current.get("wind_gusts_10m"),
            "condition": current_condition,
            "is_day": current.get("is_day"),
            "precipitation": current.get("precipitation"),
        }
        weather.update(
            {
                "location": coords["name"],
                "country": coords.get("country", ""),
                "country_code": coords.get("country_code", ""),
                "latitude": coords["latitude"],
                "longitude": coords["longitude"],
                "resolution_confidence": coords.get("resolution_confidence", "medium"),
                "resolution_note": coords.get("resolution_note", ""),
                "forecast_days": forecast_days,
                "forecast_window_hours": forecast_days * 24,
                "impact_level": impact_level,
                "impact_score": impact_score,
                "impact_reasons": impact_reasons[:3],
                "summary": summary,
                "precipitation_total_mm": round(precip_total, 1),
                "peak_hourly_precip_mm": round(peak_hourly_precip, 1),
                "peak_wind_kph": round(peak_wind, 1),
                "peak_gust_kph": round(peak_gust, 1),
                "temp_min_c": temp_min,
                "temp_max_c": temp_max,
                "source": "open-meteo",
            }
        )
        return weather

    def get_impact_for_location(
        self, location_name: str, forecast_days: int = 3
    ) -> Optional[Dict[str, Any]]:
        coords = self.get_coordinates(location_name)
        if not coords:
            return None

        payload = self._fetch_forecast_payload(
            coords["latitude"], coords["longitude"], forecast_days=forecast_days
        )
        if not payload:
            return None
        return self._build_impact_snapshot(coords, payload, forecast_days)

    def get_weather_for_location(self, location_name: str) -> Optional[Dict[str, Any]]:
        """
        Backward-compatible wrapper that now includes forecast impact fields.
        """
        return self.get_impact_for_location(location_name, forecast_days=3)
