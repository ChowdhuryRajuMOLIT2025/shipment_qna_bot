from typing import Any, Dict, List, Optional

import requests

from shipment_qna_bot.logging.logger import logger


class WeatherTool:
    """
    Tool to fetch geocoding and weather forecast data using Open-Meteo API.
    No API key required.
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
        95: "Thunderstorm: Slight or moderate",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail",
    }

    def __init__(self):
        self._geo_cache: Dict[str, Dict[str, float]] = {}

    def get_coordinates(self, location_name: str) -> Optional[Dict[str, float]]:
        """
        Translates a location name (e.g. 'Shanghai') to latitude and longitude.
        """
        if not location_name:
            return None

        # Clean location name: Remove (LOCODE) and take first part before comma
        import re

        clean_name = re.sub(r"\(.*?\)", "", location_name).split(",")[0].strip()

        name_key = clean_name.upper()
        if name_key in self._geo_cache:
            return self._geo_cache[name_key]

        try:
            params = {
                "name": clean_name,
                "count": 1,
                "language": "en",
                "format": "json",
            }
            response = requests.get(self.GEO_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            results = data.get("results")
            if not results:
                logger.warning(
                    f"No geocoding results for: {clean_name} (original: {location_name})"
                )
                return None

            top_result = results[0]
            coords = {
                "latitude": top_result["latitude"],
                "longitude": top_result["longitude"],
                "name": top_result.get("name", clean_name),
                "country": top_result.get("country", ""),
            }
            self._geo_cache[name_key] = coords
            return coords
        except Exception as e:
            logger.error(f"Geocoding failed for {location_name}: {e}")
            return None

    def get_weather(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Fetches current weather for given coordinates.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "current_weather": "true",
                "timezone": "auto",
            }
            response = requests.get(self.FORECAST_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            current = data.get("current_weather")
            if not current:
                return None

            code = current.get("weathercode", 0)
            return {
                "temp": current.get("temperature"),
                "windspeed": current.get("windspeed"),
                "condition": self.WMO_CODES.get(code, "Unknown"),
                "is_day": current.get("is_day"),
            }
        except Exception as e:
            logger.error(f"Weather fetch failed for ({lat}, {lon}): {e}")
            return None

    def get_weather_for_location(self, location_name: str) -> Optional[Dict[str, Any]]:
        """
        High-level method to get weather string for a port or city.
        """
        coords = self.get_coordinates(location_name)
        if not coords:
            return None

        weather = self.get_weather(coords["latitude"], coords["longitude"])
        if not weather:
            return None

        weather["location"] = coords["name"]
        weather["country"] = coords["country"]
        return weather
