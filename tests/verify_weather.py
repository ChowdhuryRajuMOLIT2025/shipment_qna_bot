import os
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import re

from shipment_qna_bot.tools.weather_tool import WeatherTool


def clean_location(loc: str) -> str:
    # Remove UN/LOCODEs like (USLAX) or (ATVDD)
    return re.sub(r"\(.*?\)", "", loc).strip()


def test_weather():
    tool = WeatherTool()
    test_ports = [
        "LOS ANGELES, CA(USLAX)",
        "CHARLESTON, SC(USCHS)",
        "LUXEMBOURG(LULUX)",
        "FREDERICIA(DKFRC)",
        "VIENNA DANUBEPIER HOV(ATVDD)",
    ]

    print(f"{'Original Port':<30} | {'Cleaned':<20} | {'Lat/Lon':<20} | {'Weather'}")
    print("-" * 100)

    for port in test_ports:
        coords = tool.get_coordinates(port)
        if coords:
            w = tool.get_weather(coords["latitude"], coords["longitude"])
            weather_str = f"{w['condition']} ({w['temp']}°C)" if w else "N/A"
            loc_str = f"{coords['latitude']:.2f}, {coords['longitude']:.2f}"
            cleaned = coords["name"]
        else:
            loc_str = "N/A"
            weather_str = "N/A"
            cleaned = "N/A"

        print(f"{port:<30} | {cleaned:<20} | {loc_str:<20} | {weather_str}")


if __name__ == "__main__":
    test_weather()
