import os


def is_feature_enabled(feature_name: str, default: bool = True) -> bool:
    """
    Checks if a feature is enabled via environment variables.
    Expected format: 1, true, yes, on
    """
    env_key = f"IS_{feature_name.upper()}_ENABLED"
    val = os.getenv(env_key)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def is_chart_enabled() -> bool:
    return is_feature_enabled("CHART", default=True)


def is_weather_enabled() -> bool:
    return is_feature_enabled("WEATHER", default=True)


def is_news_enabled() -> bool:
    return is_feature_enabled("NEWS", default=True)
