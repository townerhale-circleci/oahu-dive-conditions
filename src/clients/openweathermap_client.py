"""OpenWeatherMap API client for per-site wind forecasts.

Provides location-specific wind speed and direction forecasts.
Free tier: 1000 calls/day, 5-day forecast with 3-hour intervals.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo
import requests

HST = ZoneInfo("Pacific/Honolulu")

logger = logging.getLogger(__name__)


def _load_api_key() -> str:
    """Load API key from environment or .env file."""
    # First check environment variable
    key = os.environ.get("OPENWEATHERMAP_API_KEY", "")
    if key:
        return key

    # Try to load from .env file in project root
    env_paths = [
        Path(__file__).parent.parent.parent / ".env",
        Path.cwd() / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            try:
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("OPENWEATHERMAP_API_KEY="):
                            return line.split("=", 1)[1].strip()
            except Exception:
                pass
    return ""


API_KEY = _load_api_key()
BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"


class OpenWeatherMapClient:
    """Client for fetching wind forecasts from OpenWeatherMap."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the client.

        Args:
            api_key: OpenWeatherMap API key. Defaults to OPENWEATHERMAP_API_KEY env var.
        """
        self.api_key = api_key or API_KEY
        self.session = requests.Session()
        self._cache = {}  # Simple in-memory cache
        self._cache_ttl = 1800  # 30 minutes

    def get_wind_forecast(
        self,
        lat: float,
        lon: float,
        target_date: Optional[datetime] = None,
    ) -> dict:
        """Get wind forecast for a specific location and date.

        Args:
            lat: Latitude
            lon: Longitude
            target_date: Date to get forecast for. Defaults to today.

        Returns:
            Dict with keys: wind_speed_mph, wind_direction_deg, wind_gust_mph,
                           best_hour, conditions
        """
        if not self.api_key:
            logger.debug("No OpenWeatherMap API key configured")
            return {}

        # Round coordinates for cache key
        cache_key = f"{round(lat, 2)}:{round(lon, 2)}"

        # Check cache
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if datetime.now() - cached_time < timedelta(seconds=self._cache_ttl):
                return self._extract_for_date(cached_data, target_date)

        try:
            response = self.session.get(
                BASE_URL,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": self.api_key,
                    "units": "imperial",  # Get mph directly
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            # Cache the full response
            self._cache[cache_key] = (datetime.now(), data)

            return self._extract_for_date(data, target_date)

        except Exception as e:
            logger.debug(f"OpenWeatherMap API error: {e}")
            return {}

    def _extract_for_date(self, data: dict, target_date: Optional[datetime]) -> dict:
        """Extract wind data for a specific date from the forecast response."""
        if not data.get("list"):
            return {}

        target = target_date.date() if target_date else datetime.now().date()

        # Find forecast entries for the target date
        # OWM returns UTC timestamps — convert to Hawaii time for correct date/hour
        day_forecasts = []
        for entry in data["list"]:
            entry_time = datetime.fromtimestamp(entry["dt"], tz=timezone.utc).astimezone(HST)
            if entry_time.date() == target:
                day_forecasts.append({
                    "hour": entry_time.hour,
                    "time": entry_time,
                    "wind_speed_mph": entry.get("wind", {}).get("speed", 0),
                    "wind_direction_deg": entry.get("wind", {}).get("deg", 0),
                    "wind_gust_mph": entry.get("wind", {}).get("gust", 0),
                    "conditions": entry.get("weather", [{}])[0].get("main", ""),
                })

        if not day_forecasts:
            # If no data for target date, return first available
            if data["list"]:
                entry = data["list"][0]
                return {
                    "wind_speed_mph": entry.get("wind", {}).get("speed", 0),
                    "wind_direction_deg": entry.get("wind", {}).get("deg", 0),
                    "wind_gust_mph": entry.get("wind", {}).get("gust", 0),
                    "best_hour": None,
                    "best_time_range": None,
                    "conditions": entry.get("weather", [{}])[0].get("main", ""),
                }
            return {}

        # Filter to daylight hours only (5 AM - 6 PM) for dive recommendations
        daylight_forecasts = [f for f in day_forecasts if 5 <= f["hour"] <= 18]
        if not daylight_forecasts:
            daylight_forecasts = day_forecasts  # Fall back to all hours if none in daylight

        # Find the best time (lowest wind speed, preferring morning 5-9 AM)
        morning_forecasts = [f for f in daylight_forecasts if 5 <= f["hour"] <= 9]

        if morning_forecasts:
            best = min(morning_forecasts, key=lambda x: x["wind_speed_mph"])
        else:
            best = min(daylight_forecasts, key=lambda x: x["wind_speed_mph"])

        # Calculate average for the day
        avg_wind = sum(f["wind_speed_mph"] for f in day_forecasts) / len(day_forecasts)

        # Find best time range (consecutive hours with low wind)
        best_range = self._find_best_time_range(day_forecasts)

        return {
            "wind_speed_mph": best["wind_speed_mph"],
            "wind_direction_deg": best["wind_direction_deg"],
            "wind_gust_mph": best.get("wind_gust_mph", 0),
            "avg_wind_mph": avg_wind,
            "best_hour": best["hour"],
            "best_time_range": best_range,
            "conditions": best["conditions"],
            "hourly_data": day_forecasts,
        }

    def _find_best_time_range(self, forecasts: list) -> str:
        """Find the best time range for diving (lowest wind period, daylight only)."""
        if not forecasts:
            return "05:00-09:00"

        # Filter to daylight hours (5 AM - 6 PM)
        daylight = [f for f in forecasts if 5 <= f["hour"] <= 18]
        if not daylight:
            return "05:00-09:00"

        # Sort by hour
        sorted_forecasts = sorted(daylight, key=lambda x: x["hour"])

        # Find periods with wind < 12 mph
        calm_periods = []
        current_start = None

        for f in sorted_forecasts:
            if f["wind_speed_mph"] < 12:
                if current_start is None:
                    current_start = f["hour"]
            else:
                if current_start is not None:
                    calm_periods.append((current_start, f["hour"]))
                    current_start = None

        if current_start is not None:
            calm_periods.append((current_start, min(sorted_forecasts[-1]["hour"] + 3, 18)))

        if calm_periods:
            # Prefer morning periods
            morning_periods = [(s, e) for s, e in calm_periods if s < 12]
            if morning_periods:
                start, end = morning_periods[0]
            else:
                start, end = calm_periods[0]

            return f"{start:02d}:00-{min(end, 18):02d}:00"

        # Default to early morning
        return "05:00-09:00"

    def get_wind_direction_name(self, degrees: float) -> str:
        """Convert wind direction degrees to compass name."""
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                      "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        index = round(degrees / 22.5) % 16
        return directions[index]
