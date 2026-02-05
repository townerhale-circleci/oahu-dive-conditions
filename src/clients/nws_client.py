"""National Weather Service API client for gridpoint forecasts and alerts.

Provides hourly forecasts and active weather alerts for Hawaii.
No API key required.
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


NWS_BASE_URL = "https://api.weather.gov"
CACHE_TTL_SECONDS = 1800  # 30 minutes for weather data
ALERTS_CACHE_TTL_SECONDS = 300  # 5 minutes for alerts
USER_AGENT = "OahuDiveConditions/1.0 (dive-conditions-app)"


class NWSClient:
    """Client for fetching weather data from the National Weather Service API."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the NWS client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/nws.db
        """
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/geo+json"})

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "nws.db"

        self.cache_path = cache_path
        self._init_cache()
        self._gridpoint_cache: dict = {}

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nws_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _make_cache_key(self, endpoint: str, params: dict) -> str:
        """Generate a cache key for the request."""
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str, ttl_seconds: int) -> Optional[dict]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT data, created_at FROM nws_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=ttl_seconds):
                conn.execute("DELETE FROM nws_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: dict) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO nws_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _get_gridpoint(self, lat: float, lon: float) -> tuple[str, int, int]:
        """Get the NWS grid office and coordinates for a lat/lon.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Tuple of (office_id, grid_x, grid_y)
        """
        cache_key = f"{lat:.4f},{lon:.4f}"
        if cache_key in self._gridpoint_cache:
            return self._gridpoint_cache[cache_key]

        url = f"{NWS_BASE_URL}/points/{lat:.4f},{lon:.4f}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise NWSError(f"Failed to get gridpoint for {lat}, {lon}: {e}") from e

        props = data.get("properties", {})
        office = props.get("gridId")
        grid_x = props.get("gridX")
        grid_y = props.get("gridY")

        if not all([office, grid_x is not None, grid_y is not None]):
            raise NWSError(f"Invalid gridpoint response for {lat}, {lon}")

        result = (office, grid_x, grid_y)
        self._gridpoint_cache[cache_key] = result
        return result

    def get_hourly_forecast(
        self,
        lat: float,
        lon: float,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get hourly weather forecast for a location.

        Args:
            lat: Latitude
            lon: Longitude
            use_cache: Whether to use cached data

        Returns:
            DataFrame with columns: time, temperature_f, wind_speed_mph, wind_direction,
                                   precipitation_probability, short_forecast
        """
        office, grid_x, grid_y = self._get_gridpoint(lat, lon)
        endpoint = f"/gridpoints/{office}/{grid_x},{grid_y}/forecast/hourly"
        cache_key = self._make_cache_key(endpoint, {})

        if use_cache:
            cached = self._get_cached(cache_key, CACHE_TTL_SECONDS)
            if cached is not None:
                return pd.DataFrame(cached)

        url = f"{NWS_BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise NWSError(f"Failed to get hourly forecast: {e}") from e

        periods = data.get("properties", {}).get("periods", [])

        records = []
        for period in periods:
            wind_speed_str = period.get("windSpeed", "0 mph")
            try:
                wind_speed = int(wind_speed_str.split()[0]) if wind_speed_str else 0
            except (ValueError, IndexError):
                wind_speed = 0

            # Handle precipitation probability where API returns {"value": null}
            precip_data = period.get("probabilityOfPrecipitation") or {}
            precip_prob = precip_data.get("value")
            if precip_prob is None:
                precip_prob = 0

            records.append({
                "time": period.get("startTime"),
                "temperature_f": period.get("temperature"),
                "wind_speed_mph": wind_speed,
                "wind_direction": period.get("windDirection"),
                "precipitation_probability": precip_prob,
                "short_forecast": period.get("shortForecast"),
            })

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_forecast_summary(self, lat: float, lon: float) -> dict:
        """Get a summary of current and upcoming weather conditions.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dict with current conditions and forecast summary
        """
        df = self.get_hourly_forecast(lat, lon)

        if df.empty:
            return {
                "current_temp_f": None,
                "current_wind_mph": None,
                "current_wind_dir": None,
                "current_conditions": None,
                "max_wind_24h_mph": None,
                "rain_probability_24h": None,
            }

        # Get next 24 hours
        df_24h = df.head(24)

        return {
            "current_temp_f": df.iloc[0]["temperature_f"] if not df.empty else None,
            "current_wind_mph": df.iloc[0]["wind_speed_mph"] if not df.empty else None,
            "current_wind_dir": df.iloc[0]["wind_direction"] if not df.empty else None,
            "current_conditions": df.iloc[0]["short_forecast"] if not df.empty else None,
            "max_wind_24h_mph": df_24h["wind_speed_mph"].max() if not df_24h.empty else None,
            "rain_probability_24h": df_24h["precipitation_probability"].max() if not df_24h.empty else None,
        }

    def get_alerts(self, area: str = "HI", use_cache: bool = True) -> list[dict]:
        """Get active weather alerts for an area.

        Args:
            area: State/area code. Defaults to "HI" for Hawaii.
            use_cache: Whether to use cached data

        Returns:
            List of alert dictionaries with keys: event, headline, description,
                                                  severity, urgency, onset, expires
        """
        endpoint = "/alerts/active"
        params = {"area": area}
        cache_key = self._make_cache_key(endpoint, params)

        if use_cache:
            cached = self._get_cached(cache_key, ALERTS_CACHE_TTL_SECONDS)
            if cached is not None:
                return cached

        url = f"{NWS_BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise NWSError(f"Failed to get alerts: {e}") from e

        features = data.get("features", [])

        alerts = []
        for feature in features:
            props = feature.get("properties", {})
            alerts.append({
                "event": props.get("event"),
                "headline": props.get("headline"),
                "description": props.get("description"),
                "severity": props.get("severity"),
                "urgency": props.get("urgency"),
                "onset": props.get("onset"),
                "expires": props.get("expires"),
                "areas": props.get("areaDesc"),
            })

        if use_cache:
            self._set_cached(cache_key, alerts)

        return alerts

    def get_marine_alerts(self) -> list[dict]:
        """Get active marine-related alerts for Hawaii.

        Returns:
            List of marine alerts (High Surf, High Wind, etc.)
        """
        alerts = self.get_alerts(area="HI")

        marine_keywords = [
            "surf", "wave", "marine", "wind", "coastal", "beach",
            "rip current", "sea", "ocean", "small craft"
        ]

        marine_alerts = []
        for alert in alerts:
            event_lower = (alert.get("event") or "").lower()
            headline_lower = (alert.get("headline") or "").lower()

            if any(kw in event_lower or kw in headline_lower for kw in marine_keywords):
                marine_alerts.append(alert)

        return marine_alerts


class NWSError(Exception):
    """Exception raised for NWS client errors."""

    pass
