"""PacIOOS ERDDAP client for SWAN Oahu wave model data.

Fetches wave height, period, and direction from the SWAN Oahu model.
Data is ~500m resolution with 5-day hourly forecasts, updated daily ~1:30 PM HST.

Model domain: lat 21.2-21.75, lon -158.35 to -157.6 (approx)
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


ERDDAP_BASE = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu"
CACHE_TTL_SECONDS = 3600  # 1 hour
REQUEST_TIMEOUT = 10  # Reduced from 30s to fail fast
CIRCUIT_BREAKER_THRESHOLD = 3  # Open circuit after this many consecutive failures

# Model domain bounds (approximate)
LAT_MIN, LAT_MAX = 21.2, 21.75
LON_MIN, LON_MAX = -158.35, -157.6  # In -180 to 180 format

# Circuit breaker state (shared across instances)
_circuit_breaker = {
    "failures": 0,
    "is_open": False,
}


class PacIOOSClient:
    """Client for fetching wave data from PacIOOS SWAN Oahu model."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the PacIOOS client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/pacioos.db
        """
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "OahuDiveConditions/1.0"

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "pacioos.db"

        self.cache_path = cache_path
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pacioos_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _make_cache_key(self, lat: float, lon: float, hours: int) -> str:
        """Generate a cache key for the query parameters."""
        # Round to grid resolution to improve cache hits
        lat_rounded = round(lat, 2)
        lon_rounded = round(lon, 2)
        key_data = f"{lat_rounded}:{lon_rounded}:{hours}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str) -> Optional[dict]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT data, created_at FROM pacioos_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute("DELETE FROM pacioos_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: dict) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pacioos_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _lon_to_360(self, lon: float) -> float:
        """Convert longitude from -180/180 to 0/360 format."""
        if lon < 0:
            return 360 + lon
        return lon

    def _is_in_domain(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within model domain."""
        return (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX)

    def get_wave_data(
        self,
        lat: float,
        lon: float,
        hours: int = 48,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Fetch wave data for a specific location and time range.

        Args:
            lat: Latitude of the location (e.g., 21.27 for Waikiki)
            lon: Longitude of the location in -180/180 format (e.g., -157.83)
            hours: Number of hours of forecast data. Defaults to 48.
            use_cache: Whether to use cached data. Defaults to True.

        Returns:
            DataFrame with columns: time, wave_height_m, period_s, direction_deg
        """
        # Check if in domain
        if not self._is_in_domain(lat, lon):
            return pd.DataFrame(columns=["time", "wave_height_m", "period_s", "direction_deg"])

        # Circuit breaker: skip requests if service is down
        if _circuit_breaker["is_open"]:
            return pd.DataFrame(columns=["time", "wave_height_m", "period_s", "direction_deg"])

        cache_key = self._make_cache_key(lat, lon, hours)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        # Convert coordinates
        lon_360 = self._lon_to_360(lon)

        # Build time range
        now = datetime.utcnow()
        end = now + timedelta(hours=hours)
        time_start = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        time_end = end.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build ERDDAP griddap URL
        # Format: variable[(time_start):(time_end)][(depth)][(lat)][(lon)]
        url = (
            f"{ERDDAP_BASE}.csv?"
            f"shgt[({time_start}):1:({time_end})][(0.0):1:(0.0)][({lat}):1:({lat})][({lon_360}):1:({lon_360})],"
            f"mper[({time_start}):1:({time_end})][(0.0):1:(0.0)][({lat}):1:({lat})][({lon_360}):1:({lon_360})],"
            f"mdir[({time_start}):1:({time_end})][(0.0):1:(0.0)][({lat}):1:({lat})][({lon_360}):1:({lon_360})]"
        )

        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            # Success: reset circuit breaker
            _circuit_breaker["failures"] = 0
            _circuit_breaker["is_open"] = False
        except requests.RequestException as e:
            # Failure: increment counter and maybe open circuit
            _circuit_breaker["failures"] += 1
            if _circuit_breaker["failures"] >= CIRCUIT_BREAKER_THRESHOLD:
                _circuit_breaker["is_open"] = True
            raise PacIOOSError(f"Failed to fetch data from PacIOOS: {e}") from e

        # Parse CSV response
        lines = response.text.strip().split("\n")
        if len(lines) < 3:  # Header + units + at least one data row
            return pd.DataFrame(columns=["time", "wave_height_m", "period_s", "direction_deg"])

        # Skip header and units rows
        records = []
        for line in lines[2:]:
            parts = line.split(",")
            if len(parts) >= 6:
                time_str = parts[0]
                shgt = parts[4]
                mper = parts[5] if len(parts) > 5 else None
                mdir = parts[6] if len(parts) > 6 else None

                # Skip NaN values
                if shgt == "NaN" or not shgt:
                    continue

                records.append({
                    "time": time_str,
                    "wave_height_m": float(shgt) if shgt and shgt != "NaN" else None,
                    "period_s": float(mper) if mper and mper != "NaN" else None,
                    "direction_deg": float(mdir) if mdir and mdir != "NaN" else None,
                })

        df = pd.DataFrame(records)

        if use_cache and not df.empty:
            self._set_cached(cache_key, df.to_dict(orient="records"))

        return df

    def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int = 48,
    ) -> pd.DataFrame:
        """Fetch wave forecast for the next N hours.

        Args:
            lat: Latitude of the location
            lon: Longitude of the location (in -180/180 format)
            hours: Number of hours to forecast. Defaults to 48.

        Returns:
            DataFrame with wave forecast data
        """
        return self.get_wave_data(lat, lon, hours=hours)

    def get_current_conditions(self, lat: float, lon: float) -> dict:
        """Get the most recent wave conditions for a location.

        Args:
            lat: Latitude of the location
            lon: Longitude of the location (in -180/180 format)

        Returns:
            Dict with keys: time, wave_height_m, wave_height_ft, period_s, direction_deg, in_domain
        """
        # Check domain first
        if not self._is_in_domain(lat, lon):
            return {
                "time": None,
                "wave_height_m": None,
                "wave_height_ft": None,
                "period_s": None,
                "direction_deg": None,
                "in_domain": False,
            }

        df = self.get_wave_data(lat, lon, hours=6)

        if df.empty:
            return {
                "time": None,
                "wave_height_m": None,
                "wave_height_ft": None,
                "period_s": None,
                "direction_deg": None,
                "in_domain": True,
            }

        latest = df.iloc[0]
        wave_height_m = latest.get("wave_height_m")

        return {
            "time": latest.get("time"),
            "wave_height_m": wave_height_m,
            "wave_height_ft": wave_height_m * 3.28084 if wave_height_m else None,
            "period_s": latest.get("period_s"),
            "direction_deg": latest.get("direction_deg"),
            "in_domain": True,
        }


class PacIOOSError(Exception):
    """Exception raised for PacIOOS client errors."""

    pass
