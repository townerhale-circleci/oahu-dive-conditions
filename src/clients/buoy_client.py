"""CDIP/NDBC buoy client for real-time wave observations.

Provides real-time wave height, period, and direction from buoys around Oahu.
Primary buoys:
- 51201 (CDIP 106): Waimea Bay - North Shore
- 51202 (CDIP 098): Mokapu Point - Windward/East
- 51212 (CDIP 238): Kalaeloa/Barbers Point - South/West
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


# NDBC data URLs
NDBC_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2"
NDBC_SPEC_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.spec"
NDBC_TXT_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"

CACHE_TTL_SECONDS = 600  # 10 minutes for real-time buoy data

# Oahu buoys with NDBC and CDIP IDs
OAHU_BUOYS = {
    "waimea": {
        "ndbc": "51201",
        "cdip": "106",
        "name": "Waimea Bay",
        "location": "North Shore",
        "lat": 21.673,
        "lon": -158.116,
    },
    "mokapu": {
        "ndbc": "51202",
        "cdip": "098",
        "name": "Mokapu Point",
        "location": "Windward/East",
        "lat": 21.417,
        "lon": -157.680,
    },
    "kalaeloa": {
        "ndbc": "51212",
        "cdip": "238",
        "name": "Kalaeloa (Barbers Point)",
        "location": "South/West",
        "lat": 21.288,
        "lon": -158.124,
    },
    "pearl_harbor": {
        "ndbc": "51211",
        "cdip": None,
        "name": "Pearl Harbor",
        "location": "South",
        "lat": 21.303,
        "lon": -157.959,
    },
    "kaneohe": {
        "ndbc": "51207",
        "cdip": None,
        "name": "Kaneohe Bay",
        "location": "Windward",
        "lat": 21.477,
        "lon": -157.788,
    },
}

# Map swell direction to affected coasts
SWELL_DIRECTION_MAP = {
    "N": ["north_shore"],
    "NNE": ["north_shore", "windward"],
    "NE": ["windward"],
    "ENE": ["windward"],
    "E": ["windward", "southeast"],
    "ESE": ["southeast"],
    "SE": ["southeast", "south_shore"],
    "SSE": ["south_shore"],
    "S": ["south_shore"],
    "SSW": ["south_shore", "west_side"],
    "SW": ["west_side"],
    "WSW": ["west_side"],
    "W": ["west_side"],
    "WNW": ["west_side", "north_shore"],
    "NW": ["north_shore"],
    "NNW": ["north_shore"],
}


class BuoyClient:
    """Client for fetching buoy data from NDBC."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the Buoy client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/buoy.db
        """
        self.session = requests.Session()

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "buoy.db"

        self.cache_path = cache_path
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS buoy_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _make_cache_key(self, url: str) -> str:
        """Generate a cache key for the URL."""
        return hashlib.sha256(url.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str) -> Optional[list]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT data, created_at FROM buoy_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute("DELETE FROM buoy_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: list) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO buoy_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _parse_ndbc_spectral(self, text: str) -> list[dict]:
        """Parse NDBC spectral data text format."""
        lines = text.strip().split("\n")
        if len(lines) < 3:
            return []

        records = []
        for line in lines[2:]:  # Skip header lines
            parts = line.split()
            if len(parts) < 15:
                continue

            try:
                # NDBC spectral format: YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD STEEPNESS APD MWD
                year = int(parts[0])
                if year < 100:
                    year += 2000

                time_str = f"{year}-{parts[1]}-{parts[2]}T{parts[3]}:{parts[4]}:00Z"

                record = {
                    "time": time_str,
                    "wave_height_m": self._safe_float(parts[5]),
                    "swell_height_m": self._safe_float(parts[6]),
                    "swell_period_s": self._safe_float(parts[7]),
                    "wind_wave_height_m": self._safe_float(parts[8]),
                    "wind_wave_period_s": self._safe_float(parts[9]),
                    "swell_direction": parts[10] if parts[10] != "MM" else None,
                    "wind_wave_direction": parts[11] if parts[11] != "MM" else None,
                    "average_period_s": self._safe_float(parts[13]),
                    "mean_wave_direction": self._safe_float(parts[14]),
                }
                records.append(record)
            except (ValueError, IndexError):
                continue

        return records

    def _parse_ndbc_standard(self, text: str) -> list[dict]:
        """Parse NDBC standard meteorological data text format."""
        lines = text.strip().split("\n")
        if len(lines) < 3:
            return []

        records = []
        for line in lines[2:]:  # Skip header lines
            parts = line.split()
            if len(parts) < 13:
                continue

            try:
                year = int(parts[0])
                if year < 100:
                    year += 2000

                time_str = f"{year}-{parts[1]}-{parts[2]}T{parts[3]}:{parts[4]}:00Z"

                record = {
                    "time": time_str,
                    "wind_direction": self._safe_float(parts[5]),
                    "wind_speed_mps": self._safe_float(parts[6]),
                    "gust_speed_mps": self._safe_float(parts[7]),
                    "wave_height_m": self._safe_float(parts[8]),
                    "dominant_period_s": self._safe_float(parts[9]),
                    "average_period_s": self._safe_float(parts[10]),
                    "mean_wave_direction": self._safe_float(parts[11]),
                    "pressure_hpa": self._safe_float(parts[12]),
                }
                records.append(record)
            except (ValueError, IndexError):
                continue

        return records

    def _safe_float(self, value: str) -> Optional[float]:
        """Safely convert to float, returning None for missing values."""
        if value in ("MM", "999", "99.0", "9999", "99.00"):
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def get_spectral_data(
        self,
        station_id: str,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get spectral wave data (detailed swell info) from a buoy.

        Args:
            station_id: NDBC station ID (e.g., "51201")
            use_cache: Whether to use cached data

        Returns:
            DataFrame with wave height, swell height/period/direction, wind wave info
        """
        url = NDBC_SPEC_URL.format(station=station_id)
        cache_key = self._make_cache_key(url)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            records = self._parse_ndbc_spectral(response.text)
        except requests.RequestException as e:
            raise BuoyError(f"Failed to fetch spectral data for {station_id}: {e}") from e

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_standard_data(
        self,
        station_id: str,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get standard meteorological data from a buoy.

        Args:
            station_id: NDBC station ID
            use_cache: Whether to use cached data

        Returns:
            DataFrame with wind, wave, and pressure data
        """
        url = NDBC_TXT_URL.format(station=station_id)
        cache_key = self._make_cache_key(url)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            records = self._parse_ndbc_standard(response.text)
        except requests.RequestException as e:
            raise BuoyError(f"Failed to fetch standard data for {station_id}: {e}") from e

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_current_conditions(self, station_id: str) -> dict:
        """Get current wave and wind conditions from a buoy.

        Args:
            station_id: NDBC station ID

        Returns:
            Dict with current conditions
        """
        # Try spectral data first for detailed swell info
        try:
            df = self.get_spectral_data(station_id)
            if not df.empty:
                latest = df.iloc[0]
                wave_height_m = latest.get("wave_height_m")
                return {
                    "time": latest.get("time"),
                    "wave_height_m": wave_height_m,
                    "wave_height_ft": wave_height_m * 3.28084 if wave_height_m else None,
                    "swell_height_m": latest.get("swell_height_m"),
                    "swell_period_s": latest.get("swell_period_s"),
                    "swell_direction": latest.get("swell_direction"),
                    "wind_wave_height_m": latest.get("wind_wave_height_m"),
                    "mean_direction_deg": latest.get("mean_wave_direction"),
                }
        except BuoyError:
            pass

        # Fall back to standard data
        try:
            df = self.get_standard_data(station_id)
            if not df.empty:
                latest = df.iloc[0]
                wave_height_m = latest.get("wave_height_m")
                return {
                    "time": latest.get("time"),
                    "wave_height_m": wave_height_m,
                    "wave_height_ft": wave_height_m * 3.28084 if wave_height_m else None,
                    "swell_height_m": None,
                    "swell_period_s": latest.get("dominant_period_s"),
                    "swell_direction": None,
                    "wind_wave_height_m": None,
                    "mean_direction_deg": latest.get("mean_wave_direction"),
                }
        except BuoyError:
            pass

        return {
            "time": None,
            "wave_height_m": None,
            "wave_height_ft": None,
            "swell_height_m": None,
            "swell_period_s": None,
            "swell_direction": None,
            "wind_wave_height_m": None,
            "mean_direction_deg": None,
        }

    def get_buoy_for_coast(self, coast: str) -> dict:
        """Get the most relevant buoy for a coast.

        Args:
            coast: Coast name (north_shore, west_side, south_shore, southeast, windward)

        Returns:
            Buoy info dict
        """
        coast_lower = coast.lower()

        if coast_lower in ("north_shore", "north"):
            return OAHU_BUOYS["waimea"]
        elif coast_lower in ("windward", "east"):
            return OAHU_BUOYS["mokapu"]
        elif coast_lower in ("south_shore", "south", "southeast"):
            return OAHU_BUOYS["kalaeloa"]
        elif coast_lower in ("west_side", "west", "leeward"):
            return OAHU_BUOYS["kalaeloa"]
        else:
            return OAHU_BUOYS["waimea"]

    def get_all_buoy_conditions(self) -> dict[str, dict]:
        """Get current conditions from all Oahu buoys.

        Returns:
            Dict mapping buoy name to conditions
        """
        results = {}
        for name, buoy_info in OAHU_BUOYS.items():
            try:
                conditions = self.get_current_conditions(buoy_info["ndbc"])
                conditions["buoy_name"] = buoy_info["name"]
                conditions["location"] = buoy_info["location"]
                results[name] = conditions
            except BuoyError:
                results[name] = {
                    "buoy_name": buoy_info["name"],
                    "location": buoy_info["location"],
                    "error": "Data unavailable",
                }
        return results

    def direction_to_compass(self, degrees: float) -> str:
        """Convert degrees to compass direction.

        Args:
            degrees: Direction in degrees (0-360)

        Returns:
            Compass direction (N, NNE, NE, etc.)
        """
        if degrees is None:
            return "Unknown"

        directions = [
            "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
        ]
        idx = round(degrees / 22.5) % 16
        return directions[idx]

    def get_affected_coasts(self, direction_deg: float) -> list[str]:
        """Determine which coasts are affected by swell from a given direction.

        Args:
            direction_deg: Swell direction in degrees

        Returns:
            List of affected coast names
        """
        compass = self.direction_to_compass(direction_deg)
        return SWELL_DIRECTION_MAP.get(compass, [])


class BuoyError(Exception):
    """Exception raised for Buoy client errors."""

    pass
