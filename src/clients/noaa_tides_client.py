"""NOAA CO-OPS API client for tide predictions and water levels.

Provides tide predictions and observed water levels for Oahu stations.
Stations: 1612340 (Honolulu), 1612480 (Kaneohe), 1612366 (Waikiki)
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import requests


COOPS_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
CACHE_TTL_SECONDS = 3600  # 1 hour

# Oahu tide stations
OAHU_STATIONS = {
    "honolulu": "1612340",
    "kaneohe": "1612480",
    "mokuoloe": "1612480",  # Kaneohe Bay / Coconut Island
}

# Default station for different coasts
COAST_STATIONS = {
    "north_shore": "1612340",  # Honolulu (closest with data)
    "west_side": "1612340",
    "south_shore": "1612340",
    "southeast": "1612340",
    "windward": "1612480",  # Kaneohe
}


class NOAATidesClient:
    """Client for fetching tide data from NOAA CO-OPS API."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the NOAA Tides client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/tides.db
        """
        self.session = requests.Session()

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "tides.db"

        self.cache_path = cache_path
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tides_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _make_cache_key(self, params: dict) -> str:
        """Generate a cache key for the request."""
        key_data = json.dumps(params, sort_keys=True)
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str) -> Optional[list]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT data, created_at FROM tides_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute("DELETE FROM tides_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: list) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tides_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _fetch_data(self, params: dict) -> list:
        """Fetch data from CO-OPS API."""
        try:
            response = self.session.get(COOPS_BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise NOAATidesError(f"Failed to fetch tide data: {e}") from e

        if "error" in data:
            raise NOAATidesError(f"CO-OPS API error: {data['error'].get('message', 'Unknown error')}")

        return data.get("predictions", data.get("data", []))

    def get_tide_predictions(
        self,
        station_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval: Literal["h", "hilo"] = "hilo",
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get tide predictions for a station.

        Args:
            station_id: NOAA station ID (e.g., "1612340" for Honolulu)
            start_date: Start date. Defaults to today.
            end_date: End date. Defaults to 3 days from start.
            interval: "h" for hourly, "hilo" for high/low only. Defaults to "hilo".
            use_cache: Whether to use cached data.

        Returns:
            DataFrame with columns: time, water_level_ft, type (H/L for hilo)
        """
        if start_date is None:
            start_date = datetime.utcnow()
        if end_date is None:
            end_date = start_date + timedelta(days=3)

        params = {
            "station": station_id,
            "begin_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "product": "predictions",
            "datum": "MLLW",
            "units": "english",
            "time_zone": "lst_ldt",
            "format": "json",
            "interval": interval,
        }

        cache_key = self._make_cache_key(params)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        predictions = self._fetch_data(params)

        records = []
        for pred in predictions:
            record = {
                "time": pred.get("t"),
                "water_level_ft": float(pred.get("v", 0)),
            }
            if interval == "hilo":
                record["type"] = pred.get("type")  # H or L
            records.append(record)

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_water_level(
        self,
        station_id: str,
        hours: int = 24,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get observed water levels for a station.

        Args:
            station_id: NOAA station ID
            hours: Number of hours of data to retrieve. Defaults to 24.
            use_cache: Whether to use cached data.

        Returns:
            DataFrame with columns: time, water_level_ft
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(hours=hours)

        params = {
            "station": station_id,
            "begin_date": start_date.strftime("%Y%m%d %H:%M"),
            "end_date": end_date.strftime("%Y%m%d %H:%M"),
            "product": "water_level",
            "datum": "MLLW",
            "units": "english",
            "time_zone": "lst_ldt",
            "format": "json",
        }

        cache_key = self._make_cache_key(params)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        data = self._fetch_data(params)

        records = []
        for obs in data:
            records.append({
                "time": obs.get("t"),
                "water_level_ft": float(obs.get("v", 0)) if obs.get("v") else None,
            })

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_next_tides(
        self,
        station_id: str,
        count: int = 4,
    ) -> list[dict]:
        """Get the next N high/low tides.

        Args:
            station_id: NOAA station ID
            count: Number of tides to return. Defaults to 4.

        Returns:
            List of dicts with keys: time, water_level_ft, type, is_high
        """
        df = self.get_tide_predictions(station_id, interval="hilo")

        if df.empty:
            return []

        # Parse times and filter to future
        df["time_parsed"] = pd.to_datetime(df["time"])
        now = datetime.now()
        df = df[df["time_parsed"] > now]

        tides = []
        for _, row in df.head(count).iterrows():
            tides.append({
                "time": row["time"],
                "water_level_ft": row["water_level_ft"],
                "type": row["type"],
                "is_high": row["type"] == "H",
            })

        return tides

    def get_current_tide_phase(self, station_id: str) -> dict:
        """Determine current tide phase (rising/falling, hours to next high/low).

        Args:
            station_id: NOAA station ID

        Returns:
            Dict with keys: phase (rising/falling), next_high, next_low, current_level_ft
        """
        next_tides = self.get_next_tides(station_id, count=4)

        if not next_tides:
            return {
                "phase": None,
                "next_high": None,
                "next_low": None,
                "current_level_ft": None,
            }

        # Determine phase based on next tide
        next_tide = next_tides[0]
        phase = "rising" if next_tide["is_high"] else "falling"

        next_high = next((t for t in next_tides if t["is_high"]), None)
        next_low = next((t for t in next_tides if not t["is_high"]), None)

        # Try to get current water level, but don't fail if unavailable
        # Many NOAA stations only have predictions, not real-time observations
        current_level = None
        try:
            water_level_df = self.get_water_level(station_id, hours=2)
            if not water_level_df.empty:
                current_level = water_level_df.iloc[-1]["water_level_ft"]
        except NOAATidesError:
            # Real-time water level not available for this station - that's OK
            pass

        return {
            "phase": phase,
            "next_high": next_high,
            "next_low": next_low,
            "current_level_ft": current_level,
        }

    def get_station_for_coast(self, coast: str) -> str:
        """Get the appropriate tide station for a coast.

        Args:
            coast: Coast name (north_shore, west_side, south_shore, southeast, windward)

        Returns:
            Station ID
        """
        return COAST_STATIONS.get(coast.lower(), OAHU_STATIONS["honolulu"])


class NOAATidesError(Exception):
    """Exception raised for NOAA Tides client errors."""

    pass
