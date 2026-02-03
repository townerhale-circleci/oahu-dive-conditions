"""PacIOOS ERDDAP client for SWAN Oahu wave model data.

Fetches wave height, period, and direction from the SWAN Oahu model.
Data is ~500m resolution with 5-day hourly forecasts, updated daily ~1:30 PM HST.
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from erddapy import ERDDAP


ERDDAP_SERVER = "https://pae-paha.pacioos.hawaii.edu/erddap"
DATASET_ID = "swan_oahu"
VARIABLES = ["shgt", "mper", "mdir"]  # wave height, mean period, mean direction
CACHE_TTL_SECONDS = 3600  # 1 hour


class PacIOOSClient:
    """Client for fetching wave data from PacIOOS SWAN Oahu model."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the PacIOOS client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/pacioos.db
        """
        self.erddap = ERDDAP(server=ERDDAP_SERVER, protocol="griddap")
        self.erddap.dataset_id = DATASET_ID

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
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at
                ON pacioos_cache(created_at)
            """)
            conn.commit()

    def _make_cache_key(
        self,
        lat: float,
        lon: float,
        start_time: datetime,
        end_time: datetime,
    ) -> str:
        """Generate a cache key for the query parameters."""
        key_data = f"{lat:.4f}:{lon:.4f}:{start_time.isoformat()}:{end_time.isoformat()}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                """
                SELECT data, created_at FROM pacioos_cache
                WHERE cache_key = ?
                """,
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute(
                    "DELETE FROM pacioos_cache WHERE cache_key = ?", (cache_key,)
                )
                conn.commit()
                return None

            return pd.read_json(data_json, orient="records")

    def _set_cached(self, cache_key: str, df: pd.DataFrame) -> None:
        """Store data in cache."""
        data_json = df.to_json(orient="records", date_format="iso")
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pacioos_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, data_json, datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _cleanup_expired_cache(self) -> None:
        """Remove expired cache entries."""
        cutoff = datetime.utcnow() - timedelta(seconds=CACHE_TTL_SECONDS)
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                "DELETE FROM pacioos_cache WHERE created_at < ?",
                (cutoff.isoformat(),),
            )
            conn.commit()

    def get_wave_data(
        self,
        lat: float,
        lon: float,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Fetch wave data for a specific location and time range.

        Args:
            lat: Latitude of the location (e.g., 21.6512 for Shark's Cove)
            lon: Longitude of the location (e.g., -158.0635 for Shark's Cove)
            start_time: Start of time range. Defaults to now.
            end_time: End of time range. Defaults to 48 hours from start.
            use_cache: Whether to use cached data. Defaults to True.

        Returns:
            DataFrame with columns: time, shgt (wave height m), mper (period s), mdir (direction deg)
        """
        if start_time is None:
            start_time = datetime.utcnow()
        if end_time is None:
            end_time = start_time + timedelta(hours=48)

        cache_key = self._make_cache_key(lat, lon, start_time, end_time)

        if use_cache:
            cached_df = self._get_cached(cache_key)
            if cached_df is not None:
                return cached_df

        self.erddap.variables = ["time", "latitude", "longitude"] + VARIABLES

        self.erddap.constraints = {
            "time>=": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time<=": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latitude>=": lat - 0.01,
            "latitude<=": lat + 0.01,
            "longitude>=": lon - 0.01,
            "longitude<=": lon + 0.01,
        }

        try:
            df = self.erddap.to_pandas()
        except Exception as e:
            raise PacIOOSError(f"Failed to fetch data from PacIOOS: {e}") from e

        if df.empty:
            return pd.DataFrame(columns=["time", "shgt", "mper", "mdir"])

        # Clean column names (erddapy adds units in parentheses)
        df.columns = [col.split(" ")[0] for col in df.columns]

        # Find nearest grid point and filter to just that point
        if "latitude" in df.columns and "longitude" in df.columns:
            df["dist"] = ((df["latitude"] - lat) ** 2 + (df["longitude"] - lon) ** 2)
            nearest_lat = df.loc[df["dist"].idxmin(), "latitude"]
            nearest_lon = df.loc[df["dist"].idxmin(), "longitude"]
            df = df[(df["latitude"] == nearest_lat) & (df["longitude"] == nearest_lon)]
            df = df.drop(columns=["dist", "latitude", "longitude"])

        df = df.sort_values("time").reset_index(drop=True)

        if use_cache and not df.empty:
            self._set_cached(cache_key, df)

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
            lon: Longitude of the location
            hours: Number of hours to forecast. Defaults to 48.

        Returns:
            DataFrame with wave forecast data
        """
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(hours=hours)
        return self.get_wave_data(lat, lon, start_time, end_time)

    def get_current_conditions(self, lat: float, lon: float) -> dict:
        """Get the most recent wave conditions for a location.

        Args:
            lat: Latitude of the location
            lon: Longitude of the location

        Returns:
            Dict with keys: time, wave_height_m, wave_height_ft, period_s, direction_deg
        """
        df = self.get_forecast(lat, lon, hours=6)

        if df.empty:
            return {
                "time": None,
                "wave_height_m": None,
                "wave_height_ft": None,
                "period_s": None,
                "direction_deg": None,
            }

        latest = df.iloc[0]
        wave_height_m = latest.get("shgt")

        return {
            "time": latest.get("time"),
            "wave_height_m": wave_height_m,
            "wave_height_ft": wave_height_m * 3.28084 if wave_height_m else None,
            "period_s": latest.get("mper"),
            "direction_deg": latest.get("mdir"),
        }


class PacIOOSError(Exception):
    """Exception raised for PacIOOS client errors."""

    pass
