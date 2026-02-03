"""USGS Water Services client for stream discharge data.

Provides real-time stream discharge (flow) data for Oahu streams.
High discharge indicates recent rainfall and potential poor water visibility.
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


USGS_BASE_URL = "https://waterservices.usgs.gov/nwis/iv/"
CACHE_TTL_SECONDS = 900  # 15 minutes for real-time data

# Parameter codes
PARAM_DISCHARGE = "00060"  # Discharge in cubic feet per second (cfs)
PARAM_GAGE_HEIGHT = "00065"  # Gage height in feet

# Key Oahu streamgages
OAHU_GAGES = {
    "waimea": "16275000",      # Waimea River - North Shore
    "waimanalo": "16240500",   # Waimanalo Stream - Windward
    "kaneohe": "16247100",     # Kaneohe Stream - Windward
    "kalihi": "16229000",      # Kalihi Stream - South
    "nuuanu": "16227500",      # Nuuanu Stream - South
    "manoa_palolo": "16213000", # Manoa-Palolo Drainage - South
    "makiki": "16211600",      # Makiki Stream - South
}

# Discharge thresholds for visibility impact (cfs)
# These are rough estimates - actual impact varies by site
DISCHARGE_THRESHOLDS = {
    "low": 5,       # Normal baseflow, minimal impact
    "moderate": 20,  # Some turbidity expected
    "high": 50,     # Significant runoff, poor visibility likely
    "extreme": 100, # Major runoff event
}


class USGSClient:
    """Client for fetching stream data from USGS Water Services."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the USGS client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/usgs.db
        """
        self.session = requests.Session()

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "usgs.db"

        self.cache_path = cache_path
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS usgs_cache (
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
                "SELECT data, created_at FROM usgs_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute("DELETE FROM usgs_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: list) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO usgs_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def get_discharge(
        self,
        site_id: str,
        hours: int = 24,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Get stream discharge data for a USGS site.

        Args:
            site_id: USGS site ID (e.g., "16247100" for Kaneohe)
            hours: Number of hours of data to retrieve. Defaults to 24.
            use_cache: Whether to use cached data.

        Returns:
            DataFrame with columns: time, discharge_cfs
        """
        params = {
            "sites": site_id,
            "parameterCd": PARAM_DISCHARGE,
            "period": f"PT{hours}H",
            "format": "json",
            "siteStatus": "active",
        }

        cache_key = self._make_cache_key(params)

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return pd.DataFrame(cached)

        try:
            response = self.session.get(USGS_BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise USGSError(f"Failed to fetch discharge data: {e}") from e

        records = []
        time_series = data.get("value", {}).get("timeSeries", [])

        for ts in time_series:
            values = ts.get("values", [{}])[0].get("value", [])
            for v in values:
                discharge = v.get("value")
                if discharge is not None:
                    try:
                        records.append({
                            "time": v.get("dateTime"),
                            "discharge_cfs": float(discharge),
                        })
                    except (ValueError, TypeError):
                        continue

        if use_cache and records:
            self._set_cached(cache_key, records)

        return pd.DataFrame(records)

    def get_current_discharge(self, site_id: str) -> Optional[float]:
        """Get the most recent discharge reading for a site.

        Args:
            site_id: USGS site ID

        Returns:
            Current discharge in cfs, or None if unavailable
        """
        df = self.get_discharge(site_id, hours=6)

        if df.empty:
            return None

        return df.iloc[-1]["discharge_cfs"]

    def get_discharge_level(self, site_id: str) -> dict:
        """Get current discharge and its severity level.

        Args:
            site_id: USGS site ID

        Returns:
            Dict with keys: discharge_cfs, level (low/moderate/high/extreme),
                          visibility_impact (description)
        """
        discharge = self.get_current_discharge(site_id)

        if discharge is None:
            return {
                "discharge_cfs": None,
                "level": "unknown",
                "visibility_impact": "No data available",
            }

        if discharge < DISCHARGE_THRESHOLDS["low"]:
            level = "low"
            impact = "Minimal impact on water clarity"
        elif discharge < DISCHARGE_THRESHOLDS["moderate"]:
            level = "moderate"
            impact = "Some turbidity possible near shore"
        elif discharge < DISCHARGE_THRESHOLDS["high"]:
            level = "high"
            impact = "Reduced visibility likely, especially near stream mouths"
        else:
            level = "extreme"
            impact = "Poor visibility expected, significant runoff"

        return {
            "discharge_cfs": discharge,
            "level": level,
            "visibility_impact": impact,
        }

    def get_discharge_trend(self, site_id: str, hours: int = 24) -> dict:
        """Analyze discharge trend over time.

        Args:
            site_id: USGS site ID
            hours: Hours to analyze. Defaults to 24.

        Returns:
            Dict with keys: current_cfs, max_cfs, min_cfs, avg_cfs, trend (rising/falling/stable)
        """
        df = self.get_discharge(site_id, hours=hours)

        if df.empty or len(df) < 2:
            return {
                "current_cfs": None,
                "max_cfs": None,
                "min_cfs": None,
                "avg_cfs": None,
                "trend": "unknown",
            }

        current = df.iloc[-1]["discharge_cfs"]
        first_half_avg = df.head(len(df) // 2)["discharge_cfs"].mean()
        second_half_avg = df.tail(len(df) // 2)["discharge_cfs"].mean()

        if second_half_avg > first_half_avg * 1.2:
            trend = "rising"
        elif second_half_avg < first_half_avg * 0.8:
            trend = "falling"
        else:
            trend = "stable"

        return {
            "current_cfs": current,
            "max_cfs": df["discharge_cfs"].max(),
            "min_cfs": df["discharge_cfs"].min(),
            "avg_cfs": df["discharge_cfs"].mean(),
            "trend": trend,
        }

    def get_multiple_sites(
        self,
        site_ids: list[str],
        hours: int = 24,
    ) -> dict[str, pd.DataFrame]:
        """Get discharge data for multiple sites.

        Args:
            site_ids: List of USGS site IDs
            hours: Number of hours of data. Defaults to 24.

        Returns:
            Dict mapping site_id to DataFrame
        """
        results = {}
        for site_id in site_ids:
            try:
                results[site_id] = self.get_discharge(site_id, hours=hours)
            except USGSError:
                results[site_id] = pd.DataFrame()
        return results

    def get_gage_for_site(self, gage_name: str) -> Optional[str]:
        """Get USGS gage ID by name.

        Args:
            gage_name: Gage name (e.g., "kaneohe", "waimea")

        Returns:
            USGS site ID or None if not found
        """
        return OAHU_GAGES.get(gage_name.lower())


class USGSError(Exception):
    """Exception raised for USGS client errors."""

    pass
