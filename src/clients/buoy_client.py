"""CDIP/NDBC buoy client for real-time wave observations.

Provides real-time wave height, period, and direction from buoys around Oahu.
Primary buoys:
- 51201 (CDIP 106): Waimea Bay - North Shore
- 51202 (CDIP 098): Mokapu Point - Windward/East
- 51212 (CDIP 238): Kalaeloa/Barbers Point - South/West
"""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


# NDBC data URLs
NDBC_BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2"
NDBC_SPEC_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.spec"
NDBC_TXT_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"

CACHE_TTL_SECONDS = 600  # 10 minutes for real-time buoy data
MAX_OBSERVATION_AGE_SECONDS = 3 * 3600  # Reject buoy rows older than 3 hours

logger = logging.getLogger(__name__)

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

# 16-point compass -> degrees. Kept identical to surf_transform._COMPASS_TO_DEG
# and the scorer's exposure_map so all three agree on what "NW" means. Defined
# locally (not imported) to avoid a circular import: src.core.__init__ imports the
# ranker, which imports this client.
_COMPASS_TO_DEG = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
}


def _compass_to_deg(compass: Optional[str]) -> Optional[float]:
    """Map a 16-point compass string (e.g. 'NW') to degrees, or None."""
    if compass is None:
        return None
    return _COMPASS_TO_DEG.get(compass.strip().upper())


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

    @staticmethod
    def _parse_header(lines: list[str]) -> Optional[dict[str, int]]:
        """Map NDBC column names to positional indices from the header line.

        NDBC realtime2 files have two comment header lines; the first names the
        columns, e.g. ``#YY MM DD hh mm WDIR WSPD GST WVHT DPD ...``. Parsing by
        header name (not fixed index) survives column additions/reordering.
        Returns ``{COLUMN_NAME: index}`` (uppercased) or ``None`` if no header.
        """
        for line in lines:
            if line.startswith("#"):
                cols = line.lstrip("#").split()
                if cols and cols[0].upper() in ("YY", "YYYY"):
                    return {name.upper(): i for i, name in enumerate(cols)}
        return None

    @staticmethod
    def _parse_time(parts: list[str]) -> Optional[str]:
        """Build an ISO-8601 UTC timestamp from the leading date/time columns.

        NDBC realtime2 files always begin with YY MM DD hh mm in positions 0-4.
        These are used positionally (not by header name) because the header
        collides on case: month is ``MM`` and minute is ``mm`` in the same file.
        """
        try:
            year = int(parts[0])
            if year < 100:
                year += 2000
            return f"{year}-{parts[1]}-{parts[2]}T{parts[3]}:{parts[4]}:00Z"
        except (ValueError, IndexError):
            return None

    def _parse_ndbc_spectral(self, text: str) -> list[dict]:
        """Parse NDBC spectral (.spec) data using header-name column indexing.

        Header (typical): ``#YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD
        STEEPNESS APD MWD``. STEEPNESS is a word ("AVERAGE"/"STEEP"), so fixed
        indexing past it was fragile; we locate every field by name instead.
        """
        lines = text.strip().split("\n")
        cols = self._parse_header(lines)
        if cols is None:
            return []

        def col(parts, name):
            idx = cols.get(name)
            if idx is None or idx >= len(parts):
                return None
            return parts[idx]

        def col_dir(parts, name):
            """Compass-string direction column ('NW'), 'MM' -> None."""
            v = col(parts, name)
            if v is None or v == "MM":
                return None
            return v

        records = []
        for line in lines:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            # Need at least the timestamp + WVHT to be a usable row.
            if len(parts) < 6:
                continue

            time_str = self._parse_time(parts)
            if time_str is None:
                continue

            records.append({
                "time": time_str,
                "wave_height_m": self._safe_float(col(parts, "WVHT"), "wave"),
                "swell_height_m": self._safe_float(col(parts, "SWH"), "wave"),
                "swell_period_s": self._safe_float(col(parts, "SWP"), "period"),
                "wind_wave_height_m": self._safe_float(col(parts, "WWH"), "wave"),
                "wind_wave_period_s": self._safe_float(col(parts, "WWP"), "period"),
                "swell_direction": col_dir(parts, "SWD"),
                "wind_wave_direction": col_dir(parts, "WWD"),
                "average_period_s": self._safe_float(col(parts, "APD"), "period"),
                "mean_wave_direction": self._safe_float(col(parts, "MWD"), "direction"),
            })

        return records

    def _parse_ndbc_standard(self, text: str) -> list[dict]:
        """Parse NDBC standard meteorological (.txt) data by header-name indexing.

        Header (typical): ``#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES
        ...``. Oahu buoys report MM for all wind fields; those parse to None and
        are never surfaced as 0 (see notes on buoy wind).
        """
        lines = text.strip().split("\n")
        cols = self._parse_header(lines)
        if cols is None:
            return []

        def col(parts, name):
            idx = cols.get(name)
            if idx is None or idx >= len(parts):
                return None
            return parts[idx]

        records = []
        for line in lines:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) < 6:
                continue

            time_str = self._parse_time(parts)
            if time_str is None:
                continue

            records.append({
                "time": time_str,
                "wind_direction": self._safe_float(col(parts, "WDIR"), "direction"),
                "wind_speed_mps": self._safe_float(col(parts, "WSPD"), "speed"),
                "gust_speed_mps": self._safe_float(col(parts, "GST"), "speed"),
                "wave_height_m": self._safe_float(col(parts, "WVHT"), "wave"),
                "dominant_period_s": self._safe_float(col(parts, "DPD"), "period"),
                "average_period_s": self._safe_float(col(parts, "APD"), "period"),
                "mean_wave_direction": self._safe_float(col(parts, "MWD"), "direction"),
                "pressure_hpa": self._safe_float(col(parts, "PRES"), "pressure"),
            })

        return records

    def _is_stale(self, time_str: Optional[str]) -> bool:
        """Return True if an observation timestamp is older than the max age.

        NDBC timestamps are UTC ("...Z"). A row that can't be parsed is treated
        as stale (unusable) rather than silently accepted.
        """
        if not time_str:
            return True
        try:
            obs_time = datetime.fromisoformat(str(time_str).replace("Z", "+00:00"))
        except ValueError:
            return True
        if obs_time.tzinfo is None:
            obs_time = obs_time.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - obs_time
        return age > timedelta(seconds=MAX_OBSERVATION_AGE_SECONDS)

    # NDBC numeric "missing value" fills per field. Each field's sentinel is an
    # all-9s value one order of magnitude above the field's real range: WVHT/
    # DPD/APD use 99.00, WSPD/GST 99.0, MWD/WDIR 999, PRES 9999.0. We map a
    # value to None when it is >= the field's sentinel magnitude, which is
    # robust to NDBC's decimal-format variations ("99.0" vs "99.00"). The
    # thresholds are picked so no legitimate reading reaches them (e.g. wave
    # height never 99 m, direction 0-360 < 999, sea-level pressure < 9999 hPa).
    _SENTINELS = {
        "wave": 99.0,       # WVHT, SwH, WWH (metres)
        "period": 99.0,     # DPD, APD, SwP, WWP (seconds)
        "direction": 999.0, # MWD, WDIR (degrees, 0-360)
        "speed": 99.0,      # WSPD, GST (m/s)
        "pressure": 9999.0, # PRES (hPa)
    }

    def _safe_float(
        self, value: Optional[str], field: str = "wave"
    ) -> Optional[float]:
        """Convert an NDBC field to float, mapping missing sentinels to None.

        Returns None for ``MM``, blank/None, and the field's all-9s numeric fill
        (see ``_SENTINELS``). ``field`` selects the sentinel magnitude so a real
        direction (e.g. 315°) or pressure (1013 hPa) is not mistaken for a fill.
        """
        if value is None:
            return None
        v = value.strip()
        if v in ("", "MM"):
            return None
        try:
            f = float(v)
        except ValueError:
            return None
        if f >= self._SENTINELS.get(field, 99.0):
            return None
        return f

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

        The STANDARD .txt feed (WVHT + DPD + MWD + APD) is the PRIMARY source of
        the height/period/direction triple handed to callers. The surf transform's
        calibration (scripts/hindcast_audit.py) was fit on these standard fields,
        so DPD (dominant period) is the correct period to pair with WVHT.

        The .spec spectral feed is used only as a FALLBACK when .txt is
        unavailable or stale. When used, the triple is derived from the DOMINANT
        component (whichever of swell/wind-wave has the larger height) rather than
        pairing total WVHT with the swell-component period SwP, which over-shoals
        a total height that is mostly short-period windswell.

        Args:
            station_id: NDBC station ID

        Returns:
            Dict with current conditions. `swell_period_s` (read by the ranker) is
            the dominant/DPD period; `dominant_period_s` is the same value under an
            honestly-named key.
        """
        # PRIMARY: standard .txt data. WVHT paired with DPD and MWD — the exact
        # triple the surf transform was calibrated against.
        try:
            df = self.get_standard_data(station_id)
            if not df.empty:
                latest = df.iloc[0]
                if self._is_stale(latest.get("time")):
                    logger.warning(
                        "Buoy %s standard observation is stale (>3h old): %s",
                        station_id, latest.get("time"),
                    )
                else:
                    wave_height_m = latest.get("wave_height_m")
                    period_s = latest.get("dominant_period_s")
                    return {
                        "time": latest.get("time"),
                        "wave_height_m": wave_height_m,
                        "wave_height_ft": wave_height_m * 3.28084 if wave_height_m is not None else None,
                        "swell_height_m": None,
                        "dominant_period_s": period_s,
                        "swell_period_s": period_s,  # alias: kept for ranker compatibility
                        "swell_direction": None,
                        "wind_wave_height_m": None,
                        "mean_direction_deg": latest.get("mean_wave_direction"),
                        "source": "standard",
                    }
        except BuoyError:
            pass

        # FALLBACK: spectral .spec data. Derive the triple from the DOMINANT
        # component so we never attach a minor long-period swell's period (SwP)
        # to the total WVHT.
        try:
            df = self.get_spectral_data(station_id)
            if not df.empty:
                latest = df.iloc[0]
                if self._is_stale(latest.get("time")):
                    logger.warning(
                        "Buoy %s spectral observation is stale (>3h old): %s",
                        station_id, latest.get("time"),
                    )
                else:
                    wave_height_m = latest.get("wave_height_m")
                    swell_h = latest.get("swell_height_m")
                    wind_h = latest.get("wind_wave_height_m")

                    # Choose the dominant component (larger height). If SwH >= WWH
                    # use swell period/direction, else wind-wave period/direction.
                    if swell_h is not None and (wind_h is None or swell_h >= wind_h):
                        period_s = latest.get("swell_period_s")
                        dir_compass = latest.get("swell_direction")
                    elif wind_h is not None:
                        period_s = latest.get("wind_wave_period_s")
                        dir_compass = latest.get("wind_wave_direction")
                    else:
                        # Neither component height available: fall back to
                        # average period and the buoy's mean wave direction.
                        period_s = latest.get("average_period_s")
                        dir_compass = None

                    # SwD/WWD are compass strings ("NW"); convert to degrees.
                    dir_deg = _compass_to_deg(dir_compass)
                    if dir_deg is None:
                        # Fall back to the spectral MWD (already in degrees).
                        dir_deg = latest.get("mean_wave_direction")

                    return {
                        "time": latest.get("time"),
                        "wave_height_m": wave_height_m,
                        "wave_height_ft": wave_height_m * 3.28084 if wave_height_m is not None else None,
                        "swell_height_m": swell_h,
                        "dominant_period_s": period_s,
                        "swell_period_s": period_s,  # alias: kept for ranker compatibility
                        "swell_direction": dir_compass,
                        "wind_wave_height_m": wind_h,
                        "mean_direction_deg": dir_deg,
                        "source": "spectral",
                    }
        except BuoyError:
            pass

        return {
            "time": None,
            "wave_height_m": None,
            "wave_height_ft": None,
            "swell_height_m": None,
            "dominant_period_s": None,
            "swell_period_s": None,
            "swell_direction": None,
            "wind_wave_height_m": None,
            "mean_direction_deg": None,
            "source": None,
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
