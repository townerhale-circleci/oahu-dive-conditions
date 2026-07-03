"""Iowa Environmental Mesonet (IEM) ASOS observed-precipitation client.

Provides OBSERVED trailing-48h rainfall (inches) per Oahu coast, sourced from
the IEM ASOS data-download service. This is the observed counterpart to the
OpenWeatherMap forecast-window rain: the scorer's RAINFALL_* visibility
thresholds are calibrated for 48h OBSERVED totals, so forecast-window rain was a
category error (see Plan 3).

Source / URL
------------
IEM ASOS JSON/CSV download service:
  https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py
Params used (verified live against the HI_ASOS network):
  station=<ICAO>&data=p01i&year1..day2=<window>&tz=Pacific/Honolulu
  &format=onlycomma&latlon=no&missing=empty&trace=0.0001&network=HI_ASOS

Station IDs
-----------
IMPORTANT: The HI_ASOS network keys stations by their FULL 4-letter ICAO id
(PHNL, PHNG, ...), NOT the 3-letter form (HNL, NGF, ...). A verified live fetch
with the 3-letter ids returns zero rows. So the map below uses ICAO ids.

p01i semantics
--------------
p01i is the running one-hour precip accumulation in inches, reported at each
observation (~every 5-20 min). Within a single clock hour the value REPEATS and
rises as rain accumulates (e.g. 02:00=0.01, 02:10=0.03, ... 02:53=0.03). To get
the true hourly total without double-counting we take the MAX p01i per clock
hour and sum those maxes. Trace precip is coded as 0.0001 (the trace= param) and
is treated as 0; empty/missing values are skipped.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

from src.utils.timezones import HST, now_hst


logger = logging.getLogger(__name__)


IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
IEM_NETWORK = "HI_ASOS"
CACHE_TTL_SECONDS = 1800  # 30 minutes, matching other clients

# Trailing window to sum (hours) and staleness bounds on the newest observation.
LOOKBACK_HOURS = 48
STALE_WARN_SECONDS = 6 * 3600    # warn if newest ob older than 6h (still return)
STALE_REJECT_SECONDS = 24 * 3600  # return None if newest ob older than 24h

# Per-coast ASOS station (full 4-letter ICAO ids; HI_ASOS network).
#   south_shore / southeast -> PHNL (Honolulu Intl)
#   windward                -> PHNG (Kaneohe MCAS)
#   west_side               -> PHJR (Kalaeloa / John Rodgers)
#   north_shore             -> PHDH (Dillingham) primary, PHHI (Wheeler) fallback
# NOTE (verified live): PHDH currently reports NO rows in HI_ASOS, so PHHI is
# effectively the working north_shore station. PHHI (Wheeler, central Oahu) is
# the nearest reporting station to the north shore. See report/HANDOFF.
COAST_STATIONS: dict[str, list[str]] = {
    "south_shore": ["PHNL"],
    "southeast": ["PHNL"],
    "windward": ["PHNG"],
    "west_side": ["PHJR"],
    "north_shore": ["PHDH", "PHHI"],
}

# Trace precip sentinel (matches the trace= query param).
TRACE_VALUE = 0.0001


def aggregate_48h_precip(rows: list[tuple[str, str]]) -> tuple[Optional[float], Optional[datetime]]:
    """Aggregate ASOS p01i rows into a total, deduplicating within each clock hour.

    Pure function so it is testable without network. p01i repeats within a clock
    hour (running hourly accumulation), so we take the MAX per (station-agnostic)
    clock hour and sum those maxes. Trace (0.0001) and empty values contribute 0.

    Args:
        rows: list of (valid_timestamp_str, p01i_str) as returned by IEM
            (onlycomma format, HST-local timestamps like "2024-12-06 02:10").
            Empty p01i is "".

    Returns:
        (total_inches, newest_obs_datetime) where total is the sum of per-hour
        maxes, or (None, None) if there are no usable rows. newest_obs_datetime
        is naive (HST local, as returned by IEM).
    """
    hourly_max: dict[str, float] = {}
    newest_obs: Optional[datetime] = None

    for valid, p01i in rows:
        if not valid:
            continue
        try:
            obs_time = datetime.strptime(valid.strip(), "%Y-%m-%d %H:%M")
        except (ValueError, AttributeError):
            continue

        if newest_obs is None or obs_time > newest_obs:
            newest_obs = obs_time

        value_str = (p01i or "").strip()
        if not value_str:
            continue
        try:
            value = float(value_str)
        except ValueError:
            continue
        if value < 0:
            continue
        # Trace precip -> treat as 0.
        if value <= TRACE_VALUE:
            value = 0.0

        hour_key = obs_time.strftime("%Y-%m-%d %H")
        prev = hourly_max.get(hour_key)
        if prev is None or value > prev:
            hourly_max[hour_key] = value

    if newest_obs is None:
        return None, None

    total = sum(hourly_max.values())
    return total, newest_obs


class IEMPrecipClient:
    """Client for observed trailing-48h rainfall per coast from IEM ASOS."""

    def __init__(self):
        """Initialize the client."""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OahuDiveConditions/1.0 (dive-conditions-app)"
        })
        # Simple in-memory TTL cache keyed by coast, like the OWM client.
        self._cache: dict[str, tuple[datetime, Optional[float]]] = {}
        self._cache_ttl = CACHE_TTL_SECONDS

    def _fetch_station_rows(self, station: str) -> list[tuple[str, str]]:
        """Fetch raw (valid, p01i) rows for a station over the trailing window.

        Returns an empty list on any failure or if the station has no rows.
        """
        end = now_hst()
        start = end - timedelta(hours=LOOKBACK_HOURS)
        params = {
            "station": station,
            "data": "p01i",
            "year1": start.year, "month1": start.month, "day1": start.day,
            "hour1": start.hour, "minute1": start.minute,
            "year2": end.year, "month2": end.month, "day2": end.day,
            "hour2": end.hour, "minute2": end.minute,
            "tz": "Pacific/Honolulu",
            "format": "onlycomma",
            "latlon": "no",
            "missing": "empty",
            "trace": str(TRACE_VALUE),
            "network": IEM_NETWORK,
        }

        try:
            response = self.session.get(IEM_ASOS_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning("IEM fetch failed for %s: %s", station, e)
            return []

        rows: list[tuple[str, str]] = []
        lines = response.text.splitlines()
        for line in lines:
            if not line or line.startswith("#") or line.startswith("station,"):
                continue
            parts = line.split(",")
            # station,valid,p01i
            if len(parts) < 3:
                continue
            rows.append((parts[1], parts[2]))
        return rows

    def get_rainfall_48h(self, coast: str) -> Optional[float]:
        """Get observed total rainfall (inches) over the trailing 48h for a coast.

        Args:
            coast: One of south_shore, southeast, windward, west_side, north_shore

        Returns:
            Total inches over the trailing 48h ending now (HST), or None on any
            failure (network, unknown coast, no rows, all-missing, or the newest
            observation older than 24h).
        """
        stations = COAST_STATIONS.get(coast)
        if not stations:
            logger.warning("IEM: no station mapping for coast %r", coast)
            return None

        # Cache check.
        cached = self._cache.get(coast)
        if cached is not None:
            cached_time, cached_val = cached
            if now_hst() - cached_time < timedelta(seconds=self._cache_ttl):
                return cached_val

        result: Optional[float] = None
        for station in stations:
            rows = self._fetch_station_rows(station)
            if not rows:
                continue

            total, newest_obs = aggregate_48h_precip(rows)
            if total is None or newest_obs is None:
                continue

            # Staleness check on the newest observation. IEM timestamps are naive
            # HST-local; compare against now in HST (naive) to match.
            now_naive = now_hst().replace(tzinfo=None)
            age = now_naive - newest_obs
            if age > timedelta(seconds=STALE_REJECT_SECONDS):
                logger.warning(
                    "IEM %s (%s): newest ob is stale (>24h old: %s) - returning None",
                    coast, station, newest_obs,
                )
                continue
            if age > timedelta(seconds=STALE_WARN_SECONDS):
                logger.warning(
                    "IEM %s (%s): newest ob is %s old (>6h); precip gaps common, "
                    "returning sum anyway",
                    coast, station, age,
                )

            result = total
            break

        # Cache the result (including None) to avoid hammering on failures.
        self._cache[coast] = (now_hst(), result)
        return result


class IEMError(Exception):
    """Exception raised for IEM client errors."""

    pass
