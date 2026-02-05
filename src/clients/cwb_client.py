"""Hawaii Department of Health Clean Water Branch advisory checker.

Monitors brown water advisories and beach closures that affect diving safety.
These advisories typically follow heavy rainfall and indicate poor water quality
and reduced visibility.
"""

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


# Hawaii DOH Clean Water Branch advisory page
CWB_ADVISORY_URL = "https://health.hawaii.gov/cwb/clean-water-branch-beach-advisories/"
CWB_API_URL = "https://eha-cloud.doh.hawaii.gov/cwb/api/advisories"

CACHE_TTL_SECONDS = 1800  # 30 minutes

# Keywords indicating water quality issues
ADVISORY_KEYWORDS = [
    "brown water",
    "sewage",
    "spill",
    "overflow",
    "bacteria",
    "enterococci",
    "closure",
    "advisory",
    "warning",
]

# Oahu beaches/areas for filtering
OAHU_LOCATIONS = [
    "oahu",
    "honolulu",
    "waikiki",
    "ala moana",
    "hanauma",
    "kailua",
    "kaneohe",
    "north shore",
    "waimea",
    "haleiwa",
    "waianae",
    "makaha",
    "ko olina",
    "diamond head",
    "hawaii kai",
    "waimanalo",
    "lanikai",
    "sandy beach",
    "makapuu",
]


class CWBClient:
    """Client for checking Hawaii DOH Clean Water Branch advisories."""

    def __init__(self, cache_path: Optional[Path] = None):
        """Initialize the CWB client.

        Args:
            cache_path: Path to SQLite cache file. Defaults to ~/.cache/oahu-dive/cwb.db
        """
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OahuDiveConditions/1.0 (dive-conditions-app)"
        })

        if cache_path is None:
            cache_dir = Path.home() / ".cache" / "oahu-dive"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / "cwb.db"

        self.cache_path = cache_path
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the SQLite cache table."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cwb_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _make_cache_key(self, identifier: str) -> str:
        """Generate a cache key."""
        return hashlib.sha256(identifier.encode()).hexdigest()[:32]

    def _get_cached(self, cache_key: str) -> Optional[list]:
        """Retrieve data from cache if valid."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT data, created_at FROM cwb_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            data_json, created_at_str = row
            created_at = datetime.fromisoformat(created_at_str)

            if datetime.utcnow() - created_at > timedelta(seconds=CACHE_TTL_SECONDS):
                conn.execute("DELETE FROM cwb_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None

            return json.loads(data_json)

    def _set_cached(self, cache_key: str, data: list) -> None:
        """Store data in cache."""
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cwb_cache (cache_key, data, created_at)
                VALUES (?, ?, ?)
                """,
                (cache_key, json.dumps(data), datetime.utcnow().isoformat()),
            )
            conn.commit()

    def _fetch_advisories_api(self) -> list[dict]:
        """Fetch advisories from DOH API if available."""
        try:
            response = self.session.get(CWB_API_URL, timeout=15)
            response.raise_for_status()
            data = response.json()

            advisories = []
            for item in data:
                advisory = {
                    "id": item.get("id"),
                    "beach": item.get("beach_name") or item.get("location"),
                    "island": item.get("island"),
                    "type": item.get("advisory_type") or item.get("type"),
                    "reason": item.get("reason") or item.get("description"),
                    "posted_date": item.get("posted_date") or item.get("start_date"),
                    "status": item.get("status", "active"),
                }
                advisories.append(advisory)

            return advisories
        except (requests.RequestException, json.JSONDecodeError):
            return []

    def _fetch_advisories_scrape(self) -> list[dict]:
        """Scrape advisories from DOH webpage as fallback."""
        try:
            response = self.session.get(CWB_ADVISORY_URL, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            advisories = []

            # Look for advisory content in common page structures
            content = soup.find("div", class_="entry-content") or soup.find("article") or soup.body

            if content:
                # Find tables with advisory data
                tables = content.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all(["td", "th"])
                        if len(cells) >= 2:
                            advisory = {
                                "beach": cells[0].get_text(strip=True) if len(cells) > 0 else None,
                                "island": cells[1].get_text(strip=True) if len(cells) > 1 else None,
                                "type": cells[2].get_text(strip=True) if len(cells) > 2 else "Advisory",
                                "reason": cells[3].get_text(strip=True) if len(cells) > 3 else None,
                                "posted_date": cells[4].get_text(strip=True) if len(cells) > 4 else None,
                                "status": "active",
                            }
                            if advisory["beach"]:
                                advisories.append(advisory)

                # Also look for list items that might contain advisories
                lists = content.find_all(["ul", "ol"])
                for lst in lists:
                    items = lst.find_all("li")
                    for item in items:
                        text = item.get_text(strip=True)
                        if any(kw in text.lower() for kw in ADVISORY_KEYWORDS):
                            advisories.append({
                                "beach": self._extract_location(text),
                                "island": "Oahu" if self._is_oahu_location(text) else None,
                                "type": self._extract_advisory_type(text),
                                "reason": text,
                                "posted_date": None,
                                "status": "active",
                            })

            return advisories
        except requests.RequestException:
            return []

    def _extract_location(self, text: str) -> Optional[str]:
        """Extract beach/location name from advisory text."""
        text_lower = text.lower()
        for location in OAHU_LOCATIONS:
            if location in text_lower:
                return location.title()
        return None

    def _extract_advisory_type(self, text: str) -> str:
        """Extract advisory type from text."""
        text_lower = text.lower()
        if "closure" in text_lower or "closed" in text_lower:
            return "Closure"
        elif "brown water" in text_lower:
            return "Brown Water Advisory"
        elif "sewage" in text_lower:
            return "Sewage Advisory"
        elif "bacteria" in text_lower or "enterococci" in text_lower:
            return "Bacteria Advisory"
        else:
            return "Advisory"

    def _is_oahu_location(self, text: str) -> bool:
        """Check if text mentions an Oahu location."""
        text_lower = text.lower()
        return any(loc in text_lower for loc in OAHU_LOCATIONS)

    def get_advisories(self, use_cache: bool = True) -> list[dict]:
        """Get all current beach advisories.

        Args:
            use_cache: Whether to use cached data

        Returns:
            List of advisory dictionaries
        """
        cache_key = self._make_cache_key("all_advisories")

        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        # Try API first, fall back to scraping
        advisories = self._fetch_advisories_api()
        if not advisories:
            advisories = self._fetch_advisories_scrape()

        if use_cache and advisories:
            self._set_cached(cache_key, advisories)

        return advisories

    def get_oahu_advisories(self, use_cache: bool = True) -> list[dict]:
        """Get current advisories for Oahu only.

        Args:
            use_cache: Whether to use cached data

        Returns:
            List of Oahu advisory dictionaries
        """
        all_advisories = self.get_advisories(use_cache=use_cache)

        oahu_advisories = []
        for advisory in all_advisories:
            island = (advisory.get("island") or "").lower()
            beach = (advisory.get("beach") or "").lower()
            reason = (advisory.get("reason") or "").lower()

            if island == "oahu" or self._is_oahu_location(beach) or self._is_oahu_location(reason):
                oahu_advisories.append(advisory)

        return oahu_advisories

    def check_site_advisory(self, site_name: str, use_cache: bool = True) -> Optional[dict]:
        """Check if a specific dive site has an active advisory.

        Args:
            site_name: Name of the dive site
            use_cache: Whether to use cached data

        Returns:
            Advisory dict if found, None otherwise
        """
        advisories = self.get_oahu_advisories(use_cache=use_cache)
        site_lower = site_name.lower()

        for advisory in advisories:
            beach = (advisory.get("beach") or "").lower()
            reason = (advisory.get("reason") or "").lower()

            # Check for direct match or nearby location
            # Require minimum 4 chars to avoid false positives (e.g. "kai" matching "waikiki")
            if len(site_lower) >= 4 and len(beach) >= 4:
                if site_lower == beach or site_lower in beach or beach in site_lower:
                    return advisory
            elif site_lower == beach:
                return advisory

            # Check if full site name appears in the reason/description
            if len(site_lower) >= 4 and site_lower in reason:
                return advisory

        return None

    def has_active_advisory(self, site_name: str) -> bool:
        """Quick check if a site has an active advisory.

        Args:
            site_name: Name of the dive site

        Returns:
            True if advisory exists
        """
        return self.check_site_advisory(site_name) is not None

    def get_brown_water_advisories(self) -> list[dict]:
        """Get only brown water advisories (post-rain runoff).

        Returns:
            List of brown water advisories
        """
        advisories = self.get_oahu_advisories()

        brown_water = []
        for advisory in advisories:
            advisory_type = (advisory.get("type") or "").lower()
            reason = (advisory.get("reason") or "").lower()

            if "brown water" in advisory_type or "brown water" in reason:
                brown_water.append(advisory)

        return brown_water

    def get_advisory_summary(self) -> dict:
        """Get a summary of current Oahu advisories.

        Returns:
            Dict with advisory counts and affected areas
        """
        advisories = self.get_oahu_advisories()

        summary = {
            "total_advisories": len(advisories),
            "closures": 0,
            "brown_water": 0,
            "bacteria": 0,
            "other": 0,
            "affected_beaches": [],
        }

        for advisory in advisories:
            advisory_type = (advisory.get("type") or "").lower()

            if "closure" in advisory_type:
                summary["closures"] += 1
            elif "brown water" in advisory_type:
                summary["brown_water"] += 1
            elif "bacteria" in advisory_type or "sewage" in advisory_type:
                summary["bacteria"] += 1
            else:
                summary["other"] += 1

            if advisory.get("beach"):
                summary["affected_beaches"].append(advisory["beach"])

        return summary


class CWBError(Exception):
    """Exception raised for CWB client errors."""

    pass
