"""Site model and database loader.

Loads dive site definitions from sites.yaml and provides a clean interface
for accessing site properties and filtering sites by various criteria.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class Coordinates:
    """Geographic coordinates."""
    lat: float
    lon: float


@dataclass
class DepthRange:
    """Depth range in feet."""
    min_ft: int
    max_ft: int


@dataclass
class SeasonalWindow:
    """Seasonal diving window."""
    start_month: int  # 1-12
    end_month: int    # 1-12

    def is_in_season(self, month: Optional[int] = None) -> bool:
        """Check if a month is within the seasonal window.

        Args:
            month: Month to check (1-12). Defaults to current month.

        Returns:
            True if in season
        """
        if month is None:
            month = datetime.now().month

        if self.start_month <= self.end_month:
            # Normal range (e.g., May-September)
            return self.start_month <= month <= self.end_month
        else:
            # Wrapping range (e.g., October-April)
            return month >= self.start_month or month <= self.end_month


@dataclass
class Regulations:
    """Site regulations and restrictions."""
    mlcd: bool = False  # Marine Life Conservation District
    spearfishing: str = "allowed"  # allowed, prohibited
    night_diving: str = "allowed"  # allowed, prohibited
    take_rules: str = "Standard state regulations"


@dataclass
class SwellExposure:
    """Site's swell exposure characteristics."""
    primary: str  # Primary swell direction (N, NW, S, etc.)
    secondary: Optional[str] = None
    max_safe_height_ft: float = 6.0


@dataclass
class DiveSite:
    """Complete dive site model."""
    name: str
    id: str
    coast: str  # north_shore, west_side, south_shore, southeast, windward
    coordinates: Coordinates
    depth_range: DepthRange
    seasonal_window: SeasonalWindow
    skill_level: str  # beginner, intermediate, advanced, expert
    regulations: Regulations
    swell_exposure: SwellExposure
    optimal_tide: str  # any, high, low
    optimal_time: str  # morning, any
    nearest_buoy: Optional[str] = None
    nearest_streamgage: Optional[str] = None
    notes: str = ""

    def is_in_season(self, month: Optional[int] = None) -> bool:
        """Check if site is currently in season."""
        return self.seasonal_window.is_in_season(month)

    def allows_spearfishing(self) -> bool:
        """Check if spearfishing is allowed."""
        return self.regulations.spearfishing == "allowed"

    def allows_night_diving(self) -> bool:
        """Check if night diving is allowed."""
        return self.regulations.night_diving == "allowed"

    def is_mlcd(self) -> bool:
        """Check if site is a Marine Life Conservation District."""
        return self.regulations.mlcd

    @property
    def max_safe_wave_height(self) -> float:
        """Get maximum safe wave height for this site."""
        return self.swell_exposure.max_safe_height_ft


class SiteDatabase:
    """Database of dive sites loaded from YAML."""

    def __init__(self, sites_path: Optional[Path] = None):
        """Initialize the site database.

        Args:
            sites_path: Path to sites.yaml. Defaults to config/sites.yaml.
        """
        if sites_path is None:
            # Find config relative to this file or cwd
            possible_paths = [
                Path(__file__).parent.parent.parent / "config" / "sites.yaml",
                Path.cwd() / "config" / "sites.yaml",
            ]
            for path in possible_paths:
                if path.exists():
                    sites_path = path
                    break

        if sites_path is None or not sites_path.exists():
            raise FileNotFoundError(f"Could not find sites.yaml")

        self.sites_path = sites_path
        self._sites: dict[str, DiveSite] = {}
        self._coasts: dict[str, list[DiveSite]] = {}
        self._buoys: dict[str, str] = {}
        self._streamgages: dict[str, str] = {}
        self._load_sites()

    def _load_sites(self) -> None:
        """Load sites from YAML file."""
        with open(self.sites_path) as f:
            data = yaml.safe_load(f)

        # Load reference data
        self._buoys = data.get("buoys", {})
        self._streamgages = data.get("streamgages", {})

        # Load sites from each coast
        coast_keys = ["north_shore", "west_side", "south_shore", "southeast", "windward"]

        for coast in coast_keys:
            coast_data = data.get(coast, {})
            sites_list = coast_data.get("sites", [])
            self._coasts[coast] = []

            for site_data in sites_list:
                site = self._parse_site(site_data, coast)
                self._sites[site.id] = site
                self._coasts[coast].append(site)

    def _parse_site(self, data: dict, coast: str) -> DiveSite:
        """Parse a site dictionary into a DiveSite object."""
        coords = data.get("coordinates", {})
        depth = data.get("depth_range", {})
        season = data.get("seasonal_window", {})
        regs = data.get("regulations", {})
        swell = data.get("swell_exposure", {})

        return DiveSite(
            name=data.get("name", "Unknown"),
            id=data.get("id", "unknown"),
            coast=coast,
            coordinates=Coordinates(
                lat=coords.get("lat", 0),
                lon=coords.get("lon", 0),
            ),
            depth_range=DepthRange(
                min_ft=depth.get("min_ft", 0),
                max_ft=depth.get("max_ft", 0),
            ),
            seasonal_window=SeasonalWindow(
                start_month=season.get("start_month", 1),
                end_month=season.get("end_month", 12),
            ),
            skill_level=data.get("skill_level", "intermediate"),
            regulations=Regulations(
                mlcd=regs.get("mlcd", False),
                spearfishing=regs.get("spearfishing", "allowed"),
                night_diving=regs.get("night_diving", "allowed"),
                take_rules=regs.get("take_rules", "Standard state regulations"),
            ),
            swell_exposure=SwellExposure(
                primary=swell.get("primary", "N"),
                secondary=swell.get("secondary"),
                max_safe_height_ft=swell.get("max_safe_height_ft", 6.0),
            ),
            optimal_tide=data.get("optimal_tide", "any"),
            optimal_time=data.get("optimal_time", "morning"),
            nearest_buoy=data.get("nearest_buoy"),
            nearest_streamgage=data.get("nearest_streamgage"),
            notes=data.get("notes", ""),
        )

    def get_site(self, site_id: str) -> Optional[DiveSite]:
        """Get a site by ID.

        Args:
            site_id: Site identifier (e.g., "sharks_cove")

        Returns:
            DiveSite or None if not found
        """
        return self._sites.get(site_id)

    def get_site_by_name(self, name: str) -> Optional[DiveSite]:
        """Get a site by name (case-insensitive partial match).

        Args:
            name: Site name or partial name

        Returns:
            DiveSite or None if not found
        """
        name_lower = name.lower()
        for site in self._sites.values():
            if name_lower in site.name.lower():
                return site
        return None

    def get_all_sites(self) -> list[DiveSite]:
        """Get all sites."""
        return list(self._sites.values())

    def get_sites_by_coast(self, coast: str) -> list[DiveSite]:
        """Get all sites for a specific coast.

        Args:
            coast: Coast name (north_shore, west_side, south_shore, southeast, windward)

        Returns:
            List of sites
        """
        return self._coasts.get(coast.lower(), [])

    def get_in_season_sites(self, month: Optional[int] = None) -> list[DiveSite]:
        """Get all sites that are in season.

        Args:
            month: Month to check (1-12). Defaults to current month.

        Returns:
            List of in-season sites
        """
        return [site for site in self._sites.values() if site.is_in_season(month)]

    def get_sites_by_skill(self, max_skill: str) -> list[DiveSite]:
        """Get sites suitable for a given skill level.

        Args:
            max_skill: Maximum skill level (beginner, intermediate, advanced, expert)

        Returns:
            List of suitable sites
        """
        skill_order = ["beginner", "intermediate", "advanced", "expert"]

        try:
            max_idx = skill_order.index(max_skill.lower())
        except ValueError:
            max_idx = len(skill_order) - 1

        return [
            site for site in self._sites.values()
            if skill_order.index(site.skill_level) <= max_idx
        ]

    def get_spearfishing_sites(self) -> list[DiveSite]:
        """Get all sites where spearfishing is allowed."""
        return [site for site in self._sites.values() if site.allows_spearfishing()]

    def get_sites_by_buoy(self, buoy_id: str) -> list[DiveSite]:
        """Get all sites that use a specific buoy.

        Args:
            buoy_id: NDBC buoy ID (e.g., "51201")

        Returns:
            List of sites
        """
        return [
            site for site in self._sites.values()
            if site.nearest_buoy == buoy_id
        ]

    def get_buoy_id(self, buoy_name: str) -> Optional[str]:
        """Get buoy ID by name.

        Args:
            buoy_name: Buoy name (e.g., "waimea")

        Returns:
            Buoy ID or None
        """
        return self._buoys.get(buoy_name.lower())

    def get_streamgage_id(self, gage_name: str) -> Optional[str]:
        """Get streamgage ID by name.

        Args:
            gage_name: Gage name (e.g., "kaneohe")

        Returns:
            Gage ID or None
        """
        return self._streamgages.get(gage_name.lower())

    @property
    def site_count(self) -> int:
        """Get total number of sites."""
        return len(self._sites)

    @property
    def coasts(self) -> list[str]:
        """Get list of coast names."""
        return list(self._coasts.keys())

    def filter_sites(
        self,
        coast: Optional[str] = None,
        in_season: bool = False,
        skill_level: Optional[str] = None,
        spearfishing_only: bool = False,
        night_diving_only: bool = False,
        buoy_id: Optional[str] = None,
    ) -> list[DiveSite]:
        """Filter sites by multiple criteria.

        Args:
            coast: Filter by coast
            in_season: Only include in-season sites
            skill_level: Maximum skill level
            spearfishing_only: Only sites allowing spearfishing
            night_diving_only: Only sites allowing night diving
            buoy_id: Only sites using this buoy

        Returns:
            Filtered list of sites
        """
        sites = self.get_all_sites()

        if coast:
            sites = [s for s in sites if s.coast == coast.lower()]

        if in_season:
            sites = [s for s in sites if s.is_in_season()]

        if skill_level:
            skill_order = ["beginner", "intermediate", "advanced", "expert"]
            try:
                max_idx = skill_order.index(skill_level.lower())
                sites = [s for s in sites if skill_order.index(s.skill_level) <= max_idx]
            except ValueError:
                pass

        if spearfishing_only:
            sites = [s for s in sites if s.allows_spearfishing()]

        if night_diving_only:
            sites = [s for s in sites if s.allows_night_diving()]

        if buoy_id:
            sites = [s for s in sites if s.nearest_buoy == buoy_id]

        return sites


# Convenience function for quick access
_default_db: Optional[SiteDatabase] = None


def get_site_database() -> SiteDatabase:
    """Get the default site database (singleton)."""
    global _default_db
    if _default_db is None:
        _default_db = SiteDatabase()
    return _default_db


def get_site(site_id: str) -> Optional[DiveSite]:
    """Quick access to get a site by ID."""
    return get_site_database().get_site(site_id)
