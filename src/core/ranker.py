"""Site ranking engine.

Fetches live environmental data and scores/ranks dive sites.
This is the main orchestration layer that connects:
- Site database (site.py)
- API clients (clients/)
- Scoring algorithm (scorer.py)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.clients.buoy_client import BuoyClient, BuoyError
from src.clients.cwb_client import CWBClient, CWBError
from src.clients.noaa_tides_client import NOAATidesClient, NOAATidesError
from src.clients.nws_client import NWSClient, NWSError
from src.clients.pacioos_client import PacIOOSClient, PacIOOSError
from src.clients.usgs_client import USGSClient, USGSError
from src.core.scorer import DiveScorer, ScoringInput, ScoringResult
from src.core.site import DiveSite, SiteDatabase, get_site_database


logger = logging.getLogger(__name__)


@dataclass
class EnvironmentalConditions:
    """Environmental conditions for a location."""
    # Wave data
    wave_height_ft: Optional[float] = None
    wave_period_s: Optional[float] = None
    swell_direction_deg: Optional[float] = None
    wave_source: str = ""  # "buoy", "pacioos", "none"

    # Wind data
    wind_speed_mph: Optional[float] = None
    wind_direction_deg: Optional[float] = None

    # Tide data
    tide_phase: Optional[str] = None
    water_level_ft: Optional[float] = None
    next_high_tide: Optional[str] = None
    next_low_tide: Optional[str] = None

    # Visibility/water quality
    stream_discharge_cfs: Optional[float] = None
    brown_water_advisory: bool = False
    advisory_details: Optional[str] = None

    # Weather alerts
    high_surf_warning: bool = False
    high_surf_advisory: bool = False
    marine_alerts: list = field(default_factory=list)

    # Metadata
    fetch_time: Optional[datetime] = None
    errors: list = field(default_factory=list)


@dataclass
class RankedSite:
    """A site with its score and conditions."""
    site: DiveSite
    score: ScoringResult
    conditions: EnvironmentalConditions
    rank: int = 0

    @property
    def is_diveable(self) -> bool:
        return self.score.diveable

    @property
    def grade(self) -> str:
        return self.score.grade.value


class SiteRanker:
    """Ranks dive sites based on current conditions."""

    def __init__(
        self,
        site_db: Optional[SiteDatabase] = None,
        pacioos_client: Optional[PacIOOSClient] = None,
        buoy_client: Optional[BuoyClient] = None,
        nws_client: Optional[NWSClient] = None,
        tides_client: Optional[NOAATidesClient] = None,
        usgs_client: Optional[USGSClient] = None,
        cwb_client: Optional[CWBClient] = None,
    ):
        """Initialize the ranker with optional dependency injection.

        Args:
            site_db: Site database. Defaults to loading from config/sites.yaml.
            pacioos_client: PacIOOS wave model client.
            buoy_client: NDBC buoy client.
            nws_client: NWS weather client.
            tides_client: NOAA tides client.
            usgs_client: USGS stream discharge client.
            cwb_client: Clean Water Branch advisory client.
        """
        self.site_db = site_db or get_site_database()
        self.pacioos = pacioos_client or PacIOOSClient()
        self.buoy = buoy_client or BuoyClient()
        self.nws = nws_client or NWSClient()
        self.tides = tides_client or NOAATidesClient()
        self.usgs = usgs_client or USGSClient()
        self.cwb = cwb_client or CWBClient()
        self.scorer = DiveScorer()

        # Cache for shared data (alerts apply to all sites)
        self._marine_alerts_cache: Optional[list] = None
        self._advisories_cache: Optional[list] = None

    def fetch_conditions_for_site(self, site: DiveSite) -> EnvironmentalConditions:
        """Fetch all environmental conditions for a single site.

        Args:
            site: The dive site

        Returns:
            EnvironmentalConditions with all available data
        """
        conditions = EnvironmentalConditions(fetch_time=datetime.now())

        # Fetch wave data (try buoy first, then PacIOOS model)
        self._fetch_wave_data(site, conditions)

        # Fetch wind/weather from NWS
        self._fetch_weather_data(site, conditions)

        # Fetch tide data
        self._fetch_tide_data(site, conditions)

        # Fetch stream discharge for visibility proxy
        self._fetch_discharge_data(site, conditions)

        # Check for advisories and alerts
        self._fetch_alerts(site, conditions)

        return conditions

    def _fetch_wave_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch wave data from buoy or PacIOOS model."""
        # Try buoy first (real observations)
        if site.nearest_buoy:
            try:
                buoy_data = self.buoy.get_current_conditions(site.nearest_buoy)
                if buoy_data.get("wave_height_ft"):
                    conditions.wave_height_ft = buoy_data["wave_height_ft"]
                    conditions.wave_period_s = buoy_data.get("swell_period_s")
                    conditions.swell_direction_deg = buoy_data.get("mean_direction_deg")
                    conditions.wave_source = "buoy"
                    return
            except BuoyError as e:
                conditions.errors.append(f"Buoy error: {e}")
                logger.warning(f"Buoy fetch failed for {site.id}: {e}")

        # Fall back to PacIOOS SWAN model
        try:
            pacioos_data = self.pacioos.get_current_conditions(
                site.coordinates.lat,
                site.coordinates.lon,
            )
            if pacioos_data.get("wave_height_ft"):
                conditions.wave_height_ft = pacioos_data["wave_height_ft"]
                conditions.wave_period_s = pacioos_data.get("period_s")
                conditions.swell_direction_deg = pacioos_data.get("direction_deg")
                conditions.wave_source = "pacioos"
        except PacIOOSError as e:
            conditions.errors.append(f"PacIOOS error: {e}")
            logger.warning(f"PacIOOS fetch failed for {site.id}: {e}")

    def _fetch_weather_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch weather/wind data from NWS."""
        try:
            forecast = self.nws.get_forecast_summary(
                site.coordinates.lat,
                site.coordinates.lon,
            )
            conditions.wind_speed_mph = forecast.get("current_wind_mph")
            # Convert wind direction string to degrees if needed
            wind_dir = forecast.get("current_wind_dir")
            if wind_dir:
                conditions.wind_direction_deg = self._wind_dir_to_degrees(wind_dir)
        except NWSError as e:
            conditions.errors.append(f"NWS error: {e}")
            logger.warning(f"NWS fetch failed for {site.id}: {e}")

    def _fetch_tide_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch tide data from NOAA."""
        try:
            station_id = self.tides.get_station_for_coast(site.coast)
            tide_info = self.tides.get_current_tide_phase(station_id)

            conditions.tide_phase = tide_info.get("phase")
            conditions.water_level_ft = tide_info.get("current_level_ft")

            next_high = tide_info.get("next_high")
            next_low = tide_info.get("next_low")
            if next_high:
                conditions.next_high_tide = next_high.get("time")
            if next_low:
                conditions.next_low_tide = next_low.get("time")
        except NOAATidesError as e:
            conditions.errors.append(f"Tides error: {e}")
            logger.warning(f"Tides fetch failed for {site.id}: {e}")

    def _fetch_discharge_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch stream discharge data from USGS."""
        if not site.nearest_streamgage:
            return

        try:
            discharge = self.usgs.get_current_discharge(site.nearest_streamgage)
            conditions.stream_discharge_cfs = discharge
        except USGSError as e:
            conditions.errors.append(f"USGS error: {e}")
            logger.warning(f"USGS fetch failed for {site.id}: {e}")

    def _fetch_alerts(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch weather alerts and water quality advisories."""
        # Get marine alerts (cached since they apply to all of Hawaii)
        if self._marine_alerts_cache is None:
            try:
                self._marine_alerts_cache = self.nws.get_marine_alerts()
            except NWSError as e:
                self._marine_alerts_cache = []
                conditions.errors.append(f"Alerts error: {e}")

        conditions.marine_alerts = list(self._marine_alerts_cache)

        # Check for high surf warnings/advisories
        for alert in conditions.marine_alerts:
            event = (alert.get("event") or "").lower()
            if "high surf warning" in event:
                conditions.high_surf_warning = True
            elif "high surf advisory" in event:
                conditions.high_surf_advisory = True

        # Get water quality advisories (cached)
        if self._advisories_cache is None:
            try:
                self._advisories_cache = self.cwb.get_oahu_advisories()
            except CWBError as e:
                self._advisories_cache = []
                conditions.errors.append(f"CWB error: {e}")

        # Check if this site has an advisory
        advisory = self.cwb.check_site_advisory(site.name)
        if advisory:
            conditions.brown_water_advisory = True
            conditions.advisory_details = advisory.get("reason")

    def _wind_dir_to_degrees(self, direction: str) -> Optional[float]:
        """Convert wind direction string to degrees."""
        direction_map = {
            "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
            "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
            "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
            "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
        }
        return direction_map.get(direction.upper())

    def score_site(
        self,
        site: DiveSite,
        conditions: Optional[EnvironmentalConditions] = None,
    ) -> RankedSite:
        """Score a single site with its conditions.

        Args:
            site: The dive site
            conditions: Pre-fetched conditions, or None to fetch fresh

        Returns:
            RankedSite with score and conditions
        """
        if conditions is None:
            conditions = self.fetch_conditions_for_site(site)

        # Build scoring input from conditions and site data
        scoring_input = ScoringInput(
            wave_height_ft=conditions.wave_height_ft,
            wave_period_s=conditions.wave_period_s,
            swell_direction_deg=conditions.swell_direction_deg,
            wind_speed_mph=conditions.wind_speed_mph,
            wind_direction_deg=conditions.wind_direction_deg,
            stream_discharge_cfs=conditions.stream_discharge_cfs,
            brown_water_advisory=conditions.brown_water_advisory,
            tide_phase=conditions.tide_phase,
            water_level_ft=conditions.water_level_ft,
            evaluation_time=conditions.fetch_time,
            high_surf_warning=conditions.high_surf_warning,
            high_surf_advisory=conditions.high_surf_advisory,
            site_max_safe_height_ft=site.max_safe_wave_height,
            site_optimal_tide=site.optimal_tide,
            site_swell_exposure_primary=site.swell_exposure.primary,
        )

        score = self.scorer.calculate_score(scoring_input)

        return RankedSite(
            site=site,
            score=score,
            conditions=conditions,
        )

    def rank_sites(
        self,
        sites: Optional[list[DiveSite]] = None,
        in_season_only: bool = True,
        min_score: float = 0,
        top_n: Optional[int] = None,
    ) -> list[RankedSite]:
        """Rank multiple sites by current conditions.

        Args:
            sites: List of sites to rank. Defaults to all sites.
            in_season_only: Only include sites in season. Defaults to True.
            min_score: Minimum score to include. Defaults to 0.
            top_n: Return only top N sites. Defaults to all.

        Returns:
            List of RankedSite sorted by score (highest first)
        """
        if sites is None:
            if in_season_only:
                sites = self.site_db.get_in_season_sites()
            else:
                sites = self.site_db.get_all_sites()

        # Clear caches for fresh data
        self._marine_alerts_cache = None
        self._advisories_cache = None

        # Score all sites
        ranked_sites = []
        for site in sites:
            try:
                ranked = self.score_site(site)
                if ranked.score.total_score >= min_score:
                    ranked_sites.append(ranked)
            except Exception as e:
                logger.error(f"Failed to score site {site.id}: {e}")

        # Sort by score (highest first)
        ranked_sites.sort(key=lambda x: x.score.total_score, reverse=True)

        # Assign ranks
        for i, ranked in enumerate(ranked_sites, 1):
            ranked.rank = i

        # Limit to top N if requested
        if top_n:
            ranked_sites = ranked_sites[:top_n]

        return ranked_sites

    def rank_coast(
        self,
        coast: str,
        top_n: int = 5,
    ) -> list[RankedSite]:
        """Rank sites on a specific coast.

        Args:
            coast: Coast name (north_shore, west_side, south_shore, southeast, windward)
            top_n: Number of top sites to return

        Returns:
            Top ranked sites for the coast
        """
        sites = self.site_db.get_sites_by_coast(coast)
        return self.rank_sites(sites, in_season_only=False, top_n=top_n)

    def get_best_sites(
        self,
        count: int = 5,
        skill_level: Optional[str] = None,
        spearfishing: bool = False,
    ) -> list[RankedSite]:
        """Get the best dive sites for current conditions.

        Args:
            count: Number of sites to return
            skill_level: Maximum skill level (beginner, intermediate, advanced, expert)
            spearfishing: Only include spearfishing sites

        Returns:
            Top ranked diveable sites
        """
        sites = self.site_db.get_in_season_sites()

        if skill_level:
            sites = [s for s in sites if self._skill_level_ok(s.skill_level, skill_level)]

        if spearfishing:
            sites = [s for s in sites if s.allows_spearfishing()]

        ranked = self.rank_sites(sites, in_season_only=False)

        # Filter to diveable sites only
        diveable = [r for r in ranked if r.is_diveable]

        return diveable[:count]

    def _skill_level_ok(self, site_level: str, max_level: str) -> bool:
        """Check if site skill level is at or below max."""
        order = ["beginner", "intermediate", "advanced", "expert"]
        try:
            return order.index(site_level) <= order.index(max_level)
        except ValueError:
            return True


def get_top_sites(count: int = 5) -> list[RankedSite]:
    """Quick function to get top dive sites for today.

    Args:
        count: Number of sites to return

    Returns:
        Top ranked sites
    """
    ranker = SiteRanker()
    return ranker.get_best_sites(count)
