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
from src.clients.iem_precip_client import IEMPrecipClient
from src.clients.noaa_tides_client import NOAATidesClient, NOAATidesError
from src.clients.nws_client import NWSClient, NWSError
from src.clients.openweathermap_client import OpenWeatherMapClient
from src.clients.pacioos_client import PacIOOSClient, PacIOOSError
from src.clients.usgs_client import USGSClient, USGSError
from src.core.scorer import DiveScorer, ScoringResult
from src.core.scoring_input import build_scoring_input
from src.core.site import DiveSite, SiteDatabase, get_site_database
from src.core.surf_transform import effective_surf_height
from src.utils.timezones import dive_window_time, now_hst


logger = logging.getLogger(__name__)


@dataclass
class EnvironmentalConditions:
    """Environmental conditions for a location."""
    # Wave data. wave_height_ft is the EFFECTIVE surf height at the site (after
    # the directional-exposure + shoaling transform for buoy sources); the raw
    # offshore Hs is retained separately for the absolute backstop gate.
    wave_height_ft: Optional[float] = None
    raw_wave_height_ft: Optional[float] = None
    wave_transform_applied: bool = False
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
    rainfall_48h_inches: Optional[float] = None  # OBSERVED trailing 48h (IEM ASOS)
    rain_chance_pct: Optional[float] = None  # OWM forecast PoP for the dive window (soft penalty)
    brown_water_advisory: bool = False  # advisory NAME-matched to this site (gates)
    coast_brown_water: bool = False  # advisory on this site's coast (soft cap)
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
        iem_client: Optional[IEMPrecipClient] = None,
        owm_client: Optional[OpenWeatherMapClient] = None,
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
        self.iem = iem_client or IEMPrecipClient()
        # OWM provides the representative dive-window wind (day-average over the
        # daylight window) used for SCORING — the headline path must match the
        # Today table, which also uses OWM. NWS is the fallback (dawn snapshot)
        # when no OWM key is configured. Shared client so a site fetched here
        # isn't re-fetched by the digest (OWM caches per rounded lat/lon).
        self.owm = owm_client or OpenWeatherMapClient()
        self.scorer = DiveScorer()

        # Cache for shared data (alerts apply to all sites)
        self._marine_alerts_cache: Optional[list] = None
        self._advisories_cache: Optional[list] = None
        # Coast-level advisory map (beach->coast) and observed 48h rainfall, both
        # fetched once per coast rather than per site.
        self._coast_advisories_cache: Optional[dict] = None
        self._coast_rainfall_cache: dict[str, Optional[float]] = {}

    def fetch_conditions_for_site(self, site: DiveSite) -> EnvironmentalConditions:
        """Fetch all environmental conditions for a single site.

        Args:
            site: The dive site

        Returns:
            EnvironmentalConditions with all available data
        """
        conditions = EnvironmentalConditions(fetch_time=now_hst())

        # Fetch wave data (try buoy first, then PacIOOS model)
        self._fetch_wave_data(site, conditions)

        # Fetch wind/weather from NWS
        self._fetch_weather_data(site, conditions)

        # Fetch tide data
        self._fetch_tide_data(site, conditions)

        # Fetch stream discharge for visibility proxy
        self._fetch_discharge_data(site, conditions)

        # Fetch observed 48h rainfall for the site's coast (visibility proxy)
        self._fetch_rain_data(site, conditions)

        # Check for advisories and alerts
        self._fetch_alerts(site, conditions)

        return conditions

    def _get_coast_rainfall(self, coast: str) -> Optional[float]:
        """Observed trailing-48h rainfall (inches) for a coast, cached per coast."""
        if coast in self._coast_rainfall_cache:
            return self._coast_rainfall_cache[coast]
        try:
            value = self.iem.get_rainfall_48h(coast)
        except Exception as e:  # noqa: BLE001 - failure -> None (unchanged behavior)
            logger.warning("IEM rainfall fetch failed for coast %s: %s", coast, e)
            value = None
        self._coast_rainfall_cache[coast] = value
        return value

    def _fetch_rain_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch observed 48h rainfall for the site's coast (once per coast)."""
        conditions.rainfall_48h_inches = self._get_coast_rainfall(site.coast)

    def _apply_buoy_transform(
        self, site: DiveSite, conditions: EnvironmentalConditions, buoy_data: dict
    ) -> None:
        """Store buoy wave data with the directional-exposure + shoaling transform.

        wave_height_ft becomes the EFFECTIVE surf height at the site; the raw
        offshore Hs is kept in raw_wave_height_ft for the absolute backstop gate.
        """
        raw = buoy_data["wave_height_ft"]
        period = buoy_data.get("swell_period_s")
        mwd = buoy_data.get("mean_direction_deg")

        effective, applied = effective_surf_height(
            raw_hs_ft=raw,
            period_s=period,
            swell_dir_deg=mwd,
            primary_exposure=site.swell_exposure.primary,
            secondary_exposure=site.swell_exposure.secondary,
        )

        conditions.wave_height_ft = effective
        conditions.raw_wave_height_ft = raw
        conditions.wave_transform_applied = applied
        conditions.wave_period_s = period
        conditions.swell_direction_deg = mwd
        conditions.wave_source = "buoy"

    def _fetch_wave_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch wave data from buoy or PacIOOS model.

        Buoy readings are raw offshore Hs and are passed through the directional
        exposure + shoaling transform (surf_transform.py) to yield the effective
        surf height at the site. PacIOOS SWAN values are already near-shore and
        are stored as-is (factor 1.0).
        """
        # Try buoy first (real observations). On a buoy error, fall back to the
        # site's fallback_buoy (used where nearest_buoy is offline, e.g. 51207)
        # before dropping to PacIOOS.
        buoy_candidates = [b for b in (site.nearest_buoy, site.fallback_buoy) if b]
        for buoy_id in buoy_candidates:
            try:
                buoy_data = self.buoy.get_current_conditions(buoy_id)
                if buoy_data.get("wave_height_ft") is not None:
                    self._apply_buoy_transform(site, conditions, buoy_data)
                    return
            except BuoyError as e:
                conditions.errors.append(f"Buoy error ({buoy_id}): {e}")
                logger.warning(f"Buoy fetch failed for {site.id} ({buoy_id}): {e}")

        # Fall back to PacIOOS SWAN model (near-shore; no transform, factor 1.0).
        try:
            pacioos_data = self.pacioos.get_current_conditions(
                site.coordinates.lat,
                site.coordinates.lon,
            )
            if pacioos_data.get("wave_height_ft") is not None:
                conditions.wave_height_ft = pacioos_data["wave_height_ft"]
                conditions.raw_wave_height_ft = pacioos_data["wave_height_ft"]
                conditions.wave_transform_applied = False
                conditions.wave_period_s = pacioos_data.get("period_s")
                conditions.swell_direction_deg = pacioos_data.get("direction_deg")
                conditions.wave_source = "pacioos"
        except PacIOOSError as e:
            conditions.errors.append(f"PacIOOS error: {e}")
            logger.warning(f"PacIOOS fetch failed for {site.id}: {e}")

    def _fetch_weather_data(self, site: DiveSite, conditions: EnvironmentalConditions) -> None:
        """Fetch representative dive-window wind for the site.

        SCORING wind is the OWM day-average wind over the daylight window (the
        same value the Today digest table uses), NOT the NWS dawn snapshot which
        only reflects the wind at report-generation time (often calm at 5 AM) and
        made the headline "Top Sites" grade disagree with the Today table.

        NWS is the fallback when OWM returns nothing (e.g. no API key). We also
        capture the OWM window rain-chance so the digest can reuse it without a
        second fetch (the OWM client caches per rounded lat/lon regardless).
        """
        used_owm = False
        try:
            wind_data = self.owm.get_wind_forecast(
                site.coordinates.lat,
                site.coordinates.lon,
                # Score today's dive window; dive_window_time() is HST-pinned.
                dive_window_time(),
            )
            if wind_data:
                owm_wind = wind_data.get("wind_speed_mph")  # day-average, may be None
                if owm_wind is not None:
                    conditions.wind_speed_mph = owm_wind
                    used_owm = True
                    wind_dir_deg = wind_data.get("wind_direction_deg")
                    if wind_dir_deg is not None:
                        conditions.wind_direction_deg = wind_dir_deg
                rain_chance = wind_data.get("rain_chance")
                if rain_chance is not None:
                    conditions.rain_chance_pct = float(rain_chance)
        except Exception as e:  # noqa: BLE001 - any OWM failure -> NWS fallback
            logger.debug(f"OWM wind fetch failed for {site.id}: {e}")

        if used_owm:
            return

        # Fallback: NWS forecast summary (dawn snapshot).
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

        # Coast-level advisory map (fetched once, shared across sites)
        if self._coast_advisories_cache is None:
            try:
                self._coast_advisories_cache = self.cwb.get_coast_advisories("Oahu")
            except CWBError as e:
                self._coast_advisories_cache = {}
                conditions.errors.append(f"CWB coast error: {e}")

        # Check if this site has a NAME-matched advisory (safety gate)
        advisory = self.cwb.check_site_advisory(site.name)
        if advisory:
            conditions.brown_water_advisory = True
            conditions.advisory_details = advisory.get("reason")

        # Coast-matched advisory (soft cap, not a gate). Only flag when the site
        # is not already name-matched (which gates anyway).
        if not conditions.brown_water_advisory:
            coast_advisories = (self._coast_advisories_cache or {}).get(site.coast)
            if coast_advisories:
                conditions.coast_brown_water = True
                if not conditions.advisory_details:
                    conditions.advisory_details = coast_advisories[0].get("reason")

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

        # Build scoring input from conditions and site data via the single
        # shared assembler (Plan 5) so the headline path matches the digest.
        scoring_input = build_scoring_input(
            site,
            wave_height_ft=conditions.wave_height_ft,
            raw_wave_height_ft=conditions.raw_wave_height_ft,
            wave_period_s=conditions.wave_period_s,
            swell_direction_deg=conditions.swell_direction_deg,
            wind_speed_mph=conditions.wind_speed_mph,
            wind_direction_deg=conditions.wind_direction_deg,
            stream_discharge_cfs=conditions.stream_discharge_cfs,
            rainfall_48h_inches=conditions.rainfall_48h_inches,
            rain_chance_pct=conditions.rain_chance_pct,
            brown_water_advisory=conditions.brown_water_advisory,
            coast_brown_water=conditions.coast_brown_water,
            tide_phase=conditions.tide_phase,
            water_level_ft=conditions.water_level_ft,
            # Score the dive window (07:00 HST today), not wall-clock/fetch time.
            evaluation_time=dive_window_time(),
            high_surf_warning=conditions.high_surf_warning,
            high_surf_advisory=conditions.high_surf_advisory,
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
        self._coast_advisories_cache = None
        self._coast_rainfall_cache = {}

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
