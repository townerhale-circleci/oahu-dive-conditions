"""Daily dive conditions digest generator.

Fetches current conditions for all sites and generates a structured
report suitable for formatting as SMS, email, or other outputs.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.core.ranker import RankedSite, SiteRanker
from src.core.site import SiteDatabase, get_site_database
from src.clients.buoy_client import BuoyClient, OAHU_BUOYS
from src.clients.nws_client import NWSClient
from src.clients.pacioos_client import PacIOOSClient

logger = logging.getLogger(__name__)


@dataclass
class TideInfo:
    """Tide information for the digest."""
    next_high_time: Optional[str] = None
    next_high_ft: Optional[float] = None
    next_low_time: Optional[str] = None
    next_low_ft: Optional[float] = None


@dataclass
class AlertInfo:
    """Weather/marine alert information."""
    type: str  # "high_surf_warning", "high_surf_advisory", "small_craft_advisory"
    headline: str
    affected_areas: list[str] = field(default_factory=list)


@dataclass
class CoastSummary:
    """Summary for a single coast."""
    coast: str
    display_name: str
    top_sites: list[RankedSite]
    average_wave_height: Optional[float] = None
    diveable_count: int = 0
    total_count: int = 0

    @property
    def has_diveable_sites(self) -> bool:
        return self.diveable_count > 0


@dataclass
class APIStatus:
    """Status of a data source API."""
    name: str
    display_name: str
    success_count: int = 0
    failure_count: int = 0
    last_error: Optional[str] = None

    @property
    def total_calls(self) -> int:
        return self.success_count + self.failure_count

    @property
    def success_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return (self.success_count / self.total_calls) * 100


@dataclass
class ForecastDay:
    """Forecast for a single day."""
    date: datetime
    day_name: str  # "Today", "Tomorrow", "Wednesday", etc.

    # Wave conditions (aggregated across coasts)
    wave_height_min_ft: Optional[float] = None
    wave_height_max_ft: Optional[float] = None
    dominant_swell_direction: Optional[str] = None

    # Wind conditions
    wind_speed_min_mph: Optional[float] = None
    wind_speed_max_mph: Optional[float] = None
    wind_direction: Optional[str] = None

    # Weather
    conditions: Optional[str] = None  # "Sunny", "Partly Cloudy", etc.
    rain_chance: Optional[int] = None  # percentage

    # Dive outlook
    outlook: str = "Unknown"  # "Good", "Fair", "Poor", "Unsafe"
    outlook_reason: Optional[str] = None
    best_coast: Optional[str] = None

    # Per-coast breakdown
    coast_outlooks: dict = field(default_factory=dict)  # coast -> outlook


@dataclass
class DailyDigest:
    """Complete daily dive conditions digest."""
    generated_at: datetime

    # Overall summary
    total_sites: int = 0
    diveable_sites: int = 0
    best_coast: Optional[str] = None

    # Top sites across all coasts
    top_sites: list[RankedSite] = field(default_factory=list)

    # Per-coast breakdown
    coast_summaries: list[CoastSummary] = field(default_factory=list)

    # Tide information
    tide_info: Optional[TideInfo] = None

    # Active alerts
    alerts: list[AlertInfo] = field(default_factory=list)

    # Conditions summary
    wave_range: tuple[float, float] = (0, 0)  # min, max across sites
    wind_range: tuple[float, float] = (0, 0)

    # API status tracking
    api_statuses: list[APIStatus] = field(default_factory=list)

    # Multi-day forecast
    forecast_days: list[ForecastDay] = field(default_factory=list)

    # Errors during generation
    errors: list[str] = field(default_factory=list)

    @property
    def has_diveable_sites(self) -> bool:
        return self.diveable_sites > 0

    @property
    def is_flat_day(self) -> bool:
        """Check if it's a flat/calm day (small waves everywhere)."""
        return self.wave_range[1] < 2.0

    @property
    def is_big_day(self) -> bool:
        """Check if it's a big wave day."""
        return self.wave_range[1] > 6.0


class DigestGenerator:
    """Generates daily dive condition digests."""

    COAST_DISPLAY_NAMES = {
        "north_shore": "North Shore",
        "west_side": "West Side",
        "south_shore": "South Shore",
        "southeast": "Southeast",
        "windward": "Windward",
    }

    def __init__(
        self,
        site_db: Optional[SiteDatabase] = None,
        ranker: Optional[SiteRanker] = None,
        top_sites_count: int = 5,
    ):
        """Initialize the digest generator.

        Args:
            site_db: Site database. Defaults to loading from config.
            ranker: Site ranker. Defaults to creating new instance.
            top_sites_count: Number of top sites to include.
        """
        self.site_db = site_db or get_site_database()
        self.ranker = ranker or SiteRanker(site_db=self.site_db)
        self.top_sites_count = top_sites_count

    def generate(
        self,
        in_season_only: bool = True,
        include_coast_breakdown: bool = True,
    ) -> DailyDigest:
        """Generate a daily digest of dive conditions.

        Args:
            in_season_only: Only include sites currently in season.
            include_coast_breakdown: Include per-coast summaries.

        Returns:
            DailyDigest with all condition information.
        """
        digest = DailyDigest(generated_at=datetime.now())

        try:
            # Get all ranked sites
            all_ranked = self._rank_all_sites(in_season_only)

            if not all_ranked:
                digest.errors.append("No sites could be ranked")
                return digest

            # Populate overall stats
            digest.total_sites = len(all_ranked)
            digest.diveable_sites = sum(1 for r in all_ranked if r.is_diveable)
            digest.top_sites = all_ranked[:self.top_sites_count]

            # Calculate wave/wind ranges
            digest.wave_range = self._calculate_wave_range(all_ranked)
            digest.wind_range = self._calculate_wind_range(all_ranked)

            # Extract alerts from conditions
            digest.alerts = self._extract_alerts(all_ranked)

            # Extract tide info from first site with data
            digest.tide_info = self._extract_tide_info(all_ranked)

            # Generate coast summaries
            if include_coast_breakdown:
                digest.coast_summaries = self._generate_coast_summaries(all_ranked)

                # Find best coast
                best = max(
                    digest.coast_summaries,
                    key=lambda c: c.diveable_count,
                    default=None,
                )
                if best and best.diveable_count > 0:
                    digest.best_coast = best.display_name

            # Track API statuses
            digest.api_statuses = self._collect_api_statuses(all_ranked)

            # Generate 7-day forecast
            digest.forecast_days = self._generate_forecast(days=7)

        except Exception as e:
            logger.error(f"Error generating digest: {e}")
            digest.errors.append(str(e))

        return digest

    def _rank_all_sites(self, in_season_only: bool) -> list[RankedSite]:
        """Rank all sites and return sorted list."""
        return self.ranker.rank_sites(
            in_season_only=in_season_only,
            min_score=0,  # Include all sites
        )

    def _calculate_wave_range(
        self, ranked_sites: list[RankedSite]
    ) -> tuple[float, float]:
        """Calculate min/max wave heights across sites."""
        heights = [
            r.conditions.wave_height_ft
            for r in ranked_sites
            if r.conditions.wave_height_ft is not None
        ]
        if not heights:
            return (0, 0)
        return (min(heights), max(heights))

    def _calculate_wind_range(
        self, ranked_sites: list[RankedSite]
    ) -> tuple[float, float]:
        """Calculate min/max wind speeds across sites."""
        speeds = [
            r.conditions.wind_speed_mph
            for r in ranked_sites
            if r.conditions.wind_speed_mph is not None
        ]
        if not speeds:
            return (0, 0)
        return (min(speeds), max(speeds))

    def _extract_alerts(self, ranked_sites: list[RankedSite]) -> list[AlertInfo]:
        """Extract unique alerts from site conditions."""
        alerts = []
        seen_events = set()

        for ranked in ranked_sites:
            for alert in ranked.conditions.marine_alerts:
                event = alert.get("event", "")
                if event and event not in seen_events:
                    seen_events.add(event)
                    alerts.append(AlertInfo(
                        type=self._classify_alert(event),
                        headline=alert.get("headline", event),
                        affected_areas=alert.get("areaDesc", "").split("; "),
                    ))

        return alerts

    def _classify_alert(self, event: str) -> str:
        """Classify alert type from event name."""
        event_lower = event.lower()
        if "high surf warning" in event_lower:
            return "high_surf_warning"
        elif "high surf advisory" in event_lower:
            return "high_surf_advisory"
        elif "small craft" in event_lower:
            return "small_craft_advisory"
        elif "wind" in event_lower:
            return "wind_advisory"
        return "other"

    def _extract_tide_info(self, ranked_sites: list[RankedSite]) -> Optional[TideInfo]:
        """Extract tide info from first site with data."""
        for ranked in ranked_sites:
            cond = ranked.conditions
            if cond.next_high_tide or cond.next_low_tide:
                return TideInfo(
                    next_high_time=cond.next_high_tide,
                    next_low_time=cond.next_low_tide,
                )
        return None

    def _generate_coast_summaries(
        self, all_ranked: list[RankedSite]
    ) -> list[CoastSummary]:
        """Generate summary for each coast."""
        summaries = []

        for coast in self.site_db.coasts:
            coast_sites = [r for r in all_ranked if r.site.coast == coast]

            if not coast_sites:
                continue

            # Calculate average wave height
            wave_heights = [
                r.conditions.wave_height_ft
                for r in coast_sites
                if r.conditions.wave_height_ft is not None
            ]
            avg_wave = sum(wave_heights) / len(wave_heights) if wave_heights else None

            diveable = [r for r in coast_sites if r.is_diveable]

            summary = CoastSummary(
                coast=coast,
                display_name=self.COAST_DISPLAY_NAMES.get(coast, coast),
                top_sites=coast_sites[:3],  # Top 3 per coast
                average_wave_height=avg_wave,
                diveable_count=len(diveable),
                total_count=len(coast_sites),
            )
            summaries.append(summary)

        # Sort by diveable count descending
        summaries.sort(key=lambda s: s.diveable_count, reverse=True)
        return summaries

    def _collect_api_statuses(self, ranked_sites: list[RankedSite]) -> list[APIStatus]:
        """Collect API success/failure statistics from ranked sites."""
        api_stats = {
            "buoy": APIStatus("buoy", "NDBC Buoys"),
            "pacioos": APIStatus("pacioos", "PacIOOS Wave Model"),
            "nws": APIStatus("nws", "NWS Weather"),
            "tides": APIStatus("tides", "NOAA Tides"),
            "usgs": APIStatus("usgs", "USGS Streams"),
            "cwb": APIStatus("cwb", "Water Quality"),
        }

        for ranked in ranked_sites:
            # Check wave data source
            if ranked.conditions.wave_source == "buoy":
                api_stats["buoy"].success_count += 1
            elif ranked.conditions.wave_source == "pacioos":
                api_stats["pacioos"].success_count += 1
            else:
                # No wave data - mark as failure for both
                api_stats["buoy"].failure_count += 1
                api_stats["pacioos"].failure_count += 1

            # Parse errors to track other APIs
            for error in ranked.conditions.errors:
                error_lower = error.lower()
                if "buoy" in error_lower:
                    api_stats["buoy"].failure_count += 1
                    api_stats["buoy"].last_error = error
                elif "pacioos" in error_lower:
                    api_stats["pacioos"].failure_count += 1
                    api_stats["pacioos"].last_error = error
                elif "nws" in error_lower:
                    api_stats["nws"].failure_count += 1
                    api_stats["nws"].last_error = error
                elif "tide" in error_lower:
                    api_stats["tides"].failure_count += 1
                    api_stats["tides"].last_error = error
                elif "usgs" in error_lower:
                    api_stats["usgs"].failure_count += 1
                    api_stats["usgs"].last_error = error
                elif "cwb" in error_lower:
                    api_stats["cwb"].failure_count += 1
                    api_stats["cwb"].last_error = error

            # Count successes for other APIs based on data presence
            if ranked.conditions.wind_speed_mph is not None:
                api_stats["nws"].success_count += 1
            if ranked.conditions.tide_phase is not None:
                api_stats["tides"].success_count += 1
            if ranked.conditions.stream_discharge_cfs is not None:
                api_stats["usgs"].success_count += 1

        return list(api_stats.values())

    def _generate_forecast(self, days: int = 7) -> list[ForecastDay]:
        """Generate multi-day forecast.

        Args:
            days: Number of days to forecast (default 7)

        Returns:
            List of ForecastDay objects
        """
        forecasts = []
        nws = NWSClient()
        pacioos = PacIOOSClient()
        buoy = BuoyClient()

        # Reference location for weather (Honolulu)
        ref_lat, ref_lon = 21.31, -157.86

        # Get NWS hourly forecast (up to 7 days)
        try:
            nws_df = nws.get_hourly_forecast(ref_lat, ref_lon)
            nws_df["time_parsed"] = pd.to_datetime(nws_df["time"])
            nws_df["date"] = nws_df["time_parsed"].dt.date
        except Exception as e:
            logger.warning(f"Failed to get NWS forecast: {e}")
            nws_df = pd.DataFrame()

        # Offshore reference points for wave forecasts (in water, not on land)
        # These are ~1-2 miles offshore from each coast
        wave_reference_points = {
            "South Shore": (21.25, -157.85),      # Off Waikiki
            "West Side": (21.40, -158.20),        # Off Makaha
            "North Shore": (21.65, -158.05),      # Off Waimea
            "Windward": (21.45, -157.75),         # Off Kaneohe
        }

        # Get wave forecast data for each coast reference point
        buoy_forecasts = {}
        for coast_name, (lat, lon) in wave_reference_points.items():
            try:
                # PacIOOS SWAN model provides ~5 days of forecast
                wave_df = pacioos.get_forecast(lat, lon, hours=min(days * 24, 120))
                if not wave_df.empty and wave_df["wave_height_m"].notna().any():
                    wave_df["time_parsed"] = pd.to_datetime(wave_df["time"])
                    wave_df["date"] = wave_df["time_parsed"].dt.date
                    buoy_forecasts[coast_name] = wave_df
                    logger.debug(f"Got wave forecast for {coast_name}: {len(wave_df)} records")
            except Exception as e:
                logger.debug(f"No PacIOOS data for {coast_name}: {e}")

        # Get current buoy conditions for "Today"
        current_buoy_data = {}
        try:
            all_buoy_conditions = buoy.get_all_buoy_conditions()
            for name, data in all_buoy_conditions.items():
                if data.get("wave_height_ft"):
                    location = data.get("location", name)
                    current_buoy_data[location] = data["wave_height_ft"]
        except Exception as e:
            logger.debug(f"Failed to get current buoy data: {e}")

        # Generate forecast for each day
        today = datetime.now().date()
        day_names = ["Today", "Tomorrow"]

        for i in range(days):
            forecast_date = today + timedelta(days=i)

            # Day name
            if i < len(day_names):
                day_name = day_names[i]
            else:
                day_name = forecast_date.strftime("%A")

            forecast = ForecastDay(
                date=datetime.combine(forecast_date, datetime.min.time()),
                day_name=day_name,
            )

            # Extract weather data for this day
            if not nws_df.empty:
                day_weather = nws_df[nws_df["date"] == forecast_date]
                if not day_weather.empty:
                    forecast.wind_speed_min_mph = day_weather["wind_speed_mph"].min()
                    forecast.wind_speed_max_mph = day_weather["wind_speed_mph"].max()
                    forecast.wind_direction = day_weather.iloc[len(day_weather)//2]["wind_direction"]
                    forecast.conditions = day_weather.iloc[len(day_weather)//2]["short_forecast"]

                    # Rain chance
                    rain_probs = day_weather["precipitation_probability"].dropna()
                    if not rain_probs.empty:
                        forecast.rain_chance = int(rain_probs.max())

            # Extract wave data from forecasts
            wave_heights = []
            coast_outlooks = {}

            # For today, use current buoy data
            if i == 0 and current_buoy_data:
                for location, height in current_buoy_data.items():
                    wave_heights.append(height)
                    # Determine outlook for this coast
                    if height < 3:
                        coast_outlooks[location] = "Good"
                    elif height < 5:
                        coast_outlooks[location] = "Fair"
                    elif height < 8:
                        coast_outlooks[location] = "Poor"
                    else:
                        coast_outlooks[location] = "Unsafe"

            for location, wave_df in buoy_forecasts.items():
                day_waves = wave_df[wave_df["date"] == forecast_date]
                if not day_waves.empty:
                    heights = day_waves["wave_height_m"].dropna() * 3.28084  # Convert to ft
                    if not heights.empty:
                        wave_heights.extend(heights.tolist())
                        avg_height = heights.mean()

                        # Determine outlook for this coast
                        if avg_height < 3:
                            coast_outlooks[location] = "Good"
                        elif avg_height < 5:
                            coast_outlooks[location] = "Fair"
                        elif avg_height < 8:
                            coast_outlooks[location] = "Poor"
                        else:
                            coast_outlooks[location] = "Unsafe"

            if wave_heights:
                forecast.wave_height_min_ft = min(wave_heights)
                forecast.wave_height_max_ft = max(wave_heights)

            forecast.coast_outlooks = coast_outlooks

            # Determine overall outlook
            if coast_outlooks:
                outlook_priority = {"Good": 0, "Fair": 1, "Poor": 2, "Unsafe": 3}
                # Best outlook among coasts
                best_outlook = min(coast_outlooks.values(), key=lambda x: outlook_priority.get(x, 4))
                forecast.outlook = best_outlook

                # Find best coast
                for coast, outlook in coast_outlooks.items():
                    if outlook == best_outlook:
                        forecast.best_coast = coast
                        break

                # Outlook reason
                if best_outlook == "Good":
                    forecast.outlook_reason = "Small waves expected"
                elif best_outlook == "Fair":
                    forecast.outlook_reason = "Moderate conditions"
                elif best_outlook == "Poor":
                    forecast.outlook_reason = "Elevated surf"
                else:
                    forecast.outlook_reason = "Large swell expected"

            forecasts.append(forecast)

        return forecasts


def generate_daily_digest(**kwargs) -> DailyDigest:
    """Convenience function to generate a daily digest.

    Args:
        **kwargs: Arguments passed to DigestGenerator.generate()

    Returns:
        DailyDigest with current conditions.
    """
    generator = DigestGenerator()
    return generator.generate(**kwargs)
