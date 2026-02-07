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
from src.core.scorer import ScoringInput
from src.core.site import SiteDatabase, get_site_database
from src.clients.buoy_client import BuoyClient, OAHU_BUOYS
from src.clients.nws_client import NWSClient
from src.clients.pacioos_client import PacIOOSClient
from src.clients.openweathermap_client import OpenWeatherMapClient

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
class BeachForecast:
    """Forecast for a specific beach with detailed conditions."""
    name: str
    coast: str
    wave_height_ft: Optional[float] = None
    wave_period_s: Optional[float] = None
    wind_speed_mph: Optional[float] = None
    wind_type: str = "unknown"  # "offshore", "onshore", "cross-shore"
    wind_direction: Optional[str] = None
    tide_phase: Optional[str] = None  # "rising", "falling", "high", "low"
    outlook: str = "Unknown"  # Grade A-F
    score: float = 0
    best_time: Optional[str] = None  # "5-9 AM", etc.
    why_recommended: Optional[str] = None  # Detailed explanation

    @property
    def wpi(self) -> Optional[float]:
        """Calculate Wave Power Index = height² × period."""
        if self.wave_height_ft is not None and self.wave_period_s is not None:
            return self.wave_height_ft ** 2 * self.wave_period_s
        return None


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

    # Active warnings that affect this day
    has_high_surf_warning: bool = False
    has_high_surf_advisory: bool = False
    has_wind_warning: bool = False
    warning_expires: Optional[str] = None

    # Best time to dive
    best_time: Optional[str] = None  # "5-9 AM", etc.

    # Top recommended beaches for this day
    recommended_beaches: list[BeachForecast] = field(default_factory=list)

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
            # Filter out Hanauma Bay from top sites (user never goes there)
            filtered_sites = [r for r in all_ranked if "hanauma" not in r.site.name.lower()]
            digest.top_sites = filtered_sites[:self.top_sites_count]

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

            # Generate 7-day forecast (pass alerts and ranked sites for context)
            digest.forecast_days = self._generate_forecast(
                days=7,
                alerts=digest.alerts,
                ranked_sites=all_ranked,
            )

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

    def _find_best_time_window(
        self,
        pacioos: PacIOOSClient,
        lat: float,
        lon: float,
        target_date,
    ) -> Optional[str]:
        """Find the best time window for diving based on hourly wave data.

        Args:
            pacioos: PacIOOS client instance
            lat: Site latitude
            lon: Site longitude
            target_date: Date to find best time for

        Returns:
            String like "06:00-10:00" or None if no data
        """
        try:
            # Get hourly wave data for this location
            wave_df = pacioos.get_wave_data(lat, lon, hours=48)
            if wave_df.empty:
                return None

            wave_df["time_parsed"] = pd.to_datetime(wave_df["time"])
            wave_df["date"] = wave_df["time_parsed"].dt.date
            wave_df["hour"] = wave_df["time_parsed"].dt.hour

            # Filter to target date
            day_data = wave_df[wave_df["date"] == target_date].copy()
            if day_data.empty:
                return None

            # Convert wave height to feet
            day_data["wave_ft"] = day_data["wave_height_m"] * 3.28084

            # Find hours with smallest waves (daylight hours 5 AM - 6 PM)
            daylight = day_data[(day_data["hour"] >= 5) & (day_data["hour"] <= 18)]
            if daylight.empty:
                return None

            # Find the best 3-4 hour window with lowest average waves
            best_start = None
            best_avg = float("inf")

            for start_hour in range(5, 16):  # Windows starting 5 AM to 3 PM
                window = daylight[(daylight["hour"] >= start_hour) & (daylight["hour"] < start_hour + 4)]
                if len(window) >= 2:
                    avg_wave = window["wave_ft"].mean()
                    if avg_wave < best_avg:
                        best_avg = avg_wave
                        best_start = start_hour

            if best_start is not None:
                end_hour = min(best_start + 4, 18)
                return f"{best_start:02d}:00-{end_hour:02d}:00"

            # Fallback: just find the single best hour
            best_row = daylight.loc[daylight["wave_ft"].idxmin()]
            best_hour = int(best_row["hour"])
            return f"{best_hour:02d}:00-{min(best_hour + 2, 18):02d}:00"

        except Exception as e:
            logger.debug(f"Error finding best time window: {e}")
            return None

    def _generate_forecast(
        self,
        days: int = 7,
        alerts: Optional[list[AlertInfo]] = None,
        ranked_sites: Optional[list[RankedSite]] = None,
    ) -> list[ForecastDay]:
        """Generate multi-day forecast.

        Args:
            days: Number of days to forecast (default 7)
            alerts: Active weather alerts
            ranked_sites: Current ranked sites for beach recommendations

        Returns:
            List of ForecastDay objects
        """
        forecasts = []
        # Reuse clients from the ranker to share cache and circuit breaker state
        nws = self.ranker.nws
        pacioos = self.ranker.pacioos
        buoy = self.ranker.buoy
        alerts = alerts or []

        # Check for active warnings
        has_surf_warning = any(a.type == "high_surf_warning" for a in alerts)
        has_surf_advisory = any(a.type == "high_surf_advisory" for a in alerts)
        has_wind_warning = any("wind" in a.type.lower() for a in alerts)

        # Get warning expiration (look for it in headlines)
        warning_expires = None
        for alert in alerts:
            if "until" in alert.headline.lower():
                # Extract expiration from headline
                parts = alert.headline.lower().split("until")
                if len(parts) > 1:
                    warning_expires = parts[1].strip().split(" by")[0].strip()
                    break

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

        # Fallback: if PacIOOS is completely down (circuit breaker tripped),
        # use current NDBC buoy readings as wave estimates for all forecast days.
        # Buoys only give current conditions, not forecasts, but it's better than
        # showing zero beaches.
        buoy_wave_fallback = {}  # coast_display_name -> {"wave_ht": ft, "wave_period": s}
        if not buoy_forecasts:
            logger.info("PacIOOS unavailable, using NDBC buoy data as wave fallback")
            buoy_to_coast = {
                "North Shore": "waimea",
                "Windward": "mokapu",
                "South Shore": "kalaeloa",
                "West Side": "kalaeloa",
                "Southeast": "kalaeloa",
            }
            for coast_display, buoy_key in buoy_to_coast.items():
                try:
                    buoy_info = OAHU_BUOYS[buoy_key]
                    conditions = buoy.get_current_conditions(buoy_info["ndbc"])
                    wave_ft = conditions.get("wave_height_ft")
                    if wave_ft is not None:
                        buoy_wave_fallback[coast_display] = {
                            "wave_ht": wave_ft,
                            "wave_period": conditions.get("swell_period_s"),
                        }
                except Exception as e:
                    logger.debug(f"Buoy fallback failed for {coast_display}: {e}")

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

            # For days beyond PacIOOS range (~5 days), use the last available day's data
            if not coast_outlooks and buoy_forecasts:
                for location, wave_df in buoy_forecasts.items():
                    last_date = wave_df["date"].max()
                    last_day = wave_df[wave_df["date"] == last_date]
                    if not last_day.empty:
                        heights = last_day["wave_height_m"].dropna() * 3.28084
                        if not heights.empty:
                            avg_height = heights.mean()
                            wave_heights.extend(heights.tolist())
                            if avg_height < 3:
                                coast_outlooks[location] = "Good"
                            elif avg_height < 5:
                                coast_outlooks[location] = "Fair"
                            elif avg_height < 8:
                                coast_outlooks[location] = "Poor"
                            else:
                                coast_outlooks[location] = "Unsafe"
                if coast_outlooks:
                    forecast.outlook_reason = (forecast.outlook_reason or "") + " (extended forecast)"

            # Final fallback: use NDBC buoy current readings if PacIOOS is completely down
            if not coast_outlooks and buoy_wave_fallback:
                for location, data in buoy_wave_fallback.items():
                    height = data["wave_ht"]
                    wave_heights.append(height)
                    if height < 3:
                        coast_outlooks[location] = "Good"
                    elif height < 5:
                        coast_outlooks[location] = "Fair"
                    elif height < 8:
                        coast_outlooks[location] = "Poor"
                    else:
                        coast_outlooks[location] = "Unsafe"
                if coast_outlooks:
                    forecast.outlook_reason = (forecast.outlook_reason or "") + " (based on current buoy readings)"

            if wave_heights:
                forecast.wave_height_min_ft = min(wave_heights)
                forecast.wave_height_max_ft = max(wave_heights)

            forecast.coast_outlooks = coast_outlooks

            # Check if warnings apply to this day (assume warnings affect today and possibly tomorrow)
            # Most warnings expire within 24-48 hours
            if i == 0:  # Today
                forecast.has_high_surf_warning = has_surf_warning
                forecast.has_high_surf_advisory = has_surf_advisory
                forecast.has_wind_warning = has_wind_warning
                forecast.warning_expires = warning_expires
            elif i == 1 and warning_expires:  # Tomorrow - check if warning extends
                # Simple heuristic: if warning mentions tomorrow's date, it applies
                forecast.has_high_surf_warning = has_surf_warning
                forecast.has_high_surf_advisory = has_surf_advisory
                forecast.warning_expires = warning_expires

            # Determine overall outlook based on ACTUAL wave conditions
            # High Surf Warning is informational only - doesn't override local conditions
            if coast_outlooks:
                outlook_priority = {"Good": 0, "Fair": 1, "Poor": 2, "Unsafe": 3}
                # Best outlook among coasts
                best_outlook = min(coast_outlooks.values(), key=lambda x: outlook_priority.get(x, 4))

                # High surf advisory downgrades outlook
                if forecast.has_high_surf_advisory and best_outlook in ("Good", "Fair"):
                    best_outlook = "Fair"
                    forecast.outlook_reason = "High Surf Advisory - use caution"
                else:
                    # Outlook reason based on conditions
                    if best_outlook == "Good":
                        forecast.outlook_reason = "Small waves expected"
                    elif best_outlook == "Fair":
                        forecast.outlook_reason = "Moderate conditions"
                    elif best_outlook == "Poor":
                        forecast.outlook_reason = "Elevated surf"
                    else:
                        forecast.outlook_reason = "Large swell expected"

                forecast.outlook = best_outlook

                # Find best coast
                for coast, outlook in coast_outlooks.items():
                    if outlook == best_outlook:
                        forecast.best_coast = coast
                        break

                # Best time to dive (early morning is best due to calmer winds)
                if best_outlook in ("Good", "Fair"):
                    forecast.best_time = "5-9 AM"
                elif best_outlook == "Poor":
                    forecast.best_time = "Early morning only"

            # Add beach recommendations for ALL days
            recommended = []

            if i == 0 and ranked_sites:
                # TODAY: Use actual ranked site data - show ALL diveable sites
                for site in ranked_sites:
                    # Skip Hanauma Bay (user never goes there)
                    if "hanauma" in site.site.name.lower():
                        continue

                    # Only include diveable sites (wave height <= 6ft)
                    wave_ht = site.conditions.wave_height_ft
                    if wave_ht is None or wave_ht > 6:
                        continue

                    cond = site.conditions
                    score_result = site.score

                    # For TODAY: Can't use PacIOOS (forecast only starts tomorrow)
                    # Use site preferences and current conditions
                    site_optimal_time = site.site.optimal_time  # "morning" or "any"
                    site_optimal_tide = site.site.optimal_tide  # "high", "low", or "any"

                    # Build best time recommendation
                    if site_optimal_time == "morning":
                        best_time = "06:00-10:00"
                    else:
                        # Check current time and recommend accordingly
                        current_hour = datetime.now().hour
                        if current_hour < 10:
                            best_time = "now - 10:00"
                        elif current_hour < 14:
                            best_time = "now - 14:00"
                        else:
                            best_time = "now - sunset"

                    # Add tide recommendation
                    if site_optimal_tide == "high" and cond.next_high_tide:
                        best_time = f"{best_time} (high tide: {cond.next_high_tide[-5:]})"
                    elif site_optimal_tide == "low" and cond.next_low_tide:
                        best_time = f"{best_time} (low tide: {cond.next_low_tide[-5:]})"
                    elif cond.tide_phase:
                        best_time = f"{best_time} ({cond.tide_phase} tide)"

                    # Build detailed "Why" explanation
                    reasons = []
                    wind_type = score_result.wind_type if hasattr(score_result, 'wind_type') else "unknown"

                    # WPI assessment
                    wpi = None
                    if wave_ht and cond.wave_period_s:
                        wpi = wave_ht ** 2 * cond.wave_period_s
                        if wpi < 5:
                            reasons.append(f"Excellent WPI ({wpi:.0f})")
                        elif wpi < 20:
                            reasons.append(f"Good WPI ({wpi:.0f})")
                        elif wpi < 50:
                            reasons.append(f"Moderate WPI ({wpi:.0f})")
                        else:
                            reasons.append(f"High WPI ({wpi:.0f}) - challenging")

                    # Wave assessment
                    if wave_ht:
                        if wave_ht < 1:
                            reasons.append("glass-flat conditions")
                        elif wave_ht < 2:
                            reasons.append("very calm surface")
                        elif wave_ht < 3:
                            reasons.append("small manageable waves")
                        elif wave_ht < 4:
                            reasons.append("moderate chop")

                    # Wind assessment
                    if cond.wind_speed_mph:
                        if cond.wind_speed_mph < 5:
                            reasons.append("near calm winds")
                        elif wind_type == "offshore":
                            reasons.append(f"offshore wind ({cond.wind_speed_mph:.0f}mph) - good visibility")
                        elif wind_type == "onshore":
                            reasons.append(f"onshore wind ({cond.wind_speed_mph:.0f}mph) - reduced viz")
                        elif wind_type == "cross-shore" and cond.wind_speed_mph < 12:
                            reasons.append(f"light cross-shore ({cond.wind_speed_mph:.0f}mph)")
                        elif cond.wind_speed_mph > 15:
                            reasons.append(f"windy ({cond.wind_speed_mph:.0f}mph)")

                    # Tide info
                    if cond.tide_phase:
                        if site.site.optimal_tide == cond.tide_phase:
                            reasons.append(f"optimal {cond.tide_phase} tide")
                        else:
                            reasons.append(f"{cond.tide_phase} tide")

                    beach = BeachForecast(
                        name=site.site.name,
                        coast=self.COAST_DISPLAY_NAMES.get(site.site.coast, site.site.coast),
                        wave_height_ft=wave_ht,
                        wave_period_s=cond.wave_period_s,
                        wind_speed_mph=cond.wind_speed_mph,
                        wind_type=wind_type,
                        wind_direction=None,
                        tide_phase=cond.tide_phase,
                        outlook=site.grade,
                        score=score_result.total_score,
                        best_time=best_time or "morning",
                        why_recommended=" + ".join(reasons) if reasons else None,
                    )
                    recommended.append(beach)

            elif ranked_sites:
                # FUTURE DAYS: Query PacIOOS for waves and OpenWeatherMap for wind
                # This gives us per-site forecasts instead of coast/island-level
                owm = OpenWeatherMapClient()

                # Pre-compute coast-level average wave heights for this day as fallback
                coast_wave_averages = {}
                for coast_name, wave_df in buoy_forecasts.items():
                    day_waves = wave_df[wave_df["date"] == forecast_date]
                    # If no data for this date, use the last available day (extended forecast)
                    if day_waves.empty:
                        last_date = wave_df["date"].max()
                        day_waves = wave_df[wave_df["date"] == last_date]
                    if not day_waves.empty:
                        heights = day_waves["wave_height_m"].dropna() * 3.28084
                        periods = day_waves["period_s"].dropna()
                        if not heights.empty:
                            coast_wave_averages[coast_name] = {
                                "wave_ht": heights.mean(),
                                "wave_period": periods.mean() if not periods.empty else None,
                            }

                for site in ranked_sites:
                    # Skip Hanauma Bay
                    if "hanauma" in site.site.name.lower():
                        continue

                    site_lat = site.site.coordinates.lat
                    site_lon = site.site.coordinates.lon

                    # Get site-specific wave forecast from PacIOOS
                    wave_ht = None
                    wave_period = None
                    best_time_range = None
                    try:
                        site_wave_df = pacioos.get_forecast(site_lat, site_lon, hours=min((i+1) * 24, 120))
                        if not site_wave_df.empty:
                            site_wave_df["time_parsed"] = pd.to_datetime(site_wave_df["time"])
                            site_wave_df["date"] = site_wave_df["time_parsed"].dt.date
                            site_wave_df["hour"] = site_wave_df["time_parsed"].dt.hour
                            day_waves = site_wave_df[site_wave_df["date"] == forecast_date]
                            if not day_waves.empty:
                                heights = day_waves["wave_height_m"].dropna() * 3.28084
                                periods = day_waves["period_s"].dropna()
                                if not heights.empty:
                                    wave_ht = heights.mean()
                                if not periods.empty:
                                    wave_period = periods.mean()

                                # Find best time window from hourly data
                                day_waves = day_waves.copy()
                                day_waves["wave_ft"] = day_waves["wave_height_m"] * 3.28084
                                daylight = day_waves[(day_waves["hour"] >= 5) & (day_waves["hour"] <= 18)]
                                if not daylight.empty:
                                    # Find best 4-hour window
                                    best_start = None
                                    best_avg = float("inf")
                                    for start_hour in range(5, 16):
                                        window = daylight[(daylight["hour"] >= start_hour) & (daylight["hour"] < start_hour + 4)]
                                        if len(window) >= 2:
                                            avg = window["wave_ft"].mean()
                                            if avg < best_avg:
                                                best_avg = avg
                                                best_start = start_hour
                                    if best_start is not None:
                                        best_time_range = f"{best_start:02d}:00-{min(best_start + 4, 18):02d}:00"
                    except Exception as e:
                        logger.debug(f"PacIOOS query failed for {site.site.name}: {e}")

                    # Fall back to coast-level wave data if per-site data unavailable
                    if wave_ht is None:
                        site_coast_display = self.COAST_DISPLAY_NAMES.get(site.site.coast, site.site.coast)
                        coast_data = coast_wave_averages.get(site_coast_display)
                        if coast_data:
                            wave_ht = coast_data["wave_ht"]
                            wave_period = coast_data.get("wave_period")

                    # Final fallback: use NDBC buoy current readings
                    if wave_ht is None and buoy_wave_fallback:
                        site_coast_display = self.COAST_DISPLAY_NAMES.get(site.site.coast, site.site.coast)
                        buoy_data = buoy_wave_fallback.get(site_coast_display)
                        if buoy_data:
                            wave_ht = buoy_data["wave_ht"]
                            wave_period = buoy_data.get("wave_period")

                    # Skip if still no wave data or waves too high
                    if wave_ht is None or wave_ht > 6:
                        continue

                    # Get site-specific wind forecast from OpenWeatherMap
                    site_wind = None
                    wind_dir = None
                    try:
                        wind_data = owm.get_wind_forecast(site_lat, site_lon, forecast.date)
                        if wind_data:
                            site_wind = wind_data.get("wind_speed_mph")
                            wind_dir_deg = wind_data.get("wind_direction_deg")
                            if wind_dir_deg is not None:
                                wind_dir = owm.get_wind_direction_name(wind_dir_deg)
                            # Use OWM best time if we don't have wave-based time
                            if not best_time_range:
                                best_time_range = wind_data.get("best_time_range")
                    except Exception as e:
                        logger.debug(f"OpenWeatherMap query failed for {site.site.name}: {e}")

                    # Fall back to NWS hourly wind data if OWM fails
                    # Find ALL calm wind windows so the user can pick their time
                    nws_calm_windows = []
                    if site_wind is None and not nws_df.empty:
                        day_weather = nws_df[nws_df["date"] == forecast_date]
                        if not day_weather.empty:
                            day_weather = day_weather.copy()
                            day_weather["hour"] = day_weather["time_parsed"].dt.hour

                            daylight_wind = day_weather[(day_weather["hour"] >= 5) & (day_weather["hour"] <= 18)]
                            if not daylight_wind.empty:
                                # Scan hourly data to find contiguous calm periods (< 12 mph)
                                sorted_hours = daylight_wind.sort_values("hour")
                                current_window_start = None
                                current_window_speeds = []
                                current_window_dir = None

                                for _, row in sorted_hours.iterrows():
                                    if row["wind_speed_mph"] < 12:
                                        if current_window_start is None:
                                            current_window_start = int(row["hour"])
                                            current_window_dir = row["wind_direction"]
                                        current_window_speeds.append(row["wind_speed_mph"])
                                    else:
                                        if current_window_start is not None and len(current_window_speeds) >= 2:
                                            end_h = current_window_start + len(current_window_speeds)
                                            avg_spd = sum(current_window_speeds) / len(current_window_speeds)
                                            nws_calm_windows.append({
                                                "start": current_window_start,
                                                "end": min(end_h, 19),
                                                "avg_mph": round(avg_spd, 1),
                                                "direction": current_window_dir,
                                            })
                                        current_window_start = None
                                        current_window_speeds = []
                                        current_window_dir = None

                                # Close final window
                                if current_window_start is not None and len(current_window_speeds) >= 2:
                                    end_h = current_window_start + len(current_window_speeds)
                                    avg_spd = sum(current_window_speeds) / len(current_window_speeds)
                                    nws_calm_windows.append({
                                        "start": current_window_start,
                                        "end": min(end_h, 19),
                                        "avg_mph": round(avg_spd, 1),
                                        "direction": current_window_dir,
                                    })

                                # Use the calmest window for scoring
                                if nws_calm_windows:
                                    best_win = min(nws_calm_windows, key=lambda w: w["avg_mph"])
                                    site_wind = best_win["avg_mph"]
                                    wind_dir = best_win["direction"]
                                    if not best_time_range:
                                        best_time_range = f"{best_win['start']:02d}:00-{best_win['end']:02d}:00"
                                else:
                                    # No calm windows — use midday as representative
                                    midday = daylight_wind[daylight_wind["hour"].between(10, 14)]
                                    if not midday.empty:
                                        site_wind = round(midday["wind_speed_mph"].mean(), 1)
                                        wind_dir = midday.iloc[0]["wind_direction"]

                    # Default best time if we couldn't calculate it
                    if not best_time_range:
                        best_time_range = "06:00-10:00"

                    # Build detailed "Why" explanation
                    reasons = []

                    # WPI assessment (using forecast wave period)
                    if wave_ht and wave_period:
                        wpi = wave_ht ** 2 * wave_period
                        if wpi < 5:
                            reasons.append(f"Excellent WPI ({wpi:.0f})")
                        elif wpi < 20:
                            reasons.append(f"Good WPI ({wpi:.0f})")
                        elif wpi < 50:
                            reasons.append(f"Moderate WPI ({wpi:.0f})")
                        else:
                            reasons.append(f"High WPI ({wpi:.0f})")

                    # Wave conditions
                    if wave_ht < 1:
                        reasons.append("glass-flat expected")
                    elif wave_ht < 2:
                        reasons.append("very calm forecast")
                    elif wave_ht < 3:
                        reasons.append("small waves forecast")
                    elif wave_ht < 4:
                        reasons.append("moderate chop expected")
                    else:
                        reasons.append(f"{wave_ht:.1f}ft waves forecast")

                    # Wind assessment — list all calm windows so user can pick
                    if nws_calm_windows:
                        window_strs = [
                            f"{w['avg_mph']:.0f}mph {w['start']:02d}:00-{w['end']:02d}:00"
                            for w in sorted(nws_calm_windows, key=lambda w: w["start"])
                        ]
                        reasons.append(f"calm windows: {', '.join(window_strs)}")
                    elif site_wind is not None:
                        if site_wind < 5:
                            reasons.append(f"calm winds ({site_wind:.0f}mph)")
                        elif site_wind < 10:
                            reasons.append(f"light winds ({site_wind:.0f}mph)")
                        elif site_wind < 15:
                            reasons.append(f"moderate wind (~{site_wind:.0f}mph)")
                        else:
                            reasons.append(f"windy (~{site_wind:.0f}mph) - may affect viz")

                    # Use the actual scorer for consistent grade/score
                    forecast_input = ScoringInput(
                        wave_height_ft=wave_ht,
                        wave_period_s=wave_period,
                        wind_speed_mph=site_wind,
                        site_max_safe_height_ft=site.site.max_safe_wave_height,
                        site_swell_exposure_primary=site.site.swell_exposure.primary,
                    )
                    forecast_score = self.ranker.scorer.calculate_score(forecast_input)

                    beach = BeachForecast(
                        name=site.site.name,
                        coast=self.COAST_DISPLAY_NAMES.get(site.site.coast, site.site.coast),
                        wave_height_ft=wave_ht,
                        wave_period_s=wave_period,
                        wind_speed_mph=site_wind,
                        wind_type="forecast",
                        wind_direction=wind_dir,
                        tide_phase=None,
                        outlook=forecast_score.grade.value,
                        score=forecast_score.total_score,
                        best_time=best_time_range,
                        why_recommended=" | ".join(reasons) if reasons else None,
                    )
                    recommended.append(beach)

                # Sort by wave height (lower is better for diving)
                recommended.sort(key=lambda b: (b.wave_height_ft or 99, -b.score))

            forecast.recommended_beaches = recommended
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
