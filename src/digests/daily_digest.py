"""Daily dive conditions digest generator.

Fetches current conditions for all sites and generates a structured
report suitable for formatting as SMS, email, or other outputs.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.core.ranker import RankedSite, SiteRanker
from src.core.site import SiteDatabase, get_site_database

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


def generate_daily_digest(**kwargs) -> DailyDigest:
    """Convenience function to generate a daily digest.

    Args:
        **kwargs: Arguments passed to DigestGenerator.generate()

    Returns:
        DailyDigest with current conditions.
    """
    generator = DigestGenerator()
    return generator.generate(**kwargs)
