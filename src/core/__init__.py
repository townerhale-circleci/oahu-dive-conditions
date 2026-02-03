"""Core dive condition scoring and ranking engine."""

from src.core.scorer import (
    DiveScorer,
    ScoreGrade,
    ScoringInput,
    ScoringResult,
    quick_score,
)
from src.core.site import (
    Coordinates,
    DepthRange,
    DiveSite,
    Regulations,
    SeasonalWindow,
    SiteDatabase,
    SwellExposure,
    get_site,
    get_site_database,
)
from src.core.ranker import (
    EnvironmentalConditions,
    RankedSite,
    SiteRanker,
    get_top_sites,
)

__all__ = [
    # Scorer
    "DiveScorer",
    "ScoreGrade",
    "ScoringInput",
    "ScoringResult",
    "quick_score",
    # Site
    "Coordinates",
    "DepthRange",
    "DiveSite",
    "Regulations",
    "SeasonalWindow",
    "SiteDatabase",
    "SwellExposure",
    "get_site",
    "get_site_database",
    # Ranker
    "EnvironmentalConditions",
    "RankedSite",
    "SiteRanker",
    "get_top_sites",
]
