"""Single source of truth for assembling a ScoringInput.

Plan 5 ("one scoring path"): the ranker, the digest Today table, and the digest
forecast-day path all used to build ``ScoringInput`` by hand, field by field,
and drifted apart in subtle ways (different wind source, tide omitted on some
paths, ``site_optimal_tide`` silently defaulting to "any" so tide always scored
100, missing ``rain_chance_pct``, etc.). That made the headline "Top Sites"
grade disagree with the Today-table grade for the *same* site.

``build_scoring_input`` is the one assembler. Callers pass the values they have;
the differences between paths are EXPLICIT keyword arguments (e.g. a forecast day
passes ``tide_phase=None`` because observed tide is unknowable, or a predicted
phase when available), never hidden behind per-caller defaults. Neutral handling
(None -> conservative default in the scorer) is therefore identical everywhere.

This module lives in ``src.core`` and imports only from ``src.core`` so the
digest layer (which imports core) never creates a circular import.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from src.core.scorer import ScoringInput
from src.core.site import DiveSite


def build_scoring_input(
    site: DiveSite,
    *,
    wave_height_ft: Optional[float],
    raw_wave_height_ft: Optional[float] = None,
    wave_period_s: Optional[float] = None,
    swell_direction_deg: Optional[float] = None,
    wind_speed_mph: Optional[float] = None,
    wind_direction_deg: Optional[float] = None,
    stream_discharge_cfs: Optional[float] = None,
    rainfall_48h_inches: Optional[float] = None,
    rain_chance_pct: Optional[float] = None,
    brown_water_advisory: bool = False,
    coast_brown_water: bool = False,
    tide_phase: Optional[str] = None,
    water_level_ft: Optional[float] = None,
    evaluation_time: Optional[datetime] = None,
    high_surf_warning: bool = False,
    high_surf_advisory: bool = False,
) -> ScoringInput:
    """Assemble a ``ScoringInput`` for ``site`` from measured/forecast conditions.

    Site context (``max_safe_wave_height``, ``optimal_tide``, primary swell
    exposure) is pulled from ``site`` so every caller applies it identically —
    in particular ``site_optimal_tide`` is ALWAYS set from the site, so a site
    whose optimal tide is "high"/"low" gets a real tide score instead of the
    "any" -> 100 shortcut that happened when callers forgot to pass it.

    All environmental values are keyword-only and default to None/False, meaning
    "no data" -> the scorer's own conservative neutral handling. Callers must be
    explicit about what they omit (that's the whole point of Plan 5).
    """
    return ScoringInput(
        wave_height_ft=wave_height_ft,
        raw_wave_height_ft=raw_wave_height_ft,
        wave_period_s=wave_period_s,
        swell_direction_deg=swell_direction_deg,
        wind_speed_mph=wind_speed_mph,
        wind_direction_deg=wind_direction_deg,
        stream_discharge_cfs=stream_discharge_cfs,
        rainfall_48h_inches=rainfall_48h_inches,
        rain_chance_pct=rain_chance_pct,
        brown_water_advisory=brown_water_advisory,
        coast_brown_water=coast_brown_water,
        tide_phase=tide_phase,
        water_level_ft=water_level_ft,
        evaluation_time=evaluation_time,
        high_surf_warning=high_surf_warning,
        high_surf_advisory=high_surf_advisory,
        site_max_safe_height_ft=site.max_safe_wave_height,
        site_optimal_tide=site.optimal_tide,
        site_swell_exposure_primary=site.swell_exposure.primary,
    )
