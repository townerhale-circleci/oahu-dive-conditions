"""Surf transform: convert offshore buoy significant wave height (Hs) into an
effective surf height at a specific dive site.

The core accuracy failure documented in POSTMORTEM.md Block 1 was comparing raw
open-ocean buoy Hs directly against per-site beach thresholds. Offshore Hs mixes
all swell components regardless of direction, so a NE windswell counted fully
against a NW-facing North Shore site it can never reach, and short-period trade
windswell was treated the same as a long-period groundswell that shoals into a
big wave at the beach.

This module resolves that with two physical effects:

1. **Shoaling** — longer-period swell carries more energy into shallow water and
   builds into a larger breaking wave; short-period windswell mostly dissipates.
   Captured as a period-dependent multiplier on Hs.
2. **Directional exposure** — a swell only reaches a site if its mean wave
   direction (MWD) is roughly aligned with the direction the site faces. Energy
   off-axis is tapered toward a period-dependent "wrap floor" (long-period swell
   wraps around headlands and into bays; short windswell barely does).

The module is pure and unit-testable; it holds no I/O and no global state.

PacIOOS SWAN output is already a near-shore model that resolves shadowing and
shoaling, so callers should NOT apply this transform to PacIOOS values (use
factor 1.0). It is intended for raw NDBC buoy observations.
"""

from dataclasses import dataclass
from typing import Optional, Tuple


# 16-point compass → degrees. Kept identical to the scorer's exposure_map so the
# two agree on what "NW" means.
_COMPASS_TO_DEG = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
}

# Conservative multiplier applied when swell direction or period is unknown, so
# an unresolved buoy reading is neither wildly optimistic nor pessimistic.
UNKNOWN_DIRECTION_FACTOR = 0.8


@dataclass
class SurfTransformParams:
    """Tunable parameters for the offshore→beach surf transform.

    Values below are calibration starting points; the hindcast script tunes them
    against the NWS surf zone forecast (see scripts/hindcast_audit.py).
    """
    # shoaling factor by dominant period (s) bins
    # NOTE: defaults below are the CALIBRATED best set from scripts/hindcast_audit.py
    # (grid search over the 2026-06-26..07-02 ground-truth week, MAE 0.773 ft).
    shoal_short: float = 0.7      # period < 9   (calibrated; start 0.6)
    shoal_mid: float = 0.7        # 9 <= period < 12  (calibrated; start 0.8)
    shoal_long: float = 1.3       # 12 <= period < 16 (calibrated; start 1.1)
    shoal_very_long: float = 1.3  # >= 16 (held at default; no >=16s obs in grid)
    # angular taper (degrees off-axis between swell MWD and site exposure direction)
    full_exposure_deg: float = 30.0    # factor 1.0 within this
    zero_cross_deg: float = 90.0       # linear taper 1.0 -> wrap_floor between full..zero_cross
    # sheltered floor (energy that wraps around anyway), period-dependent
    wrap_floor_long: float = 0.25      # period >= 12
    wrap_floor_short: float = 0.05     # period < 9 (calibrated; start 0.08); 9-12 interpolate
    secondary_weight: float = 0.8      # secondary exposure counts at this weight
    short_period_taper: float = 0.6    # off-axis multiplier when period < 9 (calibrated; start 0.75)


def _compass_to_deg(exposure_compass: str) -> Optional[float]:
    """Map a 16-point compass string to degrees, or None if unrecognized."""
    if exposure_compass is None:
        return None
    return _COMPASS_TO_DEG.get(exposure_compass.strip().upper())


def _angular_distance(a_deg: float, b_deg: float) -> float:
    """Smallest absolute angular difference between two bearings (0-180)."""
    d = abs(a_deg - b_deg) % 360.0
    if d > 180.0:
        d = 360.0 - d
    return d


def shoal_factor(period_s: Optional[float], params: SurfTransformParams) -> float:
    """Period-dependent shoaling multiplier on Hs."""
    if period_s is None:
        # Treated by callers via the unknown fallback; return mid as a neutral.
        return params.shoal_mid
    if period_s < 9:
        return params.shoal_short
    elif period_s < 12:
        return params.shoal_mid
    elif period_s < 16:
        return params.shoal_long
    else:
        return params.shoal_very_long


def wrap_floor(period_s: Optional[float], params: SurfTransformParams) -> float:
    """Period-dependent minimum exposure factor for fully off-axis swell.

    < 9 s  -> wrap_floor_short
    >= 12 s -> wrap_floor_long
    9-12 s -> linear interpolation between the two.
    """
    if period_s is None:
        return params.wrap_floor_short
    if period_s < 9:
        return params.wrap_floor_short
    elif period_s >= 12:
        return params.wrap_floor_long
    else:
        # linear interp over [9, 12)
        frac = (period_s - 9.0) / 3.0
        return params.wrap_floor_short + frac * (params.wrap_floor_long - params.wrap_floor_short)


def exposure_factor(
    swell_dir_deg: Optional[float],
    exposure_compass: Optional[str],
    period_s: Optional[float],
    params: SurfTransformParams = SurfTransformParams(),
) -> float:
    """Directional exposure factor in [wrap_floor, 1.0].

    Args:
        swell_dir_deg: Mean wave direction (MWD) the swell comes FROM, degrees.
        exposure_compass: Direction the site faces (e.g. "NW").
        period_s: Dominant period, drives the wrap floor and short-period taper.
        params: Transform parameters.

    Returns:
        Factor from wrap_floor(period) up to 1.0. If the site has no exposure
        defined, returns 1.0 (no directional attenuation).
    """
    # Missing exposure: no directional attenuation.
    if exposure_compass is None:
        return 1.0

    exposure_deg = _compass_to_deg(exposure_compass)
    if exposure_deg is None:
        # Unrecognized compass string: don't attenuate.
        return 1.0

    if swell_dir_deg is None:
        # Direction unknown but exposure known: caller handles the unknown case
        # via effective_surf_height's fallback; here return 1.0 so we don't
        # double-penalize.
        return 1.0

    floor = wrap_floor(period_s, params)
    d = _angular_distance(swell_dir_deg, exposure_deg)

    if d <= params.full_exposure_deg:
        return 1.0
    if d >= params.zero_cross_deg:
        return floor

    # Linear interpolation from 1.0 at full_exposure_deg down to floor at zero_cross_deg.
    span = params.zero_cross_deg - params.full_exposure_deg
    factor = 1.0 - (1.0 - floor) * (d - params.full_exposure_deg) / span

    # Short-period swell barely wraps: apply an extra taper on the off-axis
    # (interpolated) factor. The wrap floor still applies as a hard minimum.
    if period_s is not None and period_s < 9:
        factor = factor * params.short_period_taper
        if factor < floor:
            factor = floor

    return factor


def effective_surf_height(
    raw_hs_ft: Optional[float],
    period_s: Optional[float],
    swell_dir_deg: Optional[float],
    primary_exposure: Optional[str],
    secondary_exposure: Optional[str] = None,
    params: SurfTransformParams = SurfTransformParams(),
) -> Tuple[Optional[float], bool]:
    """Transform offshore Hs into an effective surf height at the site.

    effective = raw_hs_ft * shoal(period) * exposure_factor

    where exposure_factor = max(exposure_factor(primary),
                                secondary_weight * exposure_factor(secondary)).

    Args:
        raw_hs_ft: Raw significant wave height at the buoy, in feet.
        period_s: Dominant period, seconds.
        swell_dir_deg: Mean wave direction (from), degrees.
        primary_exposure: Site primary exposure compass (e.g. "NW").
        secondary_exposure: Optional secondary exposure compass.
        params: Transform parameters.

    Returns:
        (effective_height_ft, transform_applied). transform_applied is False when
        direction/period were unknown and a conservative flat multiplier was used
        instead of the directional transform (so callers can flag it).
    """
    if raw_hs_ft is None:
        return None, False

    # Unknown direction or period: conservative flat multiplier, flag untransformed.
    if swell_dir_deg is None or period_s is None:
        return raw_hs_ft * UNKNOWN_DIRECTION_FACTOR, False

    primary_factor = exposure_factor(swell_dir_deg, primary_exposure, period_s, params)
    factor = primary_factor
    if secondary_exposure:
        secondary_factor = params.secondary_weight * exposure_factor(
            swell_dir_deg, secondary_exposure, period_s, params
        )
        factor = max(primary_factor, secondary_factor)

    effective = raw_hs_ft * shoal_factor(period_s, params) * factor
    return effective, True
