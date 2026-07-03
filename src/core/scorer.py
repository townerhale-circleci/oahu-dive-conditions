"""Dive condition scoring algorithm using Wave Power Index.

Scoring approach:
- Wave Power Index = height² × period
- Total score is weighted combination of multiple factors
- Safety gates can force score to 0 regardless of conditions

Wave height semantics (Plan 4 redesign):
- `wave_height_ft` is the EFFECTIVE surf height at the site, i.e. the raw
  offshore buoy Hs after the directional-exposure + shoaling transform in
  src/core/surf_transform.py (or a PacIOOS near-shore model value, which needs
  no transform). All scoring and the site safety gate operate on this.
- `raw_wave_height_ft` is the untransformed offshore significant wave height. It
  is used ONLY by the absolute backstop gate, so an extreme raw sea state still
  gates even if the site happens to be shadowed from it.

Scoring Factors (weights):
- Wave Power: 35% - Lower power = better conditions (computed on effective height)
- Wind: 25% - Offshore/calm preferred
- Visibility Proxy: 20% - Based on rainfall, discharge, advisories
- Tide: 10% - Site-specific preferences
- Time of Day: 10% - Early AM favored

Safety Gates (binary rejection):
- Brown water advisory active
- Effective surf height exceeds site threshold (typically >6ft)
- Raw offshore wave height exceeds absolute maximum (10ft) - extreme conditions
Note: High Surf Warning is NOT a safety gate - it's island-wide and informational only.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from src.utils.timezones import HST, now_hst

logger = logging.getLogger(__name__)

# Default config location: <repo>/config/config.yaml (this file lives at
# <repo>/src/core/scorer.py, so go up three levels).
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


class ScoreGrade(Enum):
    """Letter grade for dive conditions."""
    EXCELLENT = "A"
    GOOD = "B"
    FAIR = "C"
    POOR = "D"
    UNSAFE = "F"


@dataclass
class SafetyGate:
    """Result of a safety gate check."""
    passed: bool
    reason: Optional[str] = None
    gate_name: Optional[str] = None


@dataclass
class ScoringInput:
    """Input data for scoring a dive site."""
    # Wave conditions
    wave_height_ft: Optional[float] = None  # EFFECTIVE surf height at the site
    raw_wave_height_ft: Optional[float] = None  # raw offshore Hs (backstop gate only)
    wave_period_s: Optional[float] = None
    swell_direction_deg: Optional[float] = None

    # Wind conditions
    wind_speed_mph: Optional[float] = None
    wind_direction_deg: Optional[float] = None

    # Visibility factors
    rainfall_48h_inches: Optional[float] = None
    stream_discharge_cfs: Optional[float] = None
    brown_water_advisory: bool = False  # advisory NAME-matched to this site (safety gate)
    # Forecast probability of precipitation for the dive window (0-100). This is
    # a forecast, not an observation, so it only softly reduces the visibility
    # score (never below a floor) and does NOT count toward data_completeness.
    rain_chance_pct: Optional[float] = None
    # Advisory active on this site's COAST but not name-matched to the site.
    # Caps the visibility score but does not gate (unlike brown_water_advisory).
    coast_brown_water: bool = False

    # Tide
    tide_phase: Optional[str] = None  # "rising", "falling", "high", "low"
    water_level_ft: Optional[float] = None

    # Time
    evaluation_time: Optional[datetime] = None

    # Alerts
    high_surf_warning: bool = False
    high_surf_advisory: bool = False

    # Site context
    site_max_safe_height_ft: float = 6.0
    site_optimal_tide: str = "any"
    site_swell_exposure_primary: Optional[str] = None  # "N", "NW", "S", etc.


@dataclass
class ScoringResult:
    """Complete scoring result for a dive site."""
    total_score: float
    grade: ScoreGrade
    diveable: bool

    # Component scores (0-100)
    wave_power_score: float
    wind_score: float
    visibility_score: float
    tide_score: float
    time_score: float

    # Safety gate results
    safety_gates_passed: bool
    failed_gates: list[SafetyGate]

    # Computed values
    wave_power_index: Optional[float] = None
    wind_type: str = "unknown"  # "offshore", "onshore", "cross-shore", "unknown"

    # Fraction (0.0-1.0) of the 5 core scoring inputs backed by real data.
    data_completeness: float = 1.0

    # Recommendations
    summary: str = ""
    warnings: list[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class DiveScorer:
    """Scores dive conditions for a site based on environmental factors."""

    # Scoring weights (must sum to 1.0)
    WEIGHT_WAVE_POWER = 0.35
    WEIGHT_WIND = 0.25
    WEIGHT_VISIBILITY = 0.20
    WEIGHT_TIDE = 0.10
    WEIGHT_TIME = 0.10

    # Wave Power Index thresholds (calibrated for EFFECTIVE surf height, Plan 4).
    # WPI = effective_height_ft^2 * dominant_period_s. On effective heights with
    # real dominant periods the range is much wider than the old offshore world:
    # a modest 3.7 ft effective wave at 15 s is WPI ~210 yet is a legitimate ~C
    # dive day (NWS "West 4 ft"), so WPI_POOR=120 zeroed genuinely diveable
    # long-period conditions. Widened to 280 so long-period moderate swell grades
    # C/D rather than F (starting points were 12/120; see hindcast_audit.py).
    WPI_EXCELLENT = 12   # Score 100
    WPI_POOR = 280       # Score 0

    # Wind thresholds (mph)
    WIND_CALM = 5        # Score 100
    WIND_MODERATE = 15   # Score 50
    WIND_STRONG = 25     # Score 0

    # Visibility/discharge thresholds (cfs)
    DISCHARGE_LOW = 5    # Score 100
    DISCHARGE_HIGH = 50  # Score 0

    # Rainfall thresholds (inches in 48h)
    RAINFALL_NONE = 0.1   # Score 100
    RAINFALL_HEAVY = 2.0  # Score 0

    # Rain-chance (forecast PoP) thresholds. A forecast, not an observation, so
    # it only softly reduces visibility: full score at/below RAIN_CHANCE_NONE,
    # linear down to RAIN_CHANCE_FLOOR at 100% PoP (never below the floor).
    RAIN_CHANCE_NONE = 40    # PoP <= 40% -> score 100 (no penalty)
    RAIN_CHANCE_FLOOR = 40.0  # score floor at PoP 100% (never below this)

    # Cap on the visibility score when a brown-water advisory is active on the
    # site's COAST but not name-matched to the site (a soft, non-gating penalty).
    COAST_BROWN_WATER_CAP = 40.0

    # Safety gate thresholds
    MAX_WAVE_HEIGHT_FT = 6.0  # Default, can be site-specific (effective surf)
    # Hard ceiling on RAW offshore Hs regardless of site config. Raised from 8 to
    # 10 ft because 8 ft raw offshore Hs occurs in ordinary trade windswell and
    # is not itself extreme; 10 ft raw Hs indicates genuinely extreme seas.
    ABSOLUTE_MAX_WAVE_HEIGHT_FT = 10.0

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the scorer, loading tunable constants from config.yaml.

        The class-level constants above are the authoritative DEFAULTS. At init
        we overlay any values found in config/config.yaml as instance attributes
        (which shadow the class attributes the scoring methods read via self.*),
        so the two no longer have to be hand-synced. A missing or malformed
        config file is non-fatal: the defaults stand.
        """
        self._load_config(config_path)

    def _load_config(self, config_path: Optional[Path] = None) -> None:
        """Overlay scoring constants from config.yaml onto this instance.

        Only keys present in the file are applied; anything absent keeps the
        class default. Errors (missing file, bad YAML, missing 'scoring' block)
        are logged and swallowed so the scorer always constructs.
        """
        path = config_path or _DEFAULT_CONFIG_PATH
        try:
            import yaml
            with open(path) as f:
                cfg = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.info("Scorer config %s not found; using built-in defaults", path)
            return
        except Exception as e:  # bad YAML, IO error, missing pyyaml
            logger.warning("Failed to load scorer config %s (%s); using defaults", path, e)
            return

        scoring = (cfg or {}).get("scoring") or {}

        def _set(attr: str, value) -> None:
            if value is not None:
                setattr(self, attr, value)

        wpi = scoring.get("wpi_thresholds") or {}
        _set("WPI_EXCELLENT", wpi.get("excellent"))
        _set("WPI_POOR", wpi.get("poor"))

        wind = scoring.get("wind_thresholds") or {}
        _set("WIND_CALM", wind.get("calm"))
        _set("WIND_MODERATE", wind.get("moderate"))
        _set("WIND_STRONG", wind.get("strong"))

        discharge = scoring.get("discharge_thresholds") or {}
        _set("DISCHARGE_LOW", discharge.get("low"))
        _set("DISCHARGE_HIGH", discharge.get("high"))

        rainfall = scoring.get("rainfall_thresholds") or {}
        _set("RAINFALL_NONE", rainfall.get("none"))
        _set("RAINFALL_HEAVY", rainfall.get("heavy"))

        rain_chance = scoring.get("rain_chance") or {}
        _set("RAIN_CHANCE_NONE", rain_chance.get("none"))
        _set("RAIN_CHANCE_FLOOR", rain_chance.get("floor"))

        _set("COAST_BROWN_WATER_CAP", scoring.get("coast_brown_water_cap"))

        weights = scoring.get("weights") or {}
        _set("WEIGHT_WAVE_POWER", weights.get("wave_power"))
        _set("WEIGHT_WIND", weights.get("wind"))
        _set("WEIGHT_VISIBILITY", weights.get("visibility"))
        _set("WEIGHT_TIDE", weights.get("tide"))
        _set("WEIGHT_TIME", weights.get("time_of_day"))

        safety = (cfg or {}).get("safety") or {}
        _set("ABSOLUTE_MAX_WAVE_HEIGHT_FT", safety.get("absolute_max_wave_height"))
        _set("MAX_WAVE_HEIGHT_FT", safety.get("default_max_safe_wave_height"))

    def calculate_wave_power_index(
        self,
        height_ft: Optional[float],
        period_s: Optional[float],
    ) -> Optional[float]:
        """Calculate Wave Power Index = height² × period.

        Args:
            height_ft: Wave height in feet
            period_s: Wave period in seconds

        Returns:
            Wave Power Index, or None if inputs missing
        """
        if height_ft is None or period_s is None:
            return None

        if height_ft < 0 or period_s <= 0:
            return None

        return (height_ft ** 2) * period_s

    def check_safety_gates(self, inputs: ScoringInput) -> tuple[bool, list[SafetyGate]]:
        """Check all safety gates.

        Note: High Surf Warning is NOT a safety gate - it's island-wide and may not
        apply to all coasts. Sites are evaluated based on actual local wave height.

        Args:
            inputs: Scoring inputs

        Returns:
            Tuple of (all_passed, list_of_failed_gates)
        """
        failed_gates = []

        # Gate 1: Brown water advisory (site-specific water quality issue)
        if inputs.brown_water_advisory:
            failed_gates.append(SafetyGate(
                passed=False,
                reason="Brown Water Advisory active - poor visibility and water quality",
                gate_name="brown_water_advisory",
            ))

        # Gate 2: Effective surf height exceeds site threshold
        max_height = inputs.site_max_safe_height_ft if inputs.site_max_safe_height_ft is not None else self.MAX_WAVE_HEIGHT_FT
        if inputs.wave_height_ft is not None and inputs.wave_height_ft > max_height:
            failed_gates.append(SafetyGate(
                passed=False,
                reason=f"Effective surf ({inputs.wave_height_ft:.1f}ft) exceeds safe threshold ({max_height}ft)",
                gate_name="wave_height_exceeded",
            ))

        # Gate 3: Absolute ceiling on RAW offshore Hs (regardless of site config).
        # Uses the untransformed offshore height so genuinely extreme seas gate a
        # site even if it is directionally shadowed from them.
        backstop_height = (
            inputs.raw_wave_height_ft
            if inputs.raw_wave_height_ft is not None
            else inputs.wave_height_ft
        )
        if backstop_height is not None and backstop_height > self.ABSOLUTE_MAX_WAVE_HEIGHT_FT:
            if not any(g.gate_name == "wave_height_exceeded" for g in failed_gates):
                failed_gates.append(SafetyGate(
                    passed=False,
                    reason=f"Offshore wave height ({backstop_height:.1f}ft) exceeds absolute maximum ({self.ABSOLUTE_MAX_WAVE_HEIGHT_FT}ft) - extreme conditions",
                    gate_name="wave_height_exceeded",
                ))

        all_passed = len(failed_gates) == 0
        return all_passed, failed_gates

    def score_wave_power(self, wpi: Optional[float]) -> float:
        """Score based on Wave Power Index (computed on effective surf height).

        100 points if WPI <= WPI_EXCELLENT
        Linear decline to 0 at WPI >= WPI_POOR

        Args:
            wpi: Wave Power Index

        Returns:
            Score 0-100
        """
        if wpi is None:
            return 40.0  # Conservative score when data unavailable

        if wpi <= self.WPI_EXCELLENT:
            return 100.0
        elif wpi >= self.WPI_POOR:
            return 0.0
        else:
            # Linear interpolation
            return 100.0 * (self.WPI_POOR - wpi) / (self.WPI_POOR - self.WPI_EXCELLENT)

    def score_wind(
        self,
        wind_speed_mph: Optional[float],
        wind_direction_deg: Optional[float],
        site_exposure_primary: Optional[str],
    ) -> tuple[float, str]:
        """Score wind conditions based on speed and offshore/onshore direction.

        Offshore wind (land→sea): GOOD for diving - flattens waves, better visibility
        Onshore wind (sea→land): BAD for diving - choppy water, poor visibility

        Args:
            wind_speed_mph: Wind speed
            wind_direction_deg: Wind direction (where it's coming FROM)
            site_exposure_primary: Site's primary exposure direction (shore faces this way)

        Returns:
            Tuple of (score 0-100, wind_type description)
        """
        if wind_speed_mph is None:
            return 50.0, "unknown"

        # Base score from wind speed (calm is always good)
        if wind_speed_mph <= self.WIND_CALM:
            speed_score = 100.0
        elif wind_speed_mph >= self.WIND_STRONG:
            speed_score = 20.0  # Even strong offshore can be OK
        else:
            speed_score = 100.0 - (80.0 * (wind_speed_mph - self.WIND_CALM) / (self.WIND_STRONG - self.WIND_CALM))

        # Calculate offshore/onshore factor and adjust score significantly
        wind_type = "variable"
        if wind_direction_deg is not None and site_exposure_primary is not None:
            offshore_factor = self._calculate_offshore_factor(
                wind_direction_deg,
                site_exposure_primary
            )

            # offshore_factor: 1.0 = pure offshore, -1.0 = pure onshore, 0 = cross-shore

            if offshore_factor > 0.5:
                # Offshore wind - GOOD: boost score, especially at higher wind speeds
                wind_type = "offshore"
                # Offshore wind at 10-15 mph can actually improve conditions
                speed_score = min(100.0, speed_score + (offshore_factor * 30))
            elif offshore_factor < -0.5:
                # Onshore wind - BAD: significant penalty
                wind_type = "onshore"
                # Onshore wind is bad, especially with higher speeds
                penalty = abs(offshore_factor) * (40 + wind_speed_mph * 2)
                speed_score = max(0.0, speed_score - penalty)
            else:
                # Cross-shore wind - neutral to slightly negative
                wind_type = "cross-shore"
                speed_score = speed_score * 0.9

        return max(0.0, min(100.0, speed_score)), wind_type

    def _calculate_offshore_factor(
        self,
        wind_direction_deg: float,
        site_exposure: str,
    ) -> float:
        """Calculate offshore/onshore factor.

        Offshore = wind blowing FROM land TO sea (opposite of shore facing direction)
        Onshore = wind blowing FROM sea TO land (same as shore facing direction)

        Args:
            wind_direction_deg: Wind direction (where wind comes FROM, 0-360)
            site_exposure: Site's primary exposure direction (direction shore faces)

        Returns:
            Factor from -1 to +1:
              +1.0 = pure offshore (wind from opposite direction of shore)
              -1.0 = pure onshore (wind from same direction as shore faces)
               0.0 = cross-shore (wind perpendicular to shore)
        """
        # Convert exposure to degrees (direction the shore/site faces)
        exposure_map = {
            "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
            "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
            "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
            "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
        }

        site_facing_deg = exposure_map.get(site_exposure.upper(), 0)

        # Calculate angle difference between wind direction and shore facing direction
        # If wind comes FROM the same direction the shore FACES = onshore
        # If wind comes FROM the opposite direction = offshore
        diff = wind_direction_deg - site_facing_deg

        # Normalize to -180 to +180
        while diff > 180:
            diff -= 360
        while diff < -180:
            diff += 360

        # diff = 0: wind from same direction as shore faces = ONSHORE
        # diff = 180 or -180: wind from opposite direction = OFFSHORE
        # diff = 90 or -90: cross-shore

        # Convert to offshore factor: 180° diff = +1 (offshore), 0° diff = -1 (onshore)
        offshore_factor = (abs(diff) - 90) / 90.0

        return max(-1.0, min(1.0, offshore_factor))

    def score_visibility(
        self,
        rainfall_48h: Optional[float],
        discharge_cfs: Optional[float],
        brown_water_advisory: bool,
        rain_chance_pct: Optional[float] = None,
        coast_brown_water: bool = False,
    ) -> float:
        """Score visibility conditions based on runoff indicators.

        Uses rainfall, stream discharge, and advisories as proxies
        for underwater visibility.

        Args:
            rainfall_48h: OBSERVED rainfall in past 48 hours (inches)
            discharge_cfs: Stream discharge (cubic feet per second)
            brown_water_advisory: Whether a site-matched BWA is active
            rain_chance_pct: Forecast probability of precipitation (0-100) for the
                dive window. Soft penalty only; never drops the score below
                RAIN_CHANCE_FLOOR.
            coast_brown_water: Whether a BWA is active on the site's coast (not
                name-matched to the site). Caps the returned score.

        Returns:
            Score 0-100
        """
        # Brown water advisory is severe - major penalty
        if brown_water_advisory:
            return 10.0  # Not zero because gates handle full rejection

        scores = []

        # Rainfall score (observed 48h totals)
        if rainfall_48h is not None:
            if rainfall_48h <= self.RAINFALL_NONE:
                scores.append(100.0)
            elif rainfall_48h >= self.RAINFALL_HEAVY:
                scores.append(0.0)
            else:
                scores.append(100.0 * (self.RAINFALL_HEAVY - rainfall_48h) /
                            (self.RAINFALL_HEAVY - self.RAINFALL_NONE))

        # Discharge score
        if discharge_cfs is not None:
            if discharge_cfs <= self.DISCHARGE_LOW:
                scores.append(100.0)
            elif discharge_cfs >= self.DISCHARGE_HIGH:
                scores.append(0.0)
            else:
                scores.append(100.0 * (self.DISCHARGE_HIGH - discharge_cfs) /
                            (self.DISCHARGE_HIGH - self.DISCHARGE_LOW))

        # Rain-chance (forecast PoP) score. Only a soft penalty: full score at
        # low PoP, linear down to a FLOOR (never below it) because it's a
        # forecast probability, not observed rain.
        if rain_chance_pct is not None:
            if rain_chance_pct <= self.RAIN_CHANCE_NONE:
                scores.append(100.0)
            elif rain_chance_pct >= 100:
                scores.append(self.RAIN_CHANCE_FLOOR)
            else:
                scores.append(
                    100.0 - (100.0 - self.RAIN_CHANCE_FLOOR)
                    * (rain_chance_pct - self.RAIN_CHANCE_NONE)
                    / (100.0 - self.RAIN_CHANCE_NONE)
                )

        if not scores:
            score = 70.0  # Assume decent visibility if no data
        else:
            # Use minimum score (conservative approach)
            score = min(scores)

        # A coast-level advisory (not name-matched to the site) caps visibility
        # but does not gate: nearby runoff likely, but conditions may be OK.
        if coast_brown_water:
            score = min(score, self.COAST_BROWN_WATER_CAP)

        return score

    def score_tide(
        self,
        tide_phase: Optional[str],
        site_optimal_tide: str,
    ) -> float:
        """Score tide conditions based on site preferences.

        Args:
            tide_phase: Current tide phase (rising/falling/high/low)
            site_optimal_tide: Site's preferred tide (any/high/low)

        Returns:
            Score 0-100
        """
        if site_optimal_tide == "any" or site_optimal_tide is None:
            return 100.0  # All tides work for this site

        if tide_phase is None:
            return 70.0  # Neutral when no data

        tide_phase = tide_phase.lower()
        site_optimal = site_optimal_tide.lower()

        # Exact match
        if site_optimal == tide_phase:
            return 100.0

        # Partial matches
        if site_optimal == "high":
            if tide_phase == "rising":
                return 80.0  # Approaching high
            elif tide_phase == "falling":
                return 60.0  # Just past high
            else:
                return 30.0  # Low tide

        elif site_optimal == "low":
            if tide_phase == "falling":
                return 80.0  # Approaching low
            elif tide_phase == "rising":
                return 60.0  # Just past low
            else:
                return 30.0  # High tide

        return 70.0  # Default

    def score_time_of_day(
        self,
        evaluation_time: Optional[datetime],
    ) -> float:
        """Score based on time of day.

        Early AM (5-9) is best for diving due to:
        - Calmer winds
        - Better visibility
        - Less boat traffic

        Args:
            evaluation_time: Time to evaluate

        Returns:
            Score 0-100
        """
        if evaluation_time is None:
            # No explicit time: use "now" in Hawaii time, not the process TZ
            # (which is UTC in CI and would misgrade the dawn dive window).
            evaluation_time = now_hst()

        # If the caller passed a tz-aware time, convert to HST before reading the
        # hour so a 15:30 UTC scheduled run is scored as the 05:30 HST dawn window.
        if evaluation_time.tzinfo is not None:
            evaluation_time = evaluation_time.astimezone(HST)

        hour = evaluation_time.hour

        # Scoring by hour (Hawaii time assumed)
        if 5 <= hour < 7:
            return 100.0  # Dawn - excellent
        elif 7 <= hour < 9:
            return 95.0   # Early morning - great
        elif 9 <= hour < 11:
            return 80.0   # Mid-morning - good
        elif 11 <= hour < 14:
            return 60.0   # Midday - winds pick up
        elif 14 <= hour < 17:
            return 50.0   # Afternoon - typically windier
        elif 17 <= hour < 19:
            return 70.0   # Late afternoon - winds may calm
        else:
            return 40.0   # Night/early AM darkness

    def calculate_score(self, inputs: ScoringInput) -> ScoringResult:
        """Calculate complete dive condition score.

        Args:
            inputs: All input conditions

        Returns:
            Complete scoring result
        """
        warnings = []

        # Check safety gates first
        gates_passed, failed_gates = self.check_safety_gates(inputs)

        # If safety gates failed, return zero score
        if not gates_passed:
            return ScoringResult(
                total_score=0.0,
                grade=ScoreGrade.UNSAFE,
                diveable=False,
                wave_power_score=0.0,
                wind_score=0.0,
                visibility_score=0.0,
                tide_score=0.0,
                time_score=0.0,
                safety_gates_passed=False,
                failed_gates=failed_gates,
                wave_power_index=self.calculate_wave_power_index(
                    inputs.wave_height_ft, inputs.wave_period_s
                ),
                summary="CONDITIONS UNSAFE - " + "; ".join(g.reason for g in failed_gates),
                warnings=[g.reason for g in failed_gates],
            )

        # Calculate Wave Power Index
        wpi = self.calculate_wave_power_index(
            inputs.wave_height_ft,
            inputs.wave_period_s,
        )

        # Calculate component scores
        wave_power_score = self.score_wave_power(wpi)
        wind_score, wind_type = self.score_wind(
            inputs.wind_speed_mph,
            inputs.wind_direction_deg,
            inputs.site_swell_exposure_primary,
        )
        visibility_score = self.score_visibility(
            inputs.rainfall_48h_inches,
            inputs.stream_discharge_cfs,
            inputs.brown_water_advisory,
            rain_chance_pct=inputs.rain_chance_pct,
            coast_brown_water=inputs.coast_brown_water,
        )
        tide_score = self.score_tide(
            inputs.tide_phase,
            inputs.site_optimal_tide,
        )
        time_score = self.score_time_of_day(inputs.evaluation_time)

        # Calculate weighted total
        total_score = (
            wave_power_score * self.WEIGHT_WAVE_POWER +
            wind_score * self.WEIGHT_WIND +
            visibility_score * self.WEIGHT_VISIBILITY +
            tide_score * self.WEIGHT_TIDE +
            time_score * self.WEIGHT_TIME
        )

        # Determine grade
        grade = self._score_to_grade(total_score)

        # Assess data completeness across the 5 core scoring inputs. Missing data
        # is filled with conservative neutral defaults elsewhere, so a sparse
        # input set can still produce a middling grade that looks trustworthy.
        # Count how many of the 5 factors are backed by real data.
        factors_present = 0
        # 1. Wave (needs both height and period to be meaningful)
        if inputs.wave_height_ft is not None and inputs.wave_period_s is not None:
            factors_present += 1
        # 2. Wind speed
        if inputs.wind_speed_mph is not None:
            factors_present += 1
        # 3. Visibility (rainfall OR discharge)
        if inputs.rainfall_48h_inches is not None or inputs.stream_discharge_cfs is not None:
            factors_present += 1
        # 4. Tide phase. Sites where any tide works ("any") don't need a phase,
        #    so that factor counts as present.
        if inputs.tide_phase is not None or inputs.site_optimal_tide == "any":
            factors_present += 1
        # 5. Evaluation time
        if inputs.evaluation_time is not None:
            factors_present += 1

        data_completeness = factors_present / 5.0

        # Cap the grade (but not the numeric score) when data is too sparse to
        # trust, and surface why.
        if data_completeness < 0.6:
            warnings.append(
                f"Score based on incomplete data ({factors_present}/5 factors)"
            )
            grade = self._cap_grade_at_c(grade)

        # Add warnings for concerning conditions
        if inputs.high_surf_warning:
            warnings.append("High Surf Warning in effect for some areas")
        if inputs.high_surf_advisory:
            warnings.append("High Surf Advisory in effect - use caution")
        if wpi is not None and wpi > 120:
            warnings.append(f"Elevated wave power index ({wpi:.1f}) - challenging conditions")
        if wind_type == "onshore" and inputs.wind_speed_mph and inputs.wind_speed_mph > 10:
            warnings.append(f"Onshore winds ({inputs.wind_speed_mph:.0f} mph) - expect choppy conditions")
        elif inputs.wind_speed_mph is not None and inputs.wind_speed_mph > 20:
            warnings.append(f"Strong winds ({inputs.wind_speed_mph:.0f} mph)")
        if visibility_score < 50:
            warnings.append("Reduced visibility likely due to recent rainfall or runoff")

        # Generate summary
        summary = self._generate_summary(total_score, grade, wpi, inputs, wind_type)

        return ScoringResult(
            total_score=round(total_score, 1),
            grade=grade,
            diveable=total_score >= 40,  # Threshold for "diveable"
            wave_power_score=round(wave_power_score, 1),
            wind_score=round(wind_score, 1),
            visibility_score=round(visibility_score, 1),
            tide_score=round(tide_score, 1),
            time_score=round(time_score, 1),
            safety_gates_passed=True,
            failed_gates=[],
            wave_power_index=round(wpi, 2) if wpi is not None else None,
            wind_type=wind_type,
            data_completeness=round(data_completeness, 2),
            summary=summary,
            warnings=warnings,
        )

    def _cap_grade_at_c(self, grade: ScoreGrade) -> ScoreGrade:
        """Cap a grade at C (FAIR); grades already at/below C are unchanged."""
        if grade == ScoreGrade.EXCELLENT or grade == ScoreGrade.GOOD:
            return ScoreGrade.FAIR
        return grade

    def _score_to_grade(self, score: float) -> ScoreGrade:
        """Convert numeric score to letter grade.

        Args:
            score: Score 0-100

        Returns:
            Letter grade
        """
        if score >= 85:
            return ScoreGrade.EXCELLENT
        elif score >= 70:
            return ScoreGrade.GOOD
        elif score >= 55:
            return ScoreGrade.FAIR
        elif score >= 40:
            return ScoreGrade.POOR
        else:
            return ScoreGrade.UNSAFE

    def _generate_summary(
        self,
        score: float,
        grade: ScoreGrade,
        wpi: Optional[float],
        inputs: ScoringInput,
        wind_type: str = "unknown",
    ) -> str:
        """Generate human-readable summary.

        Args:
            score: Total score
            grade: Letter grade
            wpi: Wave Power Index
            inputs: Original inputs
            wind_type: Type of wind (offshore, onshore, cross-shore)

        Returns:
            Summary string
        """
        grade_descriptions = {
            ScoreGrade.EXCELLENT: "Excellent conditions",
            ScoreGrade.GOOD: "Good conditions",
            ScoreGrade.FAIR: "Fair conditions - some challenges",
            ScoreGrade.POOR: "Poor conditions - experienced divers only",
            ScoreGrade.UNSAFE: "Unsafe - diving not recommended",
        }

        summary_parts = [grade_descriptions[grade]]

        if inputs.wave_height_ft is not None:
            summary_parts.append(f"Waves: {inputs.wave_height_ft:.1f}ft")

        if inputs.wind_speed_mph is not None:
            wind_desc = f"Wind: {inputs.wind_speed_mph:.0f}mph"
            if wind_type == "offshore":
                wind_desc += " offshore ✓"
            elif wind_type == "onshore":
                wind_desc += " onshore ✗"
            summary_parts.append(wind_desc)

        if wpi is not None:
            # Thresholds track the recalibrated effective-height WPI scale
            # (WPI_EXCELLENT=12 .. WPI_POOR=280), not the old raw-offshore 10/25
            # scale which labelled ordinary long-period swell "Large swells".
            if wpi <= self.WPI_EXCELLENT:
                summary_parts.append("Calm seas")
            elif wpi < 120:
                summary_parts.append("Moderate swells")
            else:
                summary_parts.append("Large swells")

        return " | ".join(summary_parts)


def quick_score(
    wave_height_ft: float,
    wave_period_s: float,
    wind_speed_mph: float = 10,
    site_max_height_ft: float = 6.0,
) -> ScoringResult:
    """Quick scoring with minimal inputs.

    Convenience function for simple scoring.

    Args:
        wave_height_ft: Wave height in feet
        wave_period_s: Wave period in seconds
        wind_speed_mph: Wind speed (default 10)
        site_max_height_ft: Site's max safe wave height

    Returns:
        ScoringResult
    """
    scorer = DiveScorer()
    inputs = ScoringInput(
        wave_height_ft=wave_height_ft,
        wave_period_s=wave_period_s,
        wind_speed_mph=wind_speed_mph,
        site_max_safe_height_ft=site_max_height_ft,
    )
    return scorer.calculate_score(inputs)
