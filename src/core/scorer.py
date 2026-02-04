"""Dive condition scoring algorithm using Wave Power Index.

Scoring approach:
- Wave Power Index = height² × period
- Total score is weighted combination of multiple factors
- Safety gates can force score to 0 regardless of conditions

Scoring Factors (weights):
- Wave Power: 35% - Lower power = better conditions
- Wind: 25% - Offshore/calm preferred
- Visibility Proxy: 20% - Based on rainfall, discharge, advisories
- Tide: 10% - Site-specific preferences
- Time of Day: 10% - Early AM favored

Safety Gates (binary rejection):
- High surf warning active
- Brown water advisory active
- Wave height exceeds site threshold (typically >6ft)
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


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
    wave_height_ft: Optional[float] = None
    wave_period_s: Optional[float] = None
    swell_direction_deg: Optional[float] = None

    # Wind conditions
    wind_speed_mph: Optional[float] = None
    wind_direction_deg: Optional[float] = None

    # Visibility factors
    rainfall_48h_inches: Optional[float] = None
    stream_discharge_cfs: Optional[float] = None
    brown_water_advisory: bool = False

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

    # Wave Power Index thresholds
    WPI_EXCELLENT = 5    # Score 100
    WPI_POOR = 50        # Score 0

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

    # Safety gate thresholds
    MAX_WAVE_HEIGHT_FT = 6.0  # Default, can be site-specific

    def __init__(self):
        """Initialize the scorer."""
        pass

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

        # Gate 2: Wave height exceeds site threshold
        max_height = inputs.site_max_safe_height_ft or self.MAX_WAVE_HEIGHT_FT
        if inputs.wave_height_ft is not None and inputs.wave_height_ft > max_height:
            failed_gates.append(SafetyGate(
                passed=False,
                reason=f"Wave height ({inputs.wave_height_ft:.1f}ft) exceeds safe threshold ({max_height}ft)",
                gate_name="wave_height_exceeded",
            ))

        all_passed = len(failed_gates) == 0
        return all_passed, failed_gates

    def score_wave_power(self, wpi: Optional[float]) -> float:
        """Score based on Wave Power Index.

        100 points if WPI < 5
        Linear decline to 0 at WPI >= 50

        Args:
            wpi: Wave Power Index

        Returns:
            Score 0-100
        """
        if wpi is None:
            return 50.0  # Neutral score when data unavailable

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
    ) -> float:
        """Score visibility conditions based on runoff indicators.

        Uses rainfall, stream discharge, and advisories as proxies
        for underwater visibility.

        Args:
            rainfall_48h: Rainfall in past 48 hours (inches)
            discharge_cfs: Stream discharge (cubic feet per second)
            brown_water_advisory: Whether BWA is active

        Returns:
            Score 0-100
        """
        # Brown water advisory is severe - major penalty
        if brown_water_advisory:
            return 10.0  # Not zero because gates handle full rejection

        scores = []

        # Rainfall score
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

        if not scores:
            return 70.0  # Assume decent visibility if no data

        # Use minimum score (conservative approach)
        return min(scores)

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
            evaluation_time = datetime.now()

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

        # Add warnings for concerning conditions
        if inputs.high_surf_warning:
            warnings.append("High Surf Warning in effect for some areas")
        if inputs.high_surf_advisory:
            warnings.append("High Surf Advisory in effect - use caution")
        if wpi is not None and wpi > 20:
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
            wave_power_index=round(wpi, 2) if wpi else None,
            wind_type=wind_type,
            summary=summary,
            warnings=warnings,
        )

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
            if wpi < 10:
                summary_parts.append("Calm seas")
            elif wpi < 25:
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
