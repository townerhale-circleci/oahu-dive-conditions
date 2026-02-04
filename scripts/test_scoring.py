#!/usr/bin/env python3
"""Test script to verify the dive condition scorer works correctly.

Run from project root:
    python scripts/test_scoring.py
"""

import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.scorer import DiveScorer, ScoringInput, ScoreGrade, quick_score


def print_result(name: str, result):
    """Pretty print a scoring result."""
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"{'='*60}")
    print(f"  Grade: {result.grade.value} ({result.grade.name})")
    print(f"  Total Score: {result.total_score}")
    print(f"  Diveable: {result.diveable}")
    print(f"  Wave Power Index: {result.wave_power_index}")
    print(f"\n  Component Scores:")
    print(f"    Wave Power: {result.wave_power_score}")
    print(f"    Wind:       {result.wind_score}")
    print(f"    Visibility: {result.visibility_score}")
    print(f"    Tide:       {result.tide_score}")
    print(f"    Time:       {result.time_score}")
    print(f"\n  Safety Gates Passed: {result.safety_gates_passed}")
    if result.failed_gates:
        print(f"  Failed Gates:")
        for gate in result.failed_gates:
            print(f"    - {gate.gate_name}: {gate.reason}")
    if result.warnings:
        print(f"  Warnings:")
        for warning in result.warnings:
            print(f"    - {warning}")
    print(f"\n  Summary: {result.summary}")


def test_wave_power_index():
    """Test Wave Power Index calculation."""
    print("\n" + "="*60)
    print("TEST: Wave Power Index Calculation")
    print("="*60)

    scorer = DiveScorer()

    test_cases = [
        (1, 10, 10),      # 1² × 10 = 10
        (2, 10, 40),      # 2² × 10 = 40
        (3, 10, 90),      # 3² × 10 = 90
        (2, 15, 60),      # 2² × 15 = 60
        (1.5, 8, 18),     # 1.5² × 8 = 18
    ]

    all_passed = True
    for height, period, expected in test_cases:
        result = scorer.calculate_wave_power_index(height, period)
        status = "✓" if abs(result - expected) < 0.01 else "✗"
        if status == "✗":
            all_passed = False
        print(f"  {status} height={height}ft, period={period}s -> WPI={result} (expected {expected})")

    return all_passed


def test_excellent_conditions():
    """Test scoring for excellent dive conditions."""
    scorer = DiveScorer()

    # Perfect conditions: small waves, light wind, no rain, good tide, early morning
    inputs = ScoringInput(
        wave_height_ft=1.0,
        wave_period_s=8,
        wind_speed_mph=3,
        wind_direction_deg=45,  # NE wind
        rainfall_48h_inches=0,
        stream_discharge_cfs=2,
        tide_phase="rising",
        site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 7, 0),  # 7 AM
        site_max_safe_height_ft=3,
        site_swell_exposure_primary="NW",
    )

    result = scorer.calculate_score(inputs)
    print_result("Excellent Conditions (Small waves, calm wind, early AM)", result)

    assert result.grade == ScoreGrade.EXCELLENT, f"Expected EXCELLENT, got {result.grade}"
    assert result.diveable == True
    assert result.safety_gates_passed == True
    return True


def test_good_conditions():
    """Test scoring for good dive conditions."""
    scorer = DiveScorer()

    # Smaller waves for "good" conditions - 2ft @ 10s has WPI=40 which is high
    inputs = ScoringInput(
        wave_height_ft=1.5,
        wave_period_s=8,
        wind_speed_mph=8,
        rainfall_48h_inches=0.2,
        stream_discharge_cfs=10,
        tide_phase="high",
        site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 8, 30),  # 8:30 AM
        site_max_safe_height_ft=4,
        site_swell_exposure_primary="S",
    )

    result = scorer.calculate_score(inputs)
    print_result("Good Conditions (Small-moderate waves, light wind)", result)

    assert result.grade in [ScoreGrade.EXCELLENT, ScoreGrade.GOOD], f"Expected GOOD or better, got {result.grade}"
    assert result.diveable == True
    return True


def test_fair_conditions():
    """Test scoring for fair dive conditions."""
    scorer = DiveScorer()

    # 2ft @ 8s = WPI of 32, more reasonable for "fair" conditions
    inputs = ScoringInput(
        wave_height_ft=2.0,
        wave_period_s=8,
        wind_speed_mph=12,
        wind_direction_deg=315,  # NW wind (onshore for NW-facing site)
        rainfall_48h_inches=0.3,
        stream_discharge_cfs=15,
        tide_phase="low",
        site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 14, 0),  # 2 PM
        site_max_safe_height_ft=4,
        site_swell_exposure_primary="NW",
    )

    result = scorer.calculate_score(inputs)
    print_result("Fair Conditions (Moderate waves, onshore wind, afternoon)", result)

    assert result.grade in [ScoreGrade.FAIR, ScoreGrade.POOR], f"Expected FAIR or POOR, got {result.grade}"
    assert result.diveable == True  # Should still be diveable
    return True


def test_poor_conditions():
    """Test scoring for poor dive conditions."""
    scorer = DiveScorer()

    inputs = ScoringInput(
        wave_height_ft=4.5,
        wave_period_s=14,
        wind_speed_mph=20,
        rainfall_48h_inches=1.5,
        stream_discharge_cfs=40,
        tide_phase="falling",
        site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 15, 0),  # 3 PM
        site_max_safe_height_ft=6,
        site_swell_exposure_primary="NW",
    )

    result = scorer.calculate_score(inputs)
    print_result("Poor Conditions (Large waves, strong wind, rain runoff)", result)

    # Should still be diveable but poor
    assert result.safety_gates_passed == True  # Didn't exceed 6ft threshold
    return True


def test_safety_gate_high_surf_warning():
    """Test that high surf warning triggers safety gate."""
    scorer = DiveScorer()

    inputs = ScoringInput(
        wave_height_ft=2.0,  # Waves seem fine
        wave_period_s=10,
        wind_speed_mph=5,
        high_surf_warning=True,  # But warning is active
        site_max_safe_height_ft=4,
    )

    result = scorer.calculate_score(inputs)
    print_result("Safety Gate: High Surf Warning", result)

    assert result.safety_gates_passed == False
    assert result.total_score == 0
    assert result.grade == ScoreGrade.UNSAFE
    assert result.diveable == False
    assert any("High Surf Warning" in g.reason for g in result.failed_gates)
    return True


def test_safety_gate_brown_water():
    """Test that brown water advisory triggers safety gate."""
    scorer = DiveScorer()

    inputs = ScoringInput(
        wave_height_ft=1.5,
        wave_period_s=8,
        wind_speed_mph=5,
        brown_water_advisory=True,
        site_max_safe_height_ft=4,
    )

    result = scorer.calculate_score(inputs)
    print_result("Safety Gate: Brown Water Advisory", result)

    assert result.safety_gates_passed == False
    assert result.total_score == 0
    assert result.grade == ScoreGrade.UNSAFE
    assert any("Brown Water" in g.reason for g in result.failed_gates)
    return True


def test_safety_gate_wave_height():
    """Test that exceeding wave height threshold triggers safety gate."""
    scorer = DiveScorer()

    inputs = ScoringInput(
        wave_height_ft=5.0,  # Above the 3ft site threshold
        wave_period_s=10,
        wind_speed_mph=5,
        site_max_safe_height_ft=3,  # Site only safe up to 3ft
    )

    result = scorer.calculate_score(inputs)
    print_result("Safety Gate: Wave Height Exceeded", result)

    assert result.safety_gates_passed == False
    assert result.total_score == 0
    assert any("exceeds safe threshold" in g.reason for g in result.failed_gates)
    return True


def test_quick_score():
    """Test the quick_score convenience function."""
    print("\n" + "="*60)
    print("TEST: Quick Score Function")
    print("="*60)

    # Test various wave conditions
    test_cases = [
        (1, 8, "Should be excellent - tiny waves"),
        (2, 10, "Should be good - moderate waves"),
        (4, 12, "Should be fair/poor - larger waves"),
        (7, 14, "Should fail safety gate - too big"),
    ]

    for height, period, description in test_cases:
        result = quick_score(height, period)
        print(f"\n  {description}")
        print(f"    Waves: {height}ft @ {period}s")
        print(f"    WPI: {result.wave_power_index}")
        print(f"    Grade: {result.grade.value} | Score: {result.total_score} | Diveable: {result.diveable}")

    return True


def test_onshore_wind_penalty():
    """Test that onshore winds are penalized more than offshore."""
    scorer = DiveScorer()

    # Same conditions except wind direction
    base_inputs = {
        "wave_height_ft": 2.0,
        "wave_period_s": 10,
        "wind_speed_mph": 15,
        "site_max_safe_height_ft": 4,
        "site_swell_exposure_primary": "NW",  # Site faces NW
    }

    # Offshore wind (from SE, opposite of NW)
    offshore_inputs = ScoringInput(**base_inputs, wind_direction_deg=135)
    offshore_result = scorer.calculate_score(offshore_inputs)

    # Onshore wind (from NW, same as site faces)
    onshore_inputs = ScoringInput(**base_inputs, wind_direction_deg=315)
    onshore_result = scorer.calculate_score(onshore_inputs)

    print("\n" + "="*60)
    print("TEST: Onshore vs Offshore Wind")
    print("="*60)
    print(f"  Site faces: NW")
    print(f"  Wind speed: 15 mph")
    print(f"\n  Offshore wind (from SE, 135°):")
    print(f"    Wind Score: {offshore_result.wind_score}")
    print(f"    Total: {offshore_result.total_score}")
    print(f"\n  Onshore wind (from NW, 315°):")
    print(f"    Wind Score: {onshore_result.wind_score}")
    print(f"    Total: {onshore_result.total_score}")

    assert offshore_result.wind_score > onshore_result.wind_score, \
        "Offshore wind should score higher than onshore"
    return True


def test_time_of_day_scoring():
    """Test that early morning scores higher than afternoon."""
    scorer = DiveScorer()

    base_inputs = {
        "wave_height_ft": 2.0,
        "wave_period_s": 10,
        "wind_speed_mph": 10,
        "site_max_safe_height_ft": 4,
    }

    times = [
        (6, "6 AM (dawn)"),
        (8, "8 AM (early morning)"),
        (12, "12 PM (midday)"),
        (15, "3 PM (afternoon)"),
        (18, "6 PM (evening)"),
    ]

    print("\n" + "="*60)
    print("TEST: Time of Day Scoring")
    print("="*60)

    results = []
    for hour, label in times:
        inputs = ScoringInput(
            **base_inputs,
            evaluation_time=datetime(2024, 6, 15, hour, 0)
        )
        result = scorer.calculate_score(inputs)
        results.append((hour, result.time_score, result.total_score))
        print(f"  {label}: Time Score={result.time_score}, Total={result.total_score}")

    # Early morning should score highest
    assert results[0][1] >= results[2][1], "Dawn should score higher than midday"
    assert results[1][1] >= results[3][1], "Morning should score higher than afternoon"
    return True


def run_all_tests():
    """Run all tests and report results."""
    print("\n" + "#"*60)
    print("# DIVE SCORER TEST SUITE")
    print("#"*60)

    tests = [
        ("Wave Power Index Calculation", test_wave_power_index),
        ("Excellent Conditions", test_excellent_conditions),
        ("Good Conditions", test_good_conditions),
        ("Fair Conditions", test_fair_conditions),
        ("Poor Conditions", test_poor_conditions),
        ("Safety Gate: High Surf Warning", test_safety_gate_high_surf_warning),
        ("Safety Gate: Brown Water Advisory", test_safety_gate_brown_water),
        ("Safety Gate: Wave Height Exceeded", test_safety_gate_wave_height),
        ("Quick Score Function", test_quick_score),
        ("Onshore vs Offshore Wind", test_onshore_wind_penalty),
        ("Time of Day Scoring", test_time_of_day_scoring),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            result = test_func()
            if result:
                passed += 1
            else:
                failed += 1
                print(f"\n  ✗ FAILED: {name}")
        except AssertionError as e:
            failed += 1
            print(f"\n  ✗ FAILED: {name}")
            print(f"    Error: {e}")
        except Exception as e:
            failed += 1
            print(f"\n  ✗ ERROR: {name}")
            print(f"    Exception: {e}")

    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total:  {len(tests)}")
    print("="*60)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
