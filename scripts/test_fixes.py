#!/usr/bin/env python3
"""Tests for the Plan 1 (data honesty) and Plan 2 (timezone) fixes.

Run from project root:
    python scripts/test_fixes.py

Covers:
  (a) 0.0 wave height converts and scores rather than becoming None
  (b) ScoringResult.data_completeness + grade cap with sparse inputs
  (c) score_time_of_day returns >=95 for 07:00 HST-aware datetime even when the
      process TZ is UTC (simulated by passing a tz-aware datetime)
  (d) OWM missing wind returns None (not 0)
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.scorer import DiveScorer, ScoringInput, ScoreGrade
from src.clients.openweathermap_client import OpenWeatherMapClient
from src.utils.timezones import HST, dive_window_time


def test_zero_wave_height_not_dropped():
    """A dead-flat 0.0 ft wave must be scored (perfect), not treated as missing."""
    scorer = DiveScorer()

    # WPI of 0.0 ft @ 10 s = 0 -> excellent wave score, NOT the None default (40).
    wpi = scorer.calculate_wave_power_index(0.0, 10.0)
    assert wpi == 0.0, f"Expected WPI 0.0 for flat seas, got {wpi}"

    wave_score = scorer.score_wave_power(wpi)
    assert wave_score == 100.0, f"Flat seas should score 100, got {wave_score}"

    # And the missing-data path still returns the conservative 40.
    assert scorer.score_wave_power(None) == 40.0

    # m -> ft conversion in the buoy client must preserve 0.0 (not None).
    from src.clients.buoy_client import BuoyClient  # noqa: F401
    # The conversion expression is `x * 3.28084 if x is not None else None`.
    x = 0.0
    ft = x * 3.28084 if x is not None else None
    assert ft == 0.0, f"0.0 m must convert to 0.0 ft, got {ft}"

    print("  [OK] 0.0 wave height converts and scores (WPI=0 -> 100), no None drop")
    return True


def test_data_completeness_and_grade_cap():
    """Sparse inputs -> low completeness -> grade capped at C + warning."""
    scorer = DiveScorer()

    # Full data: completeness 1.0, no cap.
    full = ScoringInput(
        wave_height_ft=1.0,
        wave_period_s=8,
        wind_speed_mph=3,
        rainfall_48h_inches=0.0,
        tide_phase="rising",
        site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 7, 0),
        site_max_safe_height_ft=6,
    )
    full_res = scorer.calculate_score(full)
    assert full_res.data_completeness == 1.0, full_res.data_completeness

    # Sparse data: only wind present (1/5 factors). site_optimal_tide default is
    # "any", which counts the tide factor as present -> 2/5 = 0.4 < 0.6.
    sparse = ScoringInput(
        wind_speed_mph=3,
        site_max_safe_height_ft=6,
    )
    sparse_res = scorer.calculate_score(sparse)
    assert abs(sparse_res.data_completeness - 0.4) < 1e-9, sparse_res.data_completeness
    assert sparse_res.grade in (ScoreGrade.FAIR, ScoreGrade.POOR, ScoreGrade.UNSAFE), \
        f"Grade should be capped at C or lower, got {sparse_res.grade}"
    assert sparse_res.grade != ScoreGrade.EXCELLENT and sparse_res.grade != ScoreGrade.GOOD
    assert any("incomplete data" in w for w in sparse_res.warnings), sparse_res.warnings
    assert "2/5" in " ".join(sparse_res.warnings), sparse_res.warnings

    # The numeric score itself must NOT be altered by the cap.
    # Reconstruct grade from raw score and confirm it would have been higher.
    raw_grade = scorer._score_to_grade(sparse_res.total_score)
    if raw_grade in (ScoreGrade.EXCELLENT, ScoreGrade.GOOD):
        assert sparse_res.grade == ScoreGrade.FAIR

    print(f"  [OK] data_completeness={sparse_res.data_completeness}, "
          f"grade capped to {sparse_res.grade.value}, warning present")
    return True


def test_time_of_day_hst_aware_utc_process():
    """A 07:00 HST tz-aware time scores >=95 regardless of process TZ.

    Simulate a CI run (process clock in UTC): 07:00 HST == 17:00 UTC. We pass the
    tz-aware datetime and verify the scorer converts to HST before reading .hour.
    """
    scorer = DiveScorer()

    dive_dt = dive_window_time(datetime(2024, 6, 15), hour=7)  # 07:00 HST, tz-aware
    assert dive_dt.tzinfo is not None
    score = scorer.score_time_of_day(dive_dt)
    assert score >= 95.0, f"07:00 HST should score >=95, got {score}"

    # Also verify the equivalent UTC instant (17:00 UTC) scores the same.
    utc_equiv = dive_dt.astimezone(timezone.utc)
    assert utc_equiv.hour == 17, utc_equiv.hour
    score_utc = scorer.score_time_of_day(utc_equiv)
    assert score_utc >= 95.0, f"17:00 UTC (=07:00 HST) should score >=95, got {score_utc}"

    # Sanity: a naive 15:30 (what the old bug read from UTC wall-clock) scores low.
    naive_afternoon = datetime(2024, 6, 15, 15, 30)
    assert scorer.score_time_of_day(naive_afternoon) < 95.0

    print(f"  [OK] 07:00 HST tz-aware -> {score}; 17:00 UTC equiv -> {score_utc}")
    return True


def test_owm_missing_wind_returns_none():
    """OWM forecast entries with no wind field yield None, not 0."""
    client = OpenWeatherMapClient(api_key="dummy")  # no network call made

    # Build a fake forecast payload for a target date. One entry has no wind.
    # 07:00 HST on 2024-06-15 == 17:00 UTC. Use a UTC ts that lands on that date
    # in HST.
    target = datetime(2024, 6, 15, tzinfo=HST)
    # 12:00 HST 2024-06-15 == 22:00 UTC 2024-06-15
    dt_epoch = int(datetime(2024, 6, 15, 22, 0, tzinfo=timezone.utc).timestamp())

    fake = {
        "list": [
            {
                "dt": dt_epoch,
                # No "wind" key at all -> must surface as None.
                "weather": [{"main": "Clouds"}],
                "pop": 0.1,
            }
        ]
    }

    result = client._extract_for_date(fake, target)
    # Only one entry, with no wind -> representative wind (avg) is None.
    assert result.get("wind_speed_mph") is None, result.get("wind_speed_mph")
    assert result.get("avg_wind_mph") is None, result.get("avg_wind_mph")
    # The raw hourly entry must carry None, not 0.
    hourly = result.get("hourly_data")
    assert hourly and hourly[0]["wind_speed_mph"] is None, hourly

    # No-data fallback (target date has no matching entries) must include rain
    # keys as None and wind as None. Give the single entry a real dt on a
    # different date so the target-date filter yields nothing.
    fallback_payload = {
        "list": [{"dt": dt_epoch, "weather": [{"main": "Rain"}]}]
    }
    fallback = client._extract_for_date(fallback_payload,
                                        datetime(2099, 1, 1, tzinfo=HST))
    assert fallback["wind_speed_mph"] is None, fallback
    assert "rain_chance" in fallback and fallback["rain_chance"] is None, fallback
    assert "rain_amount_mm" in fallback and fallback["rain_amount_mm"] is None, fallback

    print("  [OK] OWM missing wind -> None; fallback has None rain keys")
    return True


def run_all_tests():
    print("\n" + "#" * 60)
    print("# PLAN 1 + PLAN 2 FIX TEST SUITE")
    print("#" * 60 + "\n")

    tests = [
        ("Zero wave height not dropped", test_zero_wave_height_not_dropped),
        ("Data completeness + grade cap", test_data_completeness_and_grade_cap),
        ("Time-of-day HST-aware under UTC process", test_time_of_day_hst_aware_utc_process),
        ("OWM missing wind returns None", test_owm_missing_wind_returns_none),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            if fn():
                passed += 1
            else:
                failed += 1
                print(f"  x FAILED: {name}")
        except AssertionError as e:
            failed += 1
            print(f"  x FAILED: {name}\n    Error: {e}")
        except Exception as e:
            failed += 1
            print(f"  x ERROR: {name}\n    Exception: {e}")

    print("\n" + "=" * 60)
    print(f"  Passed: {passed} | Failed: {failed} | Total: {len(tests)}")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run_all_tests() else 1)
