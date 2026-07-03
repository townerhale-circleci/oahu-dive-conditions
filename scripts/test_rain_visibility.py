#!/usr/bin/env python3
"""Tests for Plan 3 (rain + visibility) changes.

Run from project root:
    python scripts/test_rain_visibility.py

Covers:
  1. Observed 48h rainfall drops the visibility score and the grade.
  2. rain_chance (forecast PoP) is a soft, floored penalty; alone it can't zero a site.
  3. coast_brown_water caps visibility at 40 but does NOT gate; site-matched BWA gates.
  4. CWB coast mapping: active+fresh advisory maps to a coast; stale/non-active dropped.
  5. IEM hourly-max dedup aggregation (pure function).
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.scorer import DiveScorer, ScoringInput, ScoreGrade
from src.clients.cwb_client import (
    is_advisory_current,
    CWBClient,
    _parse_posted_date,
)
from src.clients.iem_precip_client import aggregate_48h_precip


def test_observed_rain_drops_grade():
    """2 in observed in the last 48h -> low visibility and a worse grade than dry."""
    scorer = DiveScorer()

    # score_visibility directly: 2.0 in observed -> 0 (>= RAINFALL_HEAVY).
    vis_wet = scorer.score_visibility(rainfall_48h=2.0, discharge_cfs=None,
                                      brown_water_advisory=False)
    assert vis_wet <= 5.0, f"2.0in observed should score ~0, got {vis_wet}"

    vis_dry = scorer.score_visibility(rainfall_48h=0.0, discharge_cfs=None,
                                      brown_water_advisory=False)
    assert vis_dry == 100.0, f"0.0in should score 100, got {vis_dry}"

    # Full ScoringInput: identical except rainfall. Wet must grade <= dry.
    base = dict(
        wave_height_ft=1.0, wave_period_s=8, wind_speed_mph=3,
        tide_phase="rising", site_optimal_tide="high",
        evaluation_time=datetime(2024, 6, 15, 7, 0),
        site_max_safe_height_ft=6,
    )
    wet = scorer.calculate_score(ScoringInput(rainfall_48h_inches=2.0, **base))
    dry = scorer.calculate_score(ScoringInput(rainfall_48h_inches=0.0, **base))
    assert wet.total_score < dry.total_score, (wet.total_score, dry.total_score)
    grade_order = ["A", "B", "C", "D", "F"]
    assert grade_order.index(wet.grade.value) >= grade_order.index(dry.grade.value), \
        f"wet grade {wet.grade.value} should be <= dry grade {dry.grade.value}"

    print(f"  [OK] observed rain: 2.0in vis={vis_wet}, dry vis={vis_dry}; "
          f"grade {dry.grade.value} -> {wet.grade.value}")
    return True


def test_rain_chance_soft_penalty():
    """rain_chance: 40->100, 70->between floor and 100, 100->floor; never zeroes."""
    scorer = DiveScorer()

    v40 = scorer.score_visibility(None, None, False, rain_chance_pct=40)
    assert v40 == 100.0, f"PoP 40 should be 100 (no penalty), got {v40}"

    v70 = scorer.score_visibility(None, None, False, rain_chance_pct=70)
    assert 40.0 < v70 < 100.0, f"PoP 70 should be between 40 and 100, got {v70}"
    # Linear: 40->100, 100->40. At 70: 100 - 60*(30/60) = 70.
    assert abs(v70 - 70.0) < 1e-9, v70

    v100 = scorer.score_visibility(None, None, False, rain_chance_pct=100)
    assert v100 == 40.0, f"PoP 100 should hit floor 40, got {v100}"

    # rain_chance alone does not zero a site (still diveable given calm else).
    res = scorer.calculate_score(ScoringInput(
        wave_height_ft=1.0, wave_period_s=8, wind_speed_mph=3,
        rain_chance_pct=100, tide_phase="rising", site_optimal_tide="any",
        evaluation_time=datetime(2024, 6, 15, 7, 0), site_max_safe_height_ft=6,
    ))
    assert res.diveable, f"rain_chance alone must not zero a site: {res.total_score}"
    assert res.visibility_score >= 40.0, res.visibility_score

    # rain_chance alone must NOT count factor 3 (visibility) as present.
    completeness_check = scorer.calculate_score(ScoringInput(
        wind_speed_mph=3, rain_chance_pct=80, site_max_safe_height_ft=6,
    ))
    # wind (1) + tide "any" (1) = 2/5; rain_chance must not add a 3rd.
    assert abs(completeness_check.data_completeness - 0.4) < 1e-9, \
        completeness_check.data_completeness

    print(f"  [OK] rain_chance: 40->{v40}, 70->{v70}, 100->{v100}; "
          f"alone diveable={res.diveable}, completeness unaffected")
    return True


def test_coast_brown_water_caps_not_gates():
    """coast_brown_water caps visibility at 40 but is not a gate; site BWA gates."""
    scorer = DiveScorer()

    # Cap: clean inputs would give vis=100, cap pulls it to 40.
    v = scorer.score_visibility(rainfall_48h=0.0, discharge_cfs=None,
                                brown_water_advisory=False, coast_brown_water=True)
    assert v == 40.0, f"coast_brown_water should cap at 40, got {v}"

    # Not a gate: a site with coast_brown_water can still be diveable.
    res = scorer.calculate_score(ScoringInput(
        wave_height_ft=1.0, wave_period_s=8, wind_speed_mph=3,
        rainfall_48h_inches=0.0, coast_brown_water=True,
        tide_phase="rising", site_optimal_tide="any",
        evaluation_time=datetime(2024, 6, 15, 7, 0), site_max_safe_height_ft=6,
    ))
    assert res.diveable, f"coast_brown_water must not gate: {res.total_score}"
    assert res.visibility_score == 40.0, res.visibility_score

    # Site-matched brown_water_advisory STILL gates (score 0, not diveable).
    gated = scorer.calculate_score(ScoringInput(
        wave_height_ft=1.0, wave_period_s=8, wind_speed_mph=3,
        rainfall_48h_inches=0.0, brown_water_advisory=True,
        evaluation_time=datetime(2024, 6, 15, 7, 0), site_max_safe_height_ft=6,
    ))
    assert not gated.diveable and gated.total_score == 0.0, \
        (gated.diveable, gated.total_score)
    assert gated.grade == ScoreGrade.UNSAFE

    print(f"  [OK] coast_brown_water caps vis to {v} (diveable={res.diveable}); "
          f"site BWA gates (score={gated.total_score})")
    return True


def test_cwb_coast_mapping():
    """Active+fresh advisory maps to windward; stale/non-active dropped."""
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    ten_days_ago = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")

    fresh = {"beach": "Kailua Beach Park", "island": "Oahu",
             "status": "active", "posted_date": yesterday}
    stale = {"beach": "Kailua Beach Park", "island": "Oahu",
             "status": "active", "posted_date": ten_days_ago}
    inactive = {"beach": "Kailua Beach Park", "island": "Oahu",
                "status": "cancelled", "posted_date": yesterday}

    assert is_advisory_current(fresh), "fresh active advisory should be current"
    assert not is_advisory_current(stale), "10-day-old advisory should be dropped"
    assert not is_advisory_current(inactive), "cancelled advisory should be dropped"

    # Coast mapping via the client, injecting the advisories through a stubbed
    # get_oahu_advisories (no network). Only the fresh one should map to windward.
    client = CWBClient()
    client.get_oahu_advisories = lambda use_cache=True: [
        a for a in (fresh, stale, inactive) if is_advisory_current(a)
    ]
    coast_map = client.get_coast_advisories("Oahu")
    assert "windward" in coast_map, coast_map
    assert len(coast_map["windward"]) == 1, coast_map
    assert coast_map["windward"][0]["beach"] == "Kailua Beach Park"
    # No other coast should be populated.
    assert set(coast_map.keys()) == {"windward"}, coast_map

    # Unparseable posted_date is kept (defensive).
    weird = {"beach": "Ala Moana", "island": "Oahu",
             "status": "active", "posted_date": "not-a-date"}
    assert is_advisory_current(weird), "unparseable date should be kept"
    assert _parse_posted_date("not-a-date") is None

    print(f"  [OK] CWB mapping: fresh Kailua -> windward, stale/inactive dropped; "
          f"coasts={list(coast_map)}")
    return True


def test_iem_hourly_max_dedup():
    """p01i repeats within a clock hour; sum of per-hour maxes, trace/empty -> 0."""
    # Synthetic: obs every 20 min, p01i rises within the hour (running accum).
    # Hour 02: 0.01, 0.03, 0.03 -> max 0.03. Hour 03: 0.05, 0.05 -> 0.05.
    # Hour 04: trace (0.0001) and empty -> 0.0.
    rows = [
        ("2024-12-06 02:00", "0.01"),
        ("2024-12-06 02:20", "0.03"),
        ("2024-12-06 02:40", "0.03"),
        ("2024-12-06 02:53", "0.03"),
        ("2024-12-06 03:00", "0.05"),
        ("2024-12-06 03:20", "0.05"),
        ("2024-12-06 03:53", "0.05"),
        ("2024-12-06 04:00", "0.0001"),  # trace -> 0
        ("2024-12-06 04:20", ""),        # empty -> skipped
        ("2024-12-06 04:53", "0.00"),
    ]
    total, newest = aggregate_48h_precip(rows)
    # 0.03 (hour 02 max) + 0.05 (hour 03 max) + 0.0 (hour 04) = 0.08
    assert abs(total - 0.08) < 1e-9, f"expected 0.08, got {total}"
    assert newest == datetime(2024, 12, 6, 4, 53), newest

    # All-empty -> None total, None newest.
    empty_total, empty_newest = aggregate_48h_precip([("2024-12-06 01:00", "")])
    assert empty_total is None or empty_total == 0.0, empty_total
    # newest should still be tracked from the timestamp even with empty value.
    assert empty_newest == datetime(2024, 12, 6, 1, 0), empty_newest

    # No rows at all -> (None, None).
    none_total, none_newest = aggregate_48h_precip([])
    assert none_total is None and none_newest is None

    print(f"  [OK] IEM aggregation: per-hour max sum = {total}, trace/empty -> 0")
    return True


def run_all_tests():
    print("\n" + "#" * 60)
    print("# PLAN 3 RAIN + VISIBILITY TEST SUITE")
    print("#" * 60 + "\n")

    tests = [
        ("Observed rain drops grade", test_observed_rain_drops_grade),
        ("rain_chance soft floored penalty", test_rain_chance_soft_penalty),
        ("coast_brown_water caps not gates", test_coast_brown_water_caps_not_gates),
        ("CWB coast mapping + freshness", test_cwb_coast_mapping),
        ("IEM hourly-max dedup", test_iem_hourly_max_dedup),
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
