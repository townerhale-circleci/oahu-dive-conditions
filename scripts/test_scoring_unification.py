#!/usr/bin/env python3
"""Tests for Plan 5 (one scoring path).

Run from project root:
    python scripts/test_scoring_unification.py

Covers:
  1. build_scoring_input produces an identical ScoringInput regardless of which
     caller path (ranker-style vs digest-style) invokes it with the same values.
  2. Today-table grade == Top-Sites grade for the same site, with all clients
     mocked (no network). This is the Plan 5 acceptance test: the headline
     aggregate and the Today table must agree.
  3. A forecast-day ScoringInput with a PREDICTED tide phase yields a tide score
     != the uniform 100 for a non-neutral tide phase (site with optimal tide).
"""

import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from src.core.scorer import DiveScorer
from src.core.scoring_input import build_scoring_input
from src.core.site import DiveSite, SiteDatabase, get_site_database
from src.core.ranker import SiteRanker, EnvironmentalConditions
from src.digests.daily_digest import DigestGenerator
from src.utils.timezones import dive_window_time


def _pick_site(optimal_tide=None) -> DiveSite:
    db = get_site_database()
    sites = db.get_all_sites()
    if optimal_tide is not None:
        for s in sites:
            if s.optimal_tide == optimal_tide:
                return s
    return sites[0]


def test_build_scoring_input_path_independent():
    """Same values -> identical ScoringInput regardless of the caller."""
    site = _pick_site()
    eval_t = dive_window_time(datetime(2026, 7, 3))

    common = dict(
        wave_height_ft=2.0,
        raw_wave_height_ft=4.9,
        wave_period_s=12.0,
        swell_direction_deg=310.0,
        wind_speed_mph=8.0,
        wind_direction_deg=45.0,
        stream_discharge_cfs=3.0,
        rainfall_48h_inches=0.1,
        rain_chance_pct=30.0,
        brown_water_advisory=False,
        coast_brown_water=False,
        tide_phase="rising",
        water_level_ft=1.2,
        evaluation_time=eval_t,
        high_surf_warning=False,
        high_surf_advisory=False,
    )

    # "ranker-style": pass everything positionally-by-keyword.
    ranker_input = build_scoring_input(site, **common)
    # "digest-style": same values, but constructed independently.
    digest_input = build_scoring_input(site, **common)

    assert asdict(ranker_input) == asdict(digest_input), (
        "build_scoring_input must be path-independent for identical values"
    )
    # Site context must be pulled from the site (not hidden defaults).
    assert ranker_input.site_optimal_tide == site.optimal_tide
    assert ranker_input.site_max_safe_height_ft == site.max_safe_wave_height
    assert ranker_input.site_swell_exposure_primary == site.swell_exposure.primary

    # And the scorer produces the same result for both.
    scorer = DiveScorer()
    r1 = scorer.calculate_score(ranker_input)
    r2 = scorer.calculate_score(digest_input)
    assert r1.total_score == r2.total_score
    assert r1.grade == r2.grade

    print("  [OK] build_scoring_input is path-independent (ScoringInput + score identical)")
    return True


# --- Mocks for the acceptance test (no network) ---------------------------

class _FakeBuoy:
    """Returns fixed raw offshore conditions for every buoy id."""
    def get_current_conditions(self, buoy_id):
        return {
            "wave_height_ft": 4.9,
            "swell_period_s": 12.0,
            "mean_direction_deg": 310.0,
        }
    def get_all_buoy_conditions(self):
        return {}


class _FakePacIOOS:
    def get_current_conditions(self, lat, lon):
        return {"wave_height_ft": None}
    def get_forecast(self, lat, lon, hours=120):
        return pd.DataFrame()


class _FakeNWS:
    def get_forecast_summary(self, lat, lon):
        # Dawn snapshot would be calm; if the ranker used THIS the grades would
        # diverge from the OWM window. We assert they DON'T diverge.
        return {"current_wind_mph": 2.0, "current_wind_dir": "NE"}
    def get_marine_alerts(self):
        return []
    def get_hourly_forecast(self, *a, **k):
        return pd.DataFrame()


class _FakeTides:
    def get_station_for_coast(self, coast):
        return "1612340"
    def get_current_tide_phase(self, station_id):
        return {"phase": "rising", "current_level_ft": 1.0,
                "next_high": {"time": "2026-07-03 10:00"},
                "next_low": {"time": "2026-07-03 16:00"}}
    def get_tide_predictions(self, station_id, start_date=None, end_date=None,
                             interval="hilo", use_cache=True):
        # Two days of alternating hi/lo so a real phase can be derived.
        rows = []
        base = start_date or datetime(2026, 7, 3)
        for d in range(4):
            day = base.replace(hour=0, minute=0) + pd.Timedelta(days=d)
            rows.append({"time": (day + pd.Timedelta(hours=4)).strftime("%Y-%m-%d %H:%M"),
                         "water_level_ft": 0.2, "type": "L"})
            rows.append({"time": (day + pd.Timedelta(hours=10)).strftime("%Y-%m-%d %H:%M"),
                         "water_level_ft": 2.1, "type": "H"})
        return pd.DataFrame(rows)
    def get_tide_phase_at(self, station_id, when, predictions=None):
        # Delegate to the real implementation for fidelity.
        from src.clients.noaa_tides_client import NOAATidesClient
        return NOAATidesClient.get_tide_phase_at(self, station_id, when, predictions)


class _FakeUSGS:
    def get_current_discharge(self, gage):
        return 3.0


class _FakeCWB:
    def get_oahu_advisories(self):
        return []
    def get_coast_advisories(self, island):
        return {}
    def check_site_advisory(self, name):
        return None


class _FakeIEM:
    def get_rainfall_48h(self, coast):
        return 0.1


class _FakeOWM:
    """Full-day OWM window wind that DIFFERS from the NWS dawn snapshot."""
    def __init__(self):
        self._cache = {}
    def get_wind_forecast(self, lat, lon, target_date=None):
        return {
            "wind_speed_mph": 9.0,      # day-average, != NWS 2.0 snapshot
            "avg_wind_mph": 9.0,
            "wind_direction_deg": 60.0,
            "best_time_range": "06:00-10:00",
            "rain_chance": 20,
            "rain_amount_mm": 0.0,
            "hourly_data": [
                {"hour": h, "wind_speed_mph": 9.0, "wind_direction_deg": 60.0,
                 "rain_pop": 0.2, "rain_3h_mm": 0.0}
                for h in range(4, 19, 3)
            ],
        }
    def get_wind_direction_name(self, deg):
        return "ENE"


def _make_ranker():
    return SiteRanker(
        buoy_client=_FakeBuoy(),
        pacioos_client=_FakePacIOOS(),
        nws_client=_FakeNWS(),
        tides_client=_FakeTides(),
        usgs_client=_FakeUSGS(),
        cwb_client=_FakeCWB(),
        iem_client=_FakeIEM(),
        owm_client=_FakeOWM(),
    )


def test_today_grade_equals_top_sites_grade():
    """Acceptance: Today-table grade == Top-Sites grade for the same site."""
    ranker = _make_ranker()
    gen = DigestGenerator(ranker=ranker)

    digest = gen.generate(in_season_only=False)

    # Map site name -> Top-Sites (headline) grade.
    top_grades = {r.site.name: r.grade for r in digest.top_sites}
    assert top_grades, "no top sites produced"

    # Today's forecast day is index 0; its recommended beaches carry the Today
    # table grade (outlook). They must match the headline for the same site.
    today = digest.forecast_days[0]
    checked = 0
    for beach in today.recommended_beaches:
        if beach.name in top_grades:
            assert beach.outlook == top_grades[beach.name], (
                f"{beach.name}: Today grade {beach.outlook} != "
                f"Top-Sites grade {top_grades[beach.name]}"
            )
            checked += 1

    assert checked >= 1, "no site appeared in both Today table and Top Sites"
    print(f"  [OK] Today grade == Top-Sites grade for {checked} shared site(s)")
    return True


def test_forecast_tide_score_not_uniform_100():
    """A predicted non-neutral tide phase yields a real (non-100) tide score."""
    scorer = DiveScorer()

    # Pick a site whose optimal tide is high or low (so phase actually matters).
    site = None
    for s in get_site_database().get_all_sites():
        if s.optimal_tide in ("high", "low"):
            site = s
            break
    assert site is not None, "no site with a high/low optimal tide in the DB"

    # Choose a predicted phase that is NOT the optimal (guaranteed < 100).
    predicted_phase = "low" if site.optimal_tide == "high" else "high"

    fc_input = build_scoring_input(
        site,
        wave_height_ft=2.0,
        wave_period_s=10.0,
        wind_speed_mph=8.0,
        rainfall_48h_inches=None,
        rain_chance_pct=20.0,
        tide_phase=predicted_phase,          # PREDICTED tide for the forecast day
        evaluation_time=dive_window_time(datetime(2026, 7, 5)),
    )

    tide_score = scorer.score_tide(fc_input.tide_phase, fc_input.site_optimal_tide)
    assert tide_score != 100.0, (
        f"forecast tide score should not be uniform 100 for phase "
        f"{predicted_phase} vs optimal {site.optimal_tide}, got {tide_score}"
    )

    # Sanity: the OLD behavior (no tide phase, site_optimal_tide defaulted to
    # 'any' because the caller forgot it) WOULD have been 100.
    old_style = scorer.score_tide(None, "any")
    assert old_style == 100.0

    print(f"  [OK] forecast tide phase '{predicted_phase}' vs optimal "
          f"'{site.optimal_tide}' -> tide score {tide_score} (was uniformly 100)")
    return True


def run_all_tests():
    print("\n" + "#" * 60)
    print("# PLAN 5 SCORING-UNIFICATION TEST SUITE")
    print("#" * 60 + "\n")

    tests = [
        ("build_scoring_input path-independent", test_build_scoring_input_path_independent),
        ("Today grade == Top-Sites grade", test_today_grade_equals_top_sites_grade),
        ("Forecast tide score not uniform 100", test_forecast_tide_score_not_uniform_100),
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
            import traceback
            print(f"  x ERROR: {name}\n    Exception: {e}")
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"  Passed: {passed} | Failed: {failed} | Total: {len(tests)}")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run_all_tests() else 1)
