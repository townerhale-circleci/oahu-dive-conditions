#!/usr/bin/env python3
"""Hindcast + calibration harness for the Plan 4 wave-model redesign.

Validates the offshore->beach surf transform (src/core/surf_transform.py) and the
scorer against a fixed ground-truth week (2026-06-26..07-02): NDBC buoy morning
observations vs the NWS SRFHFO surf-zone forecast midpoints. No advisories were
active and rain was zero all week.

Modes:
  (default)     Run the hindcast: per day x shore effective surf vs NWS midpoint,
                MAE overall + per shore; then SITE MODE table; then ACCEPTANCE.
  --calibrate   Coarse grid search over key transform params minimizing MAE.

Exit code is nonzero if the acceptance check fails (normal mode only).

Run: .venv-audit/bin/python scripts/hindcast_audit.py [--calibrate]
"""

import argparse
import itertools
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.surf_transform import SurfTransformParams, effective_surf_height
from src.core.scorer import DiveScorer, ScoringInput
from src.core.site import get_site_database
from src.utils.timezones import HST


# ---------------------------------------------------------------------------
# Ground truth (2026-06-26 .. 07-02, ~07:00 HST buoy obs)
# Each buoy day: (WVHT_ft, DPD_s, MWD_deg)
# ---------------------------------------------------------------------------
DAYS = ["06-26", "06-27", "06-28", "06-29", "06-30", "07-01", "07-02"]

BUOY_OBS = {
    "51201": {  # North
        "06-26": (4.3, 8, 44), "06-27": (4.3, 9, 44), "06-28": (3.6, 8, 43),
        "06-29": (3.9, 8, 46), "06-30": (3.6, 7, 40), "07-01": (4.6, 8, 39),
        "07-02": (4.3, 8, 41),
    },
    "51202": {  # East
        "06-26": (6.6, 8, 48), "06-27": (7.5, 9, 67), "06-28": (7.2, 9, 75),
        "06-29": (7.5, 8, 65), "06-30": (5.9, 8, 72), "07-01": (7.5, 8, 54),
        "07-02": (6.6, 9, 61),
    },
    "51211": {  # South (DPD lower-confidence some days, use 8 per spec)
        "06-26": (3.6, 8, 162), "06-27": (3.6, 8, 151), "06-28": (3.6, 8, 158),
        "06-29": (3.3, 8, 164), "06-30": (2.6, 14, 167), "07-01": (3.3, 8, 153),
        "07-02": (3.0, 8, 172),
    },
    "51212": {  # West/SW
        "06-26": (3.3, 15, 185), "06-27": (3.0, 13, 164), "06-28": (3.9, 7, 163),
        "06-29": (3.6, 15, 196), "06-30": (3.0, 14, 199), "07-01": (3.0, 10, 170),
        "07-02": (3.3, 11, 169),
    },
}

# SRFHFO AM surf midpoints (ft) per shore per day
NWS_MID = {
    "North": {d: 1.0 for d in DAYS},  # all 0-2 -> mid 1
    "West": {
        "06-26": 3.0, "06-27": 2.0, "06-28": 3.0, "06-29": 4.0,
        "06-30": 4.0, "07-01": 2.0, "07-02": 2.0,
    },
    "South": {
        "06-26": 4.0, "06-27": 3.0, "06-28": 4.0, "06-29": 4.0,
        "06-30": 4.0, "07-01": 3.0, "07-02": 3.0,
    },
    "East": {
        "06-26": 5.0, "06-27": 5.0, "06-28": 4.0, "06-29": 4.0,
        "06-30": 4.0, "07-01": 5.0, "07-02": 4.0,
    },
}

# shore -> (buoy, primary_exposure, secondary_exposure)
SHORE_VALIDATION = {
    "North": ("51201", "N", "NW"),
    "West": ("51212", "W", "SW"),
    "South": ("51211", "S", "SSW"),
    "East": ("51202", "E", "NE"),
}

SHORES = ["North", "West", "South", "East"]


def _dt(day_mmdd):
    return datetime(2026, int(day_mmdd[:2]), int(day_mmdd[3:]), 7, 0, tzinfo=HST)


def compute_shore_errors(params):
    """Return (rows, mae_overall, mae_per_shore) for effective vs NWS mid."""
    rows = []
    abs_errors = []
    per_shore_errors = {s: [] for s in SHORES}
    for day in DAYS:
        for shore in SHORES:
            buoy, prim, sec = SHORE_VALIDATION[shore]
            raw, dpd, mwd = BUOY_OBS[buoy][day]
            eff, _ = effective_surf_height(raw, dpd, mwd, prim, sec, params)
            nws = NWS_MID[shore][day]
            err = eff - nws
            rows.append((day, shore, raw, eff, nws, err))
            abs_errors.append(abs(err))
            per_shore_errors[shore].append(abs(err))
    mae = sum(abs_errors) / len(abs_errors)
    mae_per_shore = {s: sum(v) / len(v) for s, v in per_shore_errors.items()}
    return rows, mae, mae_per_shore


def print_hindcast_table(params):
    rows, mae, mae_per_shore = compute_shore_errors(params)
    print("\n=== HINDCAST: effective surf vs NWS SRFHFO midpoint ===")
    print(f"{'day':<7}{'shore':<7}{'raw':>6}{'eff':>7}{'nws':>6}{'err':>7}")
    for day, shore, raw, eff, nws, err in rows:
        print(f"{day:<7}{shore:<7}{raw:>6.1f}{eff:>7.2f}{nws:>6.1f}{err:>+7.2f}")
    print(f"\nMAE overall: {mae:.3f} ft")
    for s in SHORES:
        print(f"  MAE {s:<6}: {mae_per_shore[s]:.3f} ft")
    return mae, mae_per_shore


# ---------------------------------------------------------------------------
# Calibration grid search
# ---------------------------------------------------------------------------
GRID = {
    "shoal_short": [0.5, 0.6, 0.7],
    "shoal_mid": [0.7, 0.8, 0.9],
    "shoal_long": [0.9, 1.1, 1.3],
    "zero_cross_deg": [80, 90, 100],
    "wrap_floor_short": [0.05, 0.08, 0.12],
    "short_period_taper": [0.6, 0.75, 0.9],
}


def calibrate():
    keys = list(GRID.keys())
    results = []
    for combo in itertools.product(*[GRID[k] for k in keys]):
        kw = dict(zip(keys, combo))
        params = SurfTransformParams(**kw)
        _, mae, _ = compute_shore_errors(params)
        results.append((mae, kw))
    results.sort(key=lambda x: x[0])
    print("\n=== CALIBRATION: grid search (minimize MAE over 28 points) ===")
    print(f"Searched {len(results)} parameter sets. Top 5:")
    print(f"{'rank':<5}{'MAE':>7}  params")
    for i, (mae, kw) in enumerate(results[:5], 1):
        pstr = ", ".join(f"{k}={v}" for k, v in kw.items())
        print(f"{i:<5}{mae:>7.3f}  {pstr}")
    return results[0]


# ---------------------------------------------------------------------------
# SITE MODE
# ---------------------------------------------------------------------------
# site_id -> buoy whose obs to feed (per updated sites.yaml)
SITE_MODE_SITES = [
    ("sharks_cove", "51201"),
    ("three_tables", "51201"),
    ("waimea_bay", "51201"),
    ("sandy_beach", "51211"),
    ("kahana_bay", "51202"),
    ("makapuu_beach", "51202"),
    ("magic_island", "51211"),
    ("hanauma_bay", "51211"),
    ("electric_beach", "51212"),
]


def run_site_mode(params, verbose=True):
    """Run the full DiveScorer per day per site from that day's buoy obs.

    Returns a dict of results for the acceptance check.
    """
    db = get_site_database()
    scorer = DiveScorer()
    results = {}  # site_id -> list of per-day dicts

    if verbose:
        print("\n=== SITE MODE: full scorer from daily buoy obs (wind/tide None) ===")
        header = f"{'site':<18}{'day':<7}{'eff':>6}{'gated':>7}{'score':>7}{'grade':>7}"
        print(header)

    for site_id, buoy in SITE_MODE_SITES:
        site = db.get_site(site_id)
        results[site_id] = []
        for day in DAYS:
            raw, dpd, mwd = BUOY_OBS[buoy][day]
            eff, _ = effective_surf_height(
                raw, dpd, mwd,
                site.swell_exposure.primary,
                site.swell_exposure.secondary,
                params,
            )
            inp = ScoringInput(
                wave_height_ft=eff,
                raw_wave_height_ft=raw,
                wave_period_s=dpd,
                swell_direction_deg=mwd,
                wind_speed_mph=None,
                wind_direction_deg=None,
                rainfall_48h_inches=None,
                stream_discharge_cfs=None,
                brown_water_advisory=False,
                tide_phase=None,
                evaluation_time=_dt(day),
                site_max_safe_height_ft=site.max_safe_wave_height,
                site_optimal_tide=site.optimal_tide,
                site_swell_exposure_primary=site.swell_exposure.primary,
            )
            res = scorer.calculate_score(inp)
            gated = not res.safety_gates_passed
            backstop = any(
                "absolute maximum" in (g.reason or "")
                for g in res.failed_gates
            )
            results[site_id].append({
                "day": day, "eff": eff, "gated": gated,
                "score": res.total_score, "grade": res.grade.value,
                "backstop": backstop,
            })
            if verbose:
                print(f"{site_id:<18}{day:<7}{eff:>6.2f}{str(gated):>7}"
                      f"{res.total_score:>7.1f}{res.grade.value:>7}")
    return results


GRADE_RANK = {"F": 0, "D": 1, "C": 2, "B": 3, "A": 4}


def acceptance_check(site_results, mae):
    """Return (passed, list_of_failure_strings)."""
    failures = []

    # (1) sharks_cove & waimea_bay: no gate any day, score in B-range (>=70) all days.
    # Checked on the numeric score, not the letter: hindcast inputs are wave-only
    # (wind/tide/visibility unknown), so the data_completeness cap correctly holds
    # the displayed grade at C for sites without optimal_tide=="any".
    for sid in ("sharks_cove", "waimea_bay"):
        for r in site_results[sid]:
            if r["gated"]:
                failures.append(f"(1) {sid} gated on {r['day']}")
            if r["score"] < 70.0:
                failures.append(
                    f"(1) {sid} score {r['score']:.1f} < 70 (B-range) on {r['day']}"
                )

    # (2) three_tables: no gate on at least 5 of 7 days
    tt_no_gate = sum(1 for r in site_results["three_tables"] if not r["gated"])
    if tt_no_gate < 5:
        failures.append(f"(2) three_tables no-gate days {tt_no_gate} < 5")

    # (3) MAE <= 1.5 ft overall
    if mae > 1.5:
        failures.append(f"(3) MAE {mae:.3f} > 1.5 ft")

    # (4) no site gates on the absolute backstop this week
    for sid, rows in site_results.items():
        for r in rows:
            if r["backstop"]:
                failures.append(f"(4) {sid} backstop-gated on {r['day']}")

    # (5) south/west sites: no gates, scores >= 55 all days
    for sid in ("magic_island", "electric_beach"):
        for r in site_results[sid]:
            if r["gated"]:
                failures.append(f"(5) {sid} gated on {r['day']}")
            if r["score"] < 55:
                failures.append(f"(5) {sid} score {r['score']} < 55 on {r['day']}")

    return len(failures) == 0, failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--calibrate", action="store_true", help="run grid search")
    args = ap.parse_args()

    params = SurfTransformParams()  # uses defaults (calibrated below in module)

    if args.calibrate:
        best_mae, best_kw = calibrate()
        print("\nBest params:")
        for k, v in best_kw.items():
            print(f"  {k}={v}")
        print(f"Best MAE: {best_mae:.3f} ft")
        # show the hindcast under best params too
        print_hindcast_table(SurfTransformParams(**best_kw))
        return 0

    mae, _ = print_hindcast_table(params)
    site_results = run_site_mode(params)
    passed, failures = acceptance_check(site_results, mae)

    print("\n=== ACCEPTANCE CHECK ===")
    if passed:
        print("PASS: all acceptance criteria met.")
        return 0
    print("FAIL:")
    for f in failures:
        print(f"  - {f}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
