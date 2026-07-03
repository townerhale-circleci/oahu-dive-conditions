#!/usr/bin/env python3
"""Unit tests for the surf transform and its scorer integration.

Run from project root:
    .venv-audit/bin/python scripts/test_surf_transform.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.surf_transform import (
    SurfTransformParams,
    effective_surf_height,
    exposure_factor,
    wrap_floor,
    _compass_to_deg,
    UNKNOWN_DIRECTION_FACTOR,
)
from src.core.scorer import DiveScorer, ScoringInput, ScoreGrade


PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}")


def approx(a, b, tol=1e-6):
    return abs(a - b) <= tol


def test_compass_parsing():
    print("\n[compass parsing]")
    check("N -> 0", _compass_to_deg("N") == 0.0)
    check("NW -> 315", _compass_to_deg("NW") == 315.0)
    check("lowercase nw -> 315", _compass_to_deg("nw") == 315.0)
    check("whitespace ' SE ' -> 135", _compass_to_deg(" SE ") == 135.0)
    check("bogus -> None", _compass_to_deg("XYZ") is None)
    check("None -> None", _compass_to_deg(None) is None)


def test_exposure_factor_aligned():
    print("\n[exposure_factor: aligned]")
    p = SurfTransformParams()
    # Swell from exactly the facing direction (aligned) -> 1.0
    check("aligned NW swell @315 vs NW site", approx(exposure_factor(315, "NW", 12, p), 1.0))
    # Within full_exposure_deg (30) still 1.0
    check("20 deg off @12s still 1.0", approx(exposure_factor(335, "NW", 12, p), 1.0))


def test_exposure_factor_opposite():
    print("\n[exposure_factor: opposite -> wrap floor]")
    p = SurfTransformParams()
    # Opposite direction (>= zero_cross) -> wrap floor for that period
    # long period (>=12) floor = wrap_floor_long
    check("opposite long-period -> wrap_floor_long",
          approx(exposure_factor(135, "NW", 12, p), p.wrap_floor_long))
    # short period opposite -> wrap_floor_short (short taper can't go below floor)
    check("opposite short-period -> wrap_floor_short",
          approx(exposure_factor(135, "NW", 7, p), p.wrap_floor_short))


def test_exposure_factor_period_dependence():
    print("\n[exposure_factor: period dependence]")
    p = SurfTransformParams()
    # Off-axis at 60 degrees: short-period should attenuate MORE than long-period
    f_long = exposure_factor(315 - 60, "NW", 13, p)   # 255, 60 deg off
    f_short = exposure_factor(315 - 60, "NW", 7, p)    # same angle, short period
    check("short-period more attenuated off-axis than long", f_short < f_long)
    # wrap_floor interpolates between short and long for 9-12s
    wf_mid = wrap_floor(10.5, p)
    check("wrap_floor 10.5s between short and long",
          p.wrap_floor_short < wf_mid < p.wrap_floor_long)


def test_missing_direction_fallback_flag():
    print("\n[missing direction/period fallback]")
    p = SurfTransformParams()
    eff, applied = effective_surf_height(4.0, None, 44, "NW", "N", p)
    check("missing period -> flat 0.8 multiplier", approx(eff, 4.0 * UNKNOWN_DIRECTION_FACTOR))
    check("missing period -> transform_applied False", applied is False)
    eff2, applied2 = effective_surf_height(4.0, 8, None, "NW", "N", p)
    check("missing direction -> flat 0.8 multiplier", approx(eff2, 4.0 * UNKNOWN_DIRECTION_FACTOR))
    check("missing direction -> transform_applied False", applied2 is False)
    # None raw height -> None, not applied
    eff3, applied3 = effective_surf_height(None, 8, 44, "NW", "N", p)
    check("None raw -> None", eff3 is None and applied3 is False)


def test_missing_exposure():
    print("\n[missing site exposure -> factor 1.0]")
    p = SurfTransformParams()
    # No exposure: no directional attenuation, only shoaling applies
    eff, applied = effective_surf_height(4.0, 13, 200, None, None, p)
    check("no exposure -> shoal only (factor 1.0)", approx(eff, 4.0 * p.shoal_long))
    check("no exposure -> transform_applied True", applied is True)
    check("exposure_factor None exposure -> 1.0", exposure_factor(200, None, 13, p) == 1.0)


def test_effective_secondary():
    print("\n[effective_surf_height: secondary exposure]")
    p = SurfTransformParams()
    # A swell fully off the primary but aligned with the secondary should raise
    # the effective height via secondary_weight. Primary E (90) is 135 deg off a
    # NW (315) swell -> primary factor at wrap floor; secondary NW is aligned.
    prim_only, _ = effective_surf_height(4.0, 13, 315, "E", None, p)
    with_sec, _ = effective_surf_height(4.0, 13, 315, "E", "NW", p)
    check("secondary raises effective when swell hits secondary", with_sec > prim_only)


def test_scorer_integration_no_gate():
    print("\n[scorer integration: NE windswell does not gate NW site]")
    scorer = DiveScorer()
    # raw 4.3 ft NE swell @8s vs NW-facing site with 3ft threshold -> effective
    # well below threshold, no gate.
    eff, _ = effective_surf_height(4.3, 8, 44, "NW", "N")
    inp = ScoringInput(
        wave_height_ft=eff,
        raw_wave_height_ft=4.3,
        wave_period_s=8,
        swell_direction_deg=44,
        site_max_safe_height_ft=3.0,
        site_swell_exposure_primary="NW",
    )
    passed, gates = scorer.check_safety_gates(inp)
    check("effective below 3ft threshold", eff < 3.0)
    check("no safety gate fires", passed is True)


def test_scorer_integration_backstop():
    print("\n[scorer integration: raw 11ft triggers absolute backstop]")
    scorer = DiveScorer()
    # Even if a site is shadowed (effective small), an 11ft raw offshore Hs
    # must gate on the absolute backstop (>10ft).
    inp = ScoringInput(
        wave_height_ft=2.0,          # effective (shadowed) small
        raw_wave_height_ft=11.0,     # raw offshore extreme
        wave_period_s=14,
        site_max_safe_height_ft=6.0,
        site_swell_exposure_primary="NW",
    )
    passed, gates = scorer.check_safety_gates(inp)
    check("backstop gate fires on raw 11ft", passed is False)
    check("backstop reason mentions extreme conditions",
          any("extreme conditions" in (g.reason or "") for g in gates))
    # And raw 8ft (ordinary trade windswell) should NOT gate on the backstop
    inp2 = ScoringInput(
        wave_height_ft=2.0,
        raw_wave_height_ft=8.0,
        wave_period_s=8,
        site_max_safe_height_ft=6.0,
        site_swell_exposure_primary="NW",
    )
    passed2, _ = scorer.check_safety_gates(inp2)
    check("raw 8ft does NOT gate on backstop (new 10ft ceiling)", passed2 is True)


def test_scorer_gate_reason_effective():
    print("\n[scorer: site gate reason says 'Effective surf']")
    scorer = DiveScorer()
    inp = ScoringInput(
        wave_height_ft=5.0,          # effective exceeds 3ft site threshold
        raw_wave_height_ft=6.0,
        wave_period_s=10,
        site_max_safe_height_ft=3.0,
        site_swell_exposure_primary="E",
    )
    passed, gates = scorer.check_safety_gates(inp)
    check("site gate fires", passed is False)
    check("reason mentions 'Effective surf'",
          any("Effective surf" in (g.reason or "") for g in gates))


def test_buoy_txt_primary_pairing():
    print("\n[buoy client: .txt standard is primary, pairs WVHT+DPD+MWD]")
    from src.clients.buoy_client import BuoyClient

    # Standard .txt: WVHT=1.4m, DPD=8s, APD=5.4, MWD=40. .spec has a minor 0.2m
    # long-period (13.3s) component whose SwP must NOT be attached to total WVHT.
    txt = (
        "#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE\n"
        "#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft\n"
        "2026 07 03 00 26  MM   MM   MM   1.4     8   5.4  40     MM    MM  26.2    MM   MM   MM    MM\n"
    )
    spec = (
        "#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n"
        "#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT\n"
        "2026 07 03 00 56  1.4  0.2 13.3  1.4  8.3 WNW  NE    AVERAGE  5.2  44\n"
    )
    c = BuoyClient()
    # Monkeypatch the two fetchers to return parsed dataframes from fixtures.
    import pandas as pd
    c.get_standard_data = lambda sid, use_cache=True: pd.DataFrame(c._parse_ndbc_standard(txt))
    c.get_spectral_data = lambda sid, use_cache=True: pd.DataFrame(c._parse_ndbc_spectral(spec))
    # Freeze staleness (fixtures are dated in the future relative to some clocks,
    # but the guard only rejects OLD rows, so future-dated rows pass).
    out = c.get_current_conditions("51201")
    check("source is standard (.txt primary)", out["source"] == "standard")
    check("period is DPD (8s), not SwP (13.3s)", approx(out["swell_period_s"], 8.0))
    check("dominant_period_s alias equals swell_period_s", out["dominant_period_s"] == out["swell_period_s"])
    check("mean_direction_deg is MWD 40", approx(out["mean_direction_deg"], 40.0))
    check("wave_height_ft from WVHT 1.4m", approx(out["wave_height_ft"], 1.4 * 3.28084, tol=1e-3))


def test_buoy_spec_fallback_dominant_component():
    print("\n[buoy client: .spec fallback picks dominant component]")
    from src.clients.buoy_client import BuoyClient
    import pandas as pd

    # No .txt available -> fall back to .spec. Wind wave (WWH=1.4) dominates the
    # swell component (SwH=0.2), so the triple must use WWP(8.3s) + WWD(NE=45),
    # NOT SwP(13.3s)/SwD(WNW). Total WVHT stays 1.4m.
    spec = (
        "#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n"
        "#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT\n"
        "2026 07 03 00 56  1.4  0.2 13.3  1.4  8.3 WNW  NE    AVERAGE  5.2  44\n"
    )
    c = BuoyClient()
    c.get_standard_data = lambda sid, use_cache=True: pd.DataFrame()  # unavailable
    c.get_spectral_data = lambda sid, use_cache=True: pd.DataFrame(c._parse_ndbc_spectral(spec))
    out = c.get_current_conditions("51201")
    check("source is spectral (fallback)", out["source"] == "spectral")
    check("dominant period is WWP 8.3s (not SwP 13.3s)", approx(out["swell_period_s"], 8.3))
    check("dominant dir is WWD 'NE'", out["swell_direction"] == "NE")
    check("WWD 'NE' converted to 45 deg", approx(out["mean_direction_deg"], 45.0))

    # Now make swell dominate: SwH=1.5 > WWH=0.3 -> use SwP(14s)+SwD(NW=315).
    spec2 = (
        "#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n"
        "#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT\n"
        "2026 07 03 00 56  1.5  1.5   14  0.3  6.0 NW  NE    AVERAGE  9.0  310\n"
    )
    c.get_spectral_data = lambda sid, use_cache=True: pd.DataFrame(c._parse_ndbc_spectral(spec2))
    out2 = c.get_current_conditions("51201")
    check("swell dominant -> SwP 14s", approx(out2["swell_period_s"], 14.0))
    check("swell dominant -> SwD 'NW' -> 315 deg (compass conversion)",
          approx(out2["mean_direction_deg"], 315.0))


def main():
    test_compass_parsing()
    test_exposure_factor_aligned()
    test_exposure_factor_opposite()
    test_exposure_factor_period_dependence()
    test_missing_direction_fallback_flag()
    test_missing_exposure()
    test_effective_secondary()
    test_scorer_integration_no_gate()
    test_scorer_integration_backstop()
    test_scorer_gate_reason_effective()
    test_buoy_txt_primary_pairing()
    test_buoy_spec_fallback_dominant_component()

    print(f"\n{'='*50}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    print(f"{'='*50}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
