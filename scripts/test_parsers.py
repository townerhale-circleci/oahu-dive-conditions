#!/usr/bin/env python3
"""Unit tests for parser/client hardening (Plan 6).

Covers:
- NDBC standard (.txt) and spectral (.spec) parsing with header-name column
  indexing and numeric-sentinel filtering, using REAL captured payload snippets
  plus hand-crafted malformed rows (short rows, MM, 99.0 fills, shuffled column
  order with valid headers).
- PacIOOS ERDDAP CSV parsing with header-name indexing and column validation.
- NWS wind-range parsing "10 to 15 mph" -> 15 (take the MAX).

No network required. Run from project root:
    .venv-audit/bin/python scripts/test_parsers.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.buoy_client import BuoyClient
from src.clients.pacioos_client import PacIOOSClient
from src.clients.nws_client import _parse_wind_speed_mph


# ---------------------------------------------------------------------------
# Frozen REAL payload snippets (captured live 2026-07-03 from NDBC 51201).
# ---------------------------------------------------------------------------

NDBC_STANDARD_REAL = """\
#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
2026 07 03 07 56  MM   MM   MM   1.4     8   4.9  41     MM    MM  26.1    MM   MM   MM    MM
2026 07 03 07 26  MM   MM   MM   1.4     8   5.1  43     MM    MM  26.1    MM   MM   MM    MM
2026 07 03 06 56  MM   MM   MM   1.5     8   5.0  46     MM    MM  26.1    MM   MM   MM    MM
"""

NDBC_SPECTRAL_REAL = """\
#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD
#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT
2026 07 03 07 56  1.4  0.2 13.3  1.3  7.7   W  NE    AVERAGE  4.9  41
2026 07 03 07 26  1.4  0.2 12.5  1.4  8.3 WNW  NE    AVERAGE  5.1  43
2026 07 03 06 56  1.5  0.2 12.5  1.5  8.3 WNW  NE    AVERAGE  5.0  46
"""

# Real PacIOOS ERDDAP griddap CSV snippet (captured 2026-07-03).
PACIOOS_CSV_REAL = """\
time,depth,latitude,longitude,shgt,mper,mdir
UTC,m,degrees_north,degrees_east,meters,second,degrees
2026-07-03T09:00:00Z,0.0,21.28,201.87999,0.93807334,7.6051826,180.99675
2026-07-03T10:00:00Z,0.0,21.28,201.87999,0.94232047,7.5473614,180.55684
"""


def _check(cond, msg):
    if not cond:
        raise AssertionError(msg)


# ---------------------------------------------------------------------------
# NDBC standard (.txt)
# ---------------------------------------------------------------------------

def test_ndbc_standard_real():
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    recs = c._parse_ndbc_standard(NDBC_STANDARD_REAL)
    _check(len(recs) == 3, f"expected 3 rows, got {len(recs)}")
    r = recs[0]
    _check(r["time"] == "2026-07-03T07:56:00Z", r["time"])
    _check(r["wave_height_m"] == 1.4, r["wave_height_m"])
    _check(r["dominant_period_s"] == 8.0, r["dominant_period_s"])
    _check(r["average_period_s"] == 4.9, r["average_period_s"])
    _check(r["mean_wave_direction"] == 41.0, r["mean_wave_direction"])
    # Oahu buoys report MM for all wind fields -> None, never 0.
    _check(r["wind_speed_mps"] is None, "wind speed should be None (MM)")
    _check(r["wind_direction"] is None, "wind dir should be None (MM)")
    _check(r["gust_speed_mps"] is None, "gust should be None (MM)")
    _check(r["pressure_hpa"] is None, "pressure should be None (MM)")
    return True


def test_ndbc_standard_shuffled_columns():
    """Header-name indexing must survive a reordered column layout."""
    text = (
        "#YY  MM DD hh mm  WVHT WDIR DPD  MWD WSPD APD\n"
        "#yr  mo dy hr mn     m degT sec degT  m/s sec\n"
        "2026 07 03 07 56   2.0  120  10  200  5.0  7.5\n"
    )
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    recs = c._parse_ndbc_standard(text)
    _check(len(recs) == 1, f"expected 1 row, got {len(recs)}")
    r = recs[0]
    _check(r["wave_height_m"] == 2.0, r["wave_height_m"])
    _check(r["dominant_period_s"] == 10.0, r["dominant_period_s"])
    _check(r["mean_wave_direction"] == 200.0, r["mean_wave_direction"])
    _check(r["wind_direction"] == 120.0, r["wind_direction"])
    _check(r["wind_speed_mps"] == 5.0, r["wind_speed_mps"])
    return True


def test_ndbc_standard_sentinels_and_malformed():
    """99.0/999 fills -> None; a short row is skipped; MM -> None."""
    text = (
        "#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES\n"
        "#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa\n"
        # WVHT 99.00 fill, DPD 99.0 fill, MWD 999 fill, PRES real 1013.2
        "2026 07 03 07 56  MM   MM   MM  99.00 99.0   4.9 999  1013.2\n"
        # a genuinely short row (can't even form a timestamp) -> skipped
        "2026 07 03\n"
        # valid row with real values incl direction 315 (< 999, kept)
        "2026 07 03 06 56  MM   MM   MM   1.5   8.0   5.0 315  1013.0\n"
    )
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    recs = c._parse_ndbc_standard(text)
    # short row (< 6 fields) is dropped -> 2 rows
    _check(len(recs) == 2, f"expected 2 rows, got {len(recs)}")
    r0 = recs[0]
    _check(r0["wave_height_m"] is None, "99.00 WVHT should be None")
    _check(r0["dominant_period_s"] is None, "99.0 DPD should be None")
    _check(r0["mean_wave_direction"] is None, "999 MWD should be None")
    _check(r0["pressure_hpa"] == 1013.2, f"real pressure kept: {r0['pressure_hpa']}")
    r1 = recs[1]
    _check(r1["wave_height_m"] == 1.5, r1["wave_height_m"])
    _check(r1["mean_wave_direction"] == 315.0, "real direction 315 kept")
    _check(r1["pressure_hpa"] == 1013.0, r1["pressure_hpa"])
    return True


def test_ndbc_standard_no_header():
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    _check(c._parse_ndbc_standard("garbage\nmore garbage\n") == [], "no header -> []")
    _check(c._parse_ndbc_standard("") == [], "empty -> []")
    return True


# ---------------------------------------------------------------------------
# NDBC spectral (.spec)
# ---------------------------------------------------------------------------

def test_ndbc_spectral_real():
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    recs = c._parse_ndbc_spectral(NDBC_SPECTRAL_REAL)
    _check(len(recs) == 3, f"expected 3 rows, got {len(recs)}")
    r = recs[0]
    _check(r["wave_height_m"] == 1.4, r["wave_height_m"])
    _check(r["swell_height_m"] == 0.2, r["swell_height_m"])
    _check(r["swell_period_s"] == 13.3, r["swell_period_s"])
    _check(r["wind_wave_height_m"] == 1.3, r["wind_wave_height_m"])
    _check(r["wind_wave_period_s"] == 7.7, r["wind_wave_period_s"])
    # APD/MWD come AFTER the word column STEEPNESS -> only header indexing works
    _check(r["average_period_s"] == 4.9, r["average_period_s"])
    _check(r["mean_wave_direction"] == 41.0, r["mean_wave_direction"])
    # Compass direction strings preserved.
    _check(r["swell_direction"] == "W", r["swell_direction"])
    _check(r["wind_wave_direction"] == "NE", r["wind_wave_direction"])
    return True


def test_ndbc_spectral_sentinels_and_mm_dir():
    text = (
        "#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n"
        "#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT\n"
        # MM directions -> None; 99.0 SwP fill -> None
        "2026 07 03 07 56  1.4  0.2 99.0  1.3  7.7  MM  MM      N/A  4.9  41\n"
    )
    c = BuoyClient(cache_path=Path("/tmp/_test_buoy.db"))
    recs = c._parse_ndbc_spectral(text)
    _check(len(recs) == 1, f"expected 1 row, got {len(recs)}")
    r = recs[0]
    _check(r["swell_period_s"] is None, "99.0 SwP should be None")
    _check(r["swell_direction"] is None, "MM SwD should be None")
    _check(r["wind_wave_direction"] is None, "MM WWD should be None")
    _check(r["average_period_s"] == 4.9, r["average_period_s"])
    return True


# ---------------------------------------------------------------------------
# PacIOOS ERDDAP CSV
# ---------------------------------------------------------------------------

def test_pacioos_real():
    c = PacIOOSClient(cache_path=Path("/tmp/_test_pacioos.db"))
    recs = c._parse_erddap_csv(PACIOOS_CSV_REAL)
    _check(len(recs) == 2, f"expected 2 rows, got {len(recs)}")
    r = recs[0]
    _check(r["time"] == "2026-07-03T09:00:00Z", r["time"])
    _check(abs(r["wave_height_m"] - 0.93807334) < 1e-6, r["wave_height_m"])
    _check(abs(r["period_s"] - 7.6051826) < 1e-6, r["period_s"])
    _check(abs(r["direction_deg"] - 180.99675) < 1e-6, r["direction_deg"])
    return True


def test_pacioos_shuffled_columns():
    """Header-name indexing must survive reordered ERDDAP columns."""
    text = (
        "latitude,longitude,time,mdir,shgt,mper\n"
        "degrees_north,degrees_east,UTC,degrees,meters,second\n"
        "21.28,201.88,2026-07-03T09:00:00Z,175.0,1.25,9.0\n"
    )
    c = PacIOOSClient(cache_path=Path("/tmp/_test_pacioos.db"))
    recs = c._parse_erddap_csv(text)
    _check(len(recs) == 1, f"expected 1 row, got {len(recs)}")
    r = recs[0]
    _check(r["wave_height_m"] == 1.25, r["wave_height_m"])
    _check(r["period_s"] == 9.0, r["period_s"])
    _check(r["direction_deg"] == 175.0, r["direction_deg"])
    _check(r["time"] == "2026-07-03T09:00:00Z", r["time"])
    return True


def test_pacioos_nan_and_missing_column():
    # NaN wave-height row is skipped; NaN period/dir -> None.
    text = (
        "time,depth,latitude,longitude,shgt,mper,mdir\n"
        "UTC,m,degrees_north,degrees_east,meters,second,degrees\n"
        "2026-07-03T09:00:00Z,0.0,21.28,201.88,NaN,7.6,181.0\n"
        "2026-07-03T10:00:00Z,0.0,21.28,201.88,0.9,NaN,NaN\n"
    )
    c = PacIOOSClient(cache_path=Path("/tmp/_test_pacioos.db"))
    recs = c._parse_erddap_csv(text)
    _check(len(recs) == 1, f"NaN shgt row should be skipped, got {len(recs)}")
    _check(recs[0]["wave_height_m"] == 0.9, recs[0]["wave_height_m"])
    _check(recs[0]["period_s"] is None, "NaN period -> None")
    _check(recs[0]["direction_deg"] is None, "NaN dir -> None")

    # Missing required column -> empty result (validation), not misaligned parse.
    bad = (
        "time,depth,latitude,longitude,mper,mdir\n"  # no shgt
        "UTC,m,degrees_north,degrees_east,second,degrees\n"
        "2026-07-03T09:00:00Z,0.0,21.28,201.88,7.6,181.0\n"
    )
    _check(c._parse_erddap_csv(bad) == [], "missing shgt column -> []")
    return True


# ---------------------------------------------------------------------------
# NWS wind range
# ---------------------------------------------------------------------------

def test_nws_wind_range_takes_max():
    _check(_parse_wind_speed_mph("10 to 15 mph") == 15, "range -> max")
    _check(_parse_wind_speed_mph("15 to 25 mph") == 25, "range -> max")
    _check(_parse_wind_speed_mph("10 mph") == 10, "single value")
    _check(_parse_wind_speed_mph("5 mph") == 5, "single value")
    _check(_parse_wind_speed_mph(None) == 0, "None -> 0")
    _check(_parse_wind_speed_mph("") == 0, "empty -> 0")
    _check(_parse_wind_speed_mph("calm") == 0, "no number -> 0")
    return True


def run_all_tests():
    print("\n" + "#" * 60)
    print("# PARSER HARDENING TEST SUITE")
    print("#" * 60)

    tests = [
        ("NDBC standard (real payload)", test_ndbc_standard_real),
        ("NDBC standard (shuffled columns)", test_ndbc_standard_shuffled_columns),
        ("NDBC standard (sentinels + malformed)", test_ndbc_standard_sentinels_and_malformed),
        ("NDBC standard (no header)", test_ndbc_standard_no_header),
        ("NDBC spectral (real payload)", test_ndbc_spectral_real),
        ("NDBC spectral (sentinels + MM dir)", test_ndbc_spectral_sentinels_and_mm_dir),
        ("PacIOOS ERDDAP (real payload)", test_pacioos_real),
        ("PacIOOS ERDDAP (shuffled columns)", test_pacioos_shuffled_columns),
        ("PacIOOS ERDDAP (NaN + missing column)", test_pacioos_nan_and_missing_column),
        ("NWS wind range -> max", test_nws_wind_range_takes_max),
    ]

    passed = failed = 0
    for name, fn in tests:
        try:
            if fn():
                passed += 1
                print(f"  ✓ {name}")
            else:
                failed += 1
                print(f"  ✗ FAILED: {name}")
        except AssertionError as e:
            failed += 1
            print(f"  ✗ FAILED: {name}\n      {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ✗ ERROR: {name}\n      {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print(f"  Passed: {passed} | Failed: {failed} | Total: {len(tests)}")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run_all_tests() else 1)
