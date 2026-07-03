#!/usr/bin/env python3
"""Unit tests for the Plan 7 accuracy-tracking logic (pure, no network)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import accuracy_tracking as at
from src.clients.srfhfo_client import parse_srfhfo, parse_cfwhfo

_pass = 0
_fail = 0

# ---------------------------------------------------------------------------
# Frozen REAL product payloads (captured from IEM AFOS, PHFO FZHW52 / WHHW40).
# ---------------------------------------------------------------------------

# LIVE SRFHFO 2026-07-02 PM issuance ("345 PM HST Thu Jul 2"). Header block is
# "Tonight ... Friday" with sub-cols "PM AM ... AM PM" -> AM = 2nd token.
# Kauai precedes Oahu with IDENTICAL numbers, so a correct parser must anchor on
# the Oahu section (this frozen copy makes South differ so we can catch leakage
# is not the issue here; both islands share numbers, so we assert selection by
# ensuring the Oahu block is the one read).
SRFHFO_LIVE_PM = """\
000
FZHW52 PHFO 030145 CCA
SRFHFO

Surf Zone Forecast for Hawaii...CORRECTED
National Weather Service Honolulu HI
345 PM HST Thu Jul 2 2026

.DISCUSSION...
Forerunners of a small, long period south swell are filling in.

HIZ003-029>031-040245-
Kauai-
345 PM HST Thu Jul 2 2026

__________________________________________________________________
                      Tonight                    Friday

Shores                  Surf                       Surf
                     PM     AM                  AM     PM
__________________________________________________________________

North Facing         9-9    8-8                 9-9    9-9
West Facing          9-9    9-9                 9-9    9-9
South Facing         9-9    9-9                 9-9    9-9
East Facing          9-9    9-9                 9-9    9-9


.TONIGHT...
Weather.....................Mostly cloudy.

$$

HIZ006-007-009-032>035-040245-
Oahu-
345 PM HST Thu Jul 2 2026

__________________________________________________________________
                      Tonight                    Friday

Shores                  Surf                       Surf
                     PM     AM                  AM     PM
__________________________________________________________________

North Facing         0-2    0-2                 0-2    1-3
West Facing          1-3    1-3                 1-3    1-3
South Facing         2-4    3-5                 3-5    3-5
East Facing          3-5    3-5                 3-5    3-5


.TONIGHT...
Weather.....................Mostly sunny.

$$

HIZ017-018-045>050-040245-
Maui-
345 PM HST Thu Jul 2 2026

__________________________________________________________________
                      Tonight                    Friday

Shores                  Surf                       Surf
                     PM     AM                  AM     PM
__________________________________________________________________

North Facing         0-2    0-2                 0-2    1-3
West Facing          0-2    0-2                 0-2    1-3
South Facing         2-4    3-5                 3-5    3-5
East Facing          3-5    3-5                 3-5    3-5

$$
"""

# Archived SRFHFO 2026-06-28 AM issuance ("303 AM HST Sun Jun 28"). Header block
# is "Today ... Monday" with sub-cols "AM PM ... AM PM" -> AM = 1st token.
SRFHFO_ARCHIVED_AM = """\
000
FZHW52 PHFO 281303
SRFHFO

Surf Zone Forecast for Hawaii
National Weather Service Honolulu HI
303 AM HST Sun Jun 28 2026

.DISCUSSION...
South swell holding.

HIZ006-007-009-032>035-290215-
Oahu-
303 AM HST Sun Jun 28 2026

__________________________________________________________________
                       Today                     Monday

Shores                  Surf                       Surf
                     AM     PM                  AM     PM
__________________________________________________________________

North Facing         0-2    0-2                 0-2    0-2
West Facing          2-4    2-4                 2-4    2-4
South Facing         3-5    4-6                 4-6    4-6
East Facing          3-5    3-5                 3-5    3-5

$$
"""

# LIVE CFWHFO 2026-07-02: a CANCELLATION-only product. Must NOT be reported.
CFWHFO_CANCEL = """\
000
WHHW40 PHFO 191318
CFWHFO

Coastal Hazard Message
National Weather Service Honolulu HI
318 AM HST Fri Jun 19 2026

...HIGH SURF ADVISORY IS CANCELLED...

.Surf has declined below High Surf Advisory level criteria.

HIZ001-003-006-016-018-023-031>034-041-043-044-046-049>052-191430-
/O.CAN.PHFO.SU.Y.0030.000000T0000Z-260619T1600Z/
Niihau-Kauai Southwest-Waianae Coast-Kahoolawe-Maui Leeward West-
Kona-Kauai South-East Honolulu-Honolulu Metro-Ewa Plain-
Molokai Leeward South-Lanai Leeward-Lanai South-
Maui Central Valley South-South Maui/Upcountry-South Haleakala-
Big Island South-Big Island Southeast-
318 AM HST Fri Jun 19 2026

...HIGH SURF ADVISORY IS CANCELLED...

Surf along south-facing shores has dropped below advisory levels.

$$
"""

# Archived CFWHFO 2026-06-16: ACTIVE High Surf Advisory for South shores. This
# segment mixes a CANCELLED warning (SU.W) with a NEW advisory (SU.Y) -> the
# advisory must be reported, keyed to South, from the Oahu-covering zone list.
CFWHFO_ACTIVE = """\
000
WHHW40 PHFO 161305
CFWHFO

Coastal Hazard Message
National Weather Service Honolulu HI
305 AM HST Tue Jun 16 2026

...HIGH SURF ADVISORY FOR ALL SOUTH FACING SHORES THROUGH
WEDNESDAY MORNING...

HIZ001-003-006-016-018-023-026-031>034-038-041-043-044-046-
048>052-170215-
/O.CAN.PHFO.SU.W.0008.000000T0000Z-260616T1600Z/
/O.NEW.PHFO.SU.Y.0029.260616T1305Z-260617T1600Z/
/O.CON.PHFO.CF.S.0005.000000T0000Z-260617T0400Z/
Niihau-Kauai Southwest-Waianae Coast-Kahoolawe-Maui Leeward West-
Kona-Kohala-Kauai South-East Honolulu-Honolulu Metro-Ewa Plain-
Molokai Southeast-Molokai Leeward South-Lanai Leeward-Lanai South-
Maui Central Valley South-Kipahulu-South Maui/Upcountry-
South Haleakala-Big Island South-Big Island Southeast-
305 AM HST Tue Jun 16 2026

...HIGH SURF ADVISORY IN EFFECT UNTIL 6 AM HST WEDNESDAY...
...HIGH SURF WARNING IS CANCELLED...

* WHAT...Surf peaking up to 14 ft this morning.

* WHERE...South facing shores of all Hawaiian Islands

$$
"""


# --- SRFHFO parse (frozen real payloads) ------------------------------------
def test_srfhfo_oahu_table():
    got = parse_srfhfo(SRFHFO_LIVE_PM)
    # PM issuance -> AM = 2nd token: N 0-2->1, W 1-3->2, S 3-5->4, E 3-5->4.
    check("srfhfo: Oahu North (Tonight-AM 0-2)", got["North"] == 1.0)
    check("srfhfo: Oahu West (Tonight-AM 1-3)", got["West"] == 2.0)
    check("srfhfo: Oahu South (Tonight-AM 3-5)", got["South"] == 4.0)
    check("srfhfo: Oahu East (Tonight-AM 3-5)", got["East"] == 4.0)
    # Must NOT return Kauai's numbers (all 9-9 -> mid 9.0 / North AM 8-8 -> 8.0).
    check("srfhfo: NOT Kauai numbers", all(v != 9.0 and v != 8.0 for v in got.values()))


def test_srfhfo_header_variant_am():
    got = parse_srfhfo(SRFHFO_ARCHIVED_AM)
    # AM/"Today" issuance -> AM = 1st token: N 0-2->1, W 2-4->3, S 3-5->4, E 3-5->4.
    check("srfhfo AM-variant: North (Today-AM 0-2)", got["North"] == 1.0)
    check("srfhfo AM-variant: West (Today-AM 2-4)", got["West"] == 3.0)
    check("srfhfo AM-variant: South (Today-AM 3-5)", got["South"] == 4.0)
    check("srfhfo AM-variant: East (Today-AM 3-5)", got["East"] == 4.0)


def test_srfhfo_empty_and_garbage():
    check("srfhfo: empty -> all None", all(v is None for v in parse_srfhfo("").values()))
    check("srfhfo: no Oahu section -> all None",
          all(v is None for v in parse_srfhfo("random text no islands").values()))


# --- CFWHFO parse (frozen real payloads) ------------------------------------
def test_cfwhfo_cancellation_not_reported():
    got = parse_cfwhfo(CFWHFO_CANCEL)
    check("cfwhfo: cancellation reports NOTHING", got == {})


def test_cfwhfo_active_advisory():
    got = parse_cfwhfo(CFWHFO_ACTIVE)
    check("cfwhfo: active advisory reported for South", got.get("South") is not None)
    check("cfwhfo: active advisory text 'IN EFFECT'",
          "IN EFFECT" in got.get("South", ""))
    check("cfwhfo: cancelled warning not leaking as extra shores",
          set(got.keys()) == {"South"})


def check(name, cond):
    global _pass, _fail
    if cond:
        _pass += 1
        print(f"  PASS  {name}")
    else:
        _fail += 1
        print(f"  FAIL  {name}")


def _row(date, site, grade="B", obs=None, shore="North", score=80.0):
    return {
        "date": date, "site_id": site, "site_name": site.title(),
        "coast": "north_shore", "shore": shore, "pred_grade": grade,
        "pred_score": score, "pred_effective_surf_ft": 1.0,
        "pred_raw_surf_ft": 4.0, "pred_gated": "False",
        "obs_surf_ft": "" if obs is None else obs, "obs_advisory": "",
    }


# --- CSV append / dedup (idempotency) ---------------------------------------
def test_upsert_dedup():
    existing = [_row("2026-07-01", "sharks_cove"), _row("2026-07-01", "waimea_bay")]
    # Same-day re-run with the same keys but a changed grade.
    rerun = [_row("2026-07-01", "sharks_cove", grade="A")]
    out = at.upsert_rows(existing, rerun)
    check("upsert: no duplicate rows on same-day re-run", len(out) == 2)
    sc = [r for r in out if r["site_id"] == "sharks_cove"]
    check("upsert: existing row overwritten (grade A)", len(sc) == 1 and sc[0]["pred_grade"] == "A")
    check("upsert: other site untouched", any(r["site_id"] == "waimea_bay" for r in out))


def test_upsert_new_day_appends():
    existing = [_row("2026-07-01", "sharks_cove")]
    new = [_row("2026-07-02", "sharks_cove")]
    out = at.upsert_rows(existing, new)
    check("upsert: new date appends (2 rows)", len(out) == 2)
    check("upsert: output sorted by (date, site)", out[0]["date"] == "2026-07-01")


def test_csv_roundtrip():
    rows = [_row("2026-07-01", "sharks_cove"), _row("2026-07-01", "waimea_bay")]
    text = at.to_csv(rows)
    parsed = at.parse_csv(text)
    check("csv: roundtrip preserves row count", len(parsed) == 2)
    check("csv: header present", text.splitlines()[0].startswith("date,site_id"))
    check("csv: empty text -> []", at.parse_csv("") == [] and at.parse_csv("   \n") == [])


def test_backfill_idempotent():
    rows = [_row("2026-07-01", "sharks_cove", shore="North"),
            _row("2026-07-01", "magic_island", shore="South")]
    truth = {"North": 1.0, "South": 4.0, "West": None, "East": None}
    once = at.backfill_observations(rows, "2026-07-01", truth)
    twice = at.backfill_observations(once, "2026-07-01", truth)
    check("backfill: North obs filled", float(once[0]["obs_surf_ft"]) == 1.0)
    check("backfill: South obs filled", float(once[1]["obs_surf_ft"]) == 4.0)
    check("backfill: idempotent", once == twice)
    # A date not present is a no-op.
    noop = at.backfill_observations(rows, "2026-06-30", truth)
    check("backfill: other date is no-op", all(r["obs_surf_ft"] == "" for r in noop))


# --- band helpers -----------------------------------------------------------
def test_bands():
    check("surf band: 1ft->0", at.surf_ft_to_band(1.0) == 0)
    check("surf band: 3ft->1", at.surf_ft_to_band(3.0) == 1)
    check("surf band: 5ft->2", at.surf_ft_to_band(5.0) == 2)
    check("surf band: 8ft->3", at.surf_ft_to_band(8.0) == 3)
    check("surf band: None->None", at.surf_ft_to_band(None) is None)
    check("grade band: A->0", at.grade_to_band("A") == 0)
    check("grade band: F->3", at.grade_to_band("F") == 3)
    check("grade band: ''->None", at.grade_to_band(None) is None)


# --- hit-rate ---------------------------------------------------------------
def test_hit_rate():
    rows = [
        _row("2026-07-01", "a", grade="B", obs=1.0, shore="North"),   # band 0==0 HIT
        _row("2026-07-01", "b", grade="F", obs=8.0, shore="East"),    # band 3==3 HIT
        _row("2026-07-01", "c", grade="B", obs=5.0, shore="West"),    # band 0!=2 MISS
        _row("2026-07-01", "d", grade="C", obs=None, shore="South"),  # no obs -> excluded
    ]
    res = at.compute_hit_rate(rows)
    check("hit-rate: 3 scored rows (unscored excluded)", res.total == 3)
    check("hit-rate: 2 hits", res.hits == 2)
    check("hit-rate: 2/3", abs(res.hit_rate - 2 / 3) < 1e-9)
    check("hit-rate: per-shore North 1/1", res.per_shore.get("North") == (1, 1))
    check("hit-rate: per-shore West 0/1", res.per_shore.get("West") == (0, 1))


def test_trailing_dates():
    dates = ["2026-06-25", "2026-06-26", "2026-06-27", "2026-06-28",
             "2026-06-29", "2026-06-30", "2026-07-01", "2026-07-02"]
    tail = at.trailing_dates(dates, n=7)
    check("trailing: 7 dates", len(tail) == 7)
    check("trailing: oldest dropped", "2026-06-25" not in tail)
    check("trailing: newest kept", "2026-07-02" in tail)


# --- disagreement + 3-consecutive-day alert ---------------------------------
def test_disagreement_and_alert():
    # 3 straight days of a full-band disagreement (grade B => band0, obs 5ft => band2 => gap 2)
    bad = []
    for d in ("2026-06-30", "2026-07-01", "2026-07-02"):
        bad.append(_row(d, "x", grade="B", obs=5.0, shore="West"))
    dd = at.daily_disagreement(bad)
    check("disagreement: 3 dates computed", len(dd) == 3)
    check("disagreement: each day gap=2.0", all(abs(v - 2.0) < 1e-9 for v in dd.values()))
    check("alert: fires on 3 consecutive over-threshold days",
          at.consecutive_disagreement_alert(dd) is True)

    # Only 2 bad days -> no alert (needs 3).
    dd2 = at.daily_disagreement(bad[:2])
    check("alert: does NOT fire with only 2 days", at.consecutive_disagreement_alert(dd2) is False)

    # 3 days but the most recent is a good match -> tail run broken -> no alert.
    mixed = [
        _row("2026-06-30", "x", grade="B", obs=5.0),  # gap 2
        _row("2026-07-01", "x", grade="B", obs=5.0),  # gap 2
        _row("2026-07-02", "x", grade="B", obs=1.0),  # gap 0 (good)
    ]
    ddm = at.daily_disagreement(mixed)
    check("alert: does NOT fire when latest day agrees",
          at.consecutive_disagreement_alert(ddm) is False)

    # All agree -> no alert.
    good = [_row(d, "x", grade="B", obs=1.0) for d in ("2026-06-30", "2026-07-01", "2026-07-02")]
    check("alert: does NOT fire when all agree",
          at.consecutive_disagreement_alert(at.daily_disagreement(good)) is False)


# --- build_prediction_rows from stand-in objects ----------------------------
def test_build_prediction_rows():
    class _Site:
        id, name, coast = "sharks_cove", "Sharks Cove", "north_shore"

    class _Score:
        total_score, safety_gates_passed = 86.4, True

    class _Cond:
        wave_height_ft, raw_wave_height_ft = 1.23, 4.31

    class _RS:
        site, score, conditions, grade = _Site(), _Score(), _Cond(), "A"

    rows = at.build_prediction_rows("2026-07-02", [_RS()])
    r = rows[0]
    check("build: shore mapped from coast", r["shore"] == "North")
    check("build: grade captured", r["pred_grade"] == "A")
    check("build: score rounded", r["pred_score"] == 86.4)
    check("build: effective surf rounded", r["pred_effective_surf_ft"] == 1.23)
    check("build: gated False (gates passed)", r["pred_gated"] == "False")


# --- summary markdown -------------------------------------------------------
def test_summary_markdown():
    rows = [_row("2026-07-01", "a", grade="B", obs=1.0, shore="North")]
    md = at.render_summary_markdown(rows)
    check("summary: contains hit-rate header", "Trailing 7 Days" in md)
    check("summary: reports 100%", "100%" in md)
    empty = at.render_summary_markdown([_row("2026-07-01", "a", obs=None)])
    check("summary: handles no-scored-rows", "No scored rows" in empty)


def main():
    print("=" * 60)
    print("Accuracy tracking unit tests")
    print("=" * 60)
    for fn in [
        test_upsert_dedup, test_upsert_new_day_appends, test_csv_roundtrip,
        test_backfill_idempotent, test_bands, test_hit_rate, test_trailing_dates,
        test_disagreement_and_alert, test_build_prediction_rows, test_summary_markdown,
        test_srfhfo_oahu_table, test_srfhfo_header_variant_am,
        test_srfhfo_empty_and_garbage, test_cfwhfo_cancellation_not_reported,
        test_cfwhfo_active_advisory,
    ]:
        print(f"\n{fn.__name__}:")
        fn()
    print("\n" + "=" * 60)
    print(f"  Passed: {_pass} | Failed: {_fail} | Total: {_pass + _fail}")
    print("=" * 60)
    return 1 if _fail else 0


if __name__ == "__main__":
    sys.exit(main())
