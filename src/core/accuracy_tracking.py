"""Continuous accuracy validation (Plan 7).

Pure, network-free logic for the nightly CI accuracy step:

  * A persistent CSV (one row per (date, site)) holding each day's per-site
    model prediction (grade, score, effective surf, gated) plus the NEXT day's
    observed truth (NWS SRFHFO per-shore surf midpoint, any active advisory).
  * Idempotent append/dedup so a same-day re-run of the workflow does not
    duplicate rows (it overwrites the existing row for that (date, site)).
  * Trailing-7-day hit-rate: model grade band vs observed surf band per shore.
  * A disagreement score (reusing the hindcast notion: distance between the
    predicted surf band and the observed surf band) and a rule that fires an
    alert when the daily mean disagreement exceeds a threshold for N (=3)
    consecutive days.

The CSV is the single source of truth and is committed to the ``gh-pages``
branch by the workflow (the same branch the published report lives on), so it
survives across GitHub Actions runs. All the functions here operate on plain
dicts / lists so they can be unit-tested with no network and no filesystem.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import Iterable, Optional

# ---------------------------------------------------------------------------
# CSV schema
# ---------------------------------------------------------------------------
# One row per (date, site). Prediction columns are written the morning of
# ``date``; observed_* columns are backfilled the following night when the
# NWS truth for ``date`` is known.
FIELDNAMES = [
    "date",              # YYYY-MM-DD (HST dive-window date the prediction is for)
    "site_id",
    "site_name",
    "coast",             # north_shore / west_side / south_shore / southeast / windward
    "shore",             # North / West / South / East (NWS SRFHFO shore)
    "pred_grade",        # A..F
    "pred_score",        # 0..100
    "pred_effective_surf_ft",
    "pred_raw_surf_ft",
    "pred_gated",        # True/False
    "obs_surf_ft",       # NWS SRFHFO midpoint for the shore (backfilled)
    "obs_advisory",      # advisory text if any (backfilled), else ""
]

# coast -> NWS SRFHFO shore. Southeast + windward both read the East shore
# forecast (there is no separate SE surf-zone forecast).
COAST_TO_SHORE = {
    "north_shore": "North",
    "west_side": "West",
    "south_shore": "South",
    "southeast": "East",
    "windward": "East",
}

# Consecutive-day alert threshold: daily mean disagreement (in surf bands) above
# this many bands for this many days running fires the ntfy alert.
DISAGREEMENT_THRESHOLD = 1.0   # >1 band of mean daily disagreement
CONSECUTIVE_DAYS = 3


# ---------------------------------------------------------------------------
# Band helpers (shared vocabulary for model grade vs observed surf)
# ---------------------------------------------------------------------------
def surf_ft_to_band(surf_ft: Optional[float]) -> Optional[int]:
    """Map a surf height (ft) to a coarse band index.

    0: flat/ideal (0-2 ft), 1: moderate (2-4 ft), 2: large (4-6 ft),
    3: dangerous (>6 ft). Returns None if surf is unknown.
    """
    if surf_ft is None:
        return None
    if surf_ft < 2.0:
        return 0
    if surf_ft < 4.0:
        return 1
    if surf_ft < 6.0:
        return 2
    return 3


# Model letter grade -> the surf band it implies. A/B => small/ideal surf,
# C => moderate, D => large, F => dangerous/gated. This lets us compare the
# model's opinion to the observed NWS surf on the same 0-3 scale.
GRADE_TO_BAND = {"A": 0, "B": 0, "C": 1, "D": 2, "F": 3}


def grade_to_band(grade: Optional[str]) -> Optional[int]:
    """Map a model letter grade to the surf band it implies (or None)."""
    if grade is None:
        return None
    return GRADE_TO_BAND.get(grade.upper())


# ---------------------------------------------------------------------------
# CSV read / append / dedup
# ---------------------------------------------------------------------------
def parse_csv(text: str) -> list[dict]:
    """Parse CSV text into a list of row dicts. Empty/whitespace -> []."""
    if not text or not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def to_csv(rows: Iterable[dict]) -> str:
    """Serialize row dicts to CSV text (stable column order, header always)."""
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in FIELDNAMES})
    return out.getvalue()


def _key(row: dict) -> tuple[str, str]:
    return (str(row.get("date", "")), str(row.get("site_id", "")))


def upsert_rows(existing: list[dict], new_rows: list[dict]) -> list[dict]:
    """Insert-or-replace ``new_rows`` into ``existing`` keyed on (date, site_id).

    Idempotent: re-running the same day overwrites that day's rows instead of
    duplicating them. Existing rows for other (date, site_id) keys are kept.
    Output is sorted by (date, site_id) for a stable, diff-friendly file.
    """
    by_key: dict[tuple[str, str], dict] = {_key(r): dict(r) for r in existing}
    for r in new_rows:
        by_key[_key(r)] = dict(r)
    return [by_key[k] for k in sorted(by_key)]


def backfill_observations(
    rows: list[dict],
    date: str,
    shore_surf: dict[str, Optional[float]],
    shore_advisory: Optional[dict[str, str]] = None,
) -> list[dict]:
    """Fill obs_surf_ft / obs_advisory for all rows on ``date`` from truth.

    ``shore_surf`` maps shore ("North"/...) -> observed SRFHFO midpoint ft.
    ``shore_advisory`` maps shore -> advisory text (optional). Rows whose shore
    is absent from the truth dicts are left unchanged. Idempotent.
    """
    shore_advisory = shore_advisory or {}
    out = []
    for row in rows:
        row = dict(row)
        if str(row.get("date", "")) == str(date):
            shore = row.get("shore", "")
            if shore in shore_surf and shore_surf[shore] is not None:
                row["obs_surf_ft"] = shore_surf[shore]
            if shore in shore_advisory:
                row["obs_advisory"] = shore_advisory[shore] or ""
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Prediction-row construction (from a live digest / ranked sites)
# ---------------------------------------------------------------------------
def build_prediction_rows(date: str, ranked_sites: list) -> list[dict]:
    """Build per-site prediction rows for ``date`` from RankedSite objects.

    Each RankedSite exposes ``.site`` (id/name/coast), ``.grade``,
    ``.score`` (ScoringResult with total_score / safety_gates_passed), and
    ``.conditions`` (EnvironmentalConditions with wave_height_ft /
    raw_wave_height_ft). Kept tolerant of missing attributes so it can also be
    driven from lightweight stand-ins in tests.
    """
    rows = []
    for rs in ranked_sites:
        site = getattr(rs, "site", None)
        coast = getattr(site, "coast", "") if site else ""
        score = getattr(rs, "score", None)
        cond = getattr(rs, "conditions", None)
        gated = None
        if score is not None:
            gated = not getattr(score, "safety_gates_passed", True)
        rows.append({
            "date": date,
            "site_id": getattr(site, "id", "") if site else "",
            "site_name": getattr(site, "name", "") if site else "",
            "coast": coast,
            "shore": COAST_TO_SHORE.get(coast, ""),
            "pred_grade": getattr(rs, "grade", ""),
            "pred_score": round(getattr(score, "total_score", 0.0), 1) if score else "",
            "pred_effective_surf_ft": (
                round(cond.wave_height_ft, 2)
                if cond is not None and getattr(cond, "wave_height_ft", None) is not None
                else ""
            ),
            "pred_raw_surf_ft": (
                round(cond.raw_wave_height_ft, 2)
                if cond is not None and getattr(cond, "raw_wave_height_ft", None) is not None
                else ""
            ),
            "pred_gated": "" if gated is None else str(gated),
            "obs_surf_ft": "",
            "obs_advisory": "",
        })
    return rows


# ---------------------------------------------------------------------------
# Hit-rate + disagreement
# ---------------------------------------------------------------------------
@dataclass
class HitRateResult:
    total: int = 0            # rows with both a prediction band and observed band
    hits: int = 0            # prediction band == observed band
    per_shore: dict = field(default_factory=dict)  # shore -> (hits, total)

    @property
    def hit_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return self.hits / self.total


def _float_or_none(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def compute_hit_rate(rows: list[dict], dates: Optional[set] = None) -> HitRateResult:
    """Hit-rate = fraction of scored rows whose predicted band matches observed.

    Only rows that have BOTH a model grade and an observed surf value count.
    ``dates`` optionally restricts to a set of YYYY-MM-DD strings (e.g. the
    trailing 7 days).
    """
    res = HitRateResult()
    for row in rows:
        if dates is not None and str(row.get("date", "")) not in dates:
            continue
        pred_band = grade_to_band(row.get("pred_grade") or None)
        obs_band = surf_ft_to_band(_float_or_none(row.get("obs_surf_ft")))
        if pred_band is None or obs_band is None:
            continue
        shore = row.get("shore", "") or "?"
        h, t = res.per_shore.get(shore, (0, 0))
        hit = 1 if pred_band == obs_band else 0
        res.per_shore[shore] = (h + hit, t + 1)
        res.total += 1
        res.hits += hit
    return res


def trailing_dates(all_dates: Iterable[str], n: int = 7) -> set:
    """Return the set of the ``n`` most recent YYYY-MM-DD strings present."""
    uniq = sorted({str(d) for d in all_dates if d})
    return set(uniq[-n:])


def daily_disagreement(rows: list[dict]) -> dict[str, float]:
    """Mean per-row band disagreement for each date.

    Disagreement for a row = |predicted band - observed band|. A date's value
    is the mean over its scored rows. Dates with no scored rows are omitted.
    """
    by_date: dict[str, list[int]] = {}
    for row in rows:
        pred_band = grade_to_band(row.get("pred_grade") or None)
        obs_band = surf_ft_to_band(_float_or_none(row.get("obs_surf_ft")))
        if pred_band is None or obs_band is None:
            continue
        by_date.setdefault(str(row.get("date", "")), []).append(abs(pred_band - obs_band))
    return {d: sum(v) / len(v) for d, v in by_date.items() if v}


def consecutive_disagreement_alert(
    disagreement_by_date: dict[str, float],
    threshold: float = DISAGREEMENT_THRESHOLD,
    consecutive: int = CONSECUTIVE_DAYS,
) -> bool:
    """True iff the most recent ``consecutive`` dates all exceed ``threshold``.

    "Most recent" is by sorted date order. Requires at least ``consecutive``
    dated observations; the alert fires only when the tail run is all-over.
    """
    if len(disagreement_by_date) < consecutive:
        return False
    ordered = [disagreement_by_date[d] for d in sorted(disagreement_by_date)]
    tail = ordered[-consecutive:]
    return all(v > threshold for v in tail)


# ---------------------------------------------------------------------------
# Summary rendering
# ---------------------------------------------------------------------------
def render_summary_markdown(rows: list[dict], n: int = 7) -> str:
    """Render a trailing-N-day hit-rate summary as Markdown."""
    dates = trailing_dates((r.get("date", "") for r in rows), n)
    res = compute_hit_rate(rows, dates=dates)
    lines = [
        "# Dive Model Accuracy — Trailing %d Days" % n,
        "",
        "Model grade band vs observed NWS SRFHFO surf band per shore.",
        "",
    ]
    if res.hit_rate is None:
        lines.append("_No scored rows yet (predictions awaiting observed truth)._")
        return "\n".join(lines) + "\n"
    lines.append("**Overall hit-rate: %.0f%% (%d/%d)**" % (
        res.hit_rate * 100, res.hits, res.total))
    lines.append("")
    lines.append("| Shore | Hit-rate | n |")
    lines.append("|-------|----------|---|")
    for shore in sorted(res.per_shore):
        h, t = res.per_shore[shore]
        rate = (h / t * 100) if t else 0.0
        lines.append("| %s | %.0f%% | %d |" % (shore, rate, t))
    lines.append("")
    dd = daily_disagreement(rows)
    if dd:
        recent = sorted(dd)[-n:]
        lines.append("**Daily mean disagreement (bands):** " +
                     ", ".join("%s=%.2f" % (d, dd[d]) for d in recent))
        lines.append("")
    return "\n".join(lines) + "\n"
