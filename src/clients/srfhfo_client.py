"""NWS surf-zone forecast (SRFHFO) + coastal hazards (CFWHFO) truth fetcher.

Ground-truth source for the nightly accuracy check (Plan 7). Pulls the latest
SRFHFO surf-zone forecast product for Oahu from the IEM AFOS archive and parses
the AM surf-height ranges per shore into a single midpoint (ft) per shore.
CFWHFO gives active coastal-hazard advisories (High Surf Warning/Advisory,
brown-water, etc.) keyed by shore.

SRFHFO free text is not a stable machine format, so parsing is best-effort:
on any failure the fetchers return ``None`` / empty, and the accuracy pipeline
simply leaves that day's observed columns blank (those rows are then not scored
until truth is available). This keeps the nightly job resilient.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import requests

logger = logging.getLogger(__name__)

AFOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"

SHORES = ["North", "West", "South", "East"]

# --- SRFHFO structure (verified live + archived, PHFO FZHW52) ----------------
# The real product is NOT prose. It is a .DISCUSSION section followed by
# per-island sections (Kauai, Oahu, Maui, Big Island ...). Each island section
# begins with a zone-code line (e.g. "HIZ006-007-009-032>035-040245-") then the
# island name on its own line ("Oahu-") then an issuance timestamp, then a
# fixed-width surf table:
#
#     __________________________________________________________________
#                           Tonight                    Friday
#     Shores                  Surf                       Surf
#                          PM     AM                  AM     PM
#     __________________________________________________________________
#     North Facing         0-2    0-2                 0-2    1-3
#     West Facing          1-3    1-3                 1-3    1-3
#     South Facing         2-4    3-5                 3-5    3-5
#     East Facing          3-5    3-5                 3-5    3-5
#
# Each table row is "<Shore> Facing" followed by exactly 4 whitespace-separated
# range tokens (like "0-2", "3-5", occasionally "10-15"). The 4 tokens map to
# the two period-blocks in the header: the first block (2 cols) then the second
# block (2 cols).
#
# HEADER VARIANTS observed across products inspected (2026-06-27..07-02):
#   * PM issuance (e.g. "311 PM HST Sat Jun 27"): first block header is
#     "Tonight" with sub-columns "PM  AM"; second block is the next day
#     ("Saturday"/"Sunday"/"Monday"/"Friday") with "AM  PM".
#     -> tokens = [Tonight-PM, Tonight-AM, Nextday-AM, Nextday-PM]
#   * AM issuance (e.g. "303 AM HST Sun Jun 28"): first block header is "Today"
#     with sub-columns "AM  PM"; second block is the next day with "AM  PM".
#     -> tokens = [Today-AM, Today-PM, Nextday-AM, Nextday-PM]
#
# "Today's truth" for the accuracy pipeline is the EARLIEST AM surf value:
#   * If the first period block is "Tonight" (sub-cols "PM AM") the AM value is
#     the SECOND token.
#   * If the first period block starts with a day period ("Today" / a weekday,
#     sub-cols "AM PM") the AM value is the FIRST token.
# This was confirmed against the hand-derived ground-truth midpoints in
# scripts/hindcast_audit.py (matches on 11/12 shore-days; the one 1 ft outlier
# is hand-derivation noise, not a column-selection error).

# A surf range token like "0-2", "3-5", "10-15" (NOT a zone code / date).
_RANGE_RE = re.compile(r"^(\d{1,2})-(\d{1,2})$")

# Table row: "<Shore> Facing   <r1> <r2> <r3> <r4>"
_TABLE_ROW_RE = re.compile(
    r"^(North|West|South|East)\s+Facing\s+(.+)$", re.I
)

# Island section boundary markers.
_ZONE_CODE_RE = re.compile(r"^[A-Z]{2}Z\d{3}[\d>-]*-\s*$")
_ISLAND_NAME_RE = re.compile(r"^([A-Za-z][A-Za-z /]+)-\s*$")

# Header period line, e.g. "                      Tonight                    Friday".
# We only need to know whether the FIRST period block is "Tonight" or a
# day-period (Today / a weekday).
_FIRST_PERIOD_RE = re.compile(r"^\s*([A-Za-z]+)")


def _fetch_afos(pil: str, session: Optional[requests.Session] = None) -> Optional[str]:
    """Fetch the most recent text of an AFOS product; None on failure."""
    session = session or requests.Session()
    params = {"pil": pil, "fmt": "text", "limit": "1"}
    try:
        resp = session.get(AFOS_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("AFOS fetch failed for %s: %s", pil, e)
        return None
    text = resp.text
    if not text or "SRF" not in text and "CFW" not in text and pil not in text:
        # Very defensive: an empty/HTML error page.
        if not text.strip():
            return None
    return text


def _find_oahu_section(lines: list[str]) -> Optional[tuple[int, int]]:
    """Return (start, end) line indices bounding the Oahu island section.

    Start is the "Oahu-" name line (which follows a zone-code line); end is the
    next island's zone-code line, or the "$$" segment terminator, or EOF.
    """
    start = None
    for i, line in enumerate(lines):
        m = _ISLAND_NAME_RE.match(line.strip())
        if not m:
            continue
        name = m.group(1).strip().lower()
        # Must be preceded (allowing blank lines) by a zone-code line to be a
        # genuine island header (not e.g. a hyphenated word).
        prev = i - 1
        while prev >= 0 and not lines[prev].strip():
            prev -= 1
        if prev < 0 or not _ZONE_CODE_RE.match(lines[prev].strip()):
            continue
        if name == "oahu":
            start = i
        elif start is not None:
            # Next island header after Oahu -> end of Oahu section.
            return start, prev
    if start is None:
        return None
    # Oahu is the last island (or terminator handling): scan to $$ or EOF.
    for j in range(start + 1, len(lines)):
        if lines[j].strip() == "$$":
            return start, j
    return start, len(lines)


def _select_am_token(tokens: list[str], first_period_is_tonight: bool) -> Optional[str]:
    """Pick the earliest-AM range token from a table row's 4 range tokens."""
    ranges = [t for t in tokens if _RANGE_RE.match(t)]
    if len(ranges) < 2:
        return None
    # "Tonight" block -> sub-cols (PM, AM) -> AM is 2nd token.
    # "Today"/day block -> sub-cols (AM, PM) -> AM is 1st token.
    return ranges[1] if first_period_is_tonight else ranges[0]


def _midpoint(range_token: str) -> Optional[float]:
    m = _RANGE_RE.match(range_token)
    if not m:
        return None
    lo, hi = int(m.group(1)), int(m.group(2))
    return (lo + hi) / 2.0


def parse_srfhfo(text: str) -> dict[str, Optional[float]]:
    """Parse SRFHFO text into shore -> earliest-AM surf midpoint (ft) for Oahu.

    Locates the Oahu island section, reads its fixed-width surf table, and for
    each "<Shore> Facing" row selects the earliest AM range token (mapping the
    header period block: "Tonight" -> 2nd token, "Today"/day -> 1st token).
    Midpoint = (low + high) / 2. Shores with no parseable range map to None.
    """
    result: dict[str, Optional[float]] = {s: None for s in SHORES}
    if not text:
        return result

    lines = text.splitlines()
    bounds = _find_oahu_section(lines)
    if bounds is None:
        return result
    start, end = bounds
    section = lines[start:end]

    # Determine whether the first period block is "Tonight" (PM/AM ordering) or
    # a day period ("Today"/weekday, AM/PM ordering) from the period header row.
    # That row is the first non-blank line after the opening "____" rule that
    # contains an alpha word and no "Surf"/"Shores"/digits.
    first_period_is_tonight = True  # safe default (PM issuances are the nightly source)
    for line in section:
        low = line.lower()
        if "tonight" in low and "surf" not in low:
            first_period_is_tonight = True
            break
        if ("today" in low or any(
            d in low for d in ("monday", "tuesday", "wednesday", "thursday",
                                "friday", "saturday", "sunday")
        )) and "surf" not in low and "facing" not in low:
            # A period header line naming a day but not "Tonight".
            first_period_is_tonight = "tonight" in low
            break

    for line in section:
        m = _TABLE_ROW_RE.match(line.strip())
        if not m:
            continue
        shore = m.group(1).title()
        tokens = m.group(2).split()
        tok = _select_am_token(tokens, first_period_is_tonight)
        if tok is not None:
            result[shore] = _midpoint(tok)
    return result


# --- CFWHFO structure (verified live + archived, PHFO WHHW40) ----------------
# A Coastal Hazard Message is split into VTEC segments terminated by "$$". Each
# segment carries: a zone-code line, one or more VTEC action strings, zone-name
# lines, a timestamp, then "...HEADLINE..." lines and a "* WHERE..." block.
#
# VTEC action codes are the source of truth for whether a hazard is ACTIVE:
#   /O.NEW.PHFO.SU.Y.0029.../  new       -> ACTIVE
#   /O.CON.PHFO.SU.Y.0029.../  continued -> ACTIVE
#   /O.EXT / EXA / EXB         extended  -> ACTIVE
#   /O.CAN.PHFO.SU.W.0008.../  cancelled -> NOT active
#   /O.EXP...                  expired   -> NOT active
# Phenomenon.significance we care about (surf): SU.W = High Surf WARNING,
# SU.Y = High Surf ADVISORY. Others (CF.* coastal flood) are ignored here.
#
# A single segment can mix actions, e.g. a swell downgrade shows both
# "/O.CAN.PHFO.SU.W.../" (warning cancelled) and "/O.NEW.PHFO.SU.Y.../" (advisory
# issued) with headlines "...HIGH SURF WARNING IS CANCELLED..." AND
# "...HIGH SURF ADVISORY IN EFFECT UNTIL...". So we key off the per-VTEC action,
# not off headline text alone. Headlines/WHERE text are used only to derive the
# facing shore ("...ALL SOUTH FACING SHORES...", "* WHERE...South facing shores").
#
# Cancellation-only products (verified live 2026-07-02) look like:
#   /O.CAN.PHFO.SU.Y.0030.../  + "...HIGH SURF ADVISORY IS CANCELLED..." +
#   "Surf along south-facing shores has dropped below advisory levels."
# These must NOT be reported as active.

_VTEC_RE = re.compile(
    r"/[OTEX]\.(?P<action>[A-Z]{3})\.[A-Z]{4}\.(?P<phen>[A-Z]{2})\.(?P<sig>[A-Z])\."
)
_ACTIVE_ACTIONS = {"NEW", "CON", "EXT", "EXA", "EXB"}
_SURF_PHEN = "SU"  # High Surf
_SIG_LABEL = {"W": "High Surf Warning", "Y": "High Surf Advisory"}

# Oahu zone codes (HIZ0..) seen in PHFO coastal products, plus Oahu zone-name
# substrings, used to restrict advisories to Oahu.
_OAHU_ZONE_CODES = {"003", "005", "006", "007", "008", "009",
                    "031", "032", "033", "034", "035"}
_OAHU_ZONE_NAMES = re.compile(
    r"oahu|waianae|honolulu|ewa plain|olomana|koolau|makapuu|"
    r"north shore|windward oahu",
    re.I,
)
_HIZ_CODE_RE = re.compile(r"HIZ((?:\d{3}[>-])+\d{0,3})")

_SHORE_WORDS = {
    "North": re.compile(r"north[- ]facing", re.I),
    "West": re.compile(r"west[- ]facing", re.I),
    "South": re.compile(r"south[- ]facing", re.I),
    "East": re.compile(r"east[- ]facing", re.I),
}


def _segment_is_oahu(segment: str) -> bool:
    """True if the segment's zone list includes an Oahu zone (code or name)."""
    codes = set()
    for m in re.finditer(r"\bHIZ([\d>-]+)", segment):
        # Expand simple lists and ">" ranges of 3-digit zone codes.
        raw = m.group(1)
        parts = re.findall(r"\d{3}", raw)
        codes.update(parts)
        # Handle ranges like 031>034.
        for rng in re.finditer(r"(\d{3})>(\d{3})", raw):
            lo, hi = int(rng.group(1)), int(rng.group(2))
            codes.update(f"{n:03d}" for n in range(lo, hi + 1))
    if codes & _OAHU_ZONE_CODES:
        return True
    return bool(_OAHU_ZONE_NAMES.search(segment))


def parse_cfwhfo(text: str) -> dict[str, str]:
    """Parse CFWHFO into shore -> ACTIVE High Surf advisory summary.

    Only genuinely active High Surf Warning/Advisory hazards are reported, using
    per-VTEC action codes (NEW/CON/EXT/EXA/EXB = active; CAN/EXP = not) and
    restricted to segments covering an Oahu zone. Cancellations and
    "below advisory levels" text are never reported. Empty dict if none active.
    """
    advisories: dict[str, str] = {}
    if not text:
        return advisories

    # Segments are terminated by "$$". Split and process each independently.
    for segment in re.split(r"\n\$\$", text):
        if "SU." not in segment:
            continue
        if not _segment_is_oahu(segment):
            continue

        # Determine active surf hazards from VTEC actions in this segment.
        active_sigs: dict[str, bool] = {}  # sig letter -> active?
        for m in _VTEC_RE.finditer(segment):
            if m.group("phen") != _SURF_PHEN:
                continue
            sig = m.group("sig")
            is_active = m.group("action") in _ACTIVE_ACTIONS
            # An active action wins over a same-sig cancellation (e.g. an EXT).
            active_sigs[sig] = active_sigs.get(sig, False) or is_active

        active_labels = [_SIG_LABEL[s] for s, ok in active_sigs.items()
                         if ok and s in _SIG_LABEL]
        if not active_labels:
            continue

        # Derive the affected shore(s) from headline / WHERE text. Guard against
        # cancellation-only wording leaking in.
        low = segment.lower()
        shores_hit = [sh for sh, rx in _SHORE_WORDS.items() if rx.search(segment)]

        # Build a concise summary from an active "IN EFFECT" headline if present.
        summary = None
        for hb in re.findall(r"\.\.\.([^.]+?(?:IN EFFECT|REMAINS IN EFFECT)[^.]*?)\.\.\.",
                             segment):
            summary = " ".join(hb.split())[:120]
            break
        if summary is None:
            summary = "; ".join(active_labels)

        for sh in shores_hit or SHORES:
            # Prefer the most specific (warning over advisory already folded in).
            advisories[sh] = summary

    return advisories


def fetch_shore_surf(session: Optional[requests.Session] = None) -> dict[str, Optional[float]]:
    """Fetch + parse the latest SRFHFO shore surf midpoints (ft)."""
    text = _fetch_afos("SRFHFO", session)
    if text is None:
        return {s: None for s in SHORES}
    return parse_srfhfo(text)


def fetch_shore_advisories(session: Optional[requests.Session] = None) -> dict[str, str]:
    """Fetch + parse the latest CFWHFO advisories per shore."""
    text = _fetch_afos("CFWHFO", session)
    if text is None:
        return {}
    return parse_cfwhfo(text)
