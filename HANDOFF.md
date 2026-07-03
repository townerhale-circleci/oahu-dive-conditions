# HANDOFF — Dive Conditions Accuracy Work

**Updated:** 2026-07-03. **ALL 7 PLANS COMPLETE.** Start a fresh session from this file; background in `POSTMORTEM.md` + `PLANS.md`.

## State right now

- **Plans 1–7 all executed and verified** (Opus-implemented, Fable-specified/reviewed). Six local commits on `main`, **nothing pushed** — the live GH Pages report still runs old code until pushed.
- Full test suite green: test_fixes 4/4, test_rain_visibility 5/5, test_scoring_unification 3/3, test_parsers 10/10, test_surf_transform 39/39, test_scoring **11/11** (stale HSW test updated in Plan 6), test_accuracy_tracking 57/57, test_integration 4/4. Run: `.venv-audit/bin/python scripts/<name>.py`.
- Hindcast acceptance re-verified after everything landed: **MAE 0.773 ft, PASS** (`scripts/hindcast_audit.py`, audit week 2026-06-26..07-02).
- Live sanity 2026-07-03: `run_daily.py` exits 0; SRFHFO truth parses live (N 1.0 / W 2.0 / S 4.0 / E 4.0 ft, matches product); CFWHFO correctly returns no active advisories (only cancellations posted).

## What each plan delivered (commits, newest first)

- `74dc064` **Plan 7** — nightly accuracy CI: `accuracy-tracking` job appends predictions + next-day SRFHFO truth to `accuracy.csv` on gh-pages (`keep_files: true`), 7-day hit-rate summary, ntfy alert on 3 consecutive high-disagreement days (`NTFY_TOPIC` secret, skips if unset). New `src/clients/srfhfo_client.py` (Oahu-section table parser, VTEC-aware CFWHFO), `src/core/accuracy_tracking.py`, `scripts/accuracy_nightly.py`.
- `c9688e3` **Plan 6** — NDBC/PacIOOS header-name parsing + per-field numeric sentinels; NWS wind-range→max; Waimea Bay gridpoint 404 fixed via shoreward-nudge retry; scorer constants now **loaded from config/config.yaml** (defaults preserved); "Large swells"/WPI-warning text rescaled to 12/280; 5 dead stubs deleted; HSW test updated (non-gating design confirmed).
- `4450635` **Plan 5** — single `build_scoring_input` assembler (src/core/scoring_input.py) used by ranker + both digest paths; `site_optimal_tide` always set (digest tide no longer silently 100); ranker scores OWM day-average window wind (NWS = fallback, shared OWM client); forecast days get real predicted tide phase/level (one NOAA hilo call per station); digest "today" anchored to HST (Today == Top-Sites grade verified, 0 mismatches live).
- `a627982` **Plan 3** — observed trailing-48h rainfall via new IEM ASOS client (PHNL→south/southeast, PHNG→windward, PHJR→west, PHHI→north — PHDH reports no data; IEM needs FULL ICAO ids); `rainfall_48h_inches` = observed everywhere, forecast days pass None + `rain_chance_pct` soft penalty (floor 40); CWB: active+≤7d filter, beach→coast keyword map, coast advisory caps visibility at 40 (non-gating), name-matched sites still gate; ranker no longer rain-blind.
- `5305636` **Plan 4**, `5e6c829` **Plans 1–2 + audit** — see PLANS.md/POSTMORTEM.md.

## Decisions taken autonomously (confirm or revert)

1. **Rain source = IEM ASOS observed precip** (free, keyless; same provider as hindcast). OWM key stays for forecast pop% only. (Was flagged as blocking decision #2 in the previous handoff; the recommended option was taken.)
2. **HSW stays non-gating**; the stale test was rewritten to encode that (blocking decision #3 — recommended option taken).
3. Scorer constants moved to config.yaml as single source of truth (Plan 6 "preferred" option).
4. CWB coast mapping is a curated keyword dict in cwb_client.py (`COAST_KEYWORDS`); unmappable advisory beaches are excluded, not island-wide penalized.

## The one remaining human action

**Push to GitHub when satisfied**: `git push origin main`. Until then the published report + nightly accuracy job run old code. After pushing, confirm in Actions that (a) the deploy job still publishes and (b) the new `accuracy-tracking` job commits `accuracy.csv`/`accuracy-summary.md` to gh-pages. Also add the `NTFY_TOPIC` secret if disagreement alerts are wanted (already used by the digest notify step, so likely present).

## Known limitations / watch items

- **North-shore rain gauge is Wheeler (PHHI, central Oahu)** — Dillingham (PHDH) reports nothing on IEM. Nearest available, but inland; north-shore 48h rain is an approximation.
- IEM rate-limits (429) on rapid cold fetches of 5 coasts; per-coast + 30-min caching mitigates, an occasional coast may come back None for one run.
- SRFHFO/CFWHFO parsing is anchored to observed product structure (island line "Oahu-", Tonight/Today column variants, VTEC codes). Unusual phrasings degrade to "no truth for that day" (blank obs columns), never wrong truth. Weekly hit-rate only becomes meaningful after a week of pushed nightly runs.
- CWB scraper fallback path hardcodes status "active" and usually lacks posted_date, so the 7-day freshness filter mainly bites on the API path.
- Hindcast site-mode grades for non-"any"-tide sites cap at C via data-completeness cap (by design — wave-only inputs).
- Grade boundaries (A85/B70/C55/D40) exist in both config.yaml and `_score_to_grade` (currently in sync; scorer does not load these — deliberate scope cut in Plan 6).

## Ground-truth data sources (for reference)

- NDBC: `https://www.ndbc.noaa.gov/data/realtime2/<station>.txt` (~45 days; Oahu buoys report NO wind; 51207 offline).
- IEM AFOS: `retrieve.py?pil=SRFHFO|CFWHFO|CLIHNL` (+ per-date JSON API for archive).
- IEM ASOS precip: `cgi-bin/request/asos.py` with `network=HI_ASOS`, full ICAO station ids, `data=p01i` (sum per-clock-hour maxes).
- CWB brown-water history: not machine-retrievable (auth-gated SPA) — live-only signal.
