# HANDOFF — Dive Conditions Accuracy Work

**Updated:** 2026-07-02. Start a fresh session from this file; everything needed is here + `POSTMORTEM.md` + `PLANS.md`.

## State right now

- **Audit complete.** Verdict in `POSTMORTEM.md`: model fails both ways. Empirically confirmed against 7 days of ground truth (NDBC buoys, NWS SRFHFO surf forecasts, advisory archive — tables are in the post-mortem). Headline failure: on 2026-07-02, actual North Shore surf 0–2 ft (ideal), model graded Sharks Cove/Three Tables/Waimea Bay F/UNSAFE because raw offshore buoy Hs (4.9 ft) is compared to 2–3 ft beach thresholds; and every site's wave-power score was 0.0 (WPI scale miscalibrated).
- **Plans written** in `PLANS.md` — 7 plans, ordered, each tagged Opus-alone vs Fable-supervised.
- **Plans 1 & 2 EXECUTED** (by Opus subagent, verified by Fable): missing-data honesty (falsy-zero fixes, staleness guards, `data_completeness` grade cap, OWM wind=None-not-0, avg-wind for scoring) and timezone correctness (HST pinning, 07:00 HST dive-window evaluation time, tides/OWM HST dates, `TZ` in workflow). New tests: `scripts/test_fixes.py` (4/4 pass). Changes are **uncommitted-then-committed locally on `main`** (see git log); **not pushed**.
- Local dev env: `.venv-audit/` (python3.9, deps installed). Run tests: `.venv-audit/bin/python scripts/test_fixes.py` and `scripts/test_scoring.py`.

## Known-failing tests (pre-existing, intentional to leave)

- `test_scoring.py::test_safety_gate_high_surf_warning` — encodes old rejected behavior (HSW as gate). Decide: update the test or reinstate the gate. Recommend updating the test.
- `test_scoring.py::test_fair_conditions` — fails because of the WPI miscalibration; will be fixed by Plan 4 recalibration.

## Next steps, in order

1. **Plan 3 (Fable-supervised): rain/visibility.** Needs one design decision — source for *observed* past-48h rainfall (OWM forecast structurally can't provide it). Recommendation: NWS/Mesonet observed precip per coast. Also: score rain-chance (pop%), wire rain into `ranker.score_site` (headline path is still rain-blind), fix CWB advisory matching to region-based.
2. **Plan 4 (Fable-supervised): the wave-model redesign** — directional exposure filter (swell MWD vs site exposure; currently direction is fetched and ignored), prefer PacIOOS nearshore over raw buoy, recalibrate WPI + gates on *effective* height, fix `sites.yaml` buoy/streamgage misassignments (SE sites on windward buoy 51202; Laie/Goat on Waimea River gage; buoy 51207 is offline/404). Acceptance: rerun the hindcast — North Shore must grade A/B on the 0–2 ft audit week, not F.
3. Plans 5–7 (Opus-alone): unify the three scoring paths, parser hardening + dead-stub cleanup, and build `scripts/hindcast_audit.py` for continuous validation.

## Decisions the human should confirm (blocking items only)

1. **Plan 4 approach:** OK to use NWS SRFHFO per-shore surf forecast as a calibration target (and possibly as a direct input)? It's the most accurate "surf at the beach" signal available and free.
2. **Rain source (Plan 3):** OK to add NWS observed-precip (past 48 h) as a new client? OWM key stays for forecast rain-chance only.
3. **HSW test:** confirm High Surf Warning should stay non-gating (current design), so the stale test gets updated.
4. Push the local commits to GitHub when satisfied (nothing pushed yet; the live GH Pages report still runs old code until pushed).

## Ground-truth data sources used (for the hindcast harness)

- NDBC: `https://www.ndbc.noaa.gov/data/realtime2/<station>.txt` (~45-day history; Oahu buoys report NO wind — all MM; 51207 offline/404).
- NWS surf zone forecast archive: IEM AFOS `retrieve.py?pil=SRFHFO` (+ per-date JSON API for older days).
- Coastal hazards: `pil=CFWHFO`. Honolulu daily rain: `pil=CLIHNL`. CWB brown-water history: not machine-retrievable (auth-gated SPA) — treat as live-only signal.
