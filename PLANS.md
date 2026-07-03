# Implementation Plans — Dive Conditions Accuracy Fixes

Ordered by implementation sequence. Each plan lists executor: **Opus-alone** (well-specified, mechanically verifiable) or **Fable-supervised** (design judgment, calibration, or cross-cutting risk — Opus may implement, but Fable must specify the approach and review the result). Rationale for the ordering: fix the *inputs and plumbing* (Plans 1–3) before recalibrating the *model* (Plan 4), because calibration against ground truth is meaningless while inputs are wrong; consistency and hygiene (Plans 5–6) come after the numbers are right; validation (Plan 7) runs last and continuously.

---

## Plan 1 — Stop lying about missing data (Opus-alone)
**Fixes Block 3 + falsy-zero bugs. First because every later fix is unverifiable while silent data loss inflates scores.**

1. Replace all falsy truthiness checks on physical readings with `is not None`:
   - `buoy_client.py:331,350` (wave m→ft), `pacioos_client.py:274`
   - `formatter.py:74,228,388389,545-550` (wave, wind, wpi rendering)
   - `daily_digest.py:1039` (`rain_inches` when 0.0 mm), `noaa_tides_client.py:232`
   - `ranker.py:150,166` (`buoy_data.get("wave_height_ft")` truthiness — 0.0 ft skips the buoy)
2. OWM client: missing wind → `None` (not 0); include `rain_chance`/`rain_amount_mm` keys in the no-data fallback; surface avg-wind not the calmest-hour wind as the representative value (`openweathermap_client.py:132-165`).
3. Scorer: make missing-data defaults *conservative and visible*. Wave None → 40 stays, but add a `data_completeness` field to `ScoringResult` (fraction of the 5 components with real data) and cap grade at C when < 0.6; append a warning "score based on incomplete data (X/5 factors)".
4. Staleness guards: reject buoy rows older than 3 h (`buoy_client.py` — parse row timestamp), USGS readings older than 3 h, and reset the PacIOOS circuit breaker after 10 min instead of process-lifetime.
5. Workflow: log a visible warning when `OPENWEATHERMAP_API_KEY` is absent.

**Acceptance:** unit tests for 0.0-value rendering/scoring; a run with all clients mocked-dead produces grade ≤ C with "incomplete data" warnings, not a clean "Fair".

## Plan 2 — Timezone correctness (Opus-alone)
**Fixes Block 4. Small, mechanical, unblocks honest scoring in CI.**

1. Introduce `HST = ZoneInfo("Pacific/Honolulu")` in one util; pin scoring evaluation time to the *dive window* (default 07:00 HST today) rather than wall-clock: set `evaluation_time` explicitly in ranker and both digest paths.
2. Fix OWM target-date selection to HST (`openweathermap_client.py:90,108,121`).
3. Fix NOAA tides begin/end dates to HST to match `time_zone=lst_ldt` (`noaa_tides_client.py:143-146,205-206`).
4. Set `TZ: Pacific/Honolulu` in the workflow env as belt-and-braces.

**Acceptance:** test that scoring at 15:30 UTC yields time_score 95–100 (dawn window); tide window test around HST midnight.

## Plan 3 — Rain and visibility that reflect reality (Fable-supervised)
**Fixes Block 2. Supervised because the data-source choice is a design decision.**

1. Add *observed past-48h rainfall*: NWS API observed precip or Mesonet/HADS gauges per coast (design choice Fable must approve; OWM `/forecast` cannot provide it). Feed to `rainfall_48h_inches` in **all** scoring paths, including the ranker.
2. Score rain *chance* as well as volume for forecast windows: penalty proportional to pop% above 40, or scale accumulation thresholds to window length — pick one, document it.
3. Wire `rainfall_48h_inches` into `ranker.score_site()` so the headline path stops being rain-blind.
4. CWB advisories: match by island+region/coast rather than site-name substring; treat any active Oahu brown-water advisory as a coast-wide visibility penalty (and gate only on matched sites); honor real status + posted date, drop advisories older than 7 days.

**Acceptance:** simulated "rained 2 in yesterday, dry today" scenario drops visibility scores in Top Sites; simulated CWB advisory named with a DOH-style beach name gates the corresponding site.

## Plan 4 — Wave model: predict surf at the beach, not swell at the buoy (Fable-supervised — the core redesign)
**Fixes Block 1, the biggest accuracy problem in both directions. After Plans 1–3 so calibration uses clean inputs.**

Approach (Fable to finalize, options in order of preference):
1. **Directional exposure filter (minimum viable):** decompose buoy reading using MWD vs site `swell_exposure` (primary/secondary, ±45–67°). Effective height = WVHT × exposure_factor (1.0 aligned, →0.1–0.3 sheltered/opposed). This alone un-breaks North Shore summer (NE windswell no longer counts against NW-facing sites).
2. **PacIOOS SWAN as primary near-shore source:** the SWAN model already resolves nearshore transformation; prefer it over raw buoy when available, buoy as fallback *with* the exposure filter.
3. **Calibrate against NWS SRFHFO:** nightly job (or audit script) compares model effective surf height per shore vs the NWS surf zone forecast; fit/adjust the exposure factors and thresholds until agreement within ±1.5 ft over a trailing month.
4. Recalibrate WPI thresholds on *effective* height (e.g. excellent ≤ 15, poor ≥ 120 — to be fit from data, current 5/50 zeroes everything).
5. Safety gates operate on effective surf height; keep the absolute 8 ft ceiling on raw buoy Hs as a backstop.
6. Fix `sites.yaml` station assignments: SE-corner S-facing sites → 51211; Laie/Goat Island buoy → 51201 or 51207-fallback and streamgage → windward gage (not Waimea River); Hanauma/SE streamgage → closer drainage or null; remove/mark offline buoy 51207 with explicit fallback chain.
7. Use dominant vs swell period consistently (label DPD as such; don't silently substitute).

**Acceptance:** rerun the 2026-06-26→07-02 hindcast (script in Plan 7): North Shore sites must grade B/A on 0–2 ft days instead of F; windward sites must not gate on ordinary 3–5 ft trade-swell days without advisories; south shore ranks plausibly vs SRFHFO.

## Plan 5 — One scoring path (Opus-alone, after 3–4 so there's one correct path to unify onto)
**Fixes Block 5.**

1. Extract a single `build_scoring_input(site, conditions, dive_window)` used by ranker, digest-Today, and digest-forecast; forecast days pass predicted tide (NOAA predictions API already fetched) and omit only what's genuinely unknowable — with the same neutral handling everywhere.
2. Headline aggregates (Top Sites, diveable counts, best coast) computed from the same rain-aware, full-day-wind inputs as the Today table (kill the dawn-snapshot NWS wind in the ranker; use the OWM window wind).
3. Normalize coast naming between buoy outlooks and coast summaries.

**Acceptance:** integration test asserting Today-table grade == Top-Sites grade for the same site; forecast-day tide score no longer uniformly 100.

## Plan 6 — Parser/client hardening + dead code (Opus-alone)
**Fixes Block 6. Low risk, anytime after Plan 1; kept late because impact is smaller.**

1. NDBC: header-validated column parsing; numeric sentinel filtering (≥90/990 per NDBC docs); NWS wind range "10 to 15 mph" → take max; PacIOOS exact 7-column validation with header-name indexing.
2. Investigate/fix NWS gridpoint 404 at Waimea Bay coords (nudge coordinates seaward or cache the grid endpoint per site).
3. Delete or implement the 1-line stubs (`safety_gates.py`, `cache.py`, `scheduler.py`, `three_day_digest.py`, `weekly_digest.py`); remove buoy-wind expectations (Oahu NDBC buoys report no wind — confirmed all stations MM).
4. Sync `config/config.yaml` scoring constants with scorer after Plan 4 recalibration (or better: load them from config to end the dual-maintenance note).

**Acceptance:** parser unit tests with real captured NDBC/ERDDAP payloads including malformed rows.

## Plan 7 — Continuous accuracy validation (Opus-alone to build; Fable to interpret results)
**The regression harness that keeps this honest.**

1. `scripts/hindcast_audit.py`: given a date range, pull NDBC realtime2 history + IEM SRFHFO/CFWHFO archives, run the scorer per site per day at 07:00 HST, and emit a comparison table (model grade vs NWS surf per shore vs advisories) + disagreement score.
2. Nightly CI step appends the day's prediction + next day's observed truth to a CSV artifact; weekly summary of hit-rate.
3. Alert (ntfy) when disagreement exceeds threshold 3 days running.

**Acceptance:** hindcast over the audit week reproduces the ground-truth tables in POSTMORTEM.md.

---

## Ordering summary
1. Plan 1 (Opus) → 2 (Opus) → 3 (Fable-supervised) → 4 (Fable-supervised) → 5 (Opus) → 6 (Opus) → 7 (Opus build / Fable interpret).
- Plans 1+2 can run in parallel (disjoint files except minor scorer touch — sequence the scorer edit).
- Plan 4 is the largest and highest-value; do not start it before 1–2 land (clean inputs) and ideally 3 (so calibration isn't polluted by rain-blind scores).
