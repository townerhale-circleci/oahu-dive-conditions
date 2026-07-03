# Post-Mortem: Oahu Dive Conditions Model Accuracy Audit

**Date:** 2026-07-02
**Audit window:** 2026-06-26 → 2026-07-02 (7 days)
**Method:** Full code review (core scorer/ranker by Fable; client layer and digest/orchestration layer by two Opus subagents), plus an empirical audit comparing live model output against independent ground truth (NDBC buoy observations, NWS Surf Zone Forecast SRFHFO, NWS coastal hazard archive, PHNL climate reports).

## Verdict

**The model is not accurate enough to trust yet.** It fails in *both* directions:

- **Falsely UNSAFE (pessimistic):** On 2026-07-02, actual North Shore surf was 0–2 ft (NWS SRFHFO) — ideal summer diving. The model graded Sharks Cove, Three Tables, and Waimea Bay **F/UNSAFE** because it compared raw offshore buoy significant wave height (4.9 ft of short-period NE windswell at buoy 51201) against beach safety thresholds of 2–3 ft. This was true **all 7 audit days** (buoy 3.6–4.6 ft vs actual surf 0–2 ft). North Shore sites can essentially never pass their own safety gate in the current design, even in flat summer conditions.
- **Falsely FINE (optimistic):** The headline report path (Top Sites table, "X/Y diveable", best coast) is computed by `ranker.score_site()`, which **never includes rain** — `rainfall_48h_inches` is only wired into the secondary digest tables. Combined with generous defaults when data is missing (wave 40, wind 50, visibility 70, tide 100), silent API failures *raise* scores. This is the "it says fine but it's rainy" complaint.
- **Compressed/meaningless scores:** The Wave Power Index scale (0 points at WPI ≥ 50) zeroes out the 35%-weight wave component for virtually all real readings (e.g. 2.6 ft @ 14 s → WPI 98 → 0 points). Today every scored site got wave_power_score = 0.0. Rankings are therefore driven by wind + defaults, not waves.

## Empirical audit results (2026-06-26 → 07-02)

Ground truth: no High Surf Advisories/Warnings, essentially zero rain (PHNL trace/0.00), typical summer trades.

| Shore | NWS actual surf (week) | Buoy WVHT the model uses | Model outcome | Reality |
|---|---|---|---|---|
| North (51201) | 0–2 ft all week | 3.6–4.6 ft | Sites with 2–3 ft thresholds gate to F every day | Best diving of the year |
| Windward/SE (51202) | East 3–6 ft | 5.9–7.5 ft | Sandy Beach, Kahana Bay etc. gate to F | Normal trade-wind conditions, no advisories |
| South (51211) | 2–4 ft | 2.6–3.6 ft | D grades (42–49) | Decent summer mornings |
| West (51212) | 1–3 ft | 2.6–3.9 ft | Electric Beach D 49.6 = island's best | Good conditions |

Model output on a *good* day: "8 of 12 sampled sites F/UNSAFE, best site a D." A correct model would have said "North Shore A, West B."

## Root causes, grouped

### Block 1 — Wave physics: offshore Hs ≠ surf at the beach (CRITICAL, both directions)
1. Buoy WVHT (open-ocean significant wave height, all swell components mixed) is compared directly against per-site `max_safe_height_ft` and fed to WPI. No shoaling/exposure transform, no directional decomposition (`scorer.py:189-205`).
2. **Swell direction is fetched but never used.** `swell_direction_deg` flows into `ScoringInput` and is ignored by every scoring function. A NE windswell counts fully against a NW-facing site it can't reach; a wrapping S swell counts fully at a sheltered cove.
3. WPI calibration (`WPI_EXCELLENT=5`, `WPI_POOR=50`) is miscalibrated for offshore-buoy inputs by roughly an order of magnitude; wave score is ~always 0.
4. Buoy assignments in `sites.yaml` are wrong for several regions: all six SE-corner sites use 51202 (windward); Laie/Goat Island use 51202 + the *Waimea River* streamgage; Hanauma/SE sites use Waimanalo Stream. Buoy 51207 (Kaneohe) is **offline (404)** — sites assigned to it silently fall back.
5. No staleness check anywhere: a buoy or gage that died days ago is still "current" (`buoy_client.py:326`, `usgs_client.py:189`).

### Block 2 — Rain & visibility never actually work (CRITICAL, optimistic)
1. Headline scoring path (ranker) omits `rainfall_48h_inches` entirely (`ranker.py:284-300`).
2. The rain that *is* wired in (digest paths, `daily_digest.py:1049,1331`) comes from the OpenWeatherMap **forecast** — a per-calendar-day future amount. Past-48h accumulation (what actually causes brown water) is structurally unobtainable from that endpoint. Recent heavy rain is invisible.
3. Rain *chance* (pop %) is displayed in the report but never scored; only millimeters are scored, and short-window totals rarely cross the 0.1 in threshold → a "70% rain" day scores visibility 100.
4. CWB brown-water advisory matching is name-substring based (`cwb_client.py:305-315`); DOH beach names rarely match dive-site names, so a real advisory likely fails to gate. Advisory status is hardcoded "active" with no date filtering.
5. OWM UTC date bug: target "today" computed in UTC (`openweathermap_client.py:121`); from 2 pm HST onward it reads tomorrow's forecast.

### Block 3 — Missing data silently inflates scores (HIGH, optimistic)
1. Scorer defaults on None: wave 40, wind 50, visibility 70, tide 100 (`optimal_tide: any`). A site with *zero* data scores ~60 ("Fair") plus time bonus.
2. Every client converts failures/out-of-domain/circuit-breaker-open into None or empty (PacIOOS breaker never resets within a run: `pacioos_client.py:139-144`).
3. OWM missing wind defaults to **0 mph** → wind score 100 (`openweathermap_client.py:132-150`). The "best hour" wind (calmest hour) is surfaced as the representative wind.
4. Falsy-zero bugs drop legitimate readings: `if wave_height_m:` treats 0.0 m (dead flat = perfect) as missing (`buoy_client.py:331,350`, `pacioos_client.py:274`, formatter `:74,:228,:388,:550`, `daily_digest.py:1039`).

### Block 4 — Time and timezone (HIGH)
1. `score_time_of_day` uses naive `datetime.now()`; GitHub Actions runs at 15:30 **UTC**, so hour=15 → 50/100 instead of Hawaii dawn 100/100. Every site loses 5 points every scheduled run; the report claims to evaluate 5:30 AM HST but never does.
2. NOAA tides date windows are built from `utcnow()` but queried in local time (`noaa_tides_client.py:143-146,205-206`) — wrong tide phase near HST midnight.
3. Ranker "Today" wind is the first NWS hourly row (≈generation time, calm dawn), inflating the headline wind score; the OWM full-day fix (commits ef7aaac/7daed94) never propagated to the ranker path.

### Block 5 — Three divergent scoring paths (MEDIUM, consistency)
| Path | Drives | Rain | Tide | Discharge | Wind |
|---|---|---|---|---|---|
| A: ranker | Top Sites, diveable counts, best coast | ✗ | ✓ | ✓ | NWS dawn snapshot |
| B: digest "Today" beaches | Today table | ✓ | ✓ | ✓ | OWM full-day |
| C: digest forecast days | Forecast tables | ✓ | ✗ (free 100) | ✗ | OWM/NWS |

Sections of the same email can disagree; forecast days are systematically inflated vs Today (free tide=100, no discharge).

### Block 6 — Smaller correctness issues (MEDIUM/LOW)
- NWS wind range "10 to 15 mph" parsed as 10 (low end) (`nws_client.py:172`).
- DPD (dominant period) silently substituted into the swell-period slot (`buoy_client.py:352`).
- NDBC sentinel filtering is string-equality based, brittle (`buoy_client.py:236-243`).
- PacIOOS ERDDAP row parse allows 6-column rows then indexes col 6 (`pacioos_client.py:195-197`).
- NWS gridpoint 404 for Waimea Bay coordinates (observed live) → no wind, silent.
- Dead code/stubs: `safety_gates.py`, `cache.py`, `scheduler.py`, `three_day_digest.py`, `weekly_digest.py` are 1-line placeholders.
- Missing OWM API key silently drops rain from score *and* report (workflow only warns at debug level).
- 51211 buoy DPD occasionally ambiguous in parse; wind columns MM on all Oahu buoys (buoy wind is never available — remove that expectation).

## What was verified correct
Unit conversions (m→ft, OWM imperial), PacIOOS lon-360 conversion, tide `lst_ldt` request units, USGS cfs handling, safety-gate mechanics (they fire and zero correctly — they're just fed the wrong wave number), weight table consistency, diveable-threshold consistency, commit 5f03183's OWM hour fix, commit 29eb098's grade/score field alignment.

## The one-sentence diagnosis
The scoring framework is sound, but it is fed the wrong physical quantity (offshore Hs as beach surf), ignores the one variable that determines whether a swell matters (direction), omits rain from the headline path, and treats missing data as good news — so its grades correlate only weakly with real conditions in either direction.
