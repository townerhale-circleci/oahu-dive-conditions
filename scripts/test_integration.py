#!/usr/bin/env python3
"""Integration test script to verify the full pipeline works.

Tests:
1. Site database loading
2. Individual API clients
3. Single site scoring with live data
4. Multi-site ranking

Run from project root:
    python scripts/test_integration.py
"""

import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_site_database():
    """Test loading and querying the site database."""
    print("\n" + "="*60)
    print("TEST: Site Database")
    print("="*60)

    from src.core.site import SiteDatabase, get_site

    db = SiteDatabase()

    print(f"\n  Total sites loaded: {db.site_count}")
    print(f"  Coasts: {', '.join(db.coasts)}")

    # Test getting a specific site
    site = db.get_site("electric_beach")
    if site:
        print(f"\n  Sample site: {site.name}")
        print(f"    ID: {site.id}")
        print(f"    Coast: {site.coast}")
        print(f"    Coordinates: {site.coordinates.lat}, {site.coordinates.lon}")
        print(f"    Depth: {site.depth_range.min_ft}-{site.depth_range.max_ft}ft")
        print(f"    Skill: {site.skill_level}")
        print(f"    Max safe wave: {site.max_safe_wave_height}ft")
        print(f"    Buoy: {site.nearest_buoy}")
        print(f"    Streamgage: {site.nearest_streamgage}")
    else:
        print("  WARNING: Could not find electric_beach site")

    # Test filtering
    current_month = datetime.now().month
    in_season = db.get_in_season_sites(current_month)
    print(f"\n  Sites in season (month {current_month}): {len(in_season)}")

    spearfishing = db.get_spearfishing_sites()
    print(f"  Spearfishing sites: {len(spearfishing)}")

    beginners = db.get_sites_by_skill("beginner")
    print(f"  Beginner-friendly sites: {len(beginners)}")

    # Test coast filtering
    for coast in db.coasts:
        coast_sites = db.get_sites_by_coast(coast)
        print(f"  {coast}: {len(coast_sites)} sites")

    return True


def test_api_clients():
    """Test each API client can connect (with error handling)."""
    print("\n" + "="*60)
    print("TEST: API Client Connectivity")
    print("="*60)

    results = {}

    # Test Buoy Client
    print("\n  Testing Buoy Client (NDBC)...")
    try:
        from src.clients.buoy_client import BuoyClient
        client = BuoyClient()
        data = client.get_current_conditions("51201")  # Waimea buoy
        if data.get("wave_height_ft"):
            print(f"    ✓ Buoy 51201: {data['wave_height_ft']:.1f}ft @ {data.get('swell_period_s', 'N/A')}s")
            results["buoy"] = True
        else:
            print(f"    ⚠ Buoy returned no wave height data")
            results["buoy"] = False
    except Exception as e:
        print(f"    ✗ Buoy error: {e}")
        results["buoy"] = False

    # Test NWS Client
    print("\n  Testing NWS Client...")
    try:
        from src.clients.nws_client import NWSClient
        client = NWSClient()
        alerts = client.get_alerts(area="HI")
        print(f"    ✓ NWS: {len(alerts)} active alerts for Hawaii")
        results["nws"] = True
    except Exception as e:
        print(f"    ✗ NWS error: {e}")
        results["nws"] = False

    # Test NOAA Tides Client
    print("\n  Testing NOAA Tides Client...")
    try:
        from src.clients.noaa_tides_client import NOAATidesClient
        client = NOAATidesClient()
        tides = client.get_next_tides("1612340", count=2)  # Honolulu
        if tides:
            next_tide = tides[0]
            tide_type = "High" if next_tide["is_high"] else "Low"
            print(f"    ✓ Next tide: {tide_type} at {next_tide['time']} ({next_tide['water_level_ft']:.1f}ft)")
            results["tides"] = True
        else:
            print(f"    ⚠ No tide data returned")
            results["tides"] = False
    except Exception as e:
        print(f"    ✗ Tides error: {e}")
        results["tides"] = False

    # Test USGS Client
    print("\n  Testing USGS Client...")
    try:
        from src.clients.usgs_client import USGSClient
        client = USGSClient()
        discharge = client.get_current_discharge("16247100")  # Kaneohe
        if discharge is not None:
            print(f"    ✓ Kaneohe Stream: {discharge:.1f} cfs")
            results["usgs"] = True
        else:
            print(f"    ⚠ No discharge data returned")
            results["usgs"] = False
    except Exception as e:
        print(f"    ✗ USGS error: {e}")
        results["usgs"] = False

    # Test CWB Client
    print("\n  Testing Clean Water Branch Client...")
    try:
        from src.clients.cwb_client import CWBClient
        client = CWBClient()
        summary = client.get_advisory_summary()
        print(f"    ✓ CWB: {summary['total_advisories']} active advisories")
        if summary['affected_beaches']:
            print(f"      Affected: {', '.join(summary['affected_beaches'][:3])}")
        results["cwb"] = True
    except Exception as e:
        print(f"    ✗ CWB error: {e}")
        results["cwb"] = False

    # Test PacIOOS Client (may be slow)
    print("\n  Testing PacIOOS Client (SWAN model)...")
    try:
        from src.clients.pacioos_client import PacIOOSClient
        client = PacIOOSClient()
        # Electric Beach coordinates
        data = client.get_current_conditions(21.3542, -158.1314)
        if data.get("wave_height_ft"):
            print(f"    ✓ PacIOOS: {data['wave_height_ft']:.1f}ft @ {data.get('period_s', 'N/A')}s")
            results["pacioos"] = True
        else:
            print(f"    ⚠ PacIOOS returned no data (may be outside model domain)")
            results["pacioos"] = False
    except Exception as e:
        print(f"    ✗ PacIOOS error: {e}")
        results["pacioos"] = False

    # Summary
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\n  API Connectivity: {passed}/{total} clients working")

    return passed >= 3  # At least 3 clients should work


def test_single_site_scoring():
    """Test scoring a single site with live data."""
    print("\n" + "="*60)
    print("TEST: Single Site Scoring (Electric Beach)")
    print("="*60)

    from src.core.site import SiteDatabase
    from src.core.ranker import SiteRanker

    db = SiteDatabase()
    site = db.get_site("electric_beach")

    if not site:
        print("  ERROR: Could not find Electric Beach site")
        return False

    print(f"\n  Scoring: {site.name}")
    print(f"  Location: {site.coordinates.lat}, {site.coordinates.lon}")
    print(f"  Max safe wave height: {site.max_safe_wave_height}ft")

    ranker = SiteRanker(site_db=db)

    print("\n  Fetching live conditions...")
    try:
        ranked = ranker.score_site(site)
    except Exception as e:
        print(f"  ERROR: Failed to score site: {e}")
        return False

    conditions = ranked.conditions
    score = ranked.score

    print(f"\n  === ENVIRONMENTAL CONDITIONS ===")
    print(f"  Wave Height: {conditions.wave_height_ft or 'N/A'} ft (source: {conditions.wave_source or 'none'})")
    print(f"  Wave Period: {conditions.wave_period_s or 'N/A'} s")
    print(f"  Wind Speed:  {conditions.wind_speed_mph or 'N/A'} mph")
    print(f"  Tide Phase:  {conditions.tide_phase or 'N/A'}")
    print(f"  Discharge:   {conditions.stream_discharge_cfs or 'N/A'} cfs")
    print(f"  High Surf Warning: {conditions.high_surf_warning}")
    print(f"  Brown Water Advisory: {conditions.brown_water_advisory}")

    if conditions.errors:
        print(f"\n  Data fetch errors:")
        for err in conditions.errors:
            print(f"    - {err}")

    print(f"\n  === SCORING RESULT ===")
    print(f"  Grade: {score.grade.value} ({score.grade.name})")
    print(f"  Total Score: {score.total_score}")
    print(f"  Diveable: {score.diveable}")
    print(f"  Wave Power Index: {score.wave_power_index}")

    print(f"\n  Component Scores:")
    print(f"    Wave Power: {score.wave_power_score}")
    print(f"    Wind:       {score.wind_score}")
    print(f"    Visibility: {score.visibility_score}")
    print(f"    Tide:       {score.tide_score}")
    print(f"    Time:       {score.time_score}")

    print(f"\n  Safety Gates Passed: {score.safety_gates_passed}")
    if score.failed_gates:
        for gate in score.failed_gates:
            print(f"    FAILED: {gate.reason}")

    if score.warnings:
        print(f"\n  Warnings:")
        for warning in score.warnings:
            print(f"    - {warning}")

    print(f"\n  Summary: {score.summary}")

    return True


def test_multi_site_ranking():
    """Test ranking multiple sites."""
    print("\n" + "="*60)
    print("TEST: Multi-Site Ranking (West Side)")
    print("="*60)

    from src.core.site import SiteDatabase
    from src.core.ranker import SiteRanker

    db = SiteDatabase()
    ranker = SiteRanker(site_db=db)

    # Rank West Side sites (year-round, most likely to work)
    print("\n  Ranking West Side sites...")
    print("  (This may take a moment as we fetch data for each site)")

    try:
        ranked_sites = ranker.rank_coast("west_side", top_n=5)
    except Exception as e:
        print(f"  ERROR: Ranking failed: {e}")
        return False

    if not ranked_sites:
        print("  WARNING: No sites were ranked")
        return False

    print(f"\n  === TOP {len(ranked_sites)} WEST SIDE SITES ===")
    print(f"  {'Rank':<5} {'Grade':<6} {'Score':<7} {'Site':<30} {'Waves':<10}")
    print(f"  {'-'*5} {'-'*6} {'-'*7} {'-'*30} {'-'*10}")

    for ranked in ranked_sites:
        waves = f"{ranked.conditions.wave_height_ft:.1f}ft" if ranked.conditions.wave_height_ft else "N/A"
        print(f"  {ranked.rank:<5} {ranked.grade:<6} {ranked.score.total_score:<7.1f} {ranked.site.name:<30} {waves:<10}")

    # Show details for #1 site
    if ranked_sites:
        top = ranked_sites[0]
        print(f"\n  === #1 SITE DETAILS: {top.site.name} ===")
        print(f"  {top.score.summary}")
        if top.score.warnings:
            print(f"  Warnings: {'; '.join(top.score.warnings)}")

    return True


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "#"*60)
    print("# OAHU DIVE CONDITIONS - INTEGRATION TESTS")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("#"*60)

    tests = [
        ("Site Database", test_site_database),
        ("API Clients", test_api_clients),
        ("Single Site Scoring", test_single_site_scoring),
        ("Multi-Site Ranking", test_multi_site_ranking),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n  EXCEPTION in {name}: {e}")
            results.append((name, False))

    print("\n" + "="*60)
    print("INTEGRATION TEST RESULTS")
    print("="*60)

    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")

    passed_count = sum(1 for _, p in results if p)
    print(f"\n  Total: {passed_count}/{len(results)} tests passed")
    print("="*60)

    return all(p for _, p in results)


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
