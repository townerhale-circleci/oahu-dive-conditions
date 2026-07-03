#!/usr/bin/env python3
"""Nightly accuracy-tracking step (Plan 7).

Each run:
  1. Reads the persistent CSV (``--csv``, committed on the gh-pages branch).
  2. Backfills YESTERDAY's observed truth (NWS SRFHFO per-shore surf midpoints
     + CFWHFO advisories) onto yesterday's prediction rows.
  3. Runs TODAY's live ranking and appends/updates today's per-site prediction
     rows (idempotent on (date, site_id) — a same-day re-run overwrites, never
     duplicates).
  4. Writes the trailing-7-day hit-rate summary to ``--summary`` (Markdown).
  5. Evaluates the 3-consecutive-day disagreement alert; if triggered and
     NTFY_TOPIC is set, POSTs an ntfy notification. Exits 0 regardless (the
     alert is a signal, not a build failure).

Entrypoints match what the workflow calls:
    python scripts/accuracy_nightly.py --csv accuracy.csv --summary summary.md

Offline/degraded behavior: if the live ranking or the SRFHFO fetch fails, the
step logs and continues so the CSV/summary are still written (missing observed
values simply leave rows unscored until truth arrives).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import accuracy_tracking as at
from src.utils.timezones import now_hst

logger = logging.getLogger("accuracy_nightly")


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return at.parse_csv(path.read_text())


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.write_text(at.to_csv(rows))


def _get_today_rows(date_str: str) -> list[dict]:
    """Run the live ranker and build today's prediction rows. [] on failure."""
    try:
        from src.core.ranker import SiteRanker
        ranker = SiteRanker()
        ranked = ranker.rank_sites()
        return at.build_prediction_rows(date_str, ranked)
    except Exception as e:  # noqa: BLE001 - never fail the nightly job on this
        logger.warning("Live ranking failed (%s); no prediction rows this run", e)
        return []


def _get_yesterday_truth():
    """Fetch (shore_surf, shore_advisory) truth. Degrades to empty on failure."""
    try:
        from src.clients.srfhfo_client import fetch_shore_surf, fetch_shore_advisories
        return fetch_shore_surf(), fetch_shore_advisories()
    except Exception as e:  # noqa: BLE001
        logger.warning("SRFHFO/CFWHFO truth fetch failed (%s)", e)
        return {}, {}


def _send_ntfy_alert(disagreement_by_date: dict) -> bool:
    """POST an ntfy alert. Skips silently if NTFY_TOPIC unset. Returns sent?."""
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        logger.info("NTFY_TOPIC not set; skipping disagreement alert")
        return False
    try:
        import requests
        recent = sorted(disagreement_by_date)[-at.CONSECUTIVE_DAYS:]
        detail = ", ".join(f"{d}={disagreement_by_date[d]:.2f}" for d in recent)
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=(f"Dive model disagreed with NWS surf for "
                  f"{at.CONSECUTIVE_DAYS} days running. "
                  f"Mean band gap: {detail}").encode("utf-8"),
            headers={
                "Title": "Dive model accuracy alert",
                "Priority": "high",
                "Tags": "warning,ocean",
            },
            timeout=15,
        )
        logger.info("Sent ntfy disagreement alert to %s", topic)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("ntfy alert send failed: %s", e)
        return False


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="path to persistent accuracy CSV")
    ap.add_argument("--summary", default="summary.md", help="path to write summary md")
    ap.add_argument("--no-network", action="store_true",
                    help="skip live ranking + truth fetch (CSV/summary only)")
    args = ap.parse_args(argv)

    csv_path = Path(args.csv)
    today = now_hst().date()
    today_str = today.isoformat()
    yesterday_str = (now_hst().date().fromordinal(today.toordinal() - 1)).isoformat()

    rows = _read_csv(csv_path)
    logger.info("Loaded %d existing rows from %s", len(rows), csv_path)

    if not args.no_network:
        # 1. Backfill yesterday's observed truth.
        shore_surf, shore_adv = _get_yesterday_truth()
        if any(v is not None for v in shore_surf.values()):
            rows = at.backfill_observations(rows, yesterday_str, shore_surf, shore_adv)
            logger.info("Backfilled observed truth for %s: %s", yesterday_str, shore_surf)
        else:
            logger.warning("No observed surf parsed for %s; skipping backfill", yesterday_str)

        # 2. Append today's predictions (idempotent).
        today_rows = _get_today_rows(today_str)
        if today_rows:
            rows = at.upsert_rows(rows, today_rows)
            logger.info("Upserted %d prediction rows for %s", len(today_rows), today_str)

    # 3. Persist CSV.
    _write_csv(csv_path, rows)

    # 4. Summary.
    Path(args.summary).write_text(at.render_summary_markdown(rows, n=7))
    logger.info("Wrote trailing-7-day summary to %s", args.summary)

    # 5. Alert.
    dd = at.daily_disagreement(rows)
    if at.consecutive_disagreement_alert(dd):
        logger.warning("Disagreement threshold exceeded %d days running", at.CONSECUTIVE_DAYS)
        _send_ntfy_alert(dd)
    else:
        logger.info("No consecutive-disagreement alert.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
