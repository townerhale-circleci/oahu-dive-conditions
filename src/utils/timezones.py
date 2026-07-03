"""Timezone helpers for Hawaii-Standard-Time-correct scoring.

All dive-condition scoring is anchored to the Hawaii dive window (default 07:00
HST), not to wall-clock/process time (which is UTC in CI). This module is the one
place that owns the HST timezone and the dive-window helpers.
"""

from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

HST = ZoneInfo("Pacific/Honolulu")

# Default dive-window hour (local Hawaii time)
DIVE_WINDOW_HOUR = 7


def now_hst() -> datetime:
    """Return the current time as a timezone-aware HST datetime."""
    return datetime.now(HST)


def dive_window_time(date=None, hour: int = DIVE_WINDOW_HOUR) -> datetime:
    """Return a tz-aware HST datetime pinned to the dive window for a given date.

    Args:
        date: A ``datetime.date`` (or ``datetime``) for the target day. Defaults
            to today in HST.
        hour: The dive-window hour in local HST. Defaults to 07:00.

    Returns:
        A timezone-aware datetime at ``hour``:00 HST on the target date.
    """
    if date is None:
        date = now_hst().date()
    elif isinstance(date, datetime):
        date = date.date()

    return datetime.combine(date, time(hour=hour), tzinfo=HST)
