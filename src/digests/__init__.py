"""Digest generation and formatting."""

from src.digests.daily_digest import (
    AlertInfo,
    CoastSummary,
    DailyDigest,
    DigestGenerator,
    TideInfo,
    generate_daily_digest,
)
from src.digests.formatter import (
    DigestFormatter,
    format_email_html,
    format_email_text,
    format_sms,
)

__all__ = [
    # Digest
    "AlertInfo",
    "CoastSummary",
    "DailyDigest",
    "DigestGenerator",
    "TideInfo",
    "generate_daily_digest",
    # Formatter
    "DigestFormatter",
    "format_email_html",
    "format_email_text",
    "format_sms",
]
