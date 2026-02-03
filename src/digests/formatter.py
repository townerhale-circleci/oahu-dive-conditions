"""Format digest output for various delivery channels.

Supports SMS (short), email (full), and plain text formats.
"""

from datetime import datetime
from typing import Optional

from src.digests.daily_digest import DailyDigest, CoastSummary, AlertInfo
from src.core.ranker import RankedSite


class DigestFormatter:
    """Formats daily digest for different output channels."""

    # SMS character limits
    SMS_MAX_LENGTH = 1600  # Standard SMS limit with concatenation
    SMS_SEGMENT_LENGTH = 160

    def __init__(self, digest: DailyDigest):
        """Initialize formatter with a digest.

        Args:
            digest: The daily digest to format.
        """
        self.digest = digest

    def format_sms(self, include_all_coasts: bool = False) -> str:
        """Format digest for SMS delivery.

        Optimized for brevity while conveying essential information.

        Args:
            include_all_coasts: Include all coast summaries (longer message).

        Returns:
            SMS-formatted string.
        """
        lines = []
        d = self.digest

        # Header with date
        date_str = d.generated_at.strftime("%m/%d")
        lines.append(f"DIVE CONDITIONS {date_str}")
        lines.append("")

        # Alerts first (most important)
        if d.alerts:
            alert_types = set(a.type for a in d.alerts)
            if "high_surf_warning" in alert_types:
                lines.append("HIGH SURF WARNING")
            elif "high_surf_advisory" in alert_types:
                lines.append("HIGH SURF ADVISORY")
            lines.append("")

        # Overall summary
        if d.diveable_sites == 0:
            lines.append("No diveable sites today")
            if d.is_big_day:
                lines.append(f"Waves {d.wave_range[0]:.0f}-{d.wave_range[1]:.0f}ft")
        else:
            lines.append(f"{d.diveable_sites}/{d.total_sites} sites diveable")
            if d.best_coast:
                lines.append(f"Best: {d.best_coast}")

        # Top sites (if diveable)
        if d.top_sites and d.diveable_sites > 0:
            lines.append("")
            lines.append("TOP SITES:")
            for i, site in enumerate(d.top_sites[:3], 1):
                if site.is_diveable:
                    grade = site.grade
                    name = self._shorten_name(site.site.name)
                    wave = f"{site.conditions.wave_height_ft:.0f}ft" if site.conditions.wave_height_ft else "?"
                    lines.append(f"{i}. {name} ({grade}) {wave}")

        # Coast breakdown (optional, for longer SMS)
        if include_all_coasts and d.coast_summaries:
            lines.append("")
            for coast in d.coast_summaries[:3]:
                if coast.diveable_count > 0:
                    lines.append(f"{coast.display_name}: {coast.diveable_count} OK")

        # Tide info
        if d.tide_info:
            lines.append("")
            if d.tide_info.next_high_time:
                time = self._format_time_short(d.tide_info.next_high_time)
                lines.append(f"High: {time}")
            if d.tide_info.next_low_time:
                time = self._format_time_short(d.tide_info.next_low_time)
                lines.append(f"Low: {time}")

        result = "\n".join(lines)

        # Truncate if too long
        if len(result) > self.SMS_MAX_LENGTH:
            result = result[:self.SMS_MAX_LENGTH - 3] + "..."

        return result

    def format_email_html(self) -> str:
        """Format digest as HTML for email delivery.

        Returns:
            HTML-formatted string.
        """
        d = self.digest
        date_str = d.generated_at.strftime("%A, %B %d, %Y")
        time_str = d.generated_at.strftime("%I:%M %p")

        html_parts = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<style>",
            self._get_email_css(),
            "</style>",
            "</head>",
            "<body>",
            f'<div class="container">',
            f'<h1>Oahu Dive Conditions</h1>',
            f'<p class="date">{date_str} at {time_str}</p>',
        ]

        # Alerts banner
        if d.alerts:
            html_parts.append(self._format_alerts_html(d.alerts))

        # Summary section
        html_parts.append('<div class="summary">')
        if d.diveable_sites == 0:
            html_parts.append('<p class="no-dive">No diveable sites today</p>')
            html_parts.append(f'<p>Wave heights: {d.wave_range[0]:.1f} - {d.wave_range[1]:.1f} ft</p>')
        else:
            html_parts.append(f'<p class="diveable-count">{d.diveable_sites} of {d.total_sites} sites diveable</p>')
            if d.best_coast:
                html_parts.append(f'<p>Best conditions: <strong>{d.best_coast}</strong></p>')
        html_parts.append('</div>')

        # Top sites table
        if d.top_sites:
            html_parts.append('<h2>Top Sites</h2>')
            html_parts.append(self._format_top_sites_html(d.top_sites))

        # Coast breakdown
        if d.coast_summaries:
            html_parts.append('<h2>By Coast</h2>')
            html_parts.append(self._format_coast_summaries_html(d.coast_summaries))

        # Tide info
        if d.tide_info:
            html_parts.append('<h2>Tides</h2>')
            html_parts.append(self._format_tide_html(d.tide_info))

        # Footer
        html_parts.extend([
            '<div class="footer">',
            '<p>Data sources: NDBC buoys, NWS, NOAA CO-OPS, USGS, Hawaii DOH</p>',
            '</div>',
            '</div>',
            '</body>',
            '</html>',
        ])

        return "\n".join(html_parts)

    def format_email_text(self) -> str:
        """Format digest as plain text for email delivery.

        Returns:
            Plain text formatted string.
        """
        d = self.digest
        date_str = d.generated_at.strftime("%A, %B %d, %Y at %I:%M %p")

        lines = [
            "=" * 50,
            "OAHU DIVE CONDITIONS",
            date_str,
            "=" * 50,
            "",
        ]

        # Alerts
        if d.alerts:
            lines.append("*** ACTIVE ALERTS ***")
            for alert in d.alerts:
                lines.append(f"  - {alert.headline}")
            lines.append("")

        # Summary
        lines.append("SUMMARY")
        lines.append("-" * 30)
        if d.diveable_sites == 0:
            lines.append("No diveable sites today")
            lines.append(f"Wave heights: {d.wave_range[0]:.1f} - {d.wave_range[1]:.1f} ft")
        else:
            lines.append(f"Diveable sites: {d.diveable_sites} of {d.total_sites}")
            if d.best_coast:
                lines.append(f"Best conditions: {d.best_coast}")
        lines.append(f"Wind: {d.wind_range[0]:.0f} - {d.wind_range[1]:.0f} mph")
        lines.append("")

        # Top sites
        if d.top_sites:
            lines.append("TOP SITES")
            lines.append("-" * 30)
            for i, site in enumerate(d.top_sites, 1):
                status = "DIVEABLE" if site.is_diveable else "UNSAFE"
                wave = f"{site.conditions.wave_height_ft:.1f}ft" if site.conditions.wave_height_ft else "N/A"
                lines.append(f"{i}. {site.site.name}")
                lines.append(f"   Grade: {site.grade} | {status} | Waves: {wave}")
                if site.score.warnings:
                    lines.append(f"   Warning: {site.score.warnings[0]}")
            lines.append("")

        # Coast breakdown
        if d.coast_summaries:
            lines.append("BY COAST")
            lines.append("-" * 30)
            for coast in d.coast_summaries:
                wave_str = f"{coast.average_wave_height:.1f}ft avg" if coast.average_wave_height else "N/A"
                lines.append(f"{coast.display_name}: {coast.diveable_count}/{coast.total_count} diveable ({wave_str})")
            lines.append("")

        # Tides
        if d.tide_info:
            lines.append("TIDES")
            lines.append("-" * 30)
            if d.tide_info.next_high_time:
                lines.append(f"Next High: {d.tide_info.next_high_time}")
            if d.tide_info.next_low_time:
                lines.append(f"Next Low: {d.tide_info.next_low_time}")
            lines.append("")

        # Footer
        lines.extend([
            "=" * 50,
            "Data: NDBC, NWS, NOAA CO-OPS, USGS, Hawaii DOH",
            "=" * 50,
        ])

        return "\n".join(lines)

    def _shorten_name(self, name: str, max_len: int = 20) -> str:
        """Shorten site name for SMS."""
        # Remove common suffixes
        name = name.replace(" Beach", "").replace(" Bay", "").replace(" Point", " Pt")
        # Remove parenthetical
        if "(" in name:
            name = name.split("(")[0].strip()
        if len(name) > max_len:
            name = name[:max_len-2] + ".."
        return name

    def _format_time_short(self, time_str: str) -> str:
        """Format time string to short format."""
        try:
            # Parse ISO format
            if "T" in time_str:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            else:
                # Try common formats
                for fmt in ["%Y-%m-%d %H:%M", "%H:%M"]:
                    try:
                        dt = datetime.strptime(time_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return time_str[:5]  # Return first 5 chars as fallback
            return dt.strftime("%I:%M%p").lstrip("0").lower()
        except Exception:
            return time_str[:8]

    def _get_email_css(self) -> str:
        """Get CSS styles for HTML email."""
        return """
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   margin: 0; padding: 0; background: #f5f5f5; }
            .container { max-width: 600px; margin: 0 auto; padding: 20px; background: white; }
            h1 { color: #0066cc; margin-bottom: 5px; }
            h2 { color: #333; border-bottom: 2px solid #0066cc; padding-bottom: 5px; margin-top: 25px; }
            .date { color: #666; margin-top: 0; }
            .alert-banner { background: #ff6b6b; color: white; padding: 15px;
                           border-radius: 5px; margin: 15px 0; font-weight: bold; }
            .alert-banner.advisory { background: #ffa94d; }
            .summary { background: #e8f4f8; padding: 15px; border-radius: 5px; margin: 15px 0; }
            .no-dive { color: #c92a2a; font-weight: bold; font-size: 1.2em; }
            .diveable-count { color: #2b8a3e; font-weight: bold; font-size: 1.2em; }
            table { width: 100%; border-collapse: collapse; margin: 10px 0; }
            th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background: #f8f9fa; }
            .grade-A { color: #2b8a3e; font-weight: bold; }
            .grade-B { color: #5c940d; font-weight: bold; }
            .grade-C { color: #e67700; font-weight: bold; }
            .grade-D { color: #d9480f; font-weight: bold; }
            .grade-F { color: #c92a2a; font-weight: bold; }
            .unsafe { color: #c92a2a; }
            .footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd;
                     color: #666; font-size: 0.9em; }
        """

    def _format_alerts_html(self, alerts: list[AlertInfo]) -> str:
        """Format alerts as HTML banner."""
        html = []
        for alert in alerts:
            css_class = "alert-banner"
            if alert.type == "high_surf_advisory":
                css_class += " advisory"
            html.append(f'<div class="{css_class}">{alert.headline}</div>')
        return "\n".join(html)

    def _format_top_sites_html(self, sites: list[RankedSite]) -> str:
        """Format top sites as HTML table."""
        rows = ["<table>"]
        rows.append("<tr><th>Rank</th><th>Site</th><th>Grade</th><th>Waves</th><th>Status</th></tr>")

        for i, site in enumerate(sites, 1):
            grade = site.grade
            grade_class = f"grade-{grade}"
            wave = f"{site.conditions.wave_height_ft:.1f}ft" if site.conditions.wave_height_ft else "N/A"
            status = "Diveable" if site.is_diveable else '<span class="unsafe">Unsafe</span>'

            rows.append(f"<tr>")
            rows.append(f"<td>{i}</td>")
            rows.append(f"<td>{site.site.name}</td>")
            rows.append(f'<td class="{grade_class}">{grade}</td>')
            rows.append(f"<td>{wave}</td>")
            rows.append(f"<td>{status}</td>")
            rows.append(f"</tr>")

        rows.append("</table>")
        return "\n".join(rows)

    def _format_coast_summaries_html(self, summaries: list[CoastSummary]) -> str:
        """Format coast summaries as HTML."""
        rows = ["<table>"]
        rows.append("<tr><th>Coast</th><th>Diveable</th><th>Avg Waves</th></tr>")

        for coast in summaries:
            wave = f"{coast.average_wave_height:.1f}ft" if coast.average_wave_height else "N/A"
            diveable = f"{coast.diveable_count}/{coast.total_count}"

            rows.append(f"<tr>")
            rows.append(f"<td>{coast.display_name}</td>")
            rows.append(f"<td>{diveable}</td>")
            rows.append(f"<td>{wave}</td>")
            rows.append(f"</tr>")

        rows.append("</table>")
        return "\n".join(rows)

    def _format_tide_html(self, tide_info) -> str:
        """Format tide info as HTML."""
        html = ['<div class="tide-info">']
        if tide_info.next_high_time:
            html.append(f"<p><strong>Next High Tide:</strong> {tide_info.next_high_time}</p>")
        if tide_info.next_low_time:
            html.append(f"<p><strong>Next Low Tide:</strong> {tide_info.next_low_time}</p>")
        html.append("</div>")
        return "\n".join(html)


def format_sms(digest: DailyDigest, **kwargs) -> str:
    """Convenience function to format digest for SMS."""
    return DigestFormatter(digest).format_sms(**kwargs)


def format_email_html(digest: DailyDigest) -> str:
    """Convenience function to format digest as HTML email."""
    return DigestFormatter(digest).format_email_html()


def format_email_text(digest: DailyDigest) -> str:
    """Convenience function to format digest as plain text email."""
    return DigestFormatter(digest).format_email_text()
