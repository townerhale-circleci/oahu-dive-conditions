"""Format digest output for various delivery channels.

Supports SMS (short), email (full), and plain text formats.
"""

from datetime import datetime
from typing import Optional

from src.digests.daily_digest import DailyDigest, CoastSummary, AlertInfo, APIStatus, ForecastDay, BeachForecast
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
            # Show warning as informational if present
            if any(a.type == "high_surf_warning" for a in d.alerts):
                html_parts.append('<p class="warning-note">⚠️ High Surf Warning active for some areas - check local conditions</p>')
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

        # 7-Day Forecast
        if d.forecast_days:
            html_parts.append('<h2>7-Day Forecast</h2>')
            html_parts.append(self._format_forecast_html(d.forecast_days))

        # Methodology section
        html_parts.append('<h2>Methodology</h2>')
        html_parts.append(self._format_methodology_html())

        # API Status section
        if d.api_statuses:
            html_parts.append('<h2>Data Sources Status</h2>')
            html_parts.append(self._format_api_status_html(d.api_statuses))

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
            .warning-note { color: #d9480f; font-size: 0.95em; font-style: italic; }
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
            .methodology { background: #f8f9fa; padding: 15px; border-radius: 5px;
                          margin: 10px 0; font-size: 0.9em; }
            .methodology h3 { margin-top: 0; color: #495057; font-size: 1em; }
            .methodology code { background: #e9ecef; padding: 2px 6px; border-radius: 3px;
                               font-family: 'SFMono-Regular', monospace; }
            .formula { background: #e3f2fd; padding: 10px; border-radius: 5px;
                      margin: 10px 0; font-family: monospace; text-align: center; }
            .scoring-table { font-size: 0.85em; }
            .scoring-table th { background: #e9ecef; }
            .api-status { margin: 10px 0; }
            .api-status-item { display: flex; justify-content: space-between; align-items: center;
                              padding: 8px 12px; border-bottom: 1px solid #eee; }
            .api-success { color: #2b8a3e; }
            .api-partial { color: #e67700; }
            .api-fail { color: #c92a2a; }
            .status-bar { width: 100px; height: 8px; background: #eee; border-radius: 4px; overflow: hidden; }
            .status-bar-fill { height: 100%; transition: width 0.3s; }
            .status-bar-fill.success { background: #2b8a3e; }
            .status-bar-fill.partial { background: #e67700; }
            .status-bar-fill.fail { background: #c92a2a; }
            .forecast-grid { display: flex; flex-direction: column; gap: 20px; margin: 15px 0; }
            .forecast-day { background: #f8f9fa; border-radius: 8px; padding: 15px;
                           border-left: 4px solid #0066cc; width: 100%; }
            .forecast-day.outlook-good { border-left-color: #2b8a3e; }
            .forecast-day.outlook-fair { border-left-color: #5c940d; }
            .forecast-day.outlook-poor { border-left-color: #e67700; }
            .forecast-day.outlook-unsafe { border-left-color: #c92a2a; }
            .forecast-day h3 { margin: 0 0 10px 0; font-size: 1.1em; color: #333; }
            .forecast-day .date { color: #666; font-size: 0.85em; margin-bottom: 10px; }
            .forecast-outlook { font-weight: bold; font-size: 1.2em; margin: 10px 0; }
            .forecast-outlook.good { color: #2b8a3e; }
            .forecast-outlook.fair { color: #5c940d; }
            .forecast-outlook.poor { color: #e67700; }
            .forecast-outlook.unsafe { color: #c92a2a; }
            .forecast-detail { font-size: 0.9em; color: #555; margin: 5px 0; }
            .forecast-detail strong { color: #333; }
            .forecast-warning { background: #ff6b6b; color: white; padding: 5px 10px;
                               border-radius: 3px; font-size: 0.85em; font-weight: bold; margin: 5px 0; }
            .forecast-advisory { background: #ffa94d; color: white; padding: 5px 10px;
                                border-radius: 3px; font-size: 0.85em; font-weight: bold; margin: 5px 0; }
            .forecast-beaches { margin-top: 10px; padding-top: 10px; border-top: 1px solid #dee2e6; }
            .beach-card { background: white; border: 1px solid #dee2e6; border-radius: 6px;
                         padding: 10px; margin: 8px 0; }
            .beach-name { font-weight: bold; color: #0066cc; }
            .beach-location { font-size: 0.8em; color: #868e96; }
            .beach-conditions { font-size: 0.85em; margin: 5px 0; color: #495057; }
            .beach-reason { font-size: 0.85em; color: #2b8a3e; margin-top: 5px; }
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
        """Format top sites as HTML table with scoring details."""
        rows = ["<table>"]
        rows.append("<tr><th>Rank</th><th>Site</th><th>Grade</th><th>Score</th><th>Waves</th><th>WPI</th><th>Status</th></tr>")

        for i, site in enumerate(sites, 1):
            grade = site.grade
            grade_class = f"grade-{grade}"
            wave = f"{site.conditions.wave_height_ft:.1f}ft" if site.conditions.wave_height_ft else "N/A"
            wpi = f"{site.score.wave_power_index:.1f}" if site.score.wave_power_index else "N/A"
            total_score = f"{site.score.total_score:.0f}"
            status = "Diveable" if site.is_diveable else '<span class="unsafe">Unsafe</span>'

            # Add warning tooltip if present
            warning_title = ""
            if site.score.warnings:
                warning_title = f' title="{"; ".join(site.score.warnings)}"'

            rows.append(f"<tr{warning_title}>")
            rows.append(f"<td>{i}</td>")
            rows.append(f"<td>{site.site.name}</td>")
            rows.append(f'<td class="{grade_class}">{grade}</td>')
            rows.append(f"<td>{total_score}</td>")
            rows.append(f"<td>{wave}</td>")
            rows.append(f"<td>{wpi}</td>")
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

    def _format_methodology_html(self) -> str:
        """Format scoring methodology as HTML."""
        html = ['<div class="methodology">']

        # Wave Power Index explanation
        html.append('<h3>Wave Power Index (WPI)</h3>')
        html.append('<p>The primary metric for assessing dive conditions:</p>')
        html.append('<div class="formula">WPI = height² × period</div>')
        html.append('<p>Where <code>height</code> is wave height in feet and <code>period</code> is wave period in seconds.</p>')
        html.append('<p><strong>Interpretation:</strong> WPI &lt; 5 = Excellent | WPI 5-20 = Good | WPI 20-50 = Challenging | WPI &gt; 50 = Poor</p>')

        # Scoring weights
        html.append('<h3>Scoring Factors</h3>')
        html.append('<table class="scoring-table">')
        html.append('<tr><th>Factor</th><th>Weight</th><th>Description</th></tr>')
        html.append('<tr><td>Wave Power</td><td>35%</td><td>Lower WPI scores higher</td></tr>')
        html.append('<tr><td>Wind</td><td>25%</td><td>Calm/offshore winds preferred</td></tr>')
        html.append('<tr><td>Visibility</td><td>20%</td><td>Based on rainfall, discharge, advisories</td></tr>')
        html.append('<tr><td>Tide</td><td>10%</td><td>Site-specific tide preferences</td></tr>')
        html.append('<tr><td>Time of Day</td><td>10%</td><td>Early AM (5-9am) favored</td></tr>')
        html.append('</table>')

        # Safety gates
        html.append('<h3>Safety Gates</h3>')
        html.append('<p>These conditions automatically mark a site as <strong>Unsafe</strong>:</p>')
        html.append('<ul>')
        html.append('<li>Brown Water Advisory at site (water quality issue)</li>')
        html.append('<li>Wave height exceeds site threshold (typically &gt;6ft)</li>')
        html.append('</ul>')
        html.append('<p><em>Note: High Surf Warning is shown as informational but does not override local conditions. A site with small waves may still be diveable even during a warning.</em></p>')

        # Grade scale
        html.append('<h3>Grade Scale</h3>')
        html.append('<p>')
        html.append('<span class="grade-A">A (85+)</span> Excellent | ')
        html.append('<span class="grade-B">B (70-84)</span> Good | ')
        html.append('<span class="grade-C">C (55-69)</span> Fair | ')
        html.append('<span class="grade-D">D (40-54)</span> Poor | ')
        html.append('<span class="grade-F">F (&lt;40)</span> Unsafe')
        html.append('</p>')

        html.append('</div>')
        return "\n".join(html)

    def _format_forecast_html(self, forecast_days: list[ForecastDay]) -> str:
        """Format multi-day forecast as HTML."""
        html = ['<div class="forecast-grid">']

        for day in forecast_days:
            outlook_lower = day.outlook.lower()
            outlook_class = f"outlook-{outlook_lower}"

            html.append(f'<div class="forecast-day {outlook_class}">')
            html.append(f'<h3>{day.day_name}</h3>')
            html.append(f'<div class="date">{day.date.strftime("%A, %b %d")}</div>')

            # Warning indicator
            if day.has_high_surf_warning:
                html.append('<div class="forecast-warning">⚠️ HIGH SURF WARNING</div>')
            elif day.has_high_surf_advisory:
                html.append('<div class="forecast-advisory">⚠️ High Surf Advisory</div>')

            # Outlook
            html.append(f'<div class="forecast-outlook {outlook_lower}">{day.outlook}</div>')
            if day.outlook_reason:
                html.append(f'<div class="forecast-detail">{day.outlook_reason}</div>')

            # Waves
            if day.wave_height_min_ft is not None and day.wave_height_max_ft is not None:
                html.append(f'<div class="forecast-detail"><strong>Waves:</strong> {day.wave_height_min_ft:.1f}-{day.wave_height_max_ft:.1f} ft</div>')

            # Wind
            if day.wind_speed_min_mph is not None and day.wind_speed_max_mph is not None:
                wind_dir = f" {day.wind_direction}" if day.wind_direction else ""
                html.append(f'<div class="forecast-detail"><strong>Wind:</strong> {day.wind_speed_min_mph:.0f}-{day.wind_speed_max_mph:.0f} mph{wind_dir}</div>')

            # Weather
            if day.conditions:
                html.append(f'<div class="forecast-detail"><strong>Weather:</strong> {day.conditions}</div>')

            # Rain chance
            if day.rain_chance is not None and day.rain_chance > 0:
                html.append(f'<div class="forecast-detail"><strong>Rain:</strong> {day.rain_chance}% chance</div>')

            # Best time to dive
            if day.best_time:
                html.append(f'<div class="forecast-detail"><strong>Best time:</strong> {day.best_time}</div>')

            # Best coast
            if day.best_coast:
                html.append(f'<div class="forecast-detail"><strong>Best area:</strong> {day.best_coast}</div>')

            # Recommended beaches - ALL diveable sites in table format (similar to Top Sites)
            if day.recommended_beaches:
                html.append('<div class="forecast-beaches">')
                # Check if this is Today (has actual data) vs forecast days
                is_today = (day.day_name == "Today")
                if is_today:
                    html.append(f'<strong>Diveable Sites ({len(day.recommended_beaches)}) - Live Data:</strong>')
                else:
                    html.append(f'<strong>Diveable Sites ({len(day.recommended_beaches)}) - Forecast:</strong>')
                    html.append('<div style="font-size:0.8em; color:#666; margin:5px 0;">Note: Waves are coast-level forecast, wind is island-wide forecast</div>')
                html.append('<table style="font-size: 0.85em; margin-top: 8px;">')
                html.append('<tr><th>#</th><th>Site</th><th>Grade</th><th>Score</th><th>Waves</th><th>WPI</th><th>Wind</th><th>Rain</th><th>Best Time</th><th>Why</th></tr>')
                for i, beach in enumerate(day.recommended_beaches, 1):
                    grade_class = f"grade-{beach.outlook}" if len(beach.outlook) == 1 else ""
                    wave_str = f"{beach.wave_height_ft:.1f}ft" if beach.wave_height_ft is not None else "N/A"
                    score_str = f"{beach.score:.0f}" if beach.score is not None else "-"
                    wpi_str = f"{beach.wpi:.1f}" if beach.wpi is not None else "-"

                    # Wind info - clean format with direction
                    if beach.wind_speed_mph:
                        dir_str = f" {beach.wind_direction}" if beach.wind_direction else ""
                        if beach.wind_type == "offshore":
                            wind_str = f"{beach.wind_speed_mph:.0f}mph{dir_str} ✓"
                        elif beach.wind_type == "onshore":
                            wind_str = f"{beach.wind_speed_mph:.0f}mph{dir_str} ✗"
                        elif beach.wind_type == "cross-shore":
                            wind_str = f"{beach.wind_speed_mph:.0f}mph{dir_str}"
                        else:
                            wind_str = f"{beach.wind_speed_mph:.0f}mph{dir_str}"
                    else:
                        wind_str = "-"

                    # Best time column
                    time_str = beach.best_time or "05:00-09:00"

                    # Why column - show ranking reason
                    why_str = beach.why_recommended or "-"

                    html.append(f'<tr>')
                    html.append(f'<td>{i}</td>')
                    html.append(f'<td><strong>{beach.name}</strong><br><small style="color:#868e96">{beach.coast}</small></td>')
                    html.append(f'<td class="{grade_class}">{beach.outlook}</td>')
                    html.append(f'<td>{score_str}</td>')
                    html.append(f'<td>{wave_str}</td>')
                    html.append(f'<td>{wpi_str}</td>')
                    html.append(f'<td>{wind_str}</td>')

                    # Rain column with color coding
                    if beach.rain_chance is not None and beach.rain_chance > 0:
                        if beach.rain_chance <= 20:
                            rain_color = "#2b8a3e"  # green
                        elif beach.rain_chance <= 50:
                            rain_color = "#e67700"  # yellow/orange
                        elif beach.rain_chance <= 70:
                            rain_color = "#d9480f"  # orange
                        else:
                            rain_color = "#c92a2a"  # red
                        rain_str = f'<span style="color:{rain_color};font-weight:bold">{beach.rain_chance}%</span>'
                    else:
                        rain_str = "-"
                    html.append(f'<td>{rain_str}</td>')

                    html.append(f'<td>{time_str}</td>')
                    html.append(f'<td style="font-size:0.9em">{why_str}</td>')
                    html.append(f'</tr>')
                html.append('</table>')
                html.append('</div>')

            html.append('</div>')

        html.append('</div>')
        return "\n".join(html)

    def _format_api_status_html(self, api_statuses: list[APIStatus]) -> str:
        """Format API status indicators as HTML."""
        html = ['<div class="api-status">']

        for api in api_statuses:
            if api.total_calls == 0:
                status_class = "api-partial"
                status_text = "No data"
                fill_width = 0
                fill_class = "partial"
            elif api.success_rate >= 90:
                status_class = "api-success"
                status_text = f"{api.success_rate:.0f}%"
                fill_width = api.success_rate
                fill_class = "success"
            elif api.success_rate >= 50:
                status_class = "api-partial"
                status_text = f"{api.success_rate:.0f}%"
                fill_width = api.success_rate
                fill_class = "partial"
            else:
                status_class = "api-fail"
                status_text = f"{api.success_rate:.0f}%"
                fill_width = max(api.success_rate, 5)  # Show at least a sliver
                fill_class = "fail"

            html.append('<div class="api-status-item">')
            html.append(f'<span>{api.display_name}</span>')
            html.append('<div style="display: flex; align-items: center; gap: 10px;">')
            html.append(f'<div class="status-bar"><div class="status-bar-fill {fill_class}" style="width: {fill_width}%"></div></div>')
            html.append(f'<span class="{status_class}">{status_text}</span>')
            html.append('</div>')
            html.append('</div>')

        html.append('</div>')
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
