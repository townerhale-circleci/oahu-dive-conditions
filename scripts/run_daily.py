#!/usr/bin/env python3
"""Daily dive conditions runner.

Generates a daily digest and optionally sends via SMS/email.

Usage:
    # Print digest to console (default)
    python scripts/run_daily.py

    # Output as SMS format
    python scripts/run_daily.py --format sms

    # Output as HTML email
    python scripts/run_daily.py --format html

    # Send to configured recipients
    python scripts/run_daily.py --send

    # Send to specific recipients
    python scripts/run_daily.py --send --sms +18081234567 --email user@example.com

    # Save to file
    python scripts/run_daily.py --format html --output report.html
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.digests.daily_digest import DigestGenerator
from src.digests.formatter import DigestFormatter


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Quiet down noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate and send daily dive conditions digest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--format",
        choices=["text", "sms", "html"],
        default="text",
        help="Output format (default: text)",
    )

    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Write output to file instead of stdout",
    )

    parser.add_argument(
        "--send",
        action="store_true",
        help="Send to configured recipients",
    )

    parser.add_argument(
        "--sms",
        type=str,
        nargs="+",
        help="Additional SMS recipients (phone numbers)",
    )

    parser.add_argument(
        "--email",
        type=str,
        nargs="+",
        help="Additional email recipients",
    )

    parser.add_argument(
        "--all-sites",
        action="store_true",
        help="Include all sites, not just in-season",
    )

    parser.add_argument(
        "--no-coast-breakdown",
        action="store_true",
        help="Skip per-coast breakdown",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate digest but don't send (with --send)",
    )

    return parser.parse_args()


def generate_digest(args) -> "DailyDigest":
    """Generate the daily digest."""
    print("Generating dive conditions digest...", file=sys.stderr)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr)
    print(file=sys.stderr)

    generator = DigestGenerator()
    digest = generator.generate(
        in_season_only=not args.all_sites,
        include_coast_breakdown=not args.no_coast_breakdown,
    )

    return digest


def format_output(digest, format_type: str) -> str:
    """Format digest for output."""
    formatter = DigestFormatter(digest)

    if format_type == "sms":
        return formatter.format_sms()
    elif format_type == "html":
        return formatter.format_email_html()
    else:
        return formatter.format_email_text()


def send_notifications(digest, args):
    """Send notifications via SMS and/or email."""
    formatter = DigestFormatter(digest)
    results = {"sms": None, "email": None}

    # Collect recipients
    sms_recipients = args.sms or []
    email_recipients = args.email or []

    # Send SMS
    if sms_recipients:
        print(f"\nSending SMS to {len(sms_recipients)} recipient(s)...")

        if args.dry_run:
            print("  [DRY RUN] Would send to:", ", ".join(sms_recipients))
            sms_text = formatter.format_sms()
            print(f"  Message length: {len(sms_text)} chars")
        else:
            try:
                from src.delivery.twilio_sender import TwilioSender
                sender = TwilioSender()

                if not sender.is_configured:
                    print("  ERROR: Twilio not configured (missing env vars)")
                else:
                    sms_text = formatter.format_sms()
                    result = sender.send_digest(sms_recipients, sms_text)
                    results["sms"] = result
                    print(f"  Sent: {result['sent']}/{result['total']}")
                    if result["failed"] > 0:
                        for r in result["results"]:
                            if not r.success:
                                print(f"    Failed: {r.to_number} - {r.error}")
            except Exception as e:
                print(f"  ERROR: {e}")

    # Send email
    if email_recipients:
        print(f"\nSending email to {len(email_recipients)} recipient(s)...")

        if args.dry_run:
            print("  [DRY RUN] Would send to:", ", ".join(email_recipients))
        else:
            try:
                from src.delivery.sendgrid_sender import SendGridSender
                sender = SendGridSender()

                if not sender.is_configured:
                    print("  ERROR: SendGrid not configured (missing SENDGRID_API_KEY)")
                else:
                    html_content = formatter.format_email_html()
                    text_content = formatter.format_email_text()

                    date_str = digest.generated_at.strftime("%B %d, %Y")
                    subject = f"Oahu Dive Conditions - {date_str}"

                    result = sender.send_digest(
                        email_recipients, subject, html_content, text_content
                    )
                    results["email"] = result
                    print(f"  Sent: {result['sent']}/{result['total']}")
                    if result["failed"] > 0:
                        for r in result["results"]:
                            if not r.success:
                                print(f"    Failed: {r.to_email} - {r.error}")
            except Exception as e:
                print(f"  ERROR: {e}")

    return results


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    # Generate digest
    digest = generate_digest(args)

    # Check for errors
    if digest.errors:
        print("Warnings during generation:", file=sys.stderr)
        for err in digest.errors:
            print(f"  - {err}", file=sys.stderr)
        print(file=sys.stderr)

    # Format output
    output = format_output(digest, args.format)

    # Write or print output
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(output)
        print(f"Output written to: {output_path}", file=sys.stderr)
    else:
        print(output)

    # Send notifications if requested
    if args.send or args.sms or args.email:
        send_notifications(digest, args)

    # Summary (always to stderr so it doesn't pollute piped output)
    print(file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print(f"  Total sites: {digest.total_sites}", file=sys.stderr)
    print(f"  Diveable: {digest.diveable_sites}", file=sys.stderr)
    print(f"  Wave range: {digest.wave_range[0]:.1f} - {digest.wave_range[1]:.1f} ft", file=sys.stderr)
    if digest.alerts:
        print(f"  Active alerts: {len(digest.alerts)}", file=sys.stderr)
    if digest.best_coast:
        print(f"  Best coast: {digest.best_coast}", file=sys.stderr)
    print("=" * 50, file=sys.stderr)

    return 0 if digest.diveable_sites > 0 or not digest.errors else 1


if __name__ == "__main__":
    sys.exit(main())
