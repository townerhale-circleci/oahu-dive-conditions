# Oahu Dive Conditions

Automated daily dive condition reports for Oahu, Hawaii. Fetches live data from NOAA buoys, NWS weather, tide stations, and water quality sources to rank 48 dive sites by safety and conditions.

## Features

- **48 dive sites** across 5 coasts (North Shore, West Side, South Shore, Southeast, Windward)
- **Wave Power Index scoring** - `WPI = height² × period` for accurate energy assessment
- **Safety gates** - Automatic unsafe rating for High Surf Warnings, Brown Water Advisories, or waves exceeding site thresholds
- **Multi-format output** - SMS (optimized for brevity), HTML email, plain text
- **Delivery options** - Twilio SMS, SendGrid email

## Data Sources

| Source | Data |
|--------|------|
| NDBC Buoys | Wave height, period, direction |
| NWS | Weather forecasts, marine alerts |
| NOAA CO-OPS | Tide predictions |
| USGS | Stream discharge (visibility proxy) |
| Hawaii DOH | Brown water advisories |
| PacIOOS SWAN | Nearshore wave model (backup) |

## Quick Start

### 1. Install Dependencies

```bash
# Using pip
pip install -r requirements.txt

# Or install individually
pip install requests pyyaml pandas beautifulsoup4 erddapy
```

### 2. Run a Report

```bash
# Console output (plain text)
python scripts/run_daily.py

# SMS format (short)
python scripts/run_daily.py --format sms

# HTML email format
python scripts/run_daily.py --format html --output report.html
```

### 3. Send Notifications (Optional)

Set environment variables for delivery:

```bash
# Twilio (SMS)
export TWILIO_ACCOUNT_SID="your_account_sid"
export TWILIO_AUTH_TOKEN="your_auth_token"
export TWILIO_FROM_NUMBER="+1234567890"

# SendGrid (Email)
export SENDGRID_API_KEY="your_api_key"
export SENDGRID_FROM_EMAIL="dive-conditions@yourdomain.com"
```

Then send:

```bash
# Send SMS
python scripts/run_daily.py --sms +18081234567

# Send email
python scripts/run_daily.py --email you@example.com

# Both
python scripts/run_daily.py --sms +18081234567 --email you@example.com

# Dry run (preview without sending)
python scripts/run_daily.py --sms +18081234567 --dry-run
```

## Automated Daily Runs (GitHub Actions)

This repo includes a GitHub Actions workflow that runs daily at 5:30 AM Hawaii time.

### Setup

1. Push this repo to GitHub

2. Add secrets in GitHub (Settings → Secrets and variables → Actions):
   - `TWILIO_ACCOUNT_SID`
   - `TWILIO_AUTH_TOKEN`
   - `TWILIO_FROM_NUMBER`
   - `SMS_RECIPIENTS` (comma-separated phone numbers, e.g., `+18081234567,+18089876543`)

   Optional for email:
   - `SENDGRID_API_KEY`
   - `SENDGRID_FROM_EMAIL`
   - `EMAIL_RECIPIENTS` (comma-separated emails)

3. Enable Actions in your repo (Actions tab → Enable)

The workflow will run automatically every day. You can also trigger it manually from the Actions tab.

## Project Structure

```
oahu-dive-conditions/
├── config/
│   ├── sites.yaml       # 48 dive site definitions
│   └── config.yaml      # Scoring thresholds, settings
├── src/
│   ├── clients/         # API clients (buoy, NWS, tides, USGS, etc.)
│   ├── core/
│   │   ├── scorer.py    # Wave Power Index scoring algorithm
│   │   ├── site.py      # Site model and database
│   │   └── ranker.py    # Orchestrates data fetch + scoring
│   ├── digests/
│   │   ├── daily_digest.py  # Generates structured report
│   │   └── formatter.py     # SMS/HTML/text formatting
│   └── delivery/
│       ├── twilio_sender.py   # SMS via Twilio
│       └── sendgrid_sender.py # Email via SendGrid
├── scripts/
│   ├── run_daily.py         # Main CLI runner
│   ├── test_scoring.py      # Scorer unit tests
│   └── test_integration.py  # Full pipeline tests
└── .github/
    └── workflows/
        └── daily_digest.yml # GitHub Actions workflow
```

## Scoring Algorithm

### Wave Power Index (WPI)
```
WPI = wave_height² × wave_period
```

Lower WPI = calmer conditions = better diving.

### Score Weights
| Factor | Weight | Description |
|--------|--------|-------------|
| Wave Power | 35% | WPI relative to site threshold |
| Wind | 25% | Speed and direction (offshore preferred) |
| Visibility | 20% | Based on stream discharge + advisories |
| Tide | 10% | Match with site's optimal tide |
| Time of Day | 10% | Morning preferred (calmer winds) |

### Grades
| Grade | Score | Meaning |
|-------|-------|---------|
| A | ≥85 | Excellent conditions |
| B | ≥70 | Good conditions |
| C | ≥55 | Fair conditions |
| D | ≥40 | Poor but diveable |
| F | <40 or unsafe | Not recommended |

### Safety Gates (Instant Fail)
- High Surf Warning active
- Brown Water Advisory for site
- Wave height exceeds site's safe threshold

## Running Tests

```bash
# Scorer unit tests (fast, no network)
python scripts/test_scoring.py

# Integration tests (requires internet, ~30 seconds)
python scripts/test_integration.py
```

## CLI Options

```
python scripts/run_daily.py [OPTIONS]

Output:
  --format {text,sms,html}  Output format (default: text)
  -o, --output FILE         Write to file instead of stdout

Delivery:
  --sms NUMBER [NUMBER...]  Send SMS to phone number(s)
  --email ADDR [ADDR...]    Send email to address(es)
  --send                    Send to configured recipients
  --dry-run                 Preview without sending

Filtering:
  --all-sites               Include all sites (not just in-season)
  --no-coast-breakdown      Skip per-coast summary

Debug:
  -v, --verbose             Enable debug logging
```

## Example Output

### SMS Format
```
DIVE CONDITIONS 02/03

HIGH SURF WARNING

No diveable sites today
Waves 2-10ft
```

### Text Format (calm day)
```
==================================================
OAHU DIVE CONDITIONS
Monday, June 15, 2026 at 06:30 AM
==================================================

SUMMARY
------------------------------
Diveable sites: 18 of 28
Best conditions: South Shore

TOP SITES
------------------------------
1. Hanauma Bay
   Grade: A | DIVEABLE | Waves: 1.2ft
2. Electric Beach (Kahe Point)
   Grade: A | DIVEABLE | Waves: 1.5ft
...
```

## License

MIT
