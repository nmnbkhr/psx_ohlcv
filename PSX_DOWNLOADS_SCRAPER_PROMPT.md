# Claude Code Prompt: PSX DPS Downloads Scraper — Daily + Other Files

## Context

PSX Data Portal at `https://dps.psx.com.pk/downloads` has two tabs:
1. **Daily Downloads** — date-filtered files (backfillable)
2. **Other Downloads** — static reference files

This scraper downloads everything into a separate folder structure.
**DO NOT integrate into existing pakfindata code.** Store separately.
We will manually review and integrate later.

## Step 1: Check what already exists

```bash
# Check if any download scraper exists
find ~/pakfindata/src/ -name "*.py" | xargs grep -l "dps.psx.com.pk/download" 2>/dev/null
ls -la /mnt/e/psxdata/downloads/ 2>/dev/null

# Check the page structure
curl -s "https://dps.psx.com.pk/downloads" | head -100
```

Read the output before proceeding.

## Step 2: Discover all download URLs

The downloads page is server-rendered HTML. File links follow patterns:

### Daily Downloads (date-filtered)
These URLs contain a date parameter. Pattern: `https://dps.psx.com.pk/download/{category}/{filename}`

Known daily file patterns (for date `2026-03-18`):

```
MARKET SUMMARY:
  Market Summary (Closing)    — ZIP  → /download/mkt_summary/{date}.zip  OR similar
  Closing Rate Summary        — PDF  → /download/closing_rate/{date}.pdf
  Symbol Price (Upper/Lower)  — ZIP  → /download/symbol_price/{date}.zip
  Symbols Short Long Name     — ZIP  → /download/symbols_name/{date}.zip
  GIS Revaluation Rates       — CSV  → /download/gis_rates/{date}.csv
  Bai Muajjal Volume          — CSV  → /download/bai_muajjal/{date}.csv

FUTURES MARKET:
  Symbol Wise Open Interest (DFC) — PDF + XLS
  Symbol Wise Open Interest (CSF) — PDF + XLS

DFC MARKET:
  Net Blank Sale Position in DFC Market

OFF MARKET TRANSACTIONS / NDM SUMMARY:
  ND Accepted                    — PDF
  ND Rejected                    — PDF
  ND Threshold                   — PDF
  Off Market Transaction Summary — CSV
  Off Market Transaction (PDF)   — PDF

SIF MARKET:
  Open Interest Report           — CSV
  Fair Value Report              — PDF

VAR MARGINS:
  VAR Margins                    — ZIP

DAILY ANNOUNCEMENTS:
  Daily Announcements            — PDF

READY MKT SHORT SELL VOL:
  Ready Market Short Sell Vol    — PDF

DAILY QUOTATIONS:
  Daily Quotations               — PDF

POSITION LIMITS:
  Position Limits                — XLS

POST CLOSE:
  Post Close Report              — ZIP

OTHER:
  Internet Trading Subscribers List — PDF
  Constituent Data (PSX Indices)    — XLS
```

### Other Downloads (static, no date)

```
GENERAL:
  Stock Market Report             — PDF
  Index Fluctuation               — ZIP
  All Share Index & Mkt. Cap.     — ZIP
  KSE 100 Index Companies         — ZIP
  Symbol Lot Size                 — ZIP
  PSX Header                      — ZIP
  PSX Header (Tradable Indices)   — ZIP
  Ready Mkt Short Sell Volume Limit — PDF
  Companies Info                  — ZIP
  Companies Announcements         — ZIP
  Companies Announcements (With Symbols) — ZIP
  Analysis                        — URL
  Market Data Download            — ZIP

TOP SYMBOLS:
  Top Symbols By %age Decreased   — ZIP
  Top Symbols By %age Increased   — ZIP
  Top Symbols By Volume           — ZIP
  Zero Volume Securities          — ZIP

TECHNICAL DOCUMENTS:
  Market Data Download            — ZIP

INDEX DATA:
  HBLTT Index values              — CSV
```

## Step 3: Create the scraper

Create `~/pakfindata/scripts/psx_downloads_scraper.py`:

```python
"""
PSX DPS Downloads Scraper — downloads all files from dps.psx.com.pk/downloads

Supports:
  - Daily Downloads (date-filtered, backfillable)
  - Other Downloads (static reference files)
  - Date range backfill
  - Skips already downloaded files

Output: /mnt/e/psxdata/downloads/
  ├── daily/
  │   ├── 2026-03-18/
  │   │   ├── market_summary/
  │   │   │   └── market_summary_closing.zip
  │   │   ├── futures/
  │   │   │   ├── open_interest_dfc.xls
  │   │   │   └── open_interest_csf.xls
  │   │   ├── off_market/
  │   │   │   └── off_market_summary.csv
  │   │   ├── sif/
  │   │   │   └── open_interest_report.csv
  │   │   ├── margins/
  │   │   │   └── var_margins.zip
  │   │   ├── post_close/
  │   │   │   └── post_close_report.zip
  │   │   ├── indices/
  │   │   │   └── constituent_data.xls
  │   │   └── other/
  │   │       ├── symbol_price_limits.zip
  │   │       ├── gis_rates.csv
  │   │       ├── bai_muajjal.csv
  │   │       ├── position_limits.xls
  │   │       └── symbols_short_long.zip
  │   ├── 2026-03-17/
  │   │   └── ...
  │   └── ...
  └── reference/
      ├── companies_info.zip
      ├── symbol_lot_size.zip
      ├── kse100_companies.zip
      ├── psx_header.zip
      ├── index_fluctuation.zip
      ├── allshare_mktcap.zip
      ├── market_data_download.zip
      ├── hbltt_index.csv
      └── ...

Usage:
  python psx_downloads_scraper.py today                 # Today's daily files
  python psx_downloads_scraper.py daily 2026-03-18      # Specific date
  python psx_downloads_scraper.py backfill 2026-01-01 2026-03-18  # Date range
  python psx_downloads_scraper.py reference             # Static files (Other tab)
  python psx_downloads_scraper.py all                   # Everything
  python psx_downloads_scraper.py status                # Show what's downloaded
"""

import requests
import re
import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

BASE_URL = "https://dps.psx.com.pk"
DOWNLOADS_URL = f"{BASE_URL}/downloads"
OUTPUT_DIR = Path("/mnt/e/psxdata/downloads")

PKT = timezone(timedelta(hours=5))
RATE_LIMIT = 1.0  # seconds between downloads (be respectful)

session = requests.Session()
session.headers.update({
    "User-Agent": "pakfindata/1.0 (research)",
    "Accept": "*/*",
    "Referer": DOWNLOADS_URL,
})


# ═══════════════════════════════════════════════════════
# STEP 1: DISCOVER FILE URLS FROM HTML
# ═══════════════════════════════════════════════════════

def discover_daily_urls(date_str: str) -> list[dict]:
    """
    Fetch the downloads page for a specific date and extract all file URLs.
    
    The page has a date search form. We need to:
    1. POST or GET the page with the date parameter
    2. Parse the HTML for all download links
    3. Categorize each file
    """
    # Try GET with date parameter
    urls_found = []
    
    for url_pattern in [
        f"{DOWNLOADS_URL}?date={date_str}",
        f"{DOWNLOADS_URL}?search_date={date_str}",
        DOWNLOADS_URL,  # fallback — page might use JS
    ]:
        try:
            r = session.get(url_pattern, timeout=30)
            if r.status_code != 200:
                continue
            
            # Also try POST
            if not urls_found:
                r = session.post(DOWNLOADS_URL, data={"date": date_str}, timeout=30)
            
            # Extract all download links from HTML
            # Links typically look like: href="/download/..." or href="https://dps.psx.com.pk/download/..."
            links = re.findall(
                r'href=["\']([^"\']*(?:download|\.zip|\.csv|\.xls|\.xlsx|\.pdf)[^"\']*)["\']',
                r.text, re.IGNORECASE
            )
            
            for link in links:
                # Make absolute URL
                if link.startswith("/"):
                    full_url = f"{BASE_URL}{link}"
                elif link.startswith("http"):
                    full_url = link
                else:
                    full_url = f"{BASE_URL}/{link}"
                
                # Determine category and filename
                category, filename = categorize_url(full_url, date_str)
                
                urls_found.append({
                    "url": full_url,
                    "category": category,
                    "filename": filename,
                    "date": date_str,
                })
            
            if urls_found:
                break
                
        except Exception as e:
            print(f"  ⚠️ Error fetching {url_pattern}: {e}")
            continue
    
    # Deduplicate by URL
    seen = set()
    unique = []
    for item in urls_found:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)
    
    return unique


def discover_reference_urls() -> list[dict]:
    """Discover all static (Other Downloads) file URLs."""
    urls_found = []
    
    try:
        r = session.get(DOWNLOADS_URL, timeout=30)
        if r.status_code != 200:
            return []
        
        # The "Other Downloads" tab content
        # Extract all download links
        links = re.findall(
            r'href=["\']([^"\']*(?:download|\.zip|\.csv|\.xls|\.xlsx|\.pdf)[^"\']*)["\']',
            r.text, re.IGNORECASE
        )
        
        for link in links:
            if link.startswith("/"):
                full_url = f"{BASE_URL}{link}"
            elif link.startswith("http"):
                full_url = link
            else:
                full_url = f"{BASE_URL}/{link}"
            
            category, filename = categorize_url(full_url, "reference")
            urls_found.append({
                "url": full_url,
                "category": "reference",
                "filename": filename,
            })
    
    except Exception as e:
        print(f"  ⚠️ Error: {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for item in urls_found:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique.append(item)
    
    return unique


def categorize_url(url: str, date_str: str) -> tuple[str, str]:
    """Categorize a download URL into folder/filename."""
    url_lower = url.lower()
    
    # Extract filename from URL
    filename = url.split("/")[-1].split("?")[0]
    if not filename or filename == "downloads":
        filename = "unknown"
    
    # Categorize
    if any(k in url_lower for k in ["market_summary", "mkt_summary", "closing"]):
        return "market_summary", filename
    elif any(k in url_lower for k in ["open_interest", "dfc", "csf", "futures"]):
        return "futures", filename
    elif any(k in url_lower for k in ["off_market", "nd_accepted", "nd_rejected", "nd_threshold", "ndm"]):
        return "off_market", filename
    elif any(k in url_lower for k in ["sif", "fair_value"]):
        return "sif", filename
    elif any(k in url_lower for k in ["var_margin", "margin"]):
        return "margins", filename
    elif any(k in url_lower for k in ["post_close"]):
        return "post_close", filename
    elif any(k in url_lower for k in ["constituent", "index", "indices"]):
        return "indices", filename
    elif any(k in url_lower for k in ["symbol_price", "circuit", "upper", "lower"]):
        return "limits", filename
    elif any(k in url_lower for k in ["gis", "revaluation"]):
        return "gis", filename
    elif any(k in url_lower for k in ["bai_muajjal"]):
        return "bai_muajjal", filename
    elif any(k in url_lower for k in ["position_limit"]):
        return "position_limits", filename
    elif any(k in url_lower for k in ["lot_size"]):
        return "reference", filename
    elif any(k in url_lower for k in ["companies_info", "company"]):
        return "reference", filename
    elif any(k in url_lower for k in ["psx_header", "header"]):
        return "reference", filename
    elif any(k in url_lower for k in ["announcement"]):
        return "announcements", filename
    elif any(k in url_lower for k in ["quotation"]):
        return "quotations", filename
    elif any(k in url_lower for k in ["short_sell"]):
        return "short_sell", filename
    elif any(k in url_lower for k in ["hbltt"]):
        return "indices", filename
    elif any(k in url_lower for k in ["market_data"]):
        return "reference", filename
    else:
        return "other", filename


# ═══════════════════════════════════════════════════════
# STEP 2: DOWNLOAD FILES
# ═══════════════════════════════════════════════════════

def download_file(url: str, dest_path: Path) -> bool:
    """Download a file if it doesn't already exist."""
    if dest_path.exists() and dest_path.stat().st_size > 0:
        return False  # Already exists, skip
    
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        r = session.get(url, timeout=60, stream=True)
        if r.status_code != 200:
            print(f"  ❌ HTTP {r.status_code}: {url}")
            return False
        
        # Check content type — skip HTML error pages
        ct = r.headers.get("Content-Type", "")
        if "text/html" in ct and not url.endswith(".csv"):
            # Might be an error page
            content = r.content[:500]
            if b"404" in content or b"not found" in content.lower():
                print(f"  ❌ 404 Not Found: {url}")
                return False
        
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        
        size_kb = dest_path.stat().st_size / 1024
        print(f"  ✅ {dest_path.name} ({size_kb:.0f} KB)")
        return True
        
    except Exception as e:
        print(f"  ❌ Error downloading {url}: {e}")
        if dest_path.exists():
            dest_path.unlink()
        return False


# ═══════════════════════════════════════════════════════
# STEP 3: COMMANDS
# ═══════════════════════════════════════════════════════

def cmd_daily(date_str: str):
    """Download all daily files for a specific date."""
    print(f"\n═══ DAILY DOWNLOADS — {date_str} ═══")
    
    urls = discover_daily_urls(date_str)
    if not urls:
        print(f"  No files found for {date_str}")
        # Try hardcoded URL patterns as fallback
        urls = try_known_patterns(date_str)
    
    print(f"  Found {len(urls)} files")
    
    downloaded = 0
    skipped = 0
    for item in urls:
        dest = OUTPUT_DIR / "daily" / date_str / item["category"] / item["filename"]
        if download_file(item["url"], dest):
            downloaded += 1
        else:
            skipped += 1
        time.sleep(RATE_LIMIT)
    
    print(f"\n  Downloaded: {downloaded} | Skipped: {skipped}")


def try_known_patterns(date_str: str) -> list[dict]:
    """
    Try known URL patterns if HTML discovery fails.
    PSX download URLs often follow predictable patterns.
    """
    # Common patterns to try — adapt based on what you discover
    patterns = [
        # Market Summary
        (f"{BASE_URL}/download/market_report/dailystockmkt.pdf", "market_summary", "daily_quotations.pdf"),
        (f"{BASE_URL}/download/mkt_summary/{date_str}.zip", "market_summary", f"market_summary_{date_str}.zip"),
        # Post Close
        (f"{BASE_URL}/download/post_close/{date_str}.zip", "post_close", f"post_close_{date_str}.zip"),
        # VAR Margins
        (f"{BASE_URL}/download/var_margins/{date_str}.zip", "margins", f"var_margins_{date_str}.zip"),
    ]
    
    results = []
    for url, category, filename in patterns:
        try:
            r = session.head(url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                results.append({"url": url, "category": category, "filename": filename, "date": date_str})
        except:
            pass
    
    return results


def cmd_reference():
    """Download all static reference files."""
    print("\n═══ REFERENCE DOWNLOADS (Other tab) ═══")
    
    urls = discover_reference_urls()
    print(f"  Found {len(urls)} files")
    
    downloaded = 0
    for item in urls:
        dest = OUTPUT_DIR / "reference" / item["filename"]
        if download_file(item["url"], dest):
            downloaded += 1
        time.sleep(RATE_LIMIT)
    
    print(f"\n  Downloaded: {downloaded}")


def cmd_backfill(start_date: str, end_date: str):
    """Download daily files for a date range."""
    print(f"\n═══ BACKFILL — {start_date} to {end_date} ═══")
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    total_downloaded = 0
    
    while current <= end:
        # Skip weekends
        if current.weekday() < 5:
            date_str = current.strftime("%Y-%m-%d")
            cmd_daily(date_str)
        current += timedelta(days=1)
    
    print(f"\n═══ BACKFILL COMPLETE ═══")


def cmd_today():
    """Download today's files."""
    today = datetime.now(PKT).strftime("%Y-%m-%d")
    cmd_daily(today)


def cmd_status():
    """Show what's been downloaded."""
    print("\n═══ DOWNLOAD STATUS ═══")
    
    if not OUTPUT_DIR.exists():
        print("  No downloads yet")
        return
    
    # Daily
    daily_dir = OUTPUT_DIR / "daily"
    if daily_dir.exists():
        dates = sorted([d.name for d in daily_dir.iterdir() if d.is_dir()])
        total_files = sum(1 for _ in daily_dir.rglob("*") if _.is_file())
        total_size = sum(f.stat().st_size for f in daily_dir.rglob("*") if f.is_file())
        print(f"\n  📅 Daily Downloads:")
        print(f"     Dates: {len(dates)} ({dates[0] if dates else 'none'} → {dates[-1] if dates else 'none'})")
        print(f"     Files: {total_files}")
        print(f"     Size:  {total_size / 1024 / 1024:.1f} MB")
    
    # Reference
    ref_dir = OUTPUT_DIR / "reference"
    if ref_dir.exists():
        ref_files = list(ref_dir.glob("*"))
        ref_size = sum(f.stat().st_size for f in ref_files if f.is_file())
        print(f"\n  📚 Reference Downloads:")
        print(f"     Files: {len(ref_files)}")
        print(f"     Size:  {ref_size / 1024 / 1024:.1f} MB")
        for f in sorted(ref_files):
            if f.is_file():
                print(f"       {f.name} ({f.stat().st_size / 1024:.0f} KB)")
    
    # Total
    total = sum(f.stat().st_size for f in OUTPUT_DIR.rglob("*") if f.is_file())
    print(f"\n  💾 Total: {total / 1024 / 1024:.1f} MB")


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PSX DPS Downloads Scraper")
    parser.add_argument("command", choices=["today", "daily", "backfill", "reference", "all", "status"])
    parser.add_argument("args", nargs="*", help="Date (YYYY-MM-DD) or start/end dates for backfill")
    
    args = parser.parse_args()
    
    if args.command == "today":
        cmd_today()
    
    elif args.command == "daily":
        if not args.args:
            print("Usage: python psx_downloads_scraper.py daily 2026-03-18")
            sys.exit(1)
        cmd_daily(args.args[0])
    
    elif args.command == "backfill":
        if len(args.args) < 2:
            print("Usage: python psx_downloads_scraper.py backfill 2026-01-01 2026-03-18")
            sys.exit(1)
        cmd_backfill(args.args[0], args.args[1])
    
    elif args.command == "reference":
        cmd_reference()
    
    elif args.command == "all":
        cmd_reference()
        cmd_today()
    
    elif args.command == "status":
        cmd_status()
```

## Step 4: Run the scraper

IMPORTANT: First run the discovery to see actual URLs. The page might use
JavaScript to render links — if so, we need DrissionPage or to inspect 
the network requests manually.

```bash
cd ~/pakfindata

# First: test URL discovery
python scripts/psx_downloads_scraper.py status

# Download reference files (Other tab — one-time)
python scripts/psx_downloads_scraper.py reference

# Download today's daily files
python scripts/psx_downloads_scraper.py today

# Download specific date
python scripts/psx_downloads_scraper.py daily 2026-03-17

# Backfill last 30 days
python scripts/psx_downloads_scraper.py backfill 2026-02-15 2026-03-18

# Check what we have
python scripts/psx_downloads_scraper.py status
```

## Step 5: If HTML discovery fails (JavaScript rendered)

The page might load links via AJAX. In that case, manually capture URLs:

1. Open `https://dps.psx.com.pk/downloads` in Chrome
2. Open DevTools → Network tab
3. Click each download link
4. Note the actual URL (e.g., `https://dps.psx.com.pk/download/document/123456.zip`)
5. Look for patterns like `/download/document/{id}` or `/download/{type}/{date}`

Then update the `try_known_patterns()` function with discovered URLs.

Alternatively, use DrissionPage (you already have it):

```python
from DrissionPage import ChromiumPage

page = ChromiumPage()
page.get("https://dps.psx.com.pk/downloads")

# Click "Daily Downloads" tab
page.ele("text=Daily Downloads").click()
time.sleep(2)

# Set date
date_input = page.ele("tag:input@type=date") or page.ele("tag:input@placeholder")
date_input.clear()
date_input.input("2026-03-18")
page.ele("text=Search").click()
time.sleep(2)

# Get all download links
links = page.eles("tag:a")
for link in links:
    href = link.attr("href")
    text = link.text
    if href and ("download" in href or ".zip" in href or ".csv" in href):
        print(f"{text} → {href}")
```

## Step 6: High-priority files for pakfindata

After downloading, these are the files to analyze first:

```
CRITICAL (integrate into pakfindata):
├── market_summary_closing.zip  → Official OHLCV for all symbols
├── constituent_data.xls        → KSE100/KSE30/KMI30 composition + weights
├── open_interest_dfc.xls       → Futures open interest
├── var_margins.zip             → VaR margins (volatility proxy)
├── post_close_report.zip       → Settlement prices
└── symbol_price_limits.zip     → Circuit breaker limits

REFERENCE (one-time):
├── companies_info.zip          → Sector, listing date, metadata
├── symbol_lot_size.zip         → Trading lot sizes
├── kse100_companies.zip        → Index composition
├── allshare_mktcap.zip         → Market cap data
└── psx_header.zip              → Master symbol list
```

## Output folder structure

```
/mnt/e/psxdata/downloads/
├── daily/
│   ├── 2026-03-18/
│   │   ├── market_summary/
│   │   ├── futures/
│   │   ├── off_market/
│   │   ├── sif/
│   │   ├── margins/
│   │   ├── post_close/
│   │   ├── indices/
│   │   ├── limits/
│   │   └── other/
│   ├── 2026-03-17/
│   └── ...
└── reference/
    ├── companies_info.zip
    ├── symbol_lot_size.zip
    └── ...
```

## IMPORTANT

1. **This is SEPARATE from pakfindata.** Output goes to `/mnt/e/psxdata/downloads/`.
   Do not modify any existing pakfindata code or DB.

2. **Backfill carefully.** Start with 1 week, check the files, then go bigger.
   Don't hammer the server — 1 second delay between downloads.

3. **Some files may be PDFs.** Skip PDFs for now — focus on ZIP, CSV, XLS 
   which are machine-readable.

4. **URL discovery is the hard part.** The HTML might be JavaScript-rendered.
   If `discover_daily_urls()` returns empty, use the DrissionPage fallback
   or manually inspect network requests in Chrome DevTools.

5. **Market Summary (Closing) ZIP is the most valuable file.** It likely 
   contains the official closing OHLCV for all symbols — better than DPS 
   `/timeseries/eod` because it may include high/low (which DPS API doesn't).
