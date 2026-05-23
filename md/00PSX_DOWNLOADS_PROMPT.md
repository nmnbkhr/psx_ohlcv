# Claude Code Prompt: PSX DPS Downloads — Discover & Download All Files

## Context

The PSX Data Portal at `https://dps.psx.com.pk/downloads` has a date-based 
downloads page with two tabs: "Daily Downloads" and "Other Downloads".
The page is JavaScript-rendered — file links load dynamically via API calls.

The pakfindata project already has scrapers using requests/DrissionPage.
Storage: `/mnt/e/psxdata/`

## PHASE 1: DISCOVER — Find all download types (DO NOT DOWNLOAD YET)

### Step 1: Inspect the downloads page API

The page at `https://dps.psx.com.pk/downloads` loads file links via JavaScript.
We need to find the API endpoints that power it.

Try these approaches in order:

**Approach A: Probe known URL patterns**

From research, these URL patterns exist on dps.psx.com.pk:

```python
import requests

BASE = "https://dps.psx.com.pk"
today = "2026-03-16"  # adjust to latest trading day
yesterday = "2026-03-13"

# Known download URL patterns — test each one
PATTERNS = {
    "daily_stock_market_report": f"{BASE}/download/market_report/dailystockmkt.pdf",
    "off_market_transactions": f"{BASE}/download/omtpdf/{yesterday}.pdf",
    "market_summary_pdf": f"{BASE}/download/mkt_summary/{yesterday}.pdf",
    "closing_rates_csv": f"{BASE}/download/closing_rates/{yesterday}.csv",
    "closing_rates_pdf": f"{BASE}/download/closing_rates/{yesterday}.pdf",
    "market_watch_pdf": f"{BASE}/download/market_watch/{yesterday}.pdf",
    "market_watch_csv": f"{BASE}/download/market_watch/{yesterday}.csv",
    "index_report": f"{BASE}/download/index_report/{yesterday}.pdf",
    "sector_summary": f"{BASE}/download/sector_summary/{yesterday}.pdf",
    "sector_summary_csv": f"{BASE}/download/sector_summary/{yesterday}.csv",
    "turnover_report": f"{BASE}/download/turnover/{yesterday}.pdf",
    "circuit_breaker": f"{BASE}/download/circuit_breaker/{yesterday}.pdf",
    "dfc_report": f"{BASE}/download/dfc_report/{yesterday}.pdf",
    "csf_report": f"{BASE}/download/csf_report/{yesterday}.pdf",
    "eligible_scrips": f"{BASE}/download/eligible_scrips/{yesterday}.pdf",
    "top_companies": f"{BASE}/download/top_companies/{yesterday}.pdf",
    "market_report_csv": f"{BASE}/download/market_report/{yesterday}.csv",
    "daily_report_xlsx": f"{BASE}/download/daily_report/{yesterday}.xlsx",
    "historical_data": f"{BASE}/download/historical/{yesterday}.csv",
    "debt_market": f"{BASE}/download/debt_market/{yesterday}.pdf",
    "gis_rates": f"{BASE}/download/gis_rates/{yesterday}.pdf",
    "eod_data": f"{BASE}/download/eod/{yesterday}.csv",
}

print("=== PROBING DOWNLOAD URLS ===")
for name, url in PATTERNS.items():
    try:
        r = requests.head(url, timeout=10, allow_redirects=True)
        ct = r.headers.get("Content-Type", "unknown")
        cl = r.headers.get("Content-Length", "?")
        print(f"  {'✅' if r.status_code == 200 else '❌'} {r.status_code} | {name:30s} | {ct} | {cl} bytes")
        if r.status_code == 200:
            print(f"       URL: {url}")
    except Exception as e:
        print(f"  ❌ ERROR | {name:30s} | {e}")
```

**Approach B: Inspect the page JavaScript**

```python
# Fetch the page HTML and look for API endpoints in <script> tags
r = requests.get(f"{BASE}/downloads")
html = r.text

# Search for API patterns
import re
api_patterns = re.findall(r'(https?://dps\.psx\.com\.pk/[a-zA-Z0-9/_-]+)', html)
download_patterns = re.findall(r'(/download/[a-zA-Z0-9/_.-]+)', html)
api_calls = re.findall(r'(api/[a-zA-Z0-9/_-]+)', html)
fetch_calls = re.findall(r'fetch\(["\']([^"\']+)', html)

print("=== API PATTERNS IN HTML ===")
for p in set(api_patterns): print(f"  {p}")
print("=== DOWNLOAD PATTERNS ===")
for p in set(download_patterns): print(f"  {p}")
print("=== API CALLS ===")
for p in set(api_calls): print(f"  {p}")
print("=== FETCH CALLS ===")
for p in set(fetch_calls): print(f"  {p}")
```

**Approach C: Try the API endpoint directly**

PSX DPS usually has a JSON API behind the pages:

```python
# Try common API patterns for downloads listing
API_ATTEMPTS = [
    f"{BASE}/api/downloads?date={yesterday}",
    f"{BASE}/api/downloads/{yesterday}",
    f"{BASE}/api/daily-downloads?date={yesterday}",
    f"{BASE}/api/reports?date={yesterday}",
    f"{BASE}/api/download-list?date={yesterday}",
]

for url in API_ATTEMPTS:
    try:
        r = requests.get(url, timeout=10, headers={"Accept": "application/json"})
        if r.status_code == 200 and r.headers.get("Content-Type","").startswith("application/json"):
            print(f"✅ FOUND API: {url}")
            import json
            data = r.json()
            print(json.dumps(data, indent=2)[:2000])
            break
        else:
            print(f"❌ {r.status_code} | {url}")
    except:
        print(f"❌ ERROR | {url}")
```

**Approach D: Use DrissionPage if API not found**

If approaches A-C don't reveal the full list, use DrissionPage (already in project):

```python
from DrissionPage import ChromiumPage

page = ChromiumPage()
page.get("https://dps.psx.com.pk/downloads")
page.wait.load_start()

import time
time.sleep(3)  # wait for JS to render

# Find all download links
links = page.eles("tag:a")
download_links = []
for link in links:
    href = link.attr("href") or ""
    if "/download/" in href or href.endswith(".pdf") or href.endswith(".csv") or href.endswith(".xlsx"):
        text = link.text.strip()
        download_links.append({"text": text, "url": href})
        print(f"  📥 {text:40s} | {href}")

# Also check both tabs
tabs = page.eles("text:Other Downloads")
if tabs:
    tabs[0].click()
    time.sleep(2)
    other_links = page.eles("tag:a")
    for link in other_links:
        href = link.attr("href") or ""
        if "/download/" in href:
            text = link.text.strip()
            if {"text": text, "url": href} not in download_links:
                download_links.append({"text": text, "url": href})
                print(f"  📥 [OTHER] {text:40s} | {href}")

page.quit()
```

### Step 2: Also check what pakfindata already downloads

```bash
# What existing scrapers download from PSX DPS
grep -rn "dps.psx.com.pk/download\|dps.psx.com.pk/api" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

# What's already in the download folder
find /mnt/e/psxdata/ -name "*.pdf" -o -name "*.csv" -o -name "*.xlsx" | head -30

# Existing download/scraper files
find ~/pakfindata/src/ -name "*download*" -o -name "*report*" -o -name "*dps*" | grep -v __pycache__
```

### Step 3: Report findings

After running the above, produce this report:

```
═══════════════════════════════════════════════════════
  PSX DPS DOWNLOADS — DISCOVERY REPORT
═══════════════════════════════════════════════════════

DAILY DOWNLOADS (date-based, one per trading day):
| # | Name | URL Pattern | Format | Size |
|---|------|-------------|--------|------|
| 1 | Daily Stock Market Report | /download/market_report/dailystockmkt.pdf | PDF | ~200KB |
| 2 | Off-Market Transactions | /download/omtpdf/{date}.pdf | PDF | ~50KB |
| ... | ... | ... | ... | ... |

OTHER DOWNLOADS (static/periodic):
| # | Name | URL Pattern | Format |
|---|------|-------------|--------|
| 1 | ... | ... | ... |

ALREADY DOWNLOADED BY PAKFINDATA:
- [list what existing scrapers already fetch]

NEW FILES TO ADD:
- [list files not currently being downloaded]

TOTAL: {N} unique download types found
═══════════════════════════════════════════════════════
```

**STOP HERE. Show me the discovery report before proceeding to Phase 2.**

---

## PHASE 2: BUILD THE DOWNLOADER (after discovery confirmed)

### Create: `src/pakfindata/sources/psx_downloads.py`

```python
"""
PSX DPS Downloads — Fetch all daily reports from dps.psx.com.pk/downloads

Usage:
    python -m pakfindata.sources.psx_downloads                    # download today
    python -m pakfindata.sources.psx_downloads --date 2026-03-13  # specific date
    python -m pakfindata.sources.psx_downloads --backfill 30      # last 30 trading days
    python -m pakfindata.sources.psx_downloads --backfill-from 2025-01-01  # from date to today
    python -m pakfindata.sources.psx_downloads --list             # show available types
"""
```

### Architecture

```python
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time

DOWNLOAD_DIR = Path("/mnt/e/psxdata/downloads")

# Each download type maps to a subfolder + URL pattern
DOWNLOAD_TYPES = {
    # Key: subfolder name
    # Value: (url_template, file_extension, description)
    # {date} placeholder in URL gets replaced with YYYY-MM-DD
    
    "market_report": (
        "https://dps.psx.com.pk/download/market_report/dailystockmkt.pdf",
        ".pdf", "Daily Stock Market Report"
    ),
    "off_market": (
        "https://dps.psx.com.pk/download/omtpdf/{date}.pdf",
        ".pdf", "Off-Market Transactions"
    ),
    # ... ADD ALL DISCOVERED TYPES FROM PHASE 1 ...
}


class PSXDownloader:
    def __init__(self, base_dir: Path = DOWNLOAD_DIR):
        self.base_dir = base_dir
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 pakfindata/1.0"
        })
    
    def download_date(self, date: str, types: list[str] | None = None):
        """Download all (or specific) file types for a given date."""
        types = types or list(DOWNLOAD_TYPES.keys())
        results = {"success": 0, "failed": 0, "skipped": 0}
        
        for dtype in types:
            url_template, ext, desc = DOWNLOAD_TYPES[dtype]
            url = url_template.replace("{date}", date)
            
            # Create subfolder
            folder = self.base_dir / dtype
            folder.mkdir(parents=True, exist_ok=True)
            
            # Target filename
            filename = f"{date}{ext}"
            filepath = folder / filename
            
            # Skip if already downloaded
            if filepath.exists() and filepath.stat().st_size > 0:
                results["skipped"] += 1
                continue
            
            # Download
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 200 and len(r.content) > 100:
                    filepath.write_bytes(r.content)
                    size_kb = len(r.content) / 1024
                    print(f"  ✅ {dtype:25s} | {filename} | {size_kb:.0f} KB")
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except Exception as e:
                print(f"  ❌ {dtype:25s} | {e}")
                results["failed"] += 1
        
        return results
    
    def backfill(self, days: int = 30, from_date: str | None = None):
        """Download files for multiple past trading days."""
        if from_date:
            start = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            start = datetime.now() - timedelta(days=days)
        
        end = datetime.now()
        current = start
        total = {"success": 0, "failed": 0, "skipped": 0}
        
        while current <= end:
            # Skip weekends (Sat=5, Sun=6)
            if current.weekday() < 5:
                date_str = current.strftime("%Y-%m-%d")
                print(f"\n📅 {date_str}")
                result = self.download_date(date_str)
                for k in total:
                    total[k] += result[k]
                
                # Rate limit — 1 second between dates
                time.sleep(1)
            
            current += timedelta(days=1)
        
        print(f"\n{'='*50}")
        print(f"✅ Downloaded: {total['success']}")
        print(f"⏭️  Skipped (already exist): {total['skipped']}")
        print(f"❌ Failed: {total['failed']}")
    
    def list_types(self):
        """Print all available download types."""
        print("Available download types:")
        for key, (url, ext, desc) in DOWNLOAD_TYPES.items():
            folder = self.base_dir / key
            count = len(list(folder.glob(f"*{ext}"))) if folder.exists() else 0
            print(f"  {key:25s} | {ext:5s} | {count:4d} files | {desc}")
    
    def status(self):
        """Show download coverage — which dates have files, which are missing."""
        print("Download coverage:")
        for key, (url, ext, desc) in DOWNLOAD_TYPES.items():
            folder = self.base_dir / key
            if folder.exists():
                files = sorted(folder.glob(f"*{ext}"))
                if files:
                    dates = [f.stem for f in files]
                    print(f"  {key:25s} | {len(files):4d} files | {dates[0]} → {dates[-1]}")
                else:
                    print(f"  {key:25s} |    0 files")
            else:
                print(f"  {key:25s} | NOT CREATED")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PSX DPS Downloads")
    parser.add_argument("--date", help="Download for specific date (YYYY-MM-DD)")
    parser.add_argument("--backfill", type=int, help="Backfill last N days")
    parser.add_argument("--backfill-from", help="Backfill from date (YYYY-MM-DD) to today")
    parser.add_argument("--list", action="store_true", help="List available download types")
    parser.add_argument("--status", action="store_true", help="Show download coverage")
    parser.add_argument("--types", nargs="+", help="Only download specific types")
    
    args = parser.parse_args()
    dl = PSXDownloader()
    
    if args.list:
        dl.list_types()
    elif args.status:
        dl.status()
    elif args.backfill:
        dl.backfill(days=args.backfill)
    elif args.backfill_from:
        dl.backfill(from_date=args.backfill_from)
    elif args.date:
        dl.download_date(args.date, types=args.types)
    else:
        # Default: download today
        today = datetime.now().strftime("%Y-%m-%d")
        dl.download_date(today)
```

### Folder structure created:

```
/mnt/e/psxdata/downloads/
├── market_report/          # Daily Stock Market Report PDFs
│   ├── 2026-03-13.pdf
│   ├── 2026-03-12.pdf
│   └── ...
├── off_market/             # Off-Market Transactions
│   ├── 2026-03-13.pdf
│   └── ...
├── closing_rates/          # Closing rates CSV/PDF
│   ├── 2026-03-13.csv
│   └── ...
├── market_watch/           # Market watch
│   └── ...
├── sector_summary/         # Sector summaries
│   └── ...
├── index_report/           # Index reports
│   └── ...
└── [other types discovered in Phase 1]/
```

### Add to Streamlit UI (optional)

Add a "PSX Downloads" section to the Admin or Sync page:
- Show download status table (type, file count, date range)
- "Download Today" button
- "Backfill" button with date range picker
- Link to open the downloads folder

### Add daily cron trigger

The downloader should run automatically after market close (15:35 PKT).
Add to tick_service.py EOD flush, or as a separate cron entry:

```python
# In tick_service.py after EOD flush:
from pakfindata.sources.psx_downloads import PSXDownloader
dl = PSXDownloader()
dl.download_date(datetime.now().strftime("%Y-%m-%d"))
print("📥 PSX daily reports downloaded")
```

## VERIFY

```bash
# Test single date download
python -m pakfindata.sources.psx_downloads --date 2026-03-13

# Check what was downloaded
find /mnt/e/psxdata/downloads/ -type f | head -20

# Show status
python -m pakfindata.sources.psx_downloads --status

# List types
python -m pakfindata.sources.psx_downloads --list

# Backfill last 5 days
python -m pakfindata.sources.psx_downloads --backfill 5
```
