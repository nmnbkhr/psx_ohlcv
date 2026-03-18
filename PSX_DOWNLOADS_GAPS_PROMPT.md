# Claude Code Prompt: PSX Downloads — Gap Files Only

## Context

The pakfindata project already scrapes these from `dps.psx.com.pk/downloads`:
- ✅ Market Summary (Closing) → `market_summary.py`
- ✅ Post Close Report → `market_summary.py:1104`
- ✅ Closing Rates PDF → `closing_rates_pdf.py`
- ✅ Listed Companies → `listed_companies.py`

**DO NOT re-scrape these. This prompt covers ONLY the missing files.**

## Step 1: Discover actual URLs using DrissionPage

The downloads page is JavaScript-rendered — curl/requests won't see the links.
Use DrissionPage to capture real URLs.

```bash
cd ~/pakfindata
```

Create `scripts/discover_download_urls.py`:

```python
"""
Discover actual download URLs from dps.psx.com.pk/downloads.
Uses DrissionPage to handle JS-rendered content.
Run once, capture URLs, then build direct downloaders.
"""

from DrissionPage import ChromiumPage
import time
import json
from datetime import datetime

def discover_all():
    page = ChromiumPage()
    results = {"daily": {}, "other": {}}
    
    # ═══ OTHER DOWNLOADS (static) ═══
    print("═══ OTHER DOWNLOADS ═══")
    page.get("https://dps.psx.com.pk/downloads")
    time.sleep(3)
    
    # Click "Other Downloads" tab
    try:
        other_tab = page.ele("text=Other Downloads")
        if other_tab:
            other_tab.click()
            time.sleep(2)
    except:
        pass
    
    # Capture all links
    links = page.eles("tag:a")
    for link in links:
        href = link.attr("href") or ""
        text = link.text.strip()
        if href and any(ext in href.lower() for ext in [".zip", ".csv", ".xls", ".xlsx", ".pdf", "download"]):
            if href.startswith("/"):
                href = f"https://dps.psx.com.pk{href}"
            results["other"][text] = href
            print(f"  {text} → {href}")
    
    # ═══ DAILY DOWNLOADS (today) ═══
    print("\n═══ DAILY DOWNLOADS ═══")
    
    # Click "Daily Downloads" tab
    try:
        daily_tab = page.ele("text=Daily Downloads")
        if daily_tab:
            daily_tab.click()
            time.sleep(2)
    except:
        pass
    
    # Set today's date
    date_str = datetime.now().strftime("%Y-%m-%d")
    date_input = page.ele("tag:input")
    if date_input:
        date_input.clear()
        date_input.input(date_str)
        
        # Click search
        search_btn = page.ele("text=SEARCH") or page.ele("text=Search")
        if search_btn:
            search_btn.click()
            time.sleep(3)
    
    # Capture all links
    links = page.eles("tag:a")
    for link in links:
        href = link.attr("href") or ""
        text = link.text.strip()
        if href and any(ext in href.lower() for ext in [".zip", ".csv", ".xls", ".xlsx", ".pdf", ".z", "download"]):
            if href.startswith("/"):
                href = f"https://dps.psx.com.pk{href}"
            results["daily"][text] = href
            print(f"  {text} → {href}")
    
    # Save results
    with open("download_urls_discovered.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Saved to download_urls_discovered.json")
    print(f"   Other: {len(results['other'])} files")
    print(f"   Daily: {len(results['daily'])} files")
    
    page.quit()
    return results

if __name__ == "__main__":
    discover_all()
```

Run it:
```bash
python scripts/discover_download_urls.py
cat download_urls_discovered.json
```

**STOP HERE — show me the discovered URLs before proceeding.**
The actual URL patterns (probably `.Z` not `.zip`) will determine the scraper code.

## Step 2: Create gap scrapers

Based on the discovered URLs, create downloaders for these **GAP FILES ONLY**:

### Priority 1 — High value for pakfindata analytics

| File | Why | Expected format |
|------|-----|-----------------|
| **Constituent Data (PSX Indices)** | KSE100/KSE30/KMI30 weights | XLS |
| **Futures Open Interest (DFC)** | Derivatives analytics | XLS |
| **Futures Open Interest (CSF)** | Derivatives analytics | XLS |
| **VAR Margins** | Volatility/risk proxy per symbol | ZIP→CSV/XLS |
| **Symbol Price Limits** | Circuit breaker upper/lower bounds | ZIP |
| **Off Market Transaction Summary** | Block/institutional trades | CSV |
| **SIF Open Interest Report** | Single stock futures OI | CSV |

### Priority 2 — Reference data (one-time download)

| File | Why | Expected format |
|------|-----|-----------------|
| **Companies Info** | Sector, metadata for all symbols | ZIP |
| **Symbol Lot Size** | Trading lot sizes | ZIP |
| **KSE 100 Index Companies** | Index composition | ZIP |
| **All Share Index & Mkt. Cap.** | Market cap data | ZIP |
| **PSX Header** | Master symbol list | ZIP |
| **Index Fluctuation** | Index history | ZIP |
| **HBLTT Index values** | HBL Total Return Index | CSV |

### Skip (already scraped or low value)

- ❌ Market Summary (Closing) — already in `market_summary.py`
- ❌ Post Close Report — already in `market_summary.py`
- ❌ Closing Rate Summary — already in `closing_rates_pdf.py`  
- ❌ Listed Companies — already in `listed_companies.py`
- ❌ Daily Quotations — PDF, not machine-readable
- ❌ Daily Announcements — PDF
- ❌ Fair Value Report — PDF
- ❌ Ready Market Short Sell Vol — PDF
- ❌ Internet Trading Subscribers — PDF
- ❌ Stock Market Report — PDF

## Step 3: Build the scraper using discovered URLs

Create `src/pakfindata/sources/psx_downloads.py`:

```python
"""
PSX DPS Downloads — Gap files only.

Scrapes files NOT already covered by existing pakfindata scrapers.
Uses actual URLs discovered via DrissionPage.

Stores in: /mnt/e/psxdata/downloads/
  ├── daily/{date}/
  │   ├── futures_oi_dfc.xls
  │   ├── futures_oi_csf.xls
  │   ├── var_margins.zip (or .Z)
  │   ├── symbol_price_limits.zip (or .Z)
  │   ├── off_market_summary.csv
  │   ├── sif_open_interest.csv
  │   └── constituent_data.xls
  └── reference/
      ├── companies_info.zip
      ├── symbol_lot_size.zip
      ├── kse100_companies.zip
      ├── allshare_mktcap.zip
      ├── psx_header.zip
      ├── index_fluctuation.zip
      └── hbltt_index.csv

Usage:
  python -m pakfindata.sources.psx_downloads today
  python -m pakfindata.sources.psx_downloads daily 2026-03-18
  python -m pakfindata.sources.psx_downloads backfill 2026-01-01 2026-03-18
  python -m pakfindata.sources.psx_downloads reference
  python -m pakfindata.sources.psx_downloads status
"""

import requests
import argparse
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ═══════════════════════════════════════════════════════
# CONFIG — UPDATE THESE WITH DISCOVERED URLs
# ═══════════════════════════════════════════════════════

BASE = "https://dps.psx.com.pk"
OUTPUT = Path("/mnt/e/psxdata/downloads")
PKT = timezone(timedelta(hours=5))
RATE_LIMIT = 1.0

# URL TEMPLATES — replace {date} with YYYY-MM-DD
# TODO: Update these after running discover_download_urls.py
DAILY_FILES = {
    # "name": ("url_template", "category", "filename_template")
    # Example (update with real URLs):
    # "futures_oi_dfc": ("{BASE}/download/document/{id}.xls", "futures", "futures_oi_dfc_{date}.xls"),
}

REFERENCE_FILES = {
    # "name": ("url", "filename")
    # Example (update with real URLs):
    # "companies_info": ("{BASE}/download/document/{id}.zip", "companies_info.zip"),
}

session = requests.Session()
session.headers.update({"User-Agent": "pakfindata/1.0", "Referer": f"{BASE}/downloads"})


# ═══════════════════════════════════════════════════════
# DOWNLOAD
# ═══════════════════════════════════════════════════════

def download(url: str, dest: Path) -> bool:
    """Download file, skip if exists."""
    if dest.exists() and dest.stat().st_size > 100:
        return False
    
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        r = session.get(url, timeout=60, stream=True)
        if r.status_code != 200:
            print(f"  ❌ HTTP {r.status_code}: {dest.name}")
            return False
        
        # Skip HTML error pages
        ct = r.headers.get("Content-Type", "")
        if "text/html" in ct and not url.endswith((".csv", ".html")):
            print(f"  ❌ Got HTML instead of file: {dest.name}")
            return False
        
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        
        size = dest.stat().st_size / 1024
        print(f"  ✅ {dest.name} ({size:.0f} KB)")
        return True
    
    except Exception as e:
        print(f"  ❌ {dest.name}: {e}")
        if dest.exists():
            dest.unlink()
        return False


# ═══════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════

def cmd_daily(date_str: str):
    """Download gap files for specific date."""
    print(f"\n═══ GAP DOWNLOADS — {date_str} ═══")
    
    if not DAILY_FILES:
        print("  ⚠️ No daily URLs configured yet!")
        print("  Run: python scripts/discover_download_urls.py")
        print("  Then update DAILY_FILES dict in this file with discovered URLs.")
        return
    
    count = 0
    for name, (url_tpl, category, fname_tpl) in DAILY_FILES.items():
        url = url_tpl.replace("{date}", date_str).replace("{BASE}", BASE)
        fname = fname_tpl.replace("{date}", date_str)
        dest = OUTPUT / "daily" / date_str / category / fname
        if download(url, dest):
            count += 1
        time.sleep(RATE_LIMIT)
    
    print(f"  Downloaded: {count}")


def cmd_reference():
    """Download static reference files."""
    print("\n═══ REFERENCE DOWNLOADS ═══")
    
    if not REFERENCE_FILES:
        print("  ⚠️ No reference URLs configured yet!")
        print("  Run: python scripts/discover_download_urls.py")
        print("  Then update REFERENCE_FILES dict in this file with discovered URLs.")
        return
    
    count = 0
    for name, (url, fname) in REFERENCE_FILES.items():
        url = url.replace("{BASE}", BASE)
        dest = OUTPUT / "reference" / fname
        if download(url, dest):
            count += 1
        time.sleep(RATE_LIMIT)
    
    print(f"  Downloaded: {count}")


def cmd_backfill(start: str, end: str):
    """Download gap files for date range (weekdays only)."""
    print(f"\n═══ BACKFILL — {start} to {end} ═══")
    
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    
    while current <= end_dt:
        if current.weekday() < 5:
            cmd_daily(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)


def cmd_status():
    """Show download status."""
    print("\n═══ DOWNLOAD STATUS ═══")
    
    if not OUTPUT.exists():
        print("  No downloads yet")
        return
    
    for subdir in ["daily", "reference"]:
        p = OUTPUT / subdir
        if p.exists():
            files = list(p.rglob("*"))
            files = [f for f in files if f.is_file()]
            size = sum(f.stat().st_size for f in files)
            print(f"\n  📁 {subdir}/")
            print(f"     Files: {len(files)}")
            print(f"     Size:  {size / 1024 / 1024:.1f} MB")
            
            if subdir == "daily":
                dates = sorted(set(f.parent.parent.name for f in files if f.parent.parent.name != subdir))
                if dates:
                    print(f"     Dates: {dates[0]} → {dates[-1]} ({len(dates)} days)")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PSX Downloads — Gap Files")
    parser.add_argument("command", choices=["today", "daily", "backfill", "reference", "status"])
    parser.add_argument("args", nargs="*")
    
    args = parser.parse_args()
    
    if args.command == "today":
        cmd_daily(datetime.now(PKT).strftime("%Y-%m-%d"))
    elif args.command == "daily":
        cmd_daily(args.args[0] if args.args else datetime.now(PKT).strftime("%Y-%m-%d"))
    elif args.command == "backfill":
        cmd_backfill(args.args[0], args.args[1])
    elif args.command == "reference":
        cmd_reference()
    elif args.command == "status":
        cmd_status()
```

## Step 4: Two-phase execution

**Phase 1 — Discover URLs (run once):**
```bash
python scripts/discover_download_urls.py
# Shows real URLs for all files
# Save output → update DAILY_FILES and REFERENCE_FILES dicts
```

**Phase 2 — Download files:**
```bash
python -m pakfindata.sources.psx_downloads reference    # One-time
python -m pakfindata.sources.psx_downloads today         # Daily
python -m pakfindata.sources.psx_downloads backfill 2026-01-01 2026-03-18
python -m pakfindata.sources.psx_downloads status
```

## IMPORTANT

1. **Run discover_download_urls.py FIRST** — URLs are unknown until captured from browser
2. **File extensions are likely `.Z` not `.zip`** — based on existing pakfindata code
3. **DO NOT touch existing scrapers** — market_summary.py, closing_rates_pdf.py, listed_companies.py remain unchanged
4. **Store in /mnt/e/psxdata/downloads/** — separate from main DB
5. **Rate limit 1s between downloads** — respectful to PSX server
6. **Weekend dates return no files** — backfill skips Sat/Sun automatically
