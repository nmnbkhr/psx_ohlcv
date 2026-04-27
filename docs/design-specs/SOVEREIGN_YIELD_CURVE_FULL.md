# Claude Code Prompt: Full Sovereign Yield Curve — Download + Process + Extend App

## What We're Building

Pakistan's complete sovereign yield curve from 1W to 30Y, using two complementary sources:

```
SBP DFMD (sbp.org.pk/dfmd/pma.asp)
  ├── Excel archives: historical MTB + PIB auction cutoffs (DIRECT DOWNLOAD, no scraping)
  │   ├── tb.xlsx        → T-Bill cutoffs: 1M, 3M, 6M, 12M (years of history)
  │   ├── Pakinvestbonds.xlsx → PIB cutoffs: 2Y, 3Y, 5Y, 10Y, 15Y (years of history)
  │   ├── PIB-Float-Arch-SA.xlsx → Floating PIB semi-annual coupon
  │   ├── PIB-Float-Arch-Q.xlsx  → Floating PIB quarterly coupon
  │   └── BuyBack-Auction-Summary.xlsx
  ├── Live page: current KIBOR, policy rate, overnight repo
  └── PDF bid reports: MTB-BID.pdf, pib-bid.pdf (latest auction details)

MUFAP (mufap.com.pk)
  ├── Daily PKRV CSVs:   interpolated conventional curve, 21 tenors (1W–30Y)
  ├── Daily PKISRV CSVs:  interpolated Islamic curve, 21 tenors
  └── Daily PKFRV CSVs:   forward rates

Combined → Unified yield_curve table → App pages show full 1W–30Y curve
```

**Three phases in this one prompt:**
1. Download everything to disk
2. Process into database
3. Extend existing app pages

## Step 0: Audit What Exists

```bash
cd ~/pakfindata && conda activate psx

# ── What rate/yield data does the app already have? ──
echo "=== Existing rate tables ==="
sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" | tr ' ' '\n' | \
    grep -i "rate\|yield\|curve\|pkrv\|kibor\|tbill\|pib\|bond\|auction\|tenor\|sbp"

# Show schema for any found
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" | tr ' ' '\n' | \
    grep -i "rate\|yield\|curve\|pkrv\|kibor\|tbill\|pib\|bond\|auction\|tenor"); do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl"
    sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) as rows FROM $tbl;"
    sqlite3 /mnt/e/psxdata/psx.sqlite -header "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 3;"
    echo ""
done

# ── What pages display rate/yield data? ──
echo "=== Pages with yield/rate data ==="
grep -rn "yield\|PKRV\|PKISRV\|KIBOR\|kibor\|T.Bill\|PIB\|pib\|tenor\|curve" \
    ~/pakfindata/src/pakfindata/ui/page_views/ --include="*.py" -l | grep -v __pycache__

# ── What sources fetch rate data? ──
echo "=== Sources/scrapers ==="
grep -rn "yield\|PKRV\|PKISRV\|KIBOR\|sbp.*rate\|auction\|T.Bill\|PIB" \
    ~/pakfindata/src/pakfindata/sources/ --include="*.py" -l | grep -v __pycache__

# ── Current PKRV data — what tenors exist? ──
echo "=== Current tenor data ==="
for tbl in pkrv_rates pkisrv_rates yield_curve sbp_rates kibor_daily; do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/psx.sqlite -header \
        "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 3;" 2>/dev/null || echo "not found"
done

# ── Existing SBP scraper ──
echo "=== SBP scraper code ==="
grep -rn "sbp.org.pk\|ecodata\|dfmd\|pma.asp" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# ── Existing MUFAP scraper ──
echo "=== MUFAP scraper code ==="
grep -rn "mufap\|MUFAP" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# ── fi_sync service ──
echo "=== fi_sync ==="
ls -la ~/pakfindata/src/pakfindata/services/fi_sync* 2>/dev/null
head -50 ~/pakfindata/src/pakfindata/services/fi_sync*.py 2>/dev/null

# ── Existing yield curve page ──
echo "=== Yield curve page ==="
wc -l ~/pakfindata/src/pakfindata/ui/page_views/yield_curve*.py 2>/dev/null || echo "not found"
wc -l ~/pakfindata/src/pakfindata/ui/page_views/rates*.py 2>/dev/null || echo "not found"
wc -l ~/pakfindata/src/pakfindata/ui/page_views/treasury*.py 2>/dev/null || echo "not found"
wc -l ~/pakfindata/src/pakfindata/ui/page_views/bond*.py 2>/dev/null || echo "not found"
wc -l ~/pakfindata/src/pakfindata/ui/page_views/debt*.py 2>/dev/null || echo "not found"
```

**READ ALL OUTPUT. Understand what exists before writing anything.**

## Step 1: Download SBP Excel Archives

These are direct downloads — no authentication, no scraping, no AJAX. Just HTTP GET.

Create `src/pakfindata/sources/sbp_rates_downloader.py`:

```python
"""
SBP DFMD Rates Downloader

Downloads Excel archives and PDFs from SBP's Financial Markets page.
These are DIRECT URLs — no scraping needed.

Output: /mnt/e/psxdata/sbp_rates/
  ├── archives/
  │   ├── tb.xlsx                      (T-Bill auction history)
  │   ├── Pakinvestbonds.xlsx          (PIB auction history)
  │   ├── PIB-Float-Arch-SA.xlsx       (Floating PIB semi-annual)
  │   ├── PIB-Float-Arch-Q.xlsx        (Floating PIB quarterly)
  │   ├── BuyBack-Auction-Summary.xlsx (Buyback history)
  │   └── gop-ijara-summary.pdf       (Sukuk summary)
  ├── latest/
  │   ├── auction-tbills.pdf           (Latest MTB auction result)
  │   ├── Auction-Investment.pdf       (Latest PIB auction result)
  │   ├── MTB-BID.pdf                  (Latest MTB bid report)
  │   ├── pib-bid.pdf                  (Latest PIB bid report)
  │   ├── auction-treasurybills.pdf    (MTB auction calendar)
  │   └── Auction-Bond.pdf             (PIB auction calendar)
  └── page_snapshot.json               (Scraped current rates from pma.asp)

Usage:
    python -m pakfindata.sources.sbp_rates_downloader download
    python -m pakfindata.sources.sbp_rates_downloader snapshot
    python -m pakfindata.sources.sbp_rates_downloader status
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logger = logging.getLogger("sbp_rates_downloader")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

SBP_ROOT = DATA_ROOT / "sbp_rates"

# ═══════════════════════════════════════════
# DIRECT DOWNLOAD URLs — from sbp.org.pk/dfmd/pma.asp
# ═══════════════════════════════════════════

ARCHIVE_FILES = {
    # Excel archives with full history
    "tb.xlsx":                     "https://www.sbp.org.pk/ecodata/tb.xlsx",
    "Pakinvestbonds.xlsx":         "https://www.sbp.org.pk/ecodata/Pakinvestbonds.xlsx",
    "PIB-Float-Arch-SA.xlsx":      "https://www.sbp.org.pk/ecodata/PIB-Float-Arch-SA.xlsx",
    "PIB-Float-Arch-Q.xlsx":       "https://www.sbp.org.pk/ecodata/PIB-Float-Arch-Q.xlsx",
    "BuyBack-Auction-Summary.xlsx":"https://www.sbp.org.pk/dfmd/BuyBack-Auction-Summary.xlsx",
    "gop-ijara-summary.pdf":       "https://www.sbp.org.pk/ecodata/gop-ijara-summary.pdf",
}

LATEST_FILES = {
    # Current auction results and bid reports
    "auction-tbills.pdf":          "https://www.sbp.org.pk/ecodata/auction-tbills.pdf",
    "Auction-Investment.pdf":      "https://www.sbp.org.pk/ecodata/Auction-Investment.pdf",
    "MTB-BID.pdf":                 "https://www.sbp.org.pk/ecodata/MTB-BID.pdf",
    "pib-bid.pdf":                 "https://www.sbp.org.pk/ecodata/pib-bid.pdf",
    "auction-treasurybills.pdf":   "https://www.sbp.org.pk/ecodata/auction-treasurybills.pdf",
    "Auction-Bond.pdf":            "https://www.sbp.org.pk/ecodata/Auction-Bond.pdf",
}

# MTB Bid Report archive pages (HTML listing → PDF links)
MTB_BID_ARCHIVE = "https://www.sbp.org.pk/dfmd/MTB-BID-Arch.asp"
PIB_BID_ARCHIVE = "https://www.sbp.org.pk/dfmd/PIB-BID-Arch.asp"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
})


def download_archives(force: bool = False):
    """Download all Excel archive files from SBP."""
    out_dir = SBP_ROOT / "archives"
    out_dir.mkdir(parents=True, exist_ok=True)

    for filename, url in ARCHIVE_FILES.items():
        out = out_dir / filename
        if out.exists() and not force:
            age_days = (time.time() - out.stat().st_mtime) / 86400
            if age_days < 1:
                print(f"  ⏭ {filename} (downloaded {age_days:.0f}d ago)")
                continue

        print(f"  ⬇ {filename}...", end=" ", flush=True)
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 1000:
                out.write_bytes(resp.content)
                print(f"✓ ({len(resp.content) / 1024:.0f} KB)")
            else:
                print(f"✗ (HTTP {resp.status_code})")
        except Exception as e:
            print(f"✗ ({e})")
        time.sleep(0.5)


def download_latest():
    """Download latest auction PDFs from SBP."""
    out_dir = SBP_ROOT / "latest"
    out_dir.mkdir(parents=True, exist_ok=True)

    for filename, url in LATEST_FILES.items():
        out = out_dir / filename
        print(f"  ⬇ {filename}...", end=" ", flush=True)
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 500:
                out.write_bytes(resp.content)
                print(f"✓ ({len(resp.content) / 1024:.0f} KB)")
            else:
                print(f"✗ (HTTP {resp.status_code})")
        except Exception as e:
            print(f"✗ ({e})")
        time.sleep(0.5)


def scrape_current_rates() -> dict:
    """Scrape the pma.asp page for current live rates.
    
    Extracts: policy rate, KIBOR, overnight repo, MTB cutoffs, PIB cutoffs.
    Saves to page_snapshot.json for the app to read.
    """
    url = "https://www.sbp.org.pk/dfmd/pma.asp"
    snapshot = {
        "timestamp": datetime.now(PKT).isoformat(),
        "source": url,
        "policy_rate": None,
        "overnight_ceiling": None,
        "overnight_floor": None,
        "overnight_repo": None,
        "kibor": {},
        "mtb_cutoffs": {},
        "pib_cutoffs": {},
        "pib_floating": {},
        "gis": {},
        "fx": {},
    }

    try:
        resp = SESSION.get(url, timeout=20)
        if resp.status_code != 200:
            logger.warning("SBP pma.asp returned %d", resp.status_code)
            return snapshot

        html = resp.text

        # Policy Rate
        m = re.search(r'Policy.*?Rate.*?(\d+\.\d+)%', html, re.DOTALL | re.IGNORECASE)
        if m:
            snapshot["policy_rate"] = float(m.group(1))

        # Overnight ceiling/floor
        m = re.search(r'SBP Overnight.*?(\d+\.\d+)%.*?SBP Overnight.*?(\d+\.\d+)%', html, re.DOTALL)
        if m:
            snapshot["overnight_ceiling"] = float(m.group(1))
            snapshot["overnight_floor"] = float(m.group(2))

        # Weighted-average Overnight Repo
        m = re.search(r'Weighted.*?Overnight.*?Repo.*?(\d+\.\d+)%', html, re.DOTALL | re.IGNORECASE)
        if m:
            snapshot["overnight_repo"] = float(m.group(1))

        # KIBOR — extract bid/offer for 3M, 6M, 12M
        kibor_pattern = r'(\d+)-M.*?(\d+\.\d+).*?(\d+\.\d+)'
        for m in re.finditer(kibor_pattern, html):
            tenor = f"{m.group(1)}M"
            snapshot["kibor"][tenor] = {
                "bid": float(m.group(2)),
                "offer": float(m.group(3)),
            }

        # MTB cutoffs
        mtb_pattern = r'(\d+)-M.*?(\d+\.\d+)%'
        mtb_section = re.search(r'MTBs.*?Fixed-rate PIB', html, re.DOTALL | re.IGNORECASE)
        if mtb_section:
            for m in re.finditer(mtb_pattern, mtb_section.group()):
                tenor = f"{m.group(1)}M"
                snapshot["mtb_cutoffs"][tenor] = float(m.group(2))

        # PIB cutoffs
        pib_pattern = r'(\d+)-Y.*?(\d+\.\d+)%'
        pib_section = re.search(r'Fixed-rate PIB.*?Floating', html, re.DOTALL | re.IGNORECASE)
        if pib_section:
            for m in re.finditer(pib_pattern, pib_section.group()):
                tenor = f"{m.group(1)}Y"
                snapshot["pib_cutoffs"][tenor] = float(m.group(2))

        # FX
        fx_m = re.search(r'M2M.*?(\d+\.\d+)', html, re.DOTALL)
        if fx_m:
            snapshot["fx"]["usd_pkr_m2m"] = float(fx_m.group(1))

    except Exception as e:
        logger.error("Failed to scrape pma.asp: %s", e)

    # Save snapshot
    out = SBP_ROOT / "page_snapshot.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(out) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    os.replace(tmp, str(out))

    return snapshot


def show_status():
    """Print download status."""
    print(f"\nSBP Rates — {SBP_ROOT}\n")

    for subdir, files in [("archives", ARCHIVE_FILES), ("latest", LATEST_FILES)]:
        print(f"  {subdir}/")
        d = SBP_ROOT / subdir
        for filename in files:
            f = d / filename
            if f.exists():
                age = (time.time() - f.stat().st_mtime) / 86400
                print(f"    ✓ {filename:35s} {f.stat().st_size/1024:6.0f} KB  ({age:.0f}d ago)")
            else:
                print(f"    ✗ {filename:35s} not downloaded")

    snap = SBP_ROOT / "page_snapshot.json"
    if snap.exists():
        data = json.loads(snap.read_text())
        print(f"\n  Snapshot: {data.get('timestamp', '?')}")
        print(f"    Policy Rate: {data.get('policy_rate')}%")
        print(f"    KIBOR: {data.get('kibor', {})}")
        print(f"    MTB: {data.get('mtb_cutoffs', {})}")
        print(f"    PIB: {data.get('pib_cutoffs', {})}")


def main():
    parser = argparse.ArgumentParser(description="SBP Rates Downloader")
    sub = parser.add_subparsers(dest="command")

    dl = sub.add_parser("download", help="Download all archives + latest PDFs")
    dl.add_argument("--force", action="store_true", help="Re-download even if exists")

    sub.add_parser("snapshot", help="Scrape current rates from pma.asp")
    sub.add_parser("status", help="Show download status")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if args.command == "download":
        print("Downloading SBP Excel archives...")
        download_archives(force=getattr(args, 'force', False))
        print("\nDownloading latest auction PDFs...")
        download_latest()
        print("\nScraping current rates...")
        snap = scrape_current_rates()
        print(f"  Policy Rate: {snap.get('policy_rate')}%")
        print(f"  KIBOR: {snap.get('kibor')}")
        print(f"  MTB: {snap.get('mtb_cutoffs')}")
        print(f"  PIB: {snap.get('pib_cutoffs')}")
    elif args.command == "snapshot":
        snap = scrape_current_rates()
        print(json.dumps(snap, indent=2))
    elif args.command == "status":
        show_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
```

## Step 2: Download MUFAP CSVs

Use the MUFAP downloader from the previous prompt (`mufap_downloader.py`).
If that prompt hasn't been run yet, create it now — the code is in 
`MUFAP_RATES_DOWNLOADER.md`. Key command:

```bash
# Backfill all years
python -m pakfindata.sources.mufap_downloader backfill --years 2012-2026

# Daily sync
python -m pakfindata.sources.mufap_downloader today
```

## Step 3: Process SBP Excel Archives into Database

Create `src/pakfindata/sources/sbp_rates_processor.py`:

```python
"""
Process downloaded SBP Excel archives into the database.

Reads: /mnt/e/psxdata/sbp_rates/archives/*.xlsx
Writes: sovereign_curve table in psx.sqlite

This runs AFTER sbp_rates_downloader has saved the files to disk.

Usage:
    python -m pakfindata.sources.sbp_rates_processor process
    python -m pakfindata.sources.sbp_rates_processor status
"""

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger("sbp_rates_processor")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

SBP_ROOT = DATA_ROOT / "sbp_rates"
MUFAP_ROOT = DATA_ROOT / "mufap"
DB_PATH = DATA_ROOT / "psx.sqlite"

# Tenor → days mapping
TENOR_DAYS = {
    "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "12M": 365,
    "2Y": 730, "3Y": 1095, "4Y": 1460, "5Y": 1825,
    "6Y": 2190, "7Y": 2555, "8Y": 2920, "9Y": 3285, "10Y": 3650,
    "15Y": 5475, "20Y": 7300, "25Y": 9125, "30Y": 10950,
}


def _get_con():
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def create_table():
    """Create the unified sovereign yield curve table."""
    con = _get_con()
    con.execute("""
        CREATE TABLE IF NOT EXISTS sovereign_curve (
            date       TEXT    NOT NULL,
            source     TEXT    NOT NULL,   -- 'PKRV','PKISRV','PKFRV','MTB','PIB','PIB_FLOAT','KIBOR','POLICY'
            tenor      TEXT    NOT NULL,   -- '1M','3M','6M','12M','2Y','3Y','5Y','10Y','15Y','30Y'
            days       INTEGER NOT NULL,   -- days to maturity
            yield_pct  REAL    NOT NULL,   -- yield in percent (e.g., 11.50)
            bid        REAL,              -- bid rate (KIBOR)
            offer      REAL,             -- offer rate (KIBOR)
            PRIMARY KEY (date, source, tenor)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_sc_date ON sovereign_curve (date)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_sc_source_date ON sovereign_curve (source, date)
    """)
    con.commit()
    con.close()


def process_tbill_archive():
    """Parse tb.xlsx → sovereign_curve rows for MTB cutoffs."""
    xlsx_path = SBP_ROOT / "archives" / "tb.xlsx"
    if not xlsx_path.exists():
        logger.warning("tb.xlsx not found — run sbp_rates_downloader first")
        return 0

    # Read Excel — adapt sheet name and column names based on actual file
    # The file likely has columns like: Date, 3-Month, 6-Month, 12-Month
    # or: Date, 3M, 6M, 12M
    # MUST inspect actual file before finalizing this parser
    
    print(f"  Reading {xlsx_path.name}...", end=" ", flush=True)
    
    try:
        # Try reading — inspect the actual structure
        xls = pd.ExcelFile(xlsx_path)
        print(f"Sheets: {xls.sheet_names}")
        
        df = pd.read_excel(xlsx_path, sheet_name=0)
        print(f"{len(df)} rows, columns: {list(df.columns)}")
        print(f"  Sample:")
        print(df.head(3).to_string())
        
        # ─── ADAPT THIS SECTION based on actual column names ───
        # Map columns to tenors. Common patterns:
        # "3-Month" → "3M", "6-Month" → "6M", "12-Month" → "12M"
        # "3M" → "3M", "6M" → "6M", "1Y" → "12M"
        
        # Find the date column
        date_col = None
        for col in df.columns:
            if "date" in str(col).lower() or df[col].dtype == 'datetime64[ns]':
                date_col = col
                break
        
        if date_col is None:
            # First column is likely dates
            date_col = df.columns[0]
        
        # Map remaining columns to tenors
        tenor_map = {}
        for col in df.columns:
            col_str = str(col).strip().upper()
            if "1" in col_str and ("MONTH" in col_str or "M" == col_str[-1]):
                if "12" in col_str or "1Y" in col_str:
                    tenor_map[col] = "12M"
                elif "1M" in col_str or "1-M" in col_str:
                    tenor_map[col] = "1M"
            elif "3" in col_str and ("MONTH" in col_str or "M" == col_str[-1]):
                tenor_map[col] = "3M"
            elif "6" in col_str and ("MONTH" in col_str or "M" == col_str[-1]):
                tenor_map[col] = "6M"
        
        print(f"  Tenor map: {tenor_map}")
        
        if not tenor_map:
            print("  ⚠ Could not auto-detect tenor columns. Manual mapping needed.")
            print(f"  Columns: {list(df.columns)}")
            return 0
        
        # Insert into DB
        con = _get_con()
        inserted = 0
        
        for _, row in df.iterrows():
            date_val = row[date_col]
            if pd.isna(date_val):
                continue
            date_str = pd.Timestamp(date_val).strftime("%Y-%m-%d")
            
            for col, tenor in tenor_map.items():
                val = row[col]
                if pd.isna(val):
                    continue
                yield_pct = float(val)
                days = TENOR_DAYS.get(tenor, 0)
                
                con.execute("""
                    INSERT OR REPLACE INTO sovereign_curve 
                    (date, source, tenor, days, yield_pct) 
                    VALUES (?, 'MTB', ?, ?, ?)
                """, (date_str, tenor, days, yield_pct))
                inserted += 1
        
        con.commit()
        con.close()
        print(f"  ✓ {inserted} rows inserted")
        return inserted
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return 0


def process_pib_archive():
    """Parse Pakinvestbonds.xlsx → sovereign_curve rows for PIB cutoffs."""
    xlsx_path = SBP_ROOT / "archives" / "Pakinvestbonds.xlsx"
    if not xlsx_path.exists():
        logger.warning("Pakinvestbonds.xlsx not found")
        return 0

    print(f"  Reading {xlsx_path.name}...", end=" ", flush=True)
    
    try:
        xls = pd.ExcelFile(xlsx_path)
        print(f"Sheets: {xls.sheet_names}")
        
        df = pd.read_excel(xlsx_path, sheet_name=0)
        print(f"{len(df)} rows, columns: {list(df.columns)}")
        print(f"  Sample:")
        print(df.head(3).to_string())
        
        # ─── ADAPT based on actual columns ───
        # Expected: Date, 2-Year, 3-Year, 5-Year, 10-Year, 15-Year
        # or: Date, 2Y, 3Y, 5Y, 10Y, 15Y
        
        date_col = None
        for col in df.columns:
            if "date" in str(col).lower() or df[col].dtype == 'datetime64[ns]':
                date_col = col
                break
        if date_col is None:
            date_col = df.columns[0]
        
        tenor_map = {}
        for col in df.columns:
            col_str = str(col).strip().upper()
            for y in [2, 3, 5, 10, 15, 20, 30]:
                if str(y) in col_str and ("YEAR" in col_str or "Y" in col_str):
                    tenor_map[col] = f"{y}Y"
                    break
        
        print(f"  Tenor map: {tenor_map}")
        
        con = _get_con()
        inserted = 0
        
        for _, row in df.iterrows():
            date_val = row[date_col]
            if pd.isna(date_val):
                continue
            date_str = pd.Timestamp(date_val).strftime("%Y-%m-%d")
            
            for col, tenor in tenor_map.items():
                val = row[col]
                if pd.isna(val):
                    continue
                try:
                    yield_pct = float(val)
                except (ValueError, TypeError):
                    continue  # "Bids Rejected" etc
                days = TENOR_DAYS.get(tenor, 0)
                
                con.execute("""
                    INSERT OR REPLACE INTO sovereign_curve 
                    (date, source, tenor, days, yield_pct) 
                    VALUES (?, 'PIB', ?, ?, ?)
                """, (date_str, tenor, days, yield_pct))
                inserted += 1
        
        con.commit()
        con.close()
        print(f"  ✓ {inserted} rows inserted")
        return inserted
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return 0


def process_mufap_csvs(rate_type: str = "pkrv"):
    """Parse downloaded MUFAP CSVs into sovereign_curve table.
    
    Each CSV has one row per date with columns for each tenor.
    """
    import csv, io
    
    source_map = {"pkrv": "PKRV", "pkisrv": "PKISRV", "pkfrv": "PKFRV"}
    source = source_map.get(rate_type, rate_type.upper())
    
    csv_dir = MUFAP_ROOT / rate_type
    if not csv_dir.exists():
        print(f"  {rate_type}: no files downloaded yet")
        return 0
    
    csvs = sorted(csv_dir.rglob("*.csv"))
    if not csvs:
        print(f"  {rate_type}: no CSV files found")
        return 0
    
    print(f"  Processing {len(csvs)} {rate_type.upper()} CSVs...", end=" ", flush=True)
    
    con = _get_con()
    inserted = 0
    errors = 0
    
    for csv_path in csvs:
        try:
            raw = csv_path.read_text(errors="replace")
            reader = csv.reader(io.StringIO(raw))
            rows = list(reader)
            
            if len(rows) < 2:
                continue
            
            header = [h.strip().upper() for h in rows[0]]
            
            # First row is header with tenor labels
            # Map header columns to standard tenor names
            col_tenors = {}
            for i, h in enumerate(header):
                for std_tenor, _ in TENOR_DAYS.items():
                    if std_tenor in h or h == std_tenor:
                        col_tenors[i] = std_tenor
                        break
            
            if not col_tenors:
                errors += 1
                continue
            
            # Extract date from filename: PKRV02012024.csv → 2024-01-02
            import re as _re
            date_match = _re.search(r'(\d{8})', csv_path.stem)
            if date_match:
                ddmmyyyy = date_match.group(1)
                try:
                    dt = datetime.strptime(ddmmyyyy, "%d%m%Y")
                    date_str = dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            else:
                continue
            
            # Parse data rows
            for row in rows[1:]:
                for col_idx, tenor in col_tenors.items():
                    if col_idx >= len(row):
                        continue
                    val = row[col_idx].strip()
                    if not val:
                        continue
                    try:
                        yield_pct = float(val)
                        days = TENOR_DAYS[tenor]
                        con.execute("""
                            INSERT OR REPLACE INTO sovereign_curve
                            (date, source, tenor, days, yield_pct)
                            VALUES (?, ?, ?, ?, ?)
                        """, (date_str, source, tenor, days, yield_pct))
                        inserted += 1
                    except (ValueError, KeyError):
                        continue
            
        except Exception:
            errors += 1
    
    con.commit()
    con.close()
    print(f"✓ {inserted} rows from {len(csvs)} files ({errors} errors)")
    return inserted


def process_snapshot():
    """Load page_snapshot.json into sovereign_curve for today's rates."""
    snap_path = SBP_ROOT / "page_snapshot.json"
    if not snap_path.exists():
        return 0
    
    data = json.loads(snap_path.read_text())
    date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    
    con = _get_con()
    inserted = 0
    
    # Policy rate
    if data.get("policy_rate"):
        con.execute("""
            INSERT OR REPLACE INTO sovereign_curve
            (date, source, tenor, days, yield_pct)
            VALUES (?, 'POLICY', 'O/N', 1, ?)
        """, (date_str, data["policy_rate"]))
        inserted += 1
    
    # KIBOR
    for tenor, rates in data.get("kibor", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        con.execute("""
            INSERT OR REPLACE INTO sovereign_curve
            (date, source, tenor, days, yield_pct, bid, offer)
            VALUES (?, 'KIBOR', ?, ?, ?, ?, ?)
        """, (date_str, tenor, days, rates.get("offer", 0),
              rates.get("bid"), rates.get("offer")))
        inserted += 1
    
    # MTB cutoffs
    for tenor, yield_pct in data.get("mtb_cutoffs", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        con.execute("""
            INSERT OR REPLACE INTO sovereign_curve
            (date, source, tenor, days, yield_pct)
            VALUES (?, 'MTB', ?, ?, ?)
        """, (date_str, tenor, days, yield_pct))
        inserted += 1
    
    # PIB cutoffs
    for tenor, yield_pct in data.get("pib_cutoffs", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        con.execute("""
            INSERT OR REPLACE INTO sovereign_curve
            (date, source, tenor, days, yield_pct)
            VALUES (?, 'PIB', ?, ?, ?)
        """, (date_str, tenor, days, yield_pct))
        inserted += 1
    
    con.commit()
    con.close()
    return inserted


def process_all():
    """Run all processors."""
    create_table()
    
    print("\n── SBP Archives ──")
    process_tbill_archive()
    process_pib_archive()
    
    print("\n── SBP Live Snapshot ──")
    n = process_snapshot()
    print(f"  {n} rates from page snapshot")
    
    print("\n── MUFAP Curves ──")
    for rt in ["pkrv", "pkisrv", "pkfrv"]:
        process_mufap_csvs(rt)
    
    # Summary
    con = _get_con()
    total = con.execute("SELECT COUNT(*) FROM sovereign_curve").fetchone()[0]
    sources = con.execute("SELECT source, COUNT(*) FROM sovereign_curve GROUP BY source").fetchall()
    dates = con.execute("SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM sovereign_curve").fetchone()
    con.close()
    
    print(f"\n── Summary ──")
    print(f"  Total rows: {total:,}")
    print(f"  Date range: {dates[0]} → {dates[1]} ({dates[2]} dates)")
    for src, cnt in sources:
        print(f"    {src:10s}: {cnt:,}")


def main():
    parser = argparse.ArgumentParser(description="SBP/MUFAP Rates Processor")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("process", help="Process all downloaded files into DB")
    sub.add_parser("status", help="Show DB status")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if args.command == "process":
        process_all()
    elif args.command == "status":
        con = _get_con()
        try:
            total = con.execute("SELECT COUNT(*) FROM sovereign_curve").fetchone()[0]
            print(f"sovereign_curve: {total:,} rows")
            for row in con.execute("SELECT source, COUNT(*), MIN(date), MAX(date) FROM sovereign_curve GROUP BY source"):
                print(f"  {row[0]:10s}: {row[1]:>8,} rows  ({row[2]} → {row[3]})")
        except Exception:
            print("sovereign_curve table not found — run 'process' first")
        con.close()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
```

## Step 4: Extend the App — Full Yield Curve

Find the existing Yield Curve page (from Step 0 audit) and extend it to show 
the full 1W–30Y curve using data from the `sovereign_curve` table.

### 4a. Add a curve data loader

Add this function to the yield curve page (or to a shared utility):

```python
def load_sovereign_curve(date_str: str = None, source: str = "PKRV") -> pd.DataFrame:
    """Load yield curve for a given date and source.
    
    If no date, uses latest available.
    Sources: PKRV, PKISRV, MTB, PIB, KIBOR, POLICY
    """
    con = sqlite3.connect(str(Path("/mnt/e/psxdata/psx.sqlite")))
    
    if date_str is None:
        date_str = con.execute(
            "SELECT MAX(date) FROM sovereign_curve WHERE source = ?", (source,)
        ).fetchone()[0]
    
    df = pd.read_sql_query("""
        SELECT date, source, tenor, days, yield_pct, bid, offer
        FROM sovereign_curve
        WHERE date = ? AND source = ?
        ORDER BY days
    """, con, params=[date_str, source])
    
    con.close()
    return df


def load_combined_curve(date_str: str = None) -> pd.DataFrame:
    """Load all sources for one date — PKRV + MTB + PIB + KIBOR overlaid."""
    con = sqlite3.connect(str(Path("/mnt/e/psxdata/psx.sqlite")))
    
    if date_str is None:
        date_str = con.execute(
            "SELECT MAX(date) FROM sovereign_curve"
        ).fetchone()[0]
    
    df = pd.read_sql_query("""
        SELECT date, source, tenor, days, yield_pct, bid, offer
        FROM sovereign_curve
        WHERE date = ?
        ORDER BY source, days
    """, con, params=[date_str])
    
    con.close()
    return df


def load_curve_history(tenor: str, source: str = "PKRV", 
                       days: int = 365) -> pd.DataFrame:
    """Load time series for a specific tenor."""
    con = sqlite3.connect(str(Path("/mnt/e/psxdata/psx.sqlite")))
    
    df = pd.read_sql_query("""
        SELECT date, yield_pct
        FROM sovereign_curve
        WHERE source = ? AND tenor = ?
        ORDER BY date DESC
        LIMIT ?
    """, con, params=[source, tenor, days])
    
    con.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")
```

### 4b. Yield Curve Chart — extend to 30Y

Find the existing yield curve chart in the page. It currently plots up to 1Y.
Replace the chart section with a combined view:

```python
def _render_yield_curve_chart(date_str: str):
    """Render full 1W–30Y yield curve with multiple sources."""
    import plotly.graph_objects as go
    
    combined = load_combined_curve(date_str)
    if combined.empty:
        st.info("No curve data for this date")
        return
    
    fig = go.Figure()
    
    colors = {
        "PKRV": "#FFB300",      # gold — interpolated conventional
        "PKISRV": "#00BCD4",    # cyan — interpolated Islamic
        "MTB": "#2196F3",       # blue — T-Bill auction cutoffs
        "PIB": "#FF5252",       # red — PIB auction cutoffs
        "KIBOR": "#BB86FC",     # purple — interbank
        "POLICY": "#00E676",    # green — SBP policy rate
    }
    
    for source in ["PKRV", "PKISRV", "MTB", "PIB", "KIBOR", "POLICY"]:
        subset = combined[combined["source"] == source].sort_values("days")
        if subset.empty:
            continue
        
        mode = "lines+markers" if source in ("MTB", "PIB", "KIBOR", "POLICY") else "lines"
        marker_size = 8 if source in ("MTB", "PIB") else 4
        
        fig.add_trace(go.Scatter(
            x=subset["days"],
            y=subset["yield_pct"],
            mode=mode,
            name=source,
            line=dict(color=colors.get(source, "#888"), 
                      width=2 if source == "PKRV" else 1),
            marker=dict(size=marker_size),
            hovertemplate=f"{source}<br>Tenor: %{{customdata}}<br>"
                          f"Yield: %{{y:.4f}}%<br>Days: %{{x}}<extra></extra>",
            customdata=subset["tenor"],
        ))
    
    # Custom x-axis tick labels at standard tenors
    tick_vals = [7, 14, 30, 91, 182, 365, 730, 1095, 1825, 3650, 5475, 7300, 10950]
    tick_text = ["1W", "2W", "1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y", "15Y", "20Y", "30Y"]
    
    fig.update_layout(
        title=f"Pakistan Sovereign Yield Curve — {date_str}",
        xaxis=dict(
            title="Tenor",
            type="log",  # log scale spreads short end
            tickvals=tick_vals,
            ticktext=tick_text,
            gridcolor="#1E2530",
        ),
        yaxis=dict(title="Yield (%)", gridcolor="#1E2530"),
        paper_bgcolor="#0B0E11",
        plot_bgcolor="#0B0E11",
        font_color="#E0E0E0",
        height=450,
        legend=dict(orientation="h", y=1.1),
    )
    
    st.plotly_chart(fig, use_container_width=True, key="full_yield_curve")
    
    # Summary metrics
    pkrv = combined[combined["source"] == "PKRV"].sort_values("days")
    if not pkrv.empty:
        short = pkrv[pkrv["days"] <= 365]["yield_pct"].mean()
        medium = pkrv[(pkrv["days"] > 365) & (pkrv["days"] <= 1825)]["yield_pct"].mean()
        long_term = pkrv[pkrv["days"] > 1825]["yield_pct"].mean()
        spread = long_term - short if short and long_term else 0
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Short (<1Y)", f"{short:.2f}%")
        c2.metric("Medium (1-5Y)", f"{medium:.2f}%")
        c3.metric("Long (>5Y)", f"{long_term:.2f}%")
        c4.metric("Spread (bps)", f"{spread * 100:.0f}", 
                  delta_color="inverse" if spread < 0 else "normal")
```

### 4c. Integration into existing pages

Based on Step 0 findings, add/extend these:

**Yield Curve page:** Replace the current 1M–12M chart with `_render_yield_curve_chart()`.
Add curve comparison (overlay two dates).

**Treasury Terminal:** Add full curve in Yield Curves tab. Add PIB cutoff history in Auctions tab.

**Bond Market:** Overlay PKRV curve on the sovereign yield chart alongside MTB/PIB dots.

**Macro HMM engine:** Can now use 2Y–10Y spread as a regime signal input.

## Step 5: Daily Sync

Add to `scraper_service.py` or create a schedule entry:

```python
# ── 18:00 PKT — after market close ──

# 1. Download today's MUFAP CSVs
from pakfindata.sources.mufap_downloader import download_today
download_today()

# 2. Scrape SBP current rates
from pakfindata.sources.sbp_rates_downloader import scrape_current_rates
scrape_current_rates()

# 3. Process into DB
from pakfindata.sources.sbp_rates_processor import process_snapshot, process_mufap_csvs
process_snapshot()
for rt in ["pkrv", "pkisrv", "pkfrv"]:
    process_mufap_csvs(rt)

# 4. Re-download SBP archives weekly (they get updated with new auctions)
# Run on Sundays only:
from datetime import datetime, timezone, timedelta
if datetime.now(timezone(timedelta(hours=5))).weekday() == 6:
    from pakfindata.sources.sbp_rates_downloader import download_archives
    download_archives(force=True)
```

## Step 6: Test End-to-End

```bash
cd ~/pakfindata && conda activate psx

# 1. Download SBP archives
python -m pakfindata.sources.sbp_rates_downloader download

# 2. Download MUFAP CSVs (start with one year to test)
python -m pakfindata.sources.mufap_downloader backfill --years 2024

# 3. Process everything
python -m pakfindata.sources.sbp_rates_processor process

# 4. Verify data
sqlite3 /mnt/e/psxdata/psx.sqlite -header "
    SELECT source, tenor, days, yield_pct 
    FROM sovereign_curve 
    WHERE date = (SELECT MAX(date) FROM sovereign_curve WHERE source='PKRV')
    AND source = 'PKRV'
    ORDER BY days;
"

# 5. Check full curve for a date
sqlite3 /mnt/e/psxdata/psx.sqlite -header "
    SELECT source, tenor, days, yield_pct
    FROM sovereign_curve
    WHERE date = (SELECT MAX(date) FROM sovereign_curve)
    ORDER BY source, days;
"

# 6. Start Streamlit and test
streamlit run src/pakfindata/ui/app.py --server.port 8501
# Navigate to Yield Curve / Treasury Terminal — should show full 1W–30Y curve
```

## IMPORTANT NOTES

1. **SBP Excel archives are DIRECT DOWNLOADS** — no AJAX, no scraping, no auth. 
   Just `requests.get(url)`. This is the most reliable data source.

2. **Process after download.** The processor reads from disk, not from URLs. 
   If a CSV has a weird format, fix the parser — don't re-download.

3. **`sovereign_curve` table uses tall format** — one row per (date, source, tenor). 
   This makes it easy to query specific tenors, compare sources, and build charts.
   PK is `(date, source, tenor)` — INSERT OR REPLACE handles re-processing.

4. **The Excel parsers need adaptation.** Step 3 reads the actual file, prints 
   columns, and tries to auto-map. If auto-mapping fails, you'll see the actual 
   column names and can fix the mapping manually.

5. **PKRV gives all 21 tenors daily.** SBP archives give only auction dates 
   (irregular). Combined, you get: PKRV for daily smooth curve + MTB/PIB for 
   auction cutoff benchmarks.

6. **The chart uses log x-axis.** This spreads the short end (1W–12M) which is 
   where most of the action happens, while keeping 2Y–30Y visible.

7. **Existing pages are extended, not replaced.** The yield curve chart function 
   is additive — it reads from `sovereign_curve` and renders alongside existing data.

8. **SBP archives refresh weekly.** New auctions get added to tb.xlsx and 
   Pakinvestbonds.xlsx. Re-download on Sundays.

9. **MUFAP backfill is one-time (~1 hour).** After that, daily sync is 3 CSV 
   downloads (~1 second).

10. **The regex scraper for pma.asp is fragile.** SBP's HTML is table-based 
    with no semantic classes. If they redesign the page, the regex breaks. 
    The Excel archives are the stable, reliable source.
