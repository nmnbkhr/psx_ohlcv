"""
PSX DPS Downloads — Gap files only.

Downloads files NOT already covered by existing pakfindata scrapers:
  - market_summary.py (mkt_summary, post_close)
  - closing_rates_pdf.py (closing_rates)
  - listed_companies.py (listed_cmp)

Discovery method:
  POST https://dps.psx.com.pk/daily-downloads  {date: "YYYY-MM-DD"}
  GET  https://dps.psx.com.pk/other-downloads

Stores in: /mnt/e/psxdata/downloads/
  daily/{date}/  — date-filtered files
  reference/     — static one-time files

Usage:
  python -m pakfindata.sources.psx_downloads today
  python -m pakfindata.sources.psx_downloads daily 2026-03-18
  python -m pakfindata.sources.psx_downloads backfill 2026-03-01 2026-03-18
  python -m pakfindata.sources.psx_downloads reference
  python -m pakfindata.sources.psx_downloads status
"""

import argparse
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

BASE = "https://dps.psx.com.pk"
OUTPUT = Path("/mnt/e/psxdata/downloads")
PKT = timezone(timedelta(hours=5))
RATE_LIMIT = 1.0  # seconds between downloads

HEADERS = {
    "User-Agent": "pakfindata/1.0 (research)",
    "Referer": f"{BASE}/downloads",
}

# ═══════════════════════════════════════════════════════
# GAP FILES — verified URLs from dps.psx.com.pk
# ═══════════════════════════════════════════════════════

# Daily files: url template with {date} placeholder, output subfolder, filename template
DAILY_GAP_FILES = {
    # Priority 1 — high analytics value
    "symbol_price":  ("/download/symbol_price/{date}.zip",   "limits",       "symbol_price_{date}.zip"),
    "omts":          ("/download/omts/{date}.csv",           "off_market",   "off_market_summary_{date}.csv"),
    "fut_opn_int":   ("/download/fut_opn_int/{date}.xls",   "futures",      "futures_oi_dfc_{date}.xls"),
    "csf_opn_int":   ("/download/csf_opn_int/{date}.xls",   "futures",      "futures_oi_csf_{date}.xls"),
    "sif_opn_int":   ("/download/sif_opn_int/{date}.csv",   "sif",          "sif_open_interest_{date}.csv"),
    "var_margin":    ("/download/var_margin/{date}.zip",     "margins",      "var_margins_{date}.zip"),
    "indhist":       ("/download/indhist/{date}.xls",        "indices",      "constituent_data_{date}.xls"),
    # Priority 2 — supplementary
    "symbol_name":   ("/download/symbol_name/{date}.zip",    "reference",    "symbol_names_{date}.zip"),
    "reval_rates":   ("/download/reval_rates_gis/{date}.csv","gis",          "gis_reval_rates_{date}.csv"),
    "dvf_trade":     ("/download/dvf_trade/{date}.csv",      "bai_muajjal",  "bai_muajjal_{date}.csv"),
    "dfc_nbs":       ("/download/dfc_nbs/{date}.csv",        "futures",      "dfc_net_blank_sale_{date}.csv"),
    "pos_limit_fut": ("/download/pos_limit_fut/{date}.xls",  "futures",      "position_limits_{date}.xls"),
}

# Reference (static) files — one-time download
REFERENCE_FILES = {
    "kse_index":      ("/download/text/kse_index.lis.Z",           "kse_index_fluctuation.Z"),
    "allshr_new":     ("/download/text/allshr_new.lis.Z",          "allshare_mktcap.Z"),
    "kse100":         ("/download/text/kse100.lis.Z",              "kse100_companies.Z"),
    "lot_size":       ("/download/lot_size/Symbol_LotSize.zip",    "symbol_lot_size.zip"),
    "header":         ("/download/text/header.zip",                "psx_header.zip"),
    "header2":        ("/download/text/header2.zip",               "psx_header_tradable.zip"),
    "announce":       ("/download/text/announce.lis.Z",            "announcements.Z"),
    "announce_sym":   ("/download/text/announceWithSymbol.lis.Z",  "announcements_with_symbols.Z"),
    "top30dec":       ("/download/text/top30dec11.lis.Z",          "top30_pct_decreased.Z"),
    "top30inc":       ("/download/text/top30inc11.lis.Z",          "top30_pct_increased.Z"),
    "top30vol":       ("/download/text/top30vol11.lis.Z",          "top30_by_volume.Z"),
    "zerovolume":     ("/download/text/zerovolume.lis.Z",          "zero_volume.Z"),
    "readme":         ("/download/text/readme.zip",                "market_data_format_docs.zip"),
    "HBLTTI":         ("/download/historical/HBLTTI.csv",          "hbltt_index.csv"),
}


# ═══════════════════════════════════════════════════════
# DOWNLOAD
# ═══════════════════════════════════════════════════════

def download(url: str, dest: Path) -> bool:
    """Download file, skip if already exists with non-trivial size."""
    if dest.exists() and dest.stat().st_size > 100:
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=60)

        if resp.status != 200:
            print(f"  X HTTP {resp.status}: {dest.name}")
            return False

        ct = resp.headers.get("Content-Type", "")
        data = resp.read()

        # Skip HTML error pages masquerading as files
        if "text/html" in ct and not url.endswith((".csv", ".html")):
            if b"404" in data[:500] or b"not found" in data[:500].lower():
                print(f"  X 404 page: {dest.name}")
                return False

        with open(dest, "wb") as f:
            f.write(data)

        size_kb = len(data) / 1024
        print(f"  + {dest.name} ({size_kb:.0f} KB)")
        return True

    except urllib.error.HTTPError as e:
        print(f"  X HTTP {e.code}: {dest.name}")
        return False
    except Exception as e:
        print(f"  X {dest.name}: {e}")
        if dest.exists():
            dest.unlink()
        return False


# ═══════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════

def cmd_daily(date_str: str):
    """Download gap files for a specific date."""
    print(f"\n=== GAP DOWNLOADS -- {date_str} ===")

    count = 0
    errors = 0
    skipped = 0
    for name, (url_tpl, category, fname_tpl) in DAILY_GAP_FILES.items():
        url = f"{BASE}{url_tpl.replace('{date}', date_str)}"
        fname = fname_tpl.replace("{date}", date_str)
        dest = OUTPUT / "daily" / date_str / category / fname
        result = download(url, dest)
        if result:
            count += 1
        elif dest.exists():
            skipped += 1
        else:
            errors += 1
        time.sleep(RATE_LIMIT)

    print(f"\n  Downloaded: {count} | Skipped: {skipped} | Errors: {errors}")


def cmd_reference():
    """Download static reference files (one-time)."""
    print("\n=== REFERENCE DOWNLOADS ===")

    count = 0
    for name, (url_path, fname) in REFERENCE_FILES.items():
        url = f"{BASE}{url_path}"
        dest = OUTPUT / "reference" / fname
        if download(url, dest):
            count += 1
        time.sleep(RATE_LIMIT)

    print(f"\n  Downloaded: {count}")


def cmd_backfill(start: str, end: str):
    """Download gap files for a date range (weekdays only)."""
    print(f"\n=== BACKFILL -- {start} to {end} ===")

    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    days = 0

    while current <= end_dt:
        if current.weekday() < 5:  # skip weekends
            cmd_daily(current.strftime("%Y-%m-%d"))
            days += 1
        current += timedelta(days=1)

    print(f"\n=== BACKFILL COMPLETE -- {days} trading days ===")


def cmd_today():
    """Download today's gap files."""
    today = datetime.now(PKT).strftime("%Y-%m-%d")
    cmd_daily(today)


def cmd_status():
    """Show what's been downloaded."""
    print("\n=== DOWNLOAD STATUS ===")

    if not OUTPUT.exists():
        print("  No downloads yet")
        return

    for subdir in ["daily", "reference"]:
        p = OUTPUT / subdir
        if not p.exists():
            continue
        files = [f for f in p.rglob("*") if f.is_file()]
        size = sum(f.stat().st_size for f in files)
        print(f"\n  {subdir}/")
        print(f"    Files: {len(files)}")
        print(f"    Size:  {size / 1024 / 1024:.1f} MB")

        if subdir == "daily":
            dates = sorted(set(
                f.parent.parent.name for f in files
                if f.parent.parent.name != subdir
            ))
            if dates:
                print(f"    Dates: {dates[0]} -> {dates[-1]} ({len(dates)} days)")
        elif subdir == "reference":
            for f in sorted(files):
                print(f"      {f.name} ({f.stat().st_size / 1024:.0f} KB)")

    total = sum(f.stat().st_size for f in OUTPUT.rglob("*") if f.is_file())
    print(f"\n  Total: {total / 1024 / 1024:.1f} MB")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PSX Downloads -- Gap Files")
    parser.add_argument("command", choices=["today", "daily", "backfill", "reference", "status"])
    parser.add_argument("args", nargs="*")

    args = parser.parse_args()

    if args.command == "today":
        cmd_today()
    elif args.command == "daily":
        date = args.args[0] if args.args else datetime.now(PKT).strftime("%Y-%m-%d")
        cmd_daily(date)
    elif args.command == "backfill":
        if len(args.args) < 2:
            print("Usage: python -m pakfindata.sources.psx_downloads backfill START END")
            sys.exit(1)
        cmd_backfill(args.args[0], args.args[1])
    elif args.command == "reference":
        cmd_reference()
    elif args.command == "status":
        cmd_status()
