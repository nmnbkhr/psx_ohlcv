"""SBP DFMD Rates Downloader — direct Excel archive + PDF downloads.

Downloads from sbp.org.pk/ecodata/ and sbp.org.pk/dfmd/ — no scraping needed.

Output: /mnt/e/psxdata/sbp_rates/
  archives/   — Excel files with full T-Bill + PIB auction history
  latest/     — Current auction result PDFs
  page_snapshot.json — Scraped current rates from pma.asp

Usage:
    python -m pakfindata.sources.sbp_rates_downloader download
    python -m pakfindata.sources.sbp_rates_downloader snapshot
    python -m pakfindata.sources.sbp_rates_downloader status
"""

from __future__ import annotations

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

# ═══════════════════════════════════════════════════════════════════════════════
# DIRECT DOWNLOAD URLs
# ═══════════════════════════════════════════════════════════════════════════════

ARCHIVE_FILES = {
    "tb.xlsx":                      "https://www.sbp.org.pk/ecodata/tb.xlsx",
    "Pakinvestbonds.xlsx":          "https://www.sbp.org.pk/ecodata/Pakinvestbonds.xlsx",
    "PIB-Float-Arch-SA.xlsx":       "https://www.sbp.org.pk/ecodata/PIB-Float-Arch-SA.xlsx",
    "PIB-Float-Arch-Q.xlsx":        "https://www.sbp.org.pk/ecodata/PIB-Float-Arch-Q.xlsx",
    "BuyBack-Auction-Summary.xlsx": "https://www.sbp.org.pk/dfmd/BuyBack-Auction-Summary.xlsx",
    "gop-ijara-summary.pdf":        "https://www.sbp.org.pk/ecodata/gop-ijara-summary.pdf",
}

LATEST_FILES = {
    "auction-tbills.pdf":           "https://www.sbp.org.pk/ecodata/auction-tbills.pdf",
    "Auction-Investment.pdf":       "https://www.sbp.org.pk/ecodata/Auction-Investment.pdf",
    "MTB-BID.pdf":                  "https://www.sbp.org.pk/ecodata/MTB-BID.pdf",
    "pib-bid.pdf":                  "https://www.sbp.org.pk/ecodata/pib-bid.pdf",
    "auction-treasurybills.pdf":    "https://www.sbp.org.pk/ecodata/auction-treasurybills.pdf",
    "Auction-Bond.pdf":             "https://www.sbp.org.pk/ecodata/Auction-Bond.pdf",
}

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
                print(f"  SKIP {filename} (downloaded {age_days:.0f}d ago)")
                continue

        print(f"  DL {filename}...", end=" ", flush=True)
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 1000:
                out.write_bytes(resp.content)
                print(f"OK ({len(resp.content) / 1024:.0f} KB)")
            else:
                print(f"FAIL (HTTP {resp.status_code})")
        except Exception as e:
            print(f"FAIL ({e})")
        time.sleep(0.5)


def download_latest():
    """Download latest auction PDFs from SBP."""
    out_dir = SBP_ROOT / "latest"
    out_dir.mkdir(parents=True, exist_ok=True)

    for filename, url in LATEST_FILES.items():
        out = out_dir / filename
        print(f"  DL {filename}...", end=" ", flush=True)
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 500:
                out.write_bytes(resp.content)
                print(f"OK ({len(resp.content) / 1024:.0f} KB)")
            else:
                print(f"FAIL (HTTP {resp.status_code})")
        except Exception as e:
            print(f"FAIL ({e})")
        time.sleep(0.5)


def scrape_current_rates() -> dict:
    """Scrape pma.asp for current live rates. Saves to page_snapshot.json."""
    url = "https://www.sbp.org.pk/dfmd/pma.asp"
    snapshot: dict = {
        "timestamp": datetime.now(PKT).isoformat(),
        "source": url,
        "policy_rate": None,
        "overnight_ceiling": None,
        "overnight_floor": None,
        "overnight_repo": None,
        "kibor": {},
        "mtb_cutoffs": {},
        "pib_cutoffs": {},
    }

    try:
        resp = SESSION.get(url, timeout=20)
        if resp.status_code != 200:
            logger.warning("SBP pma.asp returned %d", resp.status_code)
            return snapshot

        html = resp.text

        m = re.search(r"Policy.*?Rate.*?(\d+\.\d+)%", html, re.DOTALL | re.IGNORECASE)
        if m:
            snapshot["policy_rate"] = float(m.group(1))

        kibor_pattern = r"(\d+)-M.*?(\d+\.\d+).*?(\d+\.\d+)"
        for m in re.finditer(kibor_pattern, html):
            tenor = f"{m.group(1)}M"
            snapshot["kibor"][tenor] = {
                "bid": float(m.group(2)),
                "offer": float(m.group(3)),
            }

        mtb_section = re.search(r"MTBs.*?Fixed-rate PIB", html, re.DOTALL | re.IGNORECASE)
        if mtb_section:
            for m in re.finditer(r"(\d+)-M.*?(\d+\.\d+)%", mtb_section.group()):
                snapshot["mtb_cutoffs"][f"{m.group(1)}M"] = float(m.group(2))

        pib_section = re.search(r"Fixed-rate PIB.*?Floating", html, re.DOTALL | re.IGNORECASE)
        if pib_section:
            for m in re.finditer(r"(\d+)-Y.*?(\d+\.\d+)%", pib_section.group()):
                snapshot["pib_cutoffs"][f"{m.group(1)}Y"] = float(m.group(2))

    except Exception as e:
        logger.error("Failed to scrape pma.asp: %s", e)

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
                print(f"    OK {filename:35s} {f.stat().st_size / 1024:6.0f} KB  ({age:.0f}d ago)")
            else:
                print(f"    -- {filename:35s} not downloaded")

    snap = SBP_ROOT / "page_snapshot.json"
    if snap.exists():
        data = json.loads(snap.read_text())
        print(f"\n  Snapshot: {data.get('timestamp', '?')}")
        print(f"    Policy Rate: {data.get('policy_rate')}%")
        print(f"    KIBOR: {data.get('kibor', {})}")
        print(f"    MTB: {data.get('mtb_cutoffs', {})}")
        print(f"    PIB: {data.get('pib_cutoffs', {})}")


if __name__ == "__main__":
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
        download_archives(force=getattr(args, "force", False))
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
