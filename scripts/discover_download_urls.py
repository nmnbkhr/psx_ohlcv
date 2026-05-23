"""
Discover actual download URLs from dps.psx.com.pk/downloads.

No browser needed! The DPS site has two AJAX endpoints:
  POST /daily-downloads  {date: "YYYY-MM-DD"}  → HTML fragment with links
  GET  /other-downloads                         → HTML fragment with links

Run this to refresh/verify URLs anytime.
"""

import urllib.request
import re
import json
from datetime import datetime, timedelta, timezone

BASE = "https://dps.psx.com.pk"
PKT = timezone(timedelta(hours=5))


def discover_daily(date_str: str = None) -> dict:
    """Discover daily download URLs via POST."""
    if not date_str:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")

    data = f"date={date_str}".encode("utf-8")
    req = urllib.request.Request(
        f"{BASE}/daily-downloads",
        data=data,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE}/downloads",
        },
    )
    resp = urllib.request.urlopen(req, timeout=15)
    html = resp.read().decode("utf-8", errors="replace")

    results = {}
    items = re.findall(r"<li>(.*?)</li>", html, re.DOTALL)
    for li in items:
        text = re.sub(r"<[^>]+>", "", li).strip()
        text = re.sub(r"\s+", " ", text)
        hrefs = re.findall(r'href="(/download/[^"]+)"', li)
        for href in hrefs:
            ext = href.split(".")[-1].upper()
            key = f"{text} [{ext}]" if len(hrefs) > 1 else text
            results[key] = f"{BASE}{href}"

    return results


def discover_other() -> dict:
    """Discover reference (Other Downloads) URLs via GET."""
    req = urllib.request.Request(
        f"{BASE}/other-downloads",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp = urllib.request.urlopen(req, timeout=15)
    html = resp.read().decode("utf-8", errors="replace")

    results = {}
    items = re.findall(r"<li>(.*?)</li>", html, re.DOTALL)
    for li in items:
        text = re.sub(r"<[^>]+>", "", li).strip()
        text = re.sub(r"\s+", " ", text)
        hrefs = re.findall(r'href="(/download/[^"]+)"', li)
        for href in hrefs:
            ext = href.split(".")[-1].upper()
            key = f"{text} [{ext}]" if len(hrefs) > 1 else text
            results[key] = f"{BASE}{href}"

    return results


if __name__ == "__main__":
    print("=== DAILY DOWNLOADS ===")
    daily = discover_daily()
    for name, url in daily.items():
        print(f"  {name}")
        print(f"    {url}")
    print(f"\n  Total: {len(daily)}")

    print("\n=== OTHER DOWNLOADS ===")
    other = discover_other()
    for name, url in other.items():
        print(f"  {name}")
        print(f"    {url}")
    print(f"\n  Total: {len(other)}")

    # Save
    out = {"daily": daily, "other": other}
    with open("download_urls_discovered.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to download_urls_discovered.json")
