"""PSX IPO / listing status scraper.

Source: dps.psx.com.pk/listings — requires JS rendering.
The AJAX endpoint /listings-table/{board}/{type} may return 500 errors.
This scraper provides a best-effort approach with graceful fallback.
"""

import re
import sqlite3

import requests
from bs4 import BeautifulSoup

from psx_ohlcv.db.repositories.ipo import init_ipo_schema, upsert_ipo_listing

__all__ = ["IPOScraper"]

DPS_BASE = "https://dps.psx.com.pk"
TIMEOUT = 30


class IPOScraper:
    """Scrapes IPO/listing data from PSX DPS portal."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*; q=0.01",
        })

    def scrape_listings(self, board: str = "main") -> list[dict]:
        """Scrape listings table from DPS portal.

        Args:
            board: 'main' or 'gem'.

        Returns:
            List of listing dicts, or empty list on failure.
        """
        # First visit main page for cookies
        try:
            self.session.get(f"{DPS_BASE}/listings", timeout=TIMEOUT)
        except Exception:
            pass

        listings: list[dict] = []
        for typ in ["stocks"]:
            url = f"{DPS_BASE}/listings-table/{board}/{typ}"
            try:
                resp = self.session.get(
                    url, timeout=TIMEOUT,
                    headers={"Referer": f"{DPS_BASE}/listings"},
                )
                if resp.status_code != 200:
                    print(f"  [!] {url} returned {resp.status_code}")
                    continue

                rows = self._parse_table(resp.text, board)
                listings.extend(rows)
            except Exception as e:
                print(f"  [!] Failed to fetch {url}: {e}")

        return listings

    def _parse_table(self, html: str, board: str) -> list[dict]:
        """Parse listing table HTML into dicts."""
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict] = []

        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Try to extract symbol from link
            link = row.find("a")
            symbol = None
            if link:
                href = link.get("href", "")
                # /company/SYMBOL pattern
                m = re.search(r"/company/([A-Z0-9]+)", href)
                if m:
                    symbol = m.group(1)
                if not symbol:
                    symbol = link.get_text(strip=True)

            if not symbol:
                symbol = cells[0].get_text(strip=True)

            if not symbol or len(symbol) > 20:
                continue

            company_name = cells[1].get_text(strip=True) if len(cells) > 1 else None

            results.append({
                "symbol": symbol.upper(),
                "company_name": company_name,
                "board": board,
                "status": "listed",
            })

        return results

    def sync_listings(self, con: sqlite3.Connection) -> dict:
        """Scrape and upsert IPO listings."""
        init_ipo_schema(con)
        counts = {"ok": 0, "failed": 0, "total": 0}

        for board in ["main", "gem"]:
            listings = self.scrape_listings(board)
            counts["total"] += len(listings)

            for listing in listings:
                if upsert_ipo_listing(con, listing):
                    counts["ok"] += 1
                else:
                    counts["failed"] += 1

        return counts
