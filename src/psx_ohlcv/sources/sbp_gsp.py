"""GSP (Government Securities Portal) scraper for PIB and GIS auction data.

Primary: gsp.sbp.org.pk (often unreachable from outside Pakistan)
Fallback: sbp.org.pk/dfmd/pma.asp (PMA page with latest rates)

PIB Fixed Rate data is also scraped by SBPTreasuryScraper (sbp_treasury.py).
This module adds GIS (Government Ijara Sukuk) scraping from the PMA page,
and can extend to GSP portal scraping when it becomes accessible.
"""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from psx_ohlcv.db.repositories.treasury import (
    get_gis_auctions,
    init_treasury_schema,
    upsert_gis_auction,
)

__all__ = ["GSPScraper"]

GSP_URL = "https://gsp.sbp.org.pk"
PMA_URL = "https://www.sbp.org.pk/dfmd/pma.asp"
TIMEOUT = 15


class GSPScraper:
    """Scrapes PIB and GIS auction data from SBP Government Securities Portal.

    Falls back to PMA page when GSP is unavailable.
    """

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_gis_auctions(self) -> list[dict]:
        """Scrape Govt Ijara Sukuk auction data.

        Tries GSP portal first, falls back to PMA page.
        Returns list of {auction_date, gis_type, tenor, cutoff_rental_rate, ...}
        """
        # Try GSP portal first
        gis_data = self._try_gsp()
        if gis_data:
            return gis_data

        # Fallback: scrape GIS from PMA page
        return self._scrape_gis_from_pma()

    def sync_gis(self, con: sqlite3.Connection) -> dict:
        """Scrape and upsert GIS auction data.

        Returns {ok, failed, source}.
        """
        init_treasury_schema(con)
        gis_records = self.scrape_gis_auctions()

        counts = {"ok": 0, "failed": 0, "source": "pma", "total": len(gis_records)}

        for record in gis_records:
            if upsert_gis_auction(con, record):
                counts["ok"] += 1
            else:
                counts["failed"] += 1

        return counts

    def get_gis_summary(self, con: sqlite3.Connection) -> dict:
        """Get summary of stored GIS data."""
        init_treasury_schema(con)
        df = get_gis_auctions(con)
        return {
            "total_records": len(df),
            "gis_types": df["gis_type"].nunique() if not df.empty else 0,
        }

    # ── internal ──────────────────────────────────────────────────

    def _try_gsp(self) -> list[dict] | None:
        """Try to scrape from GSP portal. Returns None if unreachable."""
        try:
            resp = self.session.get(GSP_URL, timeout=TIMEOUT)
            if resp.status_code != 200:
                return None
            # GSP portal parsing would go here when portal becomes accessible
            # For now, return None to trigger PMA fallback
            return None
        except Exception:
            return None

    def _scrape_gis_from_pma(self) -> list[dict]:
        """Extract GIS data from the PMA page.

        The PMA page has GIS Fixed Rate Return and Variable Rate Return
        sections with tenor/price data.
        """
        try:
            resp = self.session.get(PMA_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch PMA page for GIS: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()
        records: list[dict] = []

        # Find GIS FRR (Fixed Rate Return) section
        # Page format: "GIS FRR Tenor Cut-off Rental Rate/Price 3-Y 100.2842 5-Y 100.0022"
        gis_frr = re.search(
            r"GIS\s+FRR|GIS\s*[-–]?\s*Fixed\s*Rate\s*Return",
            text, re.IGNORECASE,
        )
        if gis_frr:
            section = text[gis_frr.end():gis_frr.end() + 400]
            records.extend(
                self._extract_gis_tenors(section, "GIS Fixed Rate Return")
            )

        # Find GIS VRR (Variable Rate Return) section
        # Page format: "GIS VRR Tenor Cut-off Margin/Price 3-Y 99.0800 5-Y 98.7600"
        gis_vrr = re.search(
            r"GIS\s+VRR|GIS\s*[-–]?\s*Variable\s*Rate\s*Return",
            text, re.IGNORECASE,
        )
        if gis_vrr:
            section = text[gis_vrr.end():gis_vrr.end() + 400]
            records.extend(
                self._extract_gis_tenors(section, "GIS Variable Rate Return")
            )

        # Extract GIS auction date — look for "as on DD-Mon-YYYY" near GIS section
        auction_date = self._extract_gis_date(text, gis_frr or gis_vrr)
        for record in records:
            record["auction_date"] = auction_date

        return records

    @staticmethod
    def _extract_gis_tenors(section: str, gis_type: str) -> list[dict]:
        """Extract tenor/rate pairs from a GIS section."""
        results: list[dict] = []
        # Look for patterns like "3-Y  100.2842" or "5-Year  98.76"
        tenor_patterns = {
            r"3[-\s]?[Yy](?:ear)?": "3Y",
            r"5[-\s]?[Yy](?:ear)?": "5Y",
            r"7[-\s]?[Yy](?:ear)?": "7Y",
            r"10[-\s]?[Yy](?:ear)?": "10Y",
        }

        for pattern, canonical_tenor in tenor_patterns.items():
            match = re.search(
                pattern + r"\s+(\d+\.?\d*)\s*%?",
                section,
            )
            if match:
                try:
                    rate = float(match.group(1))
                    if rate > 0:
                        results.append({
                            "gis_type": f"{gis_type} {canonical_tenor}",
                            "tenor": canonical_tenor,
                            "cutoff_rental_rate": rate,
                        })
                except ValueError:
                    pass

        return results

    @staticmethod
    def _extract_gis_date(text: str, gis_match: re.Match | None = None) -> str:
        """Extract GIS auction date from page text.

        GIS auctions happen less frequently than T-Bill auctions.
        The PMA page shows "as on DD-Mon-YYYY" after the GIS rate data.
        """
        if gis_match:
            # Look for "as on <date>" in a window after the GIS section
            window = text[gis_match.start():gis_match.start() + 600]
            as_on = re.search(
                r"as\s+on\s+(\d{1,2}[-/]\w{3}[-/]\d{4})",
                window, re.IGNORECASE,
            )
            if as_on:
                parsed = _try_parse_date(as_on.group(1))
                if parsed:
                    return parsed

            # Also try "Month DD, YYYY" format
            as_on2 = re.search(
                r"as\s+on\s+(\w+\s+\d{1,2},?\s*\d{4})",
                window, re.IGNORECASE,
            )
            if as_on2:
                parsed = _try_parse_date(as_on2.group(1))
                if parsed:
                    return parsed

        return datetime.now().strftime("%Y-%m-%d")


def _try_parse_date(text: str) -> str | None:
    """Try to parse a date string into YYYY-MM-DD format."""
    text = text.strip()
    formats = [
        "%B %d, %Y", "%B %d,%Y", "%d-%b-%Y", "%d/%b/%Y",
        "%Y-%m-%d", "%Y/%m/%d", "%b %d, %Y", "%d %B %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if 2000 <= dt.year <= 2030:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None
