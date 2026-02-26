"""SBP Treasury scraper — T-Bill, PIB, and GIS auction data from SBP.

Primary source: sbp.org.pk/dfmd/pma.asp  (latest auction cutoff rates)
The SBP Primary Market Auction (PMA) page provides the most recent
T-Bill and PIB auction results in HTML tables.

Historical auction PDFs are available at investpak.sbp.org.pk but are not
machine-parseable, so we focus on the PMA page for automated collection.
"""

import re
import sqlite3
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.treasury import (
    get_latest_pib_yields,
    get_latest_tbill_yields,
    get_tbill_auctions,
    init_treasury_schema,
    upsert_pib_auction,
    upsert_tbill_auction,
)

__all__ = ["SBPTreasuryScraper"]

PMA_URL = "https://www.sbp.org.pk/dfmd/pma.asp"
TIMEOUT = 30

# Tenor mapping: PMA page labels → canonical tenor strings
TBILL_TENORS = {
    "1-M": "1M",
    "3-M": "3M",
    "6-M": "6M",
    "12-M": "12M",
    "1-m": "1M",
    "3-m": "3M",
    "6-m": "6M",
    "12-m": "12M",
}

PIB_TENORS = {
    "2-Y": "2Y",
    "3-Y": "3Y",
    "5-Y": "5Y",
    "10-Y": "10Y",
    "15-Y": "15Y",
    "20-Y": "20Y",
    "30-Y": "30Y",
    "2-y": "2Y",
    "3-y": "3Y",
    "5-y": "5Y",
    "10-y": "10Y",
    "15-y": "15Y",
    "20-y": "20Y",
    "30-y": "30Y",
}

# PIB yields are typically 5-15% range; cutoff prices are 90-105 range.
# We use this to distinguish yields from prices on the PMA page.
PIB_YIELD_MAX = 20.0


class SBPTreasuryScraper:
    """Scrapes T-Bill and PIB auction results from SBP PMA page."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_pma_page(self) -> dict:
        """Scrape the SBP PMA page for latest T-Bill and PIB rates.

        Returns dict with keys:
            - tbills: list of {tenor, cutoff_yield, auction_date}
            - pibs: list of {tenor, cutoff_yield, auction_date}
            - auction_date: str (YYYY-MM-DD) or None
            - raw_html_length: int
        """
        result: dict = {
            "tbills": [],
            "pibs": [],
            "auction_date": None,
            "raw_html_length": 0,
        }

        try:
            resp = self.session.get(PMA_URL, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch PMA page: {e}")
            return result

        result["raw_html_length"] = len(resp.text)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract auction date from page
        auction_date = self._extract_auction_date(soup, resp.text)
        result["auction_date"] = auction_date

        # Parse from page text — more reliable than table parsing since
        # the page has KIBOR tables (BID/OFFER) before MTB auction tables.
        # We specifically look for the "MTBs" section with "Cut-off Yield".
        result["tbills"], result["pibs"] = self._parse_rates_from_text(
            soup, auction_date
        )

        return result

    def sync_treasury(self, con: sqlite3.Connection) -> dict:
        """Scrape PMA page and upsert T-Bill + PIB rates to DB.

        Returns dict with counts: {tbills_ok, pibs_ok, failed, auction_date}
        """
        init_treasury_schema(con)

        pma = self.scrape_pma_page()
        counts = {
            "tbills_ok": 0,
            "pibs_ok": 0,
            "failed": 0,
            "auction_date": pma["auction_date"],
        }

        if not pma["auction_date"]:
            print("  [!] Could not determine auction date from PMA page")
            counts["failed"] = 1
            return counts

        # Upsert T-Bills
        for tbill in pma["tbills"]:
            data = {
                "auction_date": tbill["auction_date"],
                "tenor": tbill["tenor"],
                "cutoff_yield": tbill["cutoff_yield"],
            }
            if upsert_tbill_auction(con, data):
                counts["tbills_ok"] += 1
            else:
                counts["failed"] += 1

        # Upsert PIBs
        for pib in pma["pibs"]:
            data = {
                "auction_date": pib["auction_date"],
                "tenor": pib["tenor"],
                "pib_type": "Fixed",
                "cutoff_yield": pib["cutoff_yield"],
            }
            if upsert_pib_auction(con, data):
                counts["pibs_ok"] += 1
            else:
                counts["failed"] += 1

        return counts

    def get_summary(self, con: sqlite3.Connection) -> dict:
        """Get summary of stored treasury data."""
        init_treasury_schema(con)
        tbill_yields = get_latest_tbill_yields(con)
        pib_yields = get_latest_pib_yields(con)
        tbill_df = get_tbill_auctions(con)
        return {
            "tbill_tenors": len(tbill_yields),
            "pib_tenors": len(pib_yields),
            "total_tbill_records": len(tbill_df),
            "latest_tbills": tbill_yields,
            "latest_pibs": pib_yields,
        }

    # ── helpers ──────────────────────────────────────────────────

    @staticmethod
    def _extract_auction_date(soup: BeautifulSoup, html: str) -> str | None:
        """Extract the auction date from the PMA page.

        The SBP PMA page uses "as on <date>" format near the rates table.
        We prioritize this pattern, then fall back to general date search
        preferring the most recent date found.
        """
        page_text = soup.get_text()

        # Priority 1: "as on <date>" pattern (SBP standard)
        as_on_match = re.search(
            r"as\s+on\s+(\w+\s+\d{1,2},?\s*\d{4})", page_text, re.IGNORECASE
        )
        if as_on_match:
            parsed = SBPTreasuryScraper._try_parse_date(as_on_match.group(1))
            if parsed:
                return parsed

        # Priority 2: date near "MTB" or "T-Bill" or "Auction" keywords
        for keyword in ["MTB", "T-Bill", "T-Bills", "Auction", "Cut-off"]:
            idx = page_text.find(keyword)
            if idx == -1:
                continue
            # Search in a window around the keyword
            window = page_text[max(0, idx - 200):idx + 200]
            for pattern in [
                r"(\w+ \d{1,2},?\s*\d{4})",
                r"(\d{1,2}[-/]\w{3}[-/]\d{4})",
            ]:
                match = re.search(pattern, window)
                if match:
                    parsed = SBPTreasuryScraper._try_parse_date(match.group(1))
                    if parsed:
                        return parsed

        # Priority 3: find the most recent date on the page
        all_dates = []
        for pattern in [
            r"(\w+ \d{1,2},?\s*\d{4})",
            r"(\d{1,2}[-/]\w{3}[-/]\d{4})",
            r"(\d{4}[-/]\d{2}[-/]\d{2})",
        ]:
            for match in re.finditer(pattern, page_text):
                parsed = SBPTreasuryScraper._try_parse_date(match.group(1))
                if parsed:
                    all_dates.append(parsed)

        if all_dates:
            # Return the most recent date
            return max(all_dates)

        # Fallback: use today's date
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _try_parse_date(text: str) -> str | None:
        """Try to parse a date string into YYYY-MM-DD format."""
        text = text.strip()
        formats = [
            "%B %d, %Y",       # February 04, 2026
            "%B %d,%Y",        # February 04,2026
            "%d-%b-%Y",        # 04-Feb-2026
            "%d/%b/%Y",        # 04/Feb/2026
            "%Y-%m-%d",        # 2026-02-04
            "%Y/%m/%d",        # 2026/02/04
            "%b %d, %Y",       # Feb 04, 2026
            "%d %B %Y",        # 04 February 2026
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(text.strip(), fmt)
                # Sanity check: year should be reasonable
                if 2000 <= dt.year <= 2030:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_rate(text: str) -> float | None:
        """Parse a rate value like '10.1977%' or '10.1977'."""
        if not text:
            return None
        cleaned = text.strip().replace("%", "").replace(",", "")
        match = re.search(r"(\d+\.?\d*)", cleaned)
        if match:
            try:
                val = float(match.group(1))
                # Sanity: rates should be 0-100
                if 0 < val < 100:
                    return val
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_rates_from_text(
        soup: BeautifulSoup,
        auction_date: str | None,
    ) -> tuple[list[dict], list[dict]]:
        """Parse T-Bill and PIB rates from page text.

        The PMA page layout:
          - KIBOR section (BID/OFFER — NOT auction yields, skip)
          - "MTBs ... Cut-off Yield" section (actual T-Bill auction cutoffs)
          - "Fixed-rate PIB ... Cut-off Rates" section (actual PIB yields)
          - Floating-rate PIB (cutoff prices, NOT yields — skip)

        We anchor on "Cut-off Yield" for MTBs and "Fixed-rate PIB" for PIBs.
        """
        text = soup.get_text()
        tbills: list[dict] = []
        pibs: list[dict] = []

        # ── T-Bills: anchor on "Cut-off Yield" near "MTBs" ──
        cutoff_match = re.search(r"Cut-off\s+Yield", text, re.IGNORECASE)
        if cutoff_match:
            # Extract 400 chars after "Cut-off Yield"
            mtb_section = text[cutoff_match.end():cutoff_match.end() + 400]
            seen: set[str] = set()
            for tenor_label, canonical in TBILL_TENORS.items():
                if canonical in seen:
                    continue
                pat = re.escape(tenor_label) + r"\s+(\d+\.?\d*)\s*%?"
                m = re.search(pat, mtb_section)
                if m:
                    rate = SBPTreasuryScraper._parse_rate(m.group(1))
                    if rate is not None:
                        tbills.append({
                            "tenor": canonical,
                            "cutoff_yield": rate,
                            "auction_date": auction_date,
                        })
                        seen.add(canonical)

        # ── PIBs: anchor on "Fixed-rate PIB" or "Cut-off Rates" ──
        pib_match = re.search(
            r"Fixed[-\s]*rate\s+PIB", text, re.IGNORECASE
        )
        if pib_match:
            pib_section = text[pib_match.end():pib_match.end() + 400]
            seen_pib: set[str] = set()
            for tenor_label, canonical in PIB_TENORS.items():
                if canonical in seen_pib:
                    continue
                pat = re.escape(tenor_label) + r"\s+(\d+\.?\d*)\s*%?"
                m = re.search(pat, pib_section)
                if m:
                    rate = SBPTreasuryScraper._parse_rate(m.group(1))
                    if rate is not None and rate < PIB_YIELD_MAX:
                        pibs.append({
                            "tenor": canonical,
                            "cutoff_yield": rate,
                            "auction_date": auction_date,
                        })
                        seen_pib.add(canonical)

        return tbills, pibs

    @staticmethod
    def _find_rate_nearby(lines: list[str], idx: int, window: int = 3) -> float | None:
        """Look for a rate value in lines near the given index."""
        for offset in range(1, window + 1):
            if idx + offset < len(lines):
                rate = SBPTreasuryScraper._parse_rate(lines[idx + offset])
                if rate is not None:
                    return rate
        return None
