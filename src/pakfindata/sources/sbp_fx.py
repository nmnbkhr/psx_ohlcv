"""SBP FX rate scraper — interbank WAR and PMA page USD/PKR.

The WAR page (sbp.org.pk/ecodata/rates/WAR/WAR-Current.asp) only shows
USD/PKR rates (M2M Revaluation Rate + Weighted Average Rate bid/offer).

Multi-currency rates are available at forex.pk (see forex_scraper.py).
"""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.fx_extended import (
    get_all_fx_latest,
    init_fx_extended_schema,
    upsert_fx_interbank,
)

__all__ = ["SBPFXScraper"]

PMA_URL = "https://www.sbp.org.pk/dfmd/pma.asp"
TIMEOUT = 30


class SBPFXScraper:
    """Scrapes SBP interbank FX rates (USD/PKR) from PMA page."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_interbank(self) -> list[dict]:
        """Scrape USD/PKR interbank rates from PMA page.

        Returns list of rate dicts. Currently only USD/PKR is available
        from SBP WAR page.
        """
        try:
            resp = self.session.get(PMA_URL, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch PMA page: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()
        rates: list[dict] = []

        # Extract date near M2M section
        date_match = re.search(
            r"[Aa]s\s+on\s+(\d{1,2}[-/]\w{3}[-/]\d{2,4})",
            text[text.find("M2M") - 100:text.find("M2M") + 50] if "M2M" in text else "",
        )
        rate_date = (
            _try_parse_date(date_match.group(1))
            if date_match
            else datetime.now().strftime("%Y-%m-%d")
        )

        # Extract M2M Revaluation Rate
        m2m_match = re.search(
            r"M2M\s+Revaluation\s+Rate\s+(\d+\.?\d*)", text, re.IGNORECASE
        )

        # Extract Weighted Average Rate Bid/Offer
        bid_match = re.search(r"Bid:\s*(\d+\.?\d*)", text)
        offer_match = re.search(r"Offer:\s*(\d+\.?\d*)", text)

        if bid_match and offer_match:
            try:
                rates.append({
                    "date": rate_date,
                    "currency": "USD",
                    "buying": float(bid_match.group(1)),
                    "selling": float(offer_match.group(1)),
                })
            except ValueError:
                pass

        return rates

    def sync_interbank(self, con: sqlite3.Connection) -> dict:
        """Scrape and upsert SBP interbank rates."""
        init_fx_extended_schema(con)
        rates = self.scrape_interbank()
        counts = {"ok": 0, "failed": 0, "total": len(rates)}

        for rate in rates:
            if upsert_fx_interbank(con, rate):
                counts["ok"] += 1
            else:
                counts["failed"] += 1

        return counts


def _try_parse_date(text: str) -> str | None:
    """Parse date string to YYYY-MM-DD."""
    if not text:
        return None
    for fmt in ["%d-%b-%Y", "%d-%b-%y", "%d/%b/%Y", "%d/%b/%y"]:
        try:
            dt = datetime.strptime(text.strip(), fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            if 2000 <= dt.year <= 2030:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None
