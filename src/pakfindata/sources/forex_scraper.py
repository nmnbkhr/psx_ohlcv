"""Forex.pk scraper — open market / kerb FX rates for Pakistan.

Source: forex.pk/open_market_rates.asp
Provides buying/selling rates for 25+ currencies vs PKR.
"""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.fx_extended import (
    init_fx_extended_schema,
    upsert_fx_kerb,
    upsert_fx_open_market,
)

__all__ = ["ForexPKScraper"]

FOREX_PK_URL = "https://www.forex.pk/open_market_rates.asp"
TIMEOUT = 30

# Map forex.pk currency names (as they appear on the page) to ISO codes
CURRENCY_MAP = {
    "US Dollar": "USD",
    "US Dollar DD": "USD-DD",
    "US Dollar TT": "USD-TT",
    "Australian Dollar": "AUD",
    "Bahrain Dinar": "BHD",
    "Canadian Dollar": "CAD",
    "China Yuan": "CNY",
    "Danish Krone": "DKK",
    "Euro": "EUR",
    "Hong Kong Dollar": "HKD",
    "Indian Rupee": "INR",
    "Japanese Yen": "JPY",
    "Kuwaiti Dinar": "KWD",
    "Malaysian Ringgit": "MYR",
    "NewZealand $": "NZD",
    "Norwegians Krone": "NOK",
    "Omani Riyal": "OMR",
    "Qatari Riyal": "QAR",
    "Saudi Riyal": "SAR",
    "Singapore Dollar": "SGD",
    "Swedish Korona": "SEK",
    "Swiss Franc": "CHF",
    "Thai Bhat": "THB",
    "U.A.E Dirham": "AED",
    "UK Pound Sterling": "GBP",
}

# ISO code set for quick lookup (used by symbol-column fallback)
_ISO_CODES = set(CURRENCY_MAP.values()) - {"USD-DD", "USD-TT"}


class ForexPKScraper:
    """Scrapes open market / kerb FX rates from forex.pk."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_open_market(self) -> list[dict]:
        """Scrape forex.pk for kerb/dealer rates.

        Returns list of {date, currency, buying, selling, source}.
        """
        try:
            resp = self.session.get(FOREX_PK_URL, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch forex.pk: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rates: list[dict] = []
        seen: set[str] = set()
        today = datetime.now().strftime("%Y-%m-%d")

        # Page layout: 5-column rows (Name, Symbol, Buying, Selling, Charts)
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                name_text = cells[0].get_text(strip=True)
                sym_text = cells[1].get_text(strip=True)

                # Resolve ISO code: first by name, then by symbol column
                code = CURRENCY_MAP.get(name_text)
                if not code and sym_text in _ISO_CODES:
                    code = sym_text
                if not code:
                    continue

                # Skip DD/TT variants and duplicates
                if code in ("USD-DD", "USD-TT") or code in seen:
                    continue

                # Buying = column 2, Selling = column 3
                buying = _parse_number(cells[2].get_text(strip=True))
                selling = _parse_number(cells[3].get_text(strip=True))

                if buying and selling and buying > 0 and selling > 0:
                    seen.add(code)
                    rates.append({
                        "date": today,
                        "currency": code,
                        "buying": buying,
                        "selling": selling,
                        "source": "forex.pk",
                    })

        return rates

    def sync_kerb(self, con: sqlite3.Connection) -> dict:
        """Scrape and upsert kerb + open market rates from forex.pk.

        forex.pk's open_market_rates page provides dealer/kerb rates.
        We store the same data in both forex_kerb and sbp_fx_open_market
        so the Interbank vs Open Market comparison page works.
        """
        init_fx_extended_schema(con)
        rates = self.scrape_open_market()
        counts = {"ok": 0, "failed": 0, "total": len(rates)}

        for rate in rates:
            kerb_ok = upsert_fx_kerb(con, rate)
            upsert_fx_open_market(con, rate)
            if kerb_ok:
                counts["ok"] += 1
            else:
                counts["failed"] += 1

        return counts


def _parse_number(text: str) -> float | None:
    """Parse a number from text like '279.41' or '1,089.93'."""
    if not text:
        return None
    cleaned = text.replace(",", "").strip()
    match = re.search(r"(\d+\.?\d*)", cleaned)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None
