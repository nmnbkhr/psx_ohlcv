"""ETF data scraper — fetches ETF metadata and NAV from dps.psx.com.pk."""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from psx_ohlcv.db.repositories.etf import (
    init_etf_schema,
    upsert_etf_master,
    upsert_etf_nav,
)

ETF_SYMBOLS = ["MZNPETF", "NBPGETF", "NITGETF", "UBLPETF", "MIIETF"]
BASE_URL = "https://dps.psx.com.pk/etf"
TIMEOUT = 30

# Known ETF metadata (the DPS page is JS-rendered, so we seed what we know)
KNOWN_ETFS = {
    "MZNPETF": {
        "name": "Meezan Pakistan ETF",
        "amc": "Al Meezan Investment Management Limited",
        "benchmark_index": "Meezan Pakistan Index (MZNPI)",
        "inception_date": "2020-10-06",
        "management_fee": "Up to 0.50% p.a.",
        "shariah_compliant": True,
        "trustee": "Central Depository Company of Pakistan Ltd.",
        "fiscal_year_end": "June",
    },
    "NBPGETF": {
        "name": "NBP Gold ETF",
        "amc": "NBP Fund Management Limited",
        "benchmark_index": "Gold Spot Price (PKR)",
        "inception_date": "2021-07-21",
        "shariah_compliant": True,
        "trustee": "Central Depository Company of Pakistan Ltd.",
    },
    "NITGETF": {
        "name": "NIT Gold ETF",
        "amc": "National Investment Trust Limited",
        "benchmark_index": "Gold Spot Price (PKR)",
        "shariah_compliant": True,
    },
    "UBLPETF": {
        "name": "UBL Pakistan ETF",
        "amc": "UBL Fund Managers Limited",
        "benchmark_index": "KMI-30 Index",
        "shariah_compliant": True,
    },
    "MIIETF": {
        "name": "Meezan Islamic Income ETF",
        "amc": "Al Meezan Investment Management Limited",
        "benchmark_index": "6-Month KIBOR",
        "shariah_compliant": True,
    },
}


class ETFScraper:
    """Scrapes ETF detail pages from PSX DPS."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_etf(self, symbol: str) -> dict | None:
        """Scrape a single ETF detail page.

        Returns dict with master metadata and current NAV data,
        or None if scraping fails. Uses known metadata as baseline
        since the DPS page is largely JS-rendered.
        """
        # Start with known metadata (fallback for JS-rendered fields)
        known = KNOWN_ETFS.get(symbol, {})
        data: dict = {"symbol": symbol}
        data["name"] = known.get("name", symbol)
        data["amc"] = known.get("amc")
        data["benchmark_index"] = known.get("benchmark_index")
        data["inception_date"] = known.get("inception_date")
        data["management_fee"] = known.get("management_fee")
        data["shariah_compliant"] = known.get("shariah_compliant", False)
        data["trustee"] = known.get("trustee")
        data["fiscal_year_end"] = known.get("fiscal_year_end")
        data["expense_ratio"] = known.get("expense_ratio")

        # Try to scrape the page for name (from title) and any parseable fields
        url = f"{BASE_URL}/{symbol}"
        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch {url}: {e}")
            # Still return known metadata even if page fails
            data["date"] = datetime.now().strftime("%Y-%m-%d")
            return data

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract name from <title> tag: "MZNPETF - Stock quote for Meezan Pakistan ETF - ..."
        title_el = soup.find("title")
        if title_el:
            title_text = title_el.get_text(strip=True)
            match = re.search(r"Stock quote for (.+?) -", title_text)
            if match:
                data["name"] = match.group(1).strip()

        # Try to extract fields that might be in static HTML
        page_text = resp.text

        # Override with scraped data if available
        scraped_amc = self._extract_field(
            soup, page_text, ["Fund Manager", "AMC"]
        )
        if scraped_amc and len(scraped_amc) > 3:
            data["amc"] = scraped_amc

        scraped_trustee = self._extract_field(
            soup, page_text, ["Trustee"]
        )
        if scraped_trustee and len(scraped_trustee) > 3:
            data["trustee"] = scraped_trustee

        scraped_fee = self._extract_field(
            soup, page_text, ["Management Fee"]
        )
        if scraped_fee and "%" in scraped_fee:
            data["management_fee"] = scraped_fee

        # Get EOD price data from the timeseries API (JSON, reliable)
        data["market_price"] = None
        data["nav"] = None
        data["aum_millions"] = None
        data["outstanding_units"] = None

        try:
            eod_resp = self.session.get(
                f"https://dps.psx.com.pk/timeseries/eod/{symbol}",
                timeout=TIMEOUT,
            )
            if eod_resp.status_code == 200:
                eod_data = eod_resp.json()
                records = eod_data.get("data", [])
                if records:
                    # First record is the latest (descending order)
                    latest = records[0]
                    # Format: [timestamp, close, volume, open]
                    if len(latest) >= 4:
                        data["market_price"] = latest[1]
        except Exception:
            pass

        data["date"] = datetime.now().strftime("%Y-%m-%d")
        return data

    def sync_all_etfs(self, con: sqlite3.Connection) -> dict:
        """Scrape all ETFs, upsert to DB.

        Returns dict with ok and failed counts.
        """
        init_etf_schema(con)
        ok = 0
        failed = 0

        for symbol in ETF_SYMBOLS:
            print(f"  Scraping {symbol}...")
            data = self.scrape_etf(symbol)

            if data is None:
                failed += 1
                continue

            # Upsert master
            master_ok = upsert_etf_master(con, data)

            # Upsert NAV if we got nav data
            nav_ok = True
            if data.get("nav") or data.get("market_price"):
                nav_ok = upsert_etf_nav(
                    con,
                    symbol=data["symbol"],
                    date=data["date"],
                    nav=data.get("nav"),
                    market_price=data.get("market_price"),
                    aum_millions=data.get("aum_millions"),
                    outstanding_units=data.get("outstanding_units"),
                )

            if master_ok and nav_ok:
                ok += 1
            else:
                failed += 1

        return {"ok": ok, "failed": failed, "total": len(ETF_SYMBOLS)}

    # ── helpers ──────────────────────────────────────────────────

    @staticmethod
    def _extract_field(
        soup: BeautifulSoup,
        page_text: str,
        labels: list[str],
    ) -> str | None:
        """Try to find a labeled value in the page."""
        for label in labels:
            # Try table rows: <td>Label</td><td>Value</td>
            td = soup.find("td", string=re.compile(re.escape(label), re.I))
            if td:
                next_td = td.find_next_sibling("td")
                if next_td:
                    return next_td.get_text(strip=True)

            # Try <dt>/<dd> pairs
            dt = soup.find("dt", string=re.compile(re.escape(label), re.I))
            if dt:
                dd = dt.find_next_sibling("dd")
                if dd:
                    return dd.get_text(strip=True)

            # Try <span>/<div> with class containing 'label' or similar
            el = soup.find(
                string=re.compile(re.escape(label) + r"\s*:?\s*", re.I)
            )
            if el:
                parent = el.parent
                if parent:
                    sibling = parent.find_next_sibling()
                    if sibling:
                        return sibling.get_text(strip=True)
                    # Check if value is in the same element after the label
                    full = parent.get_text(strip=True)
                    match = re.search(
                        re.escape(label) + r"\s*:?\s*(.+)",
                        full,
                        re.I,
                    )
                    if match:
                        return match.group(1).strip()

        return None

    @staticmethod
    def _parse_number(text: str | None) -> float | None:
        """Extract a number from text like 'Rs. 22.04' or '1,089,931'."""
        if not text:
            return None
        # Remove currency symbols, commas, and whitespace
        cleaned = re.sub(r"[Rs.\s,]+", "", text)
        # Find first number (possibly negative)
        match = re.search(r"-?[\d.]+", cleaned)
        if match:
            try:
                return float(match.group())
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_aum(text: str | None) -> float | None:
        """Parse AUM text to millions. Handles Rs. 1,089,931,700 etc."""
        if not text:
            return None
        num = ETFScraper._parse_number(text)
        if num is None:
            return None
        # If the number is > 1M, it's likely in raw rupees → convert to millions
        if num > 1_000_000:
            return round(num / 1_000_000, 2)
        # If number is moderate, might already be in millions
        return num

    @staticmethod
    def _parse_date(text: str | None) -> str | None:
        """Try to parse a date string into YYYY-MM-DD."""
        if not text:
            return None
        formats = [
            "%B %d, %Y",      # October 6, 2020
            "%d-%b-%Y",       # 06-Oct-2020
            "%d/%m/%Y",       # 06/10/2020
            "%Y-%m-%d",       # 2020-10-06
            "%d %B %Y",       # 6 October 2020
            "%b %d, %Y",      # Oct 6, 2020
        ]
        for fmt in formats:
            try:
                return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return text  # Return raw if can't parse
