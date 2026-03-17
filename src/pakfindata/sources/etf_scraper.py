"""ETF data scraper — fetches ETF metadata and NAV from dps.psx.com.pk."""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.etf import (
    init_etf_schema,
    upsert_etf_master,
    upsert_etf_nav,
)

ETF_SYMBOLS = [
    "MZNPETF", "NBPGETF", "NITGETF", "UBLPETF", "MIIETF",
    "ACIETF", "HBLETF", "JSMFETF",  # 3 additional ETFs
]
BASE_URL = "https://dps.psx.com.pk/etf"
TIMEOUT = 30

# PSX ticker → MUFAP internal symbol for cross-referencing
ETF_PSX_TO_MUFAP = {
    "MIIETF":  "DH87",   # Mahaana Islamic Index ETF
    "MZNPETF": "496C",   # Meezan Pakistan ETF
    "NBPGETF": "5B1D",   # NBP Pakistan Growth ETF
    "NITGETF": "G66C",   # NIT Pakistan Gateway ETF
    "UBLPETF": "CCDB",   # UBL Pakistan Enterprise ETF
    "ACIETF":  "328H",   # Alfalah Consumer Index ETF
    "HBLETF":  "1DDB",   # HBL Total Treasury ETF
    "JSMFETF": "A171",   # JS Momentum Factor ETF
}

# Legacy name-based mapping (kept for reference)
ETF_PSX_TO_MUFAP_NAME = {
    "MZNPETF": "Meezan Pakistan ETF",
    "NBPGETF": "NBP Pakistan Growth Exchange Traded Fund",
    "NITGETF": "NIT Pakistan Gateway Exchange Traded Fund",
    "UBLPETF": "UBL Pakistan Enterprise Exchange Traded Fund",
    "MIIETF": "Mahaana Islamic Index Exchange Traded Fund",
    "ACIETF": "Alfalah Consumer Index Exchange Traded Fund",
    "HBLETF": "HBL Total Treasury Exchange Traded Fund",
    "JSMFETF": "JS Momentum Factor Exchange Traded Fund",
}

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
        "name": "NBP Pakistan Growth Exchange Traded Fund",
        "amc": "NBP Fund Management Limited",
        "benchmark_index": "KSE-100 Index",
        "inception_date": "2020-10-07",
        "shariah_compliant": False,
        "trustee": "Central Depository Company of Pakistan Ltd.",
    },
    "NITGETF": {
        "name": "NIT Pakistan Gateway Exchange Traded Fund",
        "amc": "National Investment Trust Limited",
        "benchmark_index": "KSE-100 Index",
        "inception_date": "2020-03-18",
        "shariah_compliant": False,
    },
    "UBLPETF": {
        "name": "UBL Pakistan Enterprise Exchange Traded Fund",
        "amc": "UBL Fund Managers Limited",
        "benchmark_index": "KMI-30 Index",
        "inception_date": "2020-03-20",
        "shariah_compliant": True,
    },
    "MIIETF": {
        "name": "Mahaana Islamic Index Exchange Traded Fund",
        "amc": "Mahaana AMC Limited",
        "benchmark_index": "KMI All Shares Index",
        "inception_date": "2024-03-11",
        "shariah_compliant": True,
    },
    "ACIETF": {
        "name": "Alfalah Consumer Index Exchange Traded Fund",
        "amc": "Alfalah GHP Investment Management",
        "benchmark_index": "Consumer Index",
        "inception_date": "2022-01-18",
        "shariah_compliant": False,
    },
    "HBLETF": {
        "name": "HBL Total Treasury Exchange Traded Fund",
        "amc": "HBL Asset Management Limited",
        "benchmark_index": "Treasury Index",
        "inception_date": "2022-09-12",
        "shariah_compliant": False,
    },
    "JSMFETF": {
        "name": "JS Momentum Factor Exchange Traded Fund",
        "amc": "JS Investments Limited",
        "benchmark_index": "KSE-100 Index",
        "inception_date": "2022-01-07",
        "shariah_compliant": False,
    },
}


class ETFScraper:
    """Scrapes ETF detail pages from PSX DPS."""

    def __init__(self, con: sqlite3.Connection | None = None) -> None:
        self._con = con
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
                    latest = records[0]
                    if len(latest) >= 4:
                        data["market_price"] = latest[1]
        except Exception:
            pass

        data["date"] = datetime.now().strftime("%Y-%m-%d")

        # Cross-reference NAV from MUFAP mutual_fund_nav
        if self._con is not None:
            nav = self._get_etf_nav_from_mufap(symbol, data["date"])
            if nav is not None:
                data["nav"] = nav
                if data.get("market_price") and nav > 0:
                    data["premium_discount"] = round(
                        (data["market_price"] / nav - 1) * 100, 2
                    )

        return data

    def _get_etf_nav_from_mufap(self, symbol: str, date: str) -> float | None:
        """Cross-reference ETF NAV from MUFAP mutual_fund_nav table.

        Uses ETF_PSX_TO_MUFAP symbol mapping for fast lookup,
        falls back to name-based matching.
        """
        if self._con is None:
            return None

        # Primary: lookup via MUFAP symbol mapping
        mufap_sym = ETF_PSX_TO_MUFAP.get(symbol)
        if mufap_sym:
            row = self._con.execute(
                """SELECT n.nav FROM mutual_fund_nav n
                   JOIN mutual_funds mf ON mf.fund_id = n.fund_id
                   WHERE mf.symbol = ? AND n.date = ? AND n.nav > 0""",
                (mufap_sym, date),
            ).fetchone()
            if row:
                return row[0]
            # Try nearest date (within 3 days)
            row = self._con.execute(
                """SELECT n.nav FROM mutual_fund_nav n
                   JOIN mutual_funds mf ON mf.fund_id = n.fund_id
                   WHERE mf.symbol = ? AND n.date BETWEEN date(?, '-3 days') AND ?
                   AND n.nav > 0 ORDER BY n.date DESC LIMIT 1""",
                (mufap_sym, date, date),
            ).fetchone()
            if row:
                return row[0]

        # Fallback: name-based matching
        fund_name = ETF_PSX_TO_MUFAP_NAME.get(symbol)
        if not fund_name:
            return None
        row = self._con.execute(
            """SELECT n.nav FROM mutual_fund_nav n
               JOIN mutual_funds mf ON mf.fund_id = n.fund_id
               WHERE mf.fund_name = ? AND n.date BETWEEN date(?, '-3 days') AND ?
               AND n.nav > 0 ORDER BY n.date DESC LIMIT 1""",
            (fund_name, date, date),
        ).fetchone()
        return row[0] if row else None

    def sync_all_etfs(self, con: sqlite3.Connection) -> dict:
        """Scrape all ETFs, upsert to DB.

        Returns dict with ok and failed counts.
        """
        self._con = con
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

    def backfill_etf_nav(self, con: sqlite3.Connection) -> dict:
        """Fill NULL nav values in etf_nav from mutual_fund_nav."""
        self._con = con
        rows = con.execute(
            "SELECT rowid, symbol, date, market_price FROM etf_nav WHERE nav IS NULL"
        ).fetchall()

        updated = 0
        for rowid, symbol, date, mkt_price in rows:
            nav = self._get_etf_nav_from_mufap(symbol, date)
            if nav is None:
                continue
            premium = round((mkt_price / nav - 1) * 100, 2) if mkt_price and nav > 0 else None
            con.execute(
                "UPDATE etf_nav SET nav = ?, premium_discount = ? WHERE rowid = ?",
                (nav, premium, rowid),
            )
            updated += 1

        con.commit()
        return {"updated": updated, "total": len(rows)}

    def update_psx_ticker_mapping(self, con: sqlite3.Connection) -> dict:
        """Add psx_ticker column to mutual_funds and populate ETF mappings."""
        # Add column if not exists
        cols = [r[1] for r in con.execute("PRAGMA table_info(mutual_funds)").fetchall()]
        if "psx_ticker" not in cols:
            con.execute("ALTER TABLE mutual_funds ADD COLUMN psx_ticker TEXT")

        updated = 0
        for psx_ticker, mufap_sym in ETF_PSX_TO_MUFAP.items():
            cur = con.execute(
                "UPDATE mutual_funds SET psx_ticker = ? WHERE symbol = ? AND (psx_ticker IS NULL OR psx_ticker != ?)",
                (psx_ticker, mufap_sym, psx_ticker),
            )
            updated += cur.rowcount

        con.commit()
        return {"updated": updated, "total": len(ETF_PSX_TO_MUFAP)}

    def backfill_from_mufap_history(self, con: sqlite3.Connection) -> dict:
        """Backfill etf_nav with full NAV history from mutual_fund_nav.

        For each ETF, inserts rows from mutual_fund_nav (NAV only, no market_price)
        for dates not already in etf_nav. Also tries to fill market_price from
        eod_ohlcv if available.
        """
        self._con = con
        init_etf_schema(con)
        inserted = 0
        updated_mp = 0

        for psx_ticker, mufap_sym in ETF_PSX_TO_MUFAP.items():
            # Get MUFAP fund_id
            row = con.execute(
                "SELECT fund_id FROM mutual_funds WHERE symbol = ?", (mufap_sym,)
            ).fetchone()
            if not row:
                continue
            fund_id = row[0]

            # Get all NAV dates from MUFAP not already in etf_nav
            nav_rows = con.execute(
                """SELECT n.date, n.nav FROM mutual_fund_nav n
                   WHERE n.fund_id = ? AND n.nav > 0
                   AND n.date NOT IN (SELECT date FROM etf_nav WHERE symbol = ?)
                   ORDER BY n.date""",
                (fund_id, psx_ticker),
            ).fetchall()

            for nav_date, nav_val in nav_rows:
                # Try to get market_price from eod_ohlcv
                mkt_row = con.execute(
                    "SELECT close FROM eod_ohlcv WHERE symbol = ? AND date = ?",
                    (psx_ticker, nav_date),
                ).fetchone()
                mkt_price = mkt_row[0] if mkt_row else None
                premium = None
                if mkt_price and nav_val > 0:
                    premium = round((mkt_price / nav_val - 1) * 100, 2)

                con.execute(
                    """INSERT OR IGNORE INTO etf_nav
                       (symbol, date, nav, market_price, premium_discount)
                       VALUES (?, ?, ?, ?, ?)""",
                    (psx_ticker, nav_date, nav_val, mkt_price, premium),
                )
                inserted += 1
                if mkt_price:
                    updated_mp += 1

            # Ensure etf_master entry exists
            known = KNOWN_ETFS.get(psx_ticker, {})
            if known:
                upsert_etf_master(con, {
                    "symbol": psx_ticker,
                    "name": known.get("name", psx_ticker),
                    "amc": known.get("amc"),
                    "benchmark_index": known.get("benchmark_index"),
                    "inception_date": known.get("inception_date"),
                    "management_fee": known.get("management_fee"),
                    "shariah_compliant": known.get("shariah_compliant", False),
                    "trustee": known.get("trustee"),
                    "fiscal_year_end": known.get("fiscal_year_end"),
                    "expense_ratio": known.get("expense_ratio"),
                })

        con.commit()
        return {"inserted": inserted, "with_market_price": updated_mp}

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


if __name__ == "__main__":
    import argparse
    from pakfindata.config import get_db_path

    parser = argparse.ArgumentParser(description="ETF scraper tools")
    parser.add_argument("--backfill-nav", action="store_true",
                        help="Fill NULL nav in etf_nav from MUFAP data")
    parser.add_argument("--backfill-history", action="store_true",
                        help="Backfill full NAV history from MUFAP mutual_fund_nav")
    parser.add_argument("--update-mapping", action="store_true",
                        help="Update psx_ticker column in mutual_funds")
    parser.add_argument("--sync", action="store_true",
                        help="Scrape all ETFs from PSX and sync to DB")
    args = parser.parse_args()

    con = sqlite3.connect(str(get_db_path()))
    con.row_factory = sqlite3.Row
    scraper = ETFScraper(con)

    if args.update_mapping:
        result = scraper.update_psx_ticker_mapping(con)
        print(f"PSX ticker mapping: {result['updated']}/{result['total']} updated")
        for row in con.execute(
            "SELECT symbol, psx_ticker, fund_name FROM mutual_funds WHERE psx_ticker IS NOT NULL"
        ):
            print(f"  {row[0]:6s} -> {row[1]:10s} | {row[2]}")

    if args.backfill_nav:
        result = scraper.backfill_etf_nav(con)
        print(f"Backfilled NAV for {result['updated']}/{result['total']} ETF records")
        for row in con.execute(
            "SELECT symbol, date, market_price, nav, premium_discount FROM etf_nav LIMIT 10"
        ):
            print(f"  {row[0]:10s} | {row[1]} | Mkt: {row[2]} | NAV: {row[3]} | P/D: {row[4]}%")

    if args.backfill_history:
        result = scraper.backfill_from_mufap_history(con)
        print(f"Backfill history: {result['inserted']} rows inserted, {result['with_market_price']} with market price")
        for row in con.execute(
            "SELECT symbol, COUNT(*) as cnt, MIN(date) as first, MAX(date) as last FROM etf_nav GROUP BY symbol"
        ):
            print(f"  {row[0]:10s} | {row[1]:5d} days | {row[2]} -> {row[3]}")

    if args.sync:
        result = scraper.sync_all_etfs(con)
        print(f"Synced: {result['ok']}/{result['total']}, Failed: {result['failed']}")

    if not any([args.backfill_nav, args.backfill_history, args.update_mapping, args.sync]):
        parser.print_help()

    con.close()
