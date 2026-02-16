"""SBP Structure of Interest Rates (SIR) PDF parser.

Parses the SIR PDF (sbp.org.pk/ecodata/sir.pdf) which contains:
  - Page 2: T-Bill auction cutoff + WA yields, KIBOR offer rates
  - Page 3: PIB Fixed Rate auction cutoff + WA yields
  - Page 5: GIS Sukuk auction history (Fixed + Variable rental rates)

This is the primary source for backfilling ~2-4 years of historical rates.
"""

import io
import re
import sqlite3
from datetime import datetime

import pdfplumber
import requests

from psx_ohlcv.db.repositories.treasury import (
    init_treasury_schema,
    upsert_gis_auction,
    upsert_pib_auction,
    upsert_tbill_auction,
)
from psx_ohlcv.db.repositories.yield_curves import (
    init_yield_curve_schema,
    upsert_kibor_rate,
)

__all__ = ["SBPSirScraper"]

SIR_URL = "https://www.sbp.org.pk/ecodata/sir.pdf"
TIMEOUT = 60

# T-Bill tenors in order matching PDF columns
TBILL_TENORS = ["1M", "3M", "6M", "12M"]

# PIB tenors in order matching PDF columns
PIB_TENORS = ["2Y", "3Y", "5Y", "10Y", "15Y", "20Y", "30Y"]

# KIBOR tenors in order matching PDF columns (offer rates only)
KIBOR_TENORS = ["1M", "3M", "6M"]

# Values to treat as NULL
NULL_VALUES = {"NA", "R", "N", "", "na", "r", "n"}


def _parse_float(val: str | None) -> float | None:
    """Parse a float value, returning None for special/null values."""
    if val is None:
        return None
    val = val.strip()
    if val in NULL_VALUES:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _parse_sir_date(text: str) -> str | None:
    """Parse a date from SIR PDF format (DD-Mon-YY or Mon-YY)."""
    if not text or not text.strip():
        return None
    text = text.strip()
    # DD-Mon-YY (e.g., "12-Jun-24")
    for fmt in ["%d-%b-%y", "%d-%b-%Y"]:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_month_date(text: str) -> str | None:
    """Parse Mon-YY format (e.g., 'Jan-25') into YYYY-MM-01."""
    if not text or not text.strip():
        return None
    text = text.strip()
    for fmt in ["%b-%y", "%b-%Y"]:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.strftime("%Y-%m-01")
        except ValueError:
            continue
    return None


class SBPSirScraper:
    """Scrapes historical rates from SBP Structure of Interest Rates PDF."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def _download_pdf(self) -> bytes:
        """Download the SIR PDF."""
        resp = self.session.get(SIR_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def scrape_sir(self, pdf_bytes: bytes | None = None) -> dict:
        """Parse the SIR PDF and extract all rates.

        Returns dict with:
          - tbills: list of {auction_date, tenor, cutoff_yield, weighted_avg_yield}
          - pibs: list of {auction_date, tenor, cutoff_yield, weighted_avg_yield}
          - kibor: list of {date, tenor, offer}
          - gis_variable: list of {auction_date, tenor, cutoff_rental_rate}
          - gis_fixed: list of {auction_date, tenor, cutoff_rental_rate}
        """
        if pdf_bytes is None:
            pdf_bytes = self._download_pdf()

        result = {
            "tbills": [],
            "pibs": [],
            "kibor": [],
            "gis_variable": [],
            "gis_fixed": [],
        }

        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))

        if len(pdf.pages) >= 2:
            tbills, kibor = self._parse_page2(pdf.pages[1])
            result["tbills"] = tbills
            result["kibor"] = kibor

        if len(pdf.pages) >= 3:
            result["pibs"] = self._parse_page3(pdf.pages[2])

        if len(pdf.pages) >= 5:
            gis_var, gis_fix = self._parse_page5(pdf.pages[4])
            result["gis_variable"] = gis_var
            result["gis_fixed"] = gis_fix

        pdf.close()
        return result

    def sync_sir(self, con: sqlite3.Connection, pdf_bytes: bytes | None = None) -> dict:
        """Download, parse, and sync SIR data to DB.

        Returns counts: {tbills, pibs, kibor, gis, failed}
        """
        init_treasury_schema(con)
        init_yield_curve_schema(con)

        data = self.scrape_sir(pdf_bytes)
        counts = {"tbills": 0, "pibs": 0, "kibor": 0, "gis": 0, "failed": 0}

        # Sync T-Bills
        for rec in data["tbills"]:
            if upsert_tbill_auction(con, rec):
                counts["tbills"] += 1
            else:
                counts["failed"] += 1

        # Sync PIBs
        for rec in data["pibs"]:
            if upsert_pib_auction(con, rec):
                counts["pibs"] += 1
            else:
                counts["failed"] += 1

        # Sync KIBOR (offer rate only — SIR doesn't have bid)
        for rec in data["kibor"]:
            if upsert_kibor_rate(con, rec):
                counts["kibor"] += 1
            else:
                counts["failed"] += 1

        # Sync GIS
        for rec in data["gis_variable"]:
            if upsert_gis_auction(con, rec):
                counts["gis"] += 1
            else:
                counts["failed"] += 1

        for rec in data["gis_fixed"]:
            if upsert_gis_auction(con, rec):
                counts["gis"] += 1
            else:
                counts["failed"] += 1

        return counts

    # ── Page parsers ─────────────────────────────────────────────

    @staticmethod
    def _parse_page2(page) -> tuple[list[dict], list[dict]]:
        """Parse page 2: T-Bill auctions + KIBOR offer rates.

        Table structure (13 cols):
          [0] Date  [1] 1-m  [2] 3-m  [3] 6-m  [4] 12-m   (cutoff yield)
          [5] 1-m   [6] 3-m  [7] 6-m  [8] 12-m             (WA yield)
          [9] Date  [10] 1-m [11] 3-m  [12] 6-m             (KIBOR offer)
        """
        tbills: list[dict] = []
        kibor: list[dict] = []

        tables = page.extract_tables(
            {"vertical_strategy": "text", "horizontal_strategy": "text"}
        )
        if not tables:
            return tbills, kibor

        table = tables[0]

        for row in table:
            if len(row) < 13:
                continue

            # ── T-Bill auction (left side, cols 0-8) ──
            auction_date = _parse_sir_date(row[0])
            if auction_date:
                for i, tenor in enumerate(TBILL_TENORS):
                    cutoff = _parse_float(row[1 + i])
                    wa_yield = _parse_float(row[5 + i])
                    if cutoff is not None or wa_yield is not None:
                        tbills.append({
                            "auction_date": auction_date,
                            "tenor": tenor,
                            "cutoff_yield": cutoff,
                            "weighted_avg_yield": wa_yield,
                        })

            # ── KIBOR offer rate (right side, cols 9-12) ──
            kibor_date_raw = (row[9] or "").strip()
            # Skip section headers like "Monthly Average", "Daily Average"
            if not kibor_date_raw or "verage" in kibor_date_raw or "onthly" in kibor_date_raw:
                continue

            # Try daily date (DD-Mon-YY) first, then monthly (Mon-YY)
            kibor_date = _parse_sir_date(kibor_date_raw)
            if not kibor_date:
                kibor_date = _parse_month_date(kibor_date_raw)
            if not kibor_date:
                continue

            for i, tenor in enumerate(KIBOR_TENORS):
                offer = _parse_float(row[10 + i])
                if offer is not None:
                    kibor.append({
                        "date": kibor_date,
                        "tenor": tenor,
                        "offer": offer,
                        "bid": None,
                    })

        return tbills, kibor

    @staticmethod
    def _parse_page3(page) -> list[dict]:
        """Parse page 3: PIB Fixed Rate auction results.

        Table structure (15 cols):
          [0] Date
          [1] 2-y  [2] 3-y  [3] 5-y  [4] 10-y  [5] 15-y  [6] 20-y  [7] 30-y  (cutoff)
          [8] 2-y  [9] 3-y  [10] 5-y [11] 10-y  [12] 15-y [13] 20-y [14] 30-y  (WA)
        """
        pibs: list[dict] = []

        tables = page.extract_tables(
            {"vertical_strategy": "text", "horizontal_strategy": "text"}
        )
        if not tables:
            return pibs

        table = tables[0]

        for row in table:
            if len(row) < 15:
                continue

            auction_date = _parse_sir_date(row[0])
            if not auction_date:
                continue

            for i, tenor in enumerate(PIB_TENORS):
                cutoff = _parse_float(row[1 + i])
                wa_yield = _parse_float(row[8 + i])
                if cutoff is not None or wa_yield is not None:
                    pibs.append({
                        "auction_date": auction_date,
                        "tenor": tenor,
                        "pib_type": "Fixed",
                        "cutoff_yield": cutoff,
                        "weighted_avg_yield": wa_yield,
                    })

        return pibs

    @staticmethod
    def _parse_page5(page) -> tuple[list[dict], list[dict]]:
        """Parse page 5: GIS Sukuk auction history.

        Text layout has two side-by-side columns:
          Variable Rental Rate (left) | Fixed Rental Rate (right)
        Each has: Auction Date, 1-y, 3-y, 5-y rental rates.
        """
        gis_variable: list[dict] = []
        gis_fixed: list[dict] = []

        text = page.extract_text()
        if not text:
            return gis_variable, gis_fixed

        lines = text.split("\n")

        # Find the data section (after header lines)
        data_started = False
        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Skip headers/footers
            if any(kw in line for kw in [
                "Structure of", "Percent", "Auction of", "State Bank",
                "Variable Rental", "Fixed Rental", "Rental Rate",
                "1-y 3-y 5-y", "N: No BID", "R: BIDS",
                "* First rental",
            ]):
                data_started = True
                continue

            if not data_started:
                continue

            # Data lines have dates and numbers — two entries per line
            # Pattern: date  rate1  rate2  rate3  date  rate1  rate2  rate3
            # But spacing is messy — numbers like "1 3.39" instead of "13.39"
            # Use regex to find date-like patterns and numeric values

            # Find all date matches (DD-Mon-YY)
            date_pattern = r"(\d{1,2}-\w{3}-\d{2})"
            dates = re.findall(date_pattern, line)

            if len(dates) < 1:
                continue

            # Extract numbers from the line — rebuild by fixing split numbers
            # Replace patterns like "1 3.39" → "13.39" (digit space digit+dot)
            cleaned = re.sub(r"(\d)\s+(\d+\.\d+)", r"\1\2", line)
            # Also handle "NA" and "R" as tokens
            tokens = re.findall(r"\d{1,2}-\w{3}-\d{2}|NA|R|N|\d+\.?\d*", cleaned)

            # Parse variable rate (first date + next 3 values)
            if len(dates) >= 1:
                var_date = _parse_sir_date(dates[0])
                if var_date:
                    # Find position of first date in tokens
                    try:
                        idx = tokens.index(dates[0])
                    except ValueError:
                        idx = -1
                    if idx >= 0 and idx + 3 < len(tokens):
                        rates = [_parse_float(tokens[idx + j]) for j in range(1, 4)]
                        tenor_map = {0: "1Y", 1: "3Y", 2: "5Y"}
                        for j, rate in enumerate(rates):
                            if rate is not None:
                                gis_variable.append({
                                    "auction_date": var_date,
                                    "gis_type": "GIS Variable Rate Return",
                                    "tenor": tenor_map[j],
                                    "cutoff_rental_rate": rate,
                                })

            # Parse fixed rate (second date + next 3 values)
            if len(dates) >= 2:
                fix_date = _parse_sir_date(dates[1])
                if fix_date:
                    try:
                        idx = tokens.index(dates[1])
                    except ValueError:
                        idx = -1
                    if idx >= 0 and idx + 3 < len(tokens):
                        rates = [_parse_float(tokens[idx + j]) for j in range(1, 4)]
                        tenor_map = {0: "1Y", 1: "3Y", 2: "5Y"}
                        for j, rate in enumerate(rates):
                            if rate is not None:
                                gis_fixed.append({
                                    "auction_date": fix_date,
                                    "gis_type": "GIS Fixed Rate Return",
                                    "tenor": tenor_map[j],
                                    "cutoff_rental_rate": rate,
                                })

        return gis_variable, gis_fixed
