"""SBP PIB Archive PDF parser.

Parses the 42-page PIB auction archive
(sbp.org.pk/ecodata/Pakinvestbonds.pdf) which contains every PIB
auction since December 2000 — approximately 25 years of data.

Columns: Auction #, Settlement Date, Tenor, Coupon Rate,
         Amount Accepted (millions), Weighted Average Yield%
"""

import io
import re
import sqlite3
from datetime import datetime

import pdfplumber
import requests

from pakfindata.db.repositories.treasury import (
    init_treasury_schema,
    upsert_pib_auction,
)

__all__ = ["SBPPibArchiveScraper"]

PIB_ARCHIVE_URL = "https://www.sbp.org.pk/ecodata/Pakinvestbonds.pdf"
TIMEOUT = 120

# Tenor patterns → canonical tenor names
TENOR_MAP = {
    "2": "2Y", "3": "3Y", "5": "5Y", "10": "10Y",
    "15": "15Y", "20": "20Y", "30": "30Y",
}

# Long-form date patterns
_LONG_DATE_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+\d{1,2},?\s+\d{4}"
)

# Tenor line: captures tenor-number, coupon rate, amount, WA yield
# e.g. "3 -Year 12.50% 1,999.00 12.4507%"  or "10-Year 14.00% 2,222.00 13.9667%"
# Also handles "3 - Year" (with extra space), "15 - Year", etc.
_TENOR_LINE_RE = re.compile(
    r"(\d{1,2})\s*-\s*Year\s+"      # tenor number (2,3,5,10,15,20,30)
    r"([\d.]+%|Zero)\s+"            # coupon rate or "Zero"
    r"([\d,. ]+?)\s+"               # amount accepted (may have spaces in digits)
    r"([\d.]+%)"                     # WA yield
)

# Tenor line with "Bid Rejected" or "No Bid Received"
_TENOR_REJECTED_RE = re.compile(
    r"(\d{1,2})\s*-\s*Year\s+"
    r"([\d.]+%|Zero)\s+"
    r"(?:Bid\s+Rejected|No\s+Bid|BIDS\s+REJECTED|-)"
)


def _parse_long_date(text: str) -> str | None:
    """Parse 'December 14, 2000' → '2000-12-14'."""
    text = text.strip().replace(",", "")
    for fmt in ["%B %d %Y", "%B %d, %Y"]:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _clean_amount(raw: str) -> float | None:
    """Parse amount string, handling spaces inside numbers.

    e.g. '1,999.00' → 1999.0, '2 20,590.30' → 220590.3
    """
    if not raw or not raw.strip():
        return None
    # Remove spaces and commas
    cleaned = raw.replace(" ", "").replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_coupon(raw: str) -> float | None:
    """Parse coupon rate like '12.50%' or 'Zero' → float or None."""
    if not raw:
        return None
    raw = raw.strip().rstrip("%")
    if raw.lower() == "zero":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_yield(raw: str) -> float | None:
    """Parse WA yield like '12.4507%' → float."""
    if not raw:
        return None
    raw = raw.strip().rstrip("%")
    try:
        return float(raw)
    except ValueError:
        return None


class SBPPibArchiveScraper:
    """Scrapes all historical PIB auction data from SBP archive PDF."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def _download_pdf(self) -> bytes:
        """Download the PIB archive PDF."""
        resp = self.session.get(PIB_ARCHIVE_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def scrape_pib_archive(self, pdf_bytes: bytes | None = None) -> list[dict]:
        """Parse the PIB archive PDF and extract all auction records.

        Returns list of dicts with:
          auction_date, tenor, pib_type, coupon_rate,
          amount_accepted_billions, cutoff_yield (=WA yield)
        """
        if pdf_bytes is None:
            pdf_bytes = self._download_pdf()

        records: list[dict] = []
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))

        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            records.extend(self._parse_page_text(text))

        pdf.close()

        # Deduplicate by (auction_date, tenor) — later pages may repeat
        seen: set[tuple[str, str]] = set()
        unique: list[dict] = []
        for rec in records:
            key = (rec["auction_date"], rec["tenor"])
            if key not in seen:
                seen.add(key)
                unique.append(rec)

        return unique

    def sync_pib_archive(self, con: sqlite3.Connection, pdf_bytes: bytes | None = None) -> dict:
        """Download, parse, and sync PIB archive to DB.

        Returns counts: {inserted, failed, total}
        """
        init_treasury_schema(con)
        records = self.scrape_pib_archive(pdf_bytes)
        counts = {"inserted": 0, "failed": 0, "total": len(records)}

        for rec in records:
            if upsert_pib_auction(con, rec):
                counts["inserted"] += 1
            else:
                counts["failed"] += 1

        return counts

    @staticmethod
    def _parse_page_text(text: str) -> list[dict]:
        """Parse text of a single page into auction records."""
        records: list[dict] = []
        current_date: str | None = None

        lines = text.split("\n")

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Skip header lines
            if any(kw in stripped for kw in [
                "PAKISTAN INVESTMENT", "AUCTION PROFILE", "FACE VALUE",
                "Auction Settlement", "Tenor", "Rate Accepted",
                "Amount in millions", "Contact:", "money.market",
            ]):
                continue

            # Skip "Total" lines
            if stripped.startswith("Total") or stripped.startswith("total"):
                continue

            # Skip standalone "Coupon Rate" header
            if stripped == "Coupon Rate":
                continue

            # Skip EFFECTIVE DATE lines (coupon rate changes)
            if "EFFECTIVE DATE" in stripped:
                continue

            # Look for settlement dates in the line
            date_match = _LONG_DATE_RE.search(stripped)
            if date_match:
                parsed = _parse_long_date(date_match.group())
                if parsed:
                    current_date = parsed

            # Look for tenor data lines
            if current_date:
                tenor_match = _TENOR_LINE_RE.search(stripped)
                if tenor_match:
                    tenor_num = tenor_match.group(1)
                    coupon_raw = tenor_match.group(2)
                    amount_raw = tenor_match.group(3)
                    yield_raw = tenor_match.group(4)

                    tenor = TENOR_MAP.get(tenor_num)
                    if tenor:
                        amount_millions = _clean_amount(amount_raw)
                        records.append({
                            "auction_date": current_date,
                            "tenor": tenor,
                            "pib_type": "Fixed",
                            "coupon_rate": _parse_coupon(coupon_raw),
                            "amount_accepted_billions": (
                                amount_millions / 1000.0 if amount_millions else None
                            ),
                            "cutoff_yield": _parse_yield(yield_raw),
                        })
                    continue

                # Check for rejected bids
                rejected_match = _TENOR_REJECTED_RE.search(stripped)
                if rejected_match:
                    tenor_num = rejected_match.group(1)
                    tenor = TENOR_MAP.get(tenor_num)
                    if tenor:
                        records.append({
                            "auction_date": current_date,
                            "tenor": tenor,
                            "pib_type": "Fixed",
                            "coupon_rate": _parse_coupon(rejected_match.group(2)),
                            "amount_accepted_billions": None,
                            "cutoff_yield": None,
                        })

        return records
