"""SBP rates scraper — PKRV yield curve, KONIA, KIBOR from PMA page.

The PMA page (sbp.org.pk/dfmd/pma.asp) provides:
  - SBP Policy Rate
  - Overnight Weighted Average Repo Rate (proxy for KONIA)
  - KIBOR rates (3M, 6M, 12M bid/offer)
  - MTB auction cutoff yields (1M, 3M, 6M, 12M)
  - PIB auction cutoff yields (2Y, 3Y, 5Y, 10Y)

We construct a PKRV-like yield curve from MTB + PIB auction yields.
"""

import re
import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.yield_curves import (
    get_konia_history,
    get_latest_konia,
    get_pkrv_curve,
    init_yield_curve_schema,
    upsert_kibor_rate,
    upsert_konia_rate,
    upsert_pkrv_point,
)

__all__ = ["SBPRatesScraper"]

PMA_URL = "https://www.sbp.org.pk/dfmd/pma.asp"
TIMEOUT = 30

# Map tenor labels to months for yield curve construction
TENOR_TO_MONTHS = {
    "1M": 1, "3M": 3, "6M": 6, "12M": 12,
    "2Y": 24, "3Y": 36, "5Y": 60, "10Y": 120,
    "15Y": 180, "20Y": 240, "30Y": 360,
}


class SBPRatesScraper:
    """Scrapes rates data from SBP PMA page for yield curve construction."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_all_rates(self) -> dict:
        """Scrape all rates from PMA page.

        Returns dict with:
          - overnight_rate: float (weighted avg repo rate)
          - overnight_date: str
          - policy_rate: float
          - kibor: list of {tenor, bid, offer, date}
          - yield_curve: list of {tenor_months, yield_pct, source, date}
        """
        result: dict = {
            "overnight_rate": None,
            "overnight_date": None,
            "policy_rate": None,
            "kibor": [],
            "yield_curve": [],
        }

        try:
            resp = self.session.get(PMA_URL, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [!] Failed to fetch PMA page: {e}")
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # Extract overnight rate (KONIA proxy)
        result.update(self._extract_overnight_rate(text))

        # Extract KIBOR
        result["kibor"] = self._extract_kibor(text)

        # Extract policy rate
        result["policy_rate"] = self._extract_policy_rate(text)

        # Build yield curve from MTB + PIB auction cutoffs
        # Use today's date as the curve date since the curve represents
        # "latest known yields" from different auction dates
        result["yield_curve"] = self._build_yield_curve(
            text, curve_date=datetime.now().strftime("%Y-%m-%d")
        )

        return result

    def sync_rates(self, con: sqlite3.Connection) -> dict:
        """Scrape and sync all rates to DB.

        Returns counts: {konia_ok, kibor_ok, pkrv_points, failed}
        """
        init_yield_curve_schema(con)
        rates = self.scrape_all_rates()

        counts = {
            "konia_ok": False,
            "kibor_ok": 0,
            "pkrv_points": 0,
            "failed": 0,
        }

        # Sync overnight rate (KONIA proxy)
        if rates["overnight_rate"] and rates["overnight_date"]:
            ok = upsert_konia_rate(con, {
                "date": rates["overnight_date"],
                "rate_pct": rates["overnight_rate"],
            })
            counts["konia_ok"] = ok
            if not ok:
                counts["failed"] += 1

        # Sync KIBOR
        for kibor in rates["kibor"]:
            if upsert_kibor_rate(con, kibor):
                counts["kibor_ok"] += 1
            else:
                counts["failed"] += 1

        # Sync yield curve
        for point in rates["yield_curve"]:
            if upsert_pkrv_point(con, point):
                counts["pkrv_points"] += 1
            else:
                counts["failed"] += 1

        return counts

    def get_summary(self, con: sqlite3.Connection) -> dict:
        """Get summary of stored rates data."""
        init_yield_curve_schema(con)
        konia = get_latest_konia(con)
        curve = get_pkrv_curve(con)
        konia_df = get_konia_history(con)
        return {
            "latest_konia": konia,
            "curve_points": len(curve),
            "konia_days": len(konia_df),
        }

    # ── extractors ───────────────────────────────────────────────

    @staticmethod
    def _extract_overnight_rate(text: str) -> dict:
        """Extract overnight weighted average repo rate."""
        result: dict = {"overnight_rate": None, "overnight_date": None}

        # Pattern: "Weighted-average Overnight Repo Rate" ... "11.16% p.a."
        # Also try: "Overnight Weighted Average Rate" or just number near "Overnight"
        match = re.search(
            r"(?:Weighted[-\s]average\s+)?Overnight\s+(?:Repo\s+)?Rate[^%]*?(\d+\.?\d*)\s*%",
            text, re.IGNORECASE,
        )
        if match:
            try:
                rate = float(match.group(1))
                if 0 < rate < 50:
                    result["overnight_rate"] = rate
            except ValueError:
                pass

        # Extract date near "As on DD-Mon-YY" before overnight section
        as_on = re.search(
            r"[Aa]s\s+on\s+(\d{1,2}[-/]\w{3}[-/]\d{2,4})",
            text[:text.find("Overnight") if "Overnight" in text else 500],
        )
        if as_on:
            result["overnight_date"] = _try_parse_date(as_on.group(1))

        if not result["overnight_date"]:
            result["overnight_date"] = datetime.now().strftime("%Y-%m-%d")

        return result

    @staticmethod
    def _extract_kibor(text: str) -> list[dict]:
        """Extract KIBOR rates (bid/offer) for each tenor."""
        kibor_rates: list[dict] = []

        # Find KIBOR section
        kibor_match = re.search(r"KIBOR", text, re.IGNORECASE)
        if not kibor_match:
            return kibor_rates

        section = text[kibor_match.end():kibor_match.end() + 500]

        # Extract date near KIBOR
        date_match = re.search(
            r"[Aa]s\s+on\s+(\d{1,2}[-/]\w{3}[-/]\d{2,4})",
            text[max(0, kibor_match.start() - 100):kibor_match.end() + 200],
        )
        kibor_date = (
            _try_parse_date(date_match.group(1))
            if date_match
            else datetime.now().strftime("%Y-%m-%d")
        )

        # Pattern: "3-M  10.26  10.51" (tenor bid offer)
        tenor_map = {"3-M": "3M", "6-M": "6M", "12-M": "12M"}
        for label, canonical in tenor_map.items():
            pat = re.escape(label) + r"\s+(\d+\.?\d*)\s+(\d+\.?\d*)"
            m = re.search(pat, section)
            if m:
                try:
                    kibor_rates.append({
                        "date": kibor_date,
                        "tenor": canonical,
                        "bid": float(m.group(1)),
                        "offer": float(m.group(2)),
                    })
                except ValueError:
                    pass

        return kibor_rates

    @staticmethod
    def _extract_policy_rate(text: str) -> float | None:
        """Extract SBP policy rate."""
        match = re.search(
            r"(?:SBP\s+)?Policy\s+Rate[^%]*?(\d+\.?\d*)\s*%",
            text, re.IGNORECASE,
        )
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        return None

    @staticmethod
    def _build_yield_curve(
        text: str, curve_date: str | None = None
    ) -> list[dict]:
        """Build yield curve from MTB + PIB auction cutoff yields.

        Uses the MTB "Cut-off Yield" section and PIB "Fixed-rate" section
        from the PMA page to construct a yield curve. All points use the
        same curve_date since they represent latest known yields.
        """
        if not curve_date:
            curve_date = datetime.now().strftime("%Y-%m-%d")

        curve: list[dict] = []
        seen: set[int] = set()

        # Extract MTB cutoff yields
        cutoff_match = re.search(r"Cut-off\s+Yield", text, re.IGNORECASE)
        if cutoff_match:
            mtb_section = text[cutoff_match.end():cutoff_match.end() + 400]

            tenor_map = {"1-M": 1, "3-M": 3, "6-M": 6, "12-M": 12}
            for label, months in tenor_map.items():
                pat = re.escape(label) + r"\s+(\d+\.?\d*)\s*%?"
                m = re.search(pat, mtb_section)
                if m and months not in seen:
                    try:
                        rate = float(m.group(1))
                        if 0 < rate < 50:
                            curve.append({
                                "date": curve_date,
                                "tenor_months": months,
                                "yield_pct": rate,
                                "source": "MTB Auction",
                            })
                            seen.add(months)
                    except ValueError:
                        pass

        # Extract PIB Fixed Rate cutoff yields
        pib_match = re.search(
            r"Fixed[-\s]*rate\s+PIB", text, re.IGNORECASE
        )
        if pib_match:
            pib_section = text[pib_match.end():pib_match.end() + 400]

            tenor_map = {
                "2-Y": 24, "3-Y": 36, "5-Y": 60, "10-Y": 120,
                "15-Y": 180, "20-Y": 240, "30-Y": 360,
            }
            for label, months in tenor_map.items():
                pat = re.escape(label) + r"\s+(\d+\.?\d*)\s*%?"
                m = re.search(pat, pib_section)
                if m and months not in seen:
                    try:
                        rate = float(m.group(1))
                        if 0 < rate < 20:  # PIB yields typically 5-15%
                            curve.append({
                                "date": curve_date,
                                "tenor_months": months,
                                "yield_pct": rate,
                                "source": "PIB Auction",
                            })
                            seen.add(months)
                    except ValueError:
                        pass

        return sorted(curve, key=lambda x: x["tenor_months"])


def _try_parse_date(text: str) -> str | None:
    """Try to parse a date string into YYYY-MM-DD format."""
    if not text:
        return None
    text = text.strip()
    formats = [
        "%B %d, %Y", "%B %d,%Y", "%d-%b-%Y", "%d/%b/%Y",
        "%Y-%m-%d", "%Y/%m/%d", "%b %d, %Y", "%d %B %Y",
        "%d-%b-%y",  # 04-Feb-26 (2-digit year)
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            # Handle 2-digit years
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            if 2000 <= dt.year <= 2030:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None
