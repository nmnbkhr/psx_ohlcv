"""Naya Pakistan Certificate (NPC) rate scraper.

Scrapes SBP-published conventional NPC rates for USD, GBP, EUR, PKR
across 5 tenors (3M, 6M, 12M, 3Y, 5Y).

Primary source: https://www.sbp.org.pk/NPC-/page-npc.html
NPC rates are NOT daily market rates — they change when GoP issues a new SRO.
"""

import logging
from datetime import date

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

TENORS = ["3M", "6M", "12M", "3Y", "5Y"]

# Map lowercase text found in currency label rows to ISO codes
CURRENCY_MAP = {
    "usd": "USD",
    "pkr": "PKR",
    "gbp": "GBP",
    "euro": "EUR",
    "eur": "EUR",
}

SBP_NPC_URL = "https://www.sbp.org.pk/NPC-/page-npc.html"


class NPCRatesScraper:
    """Scrape Naya Pakistan Certificate rates from SBP website."""

    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "PSX-OHLCV-Research/3.5"})

    def scrape_sbp(self) -> list[dict]:
        """Scrape NPC rates from SBP NPC page.

        SBP table structure (as of Feb 2026):
            Row 0: tenor headers (3M, 6M, 12M, 3Y, 5Y)
            Row 1: "USD (%, Annualized)"  (1 cell — currency label)
            Row 2: 5.50, 5.50, 5.75, ...   (5 cells — rate values)
            Row 3: "PKR (%, Annualized)"
            Row 4: rates ...
            ... and so on for GBP, Euro
        """
        logger.info("Fetching NPC rates from SBP: %s", SBP_NPC_URL)
        resp = self.session.get(SBP_NPC_URL, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the rate table — the one containing "Annualized"
        rate_table = None
        for table in soup.find_all("table"):
            if "annualized" in table.get_text().lower():
                rate_table = table
                break

        if not rate_table:
            logger.error("Could not find NPC rate table on SBP page")
            return []

        rows = rate_table.find_all("tr")
        results = []
        today = date.today().isoformat()
        current_currency = None

        for row in rows:
            cells = row.find_all(["td", "th"])
            text = [c.get_text(strip=True) for c in cells]
            if not text:
                continue

            # Single-cell row = currency label (e.g. "USD (%, Annualized)")
            if len(cells) == 1:
                label = text[0].lower()
                for key, code in CURRENCY_MAP.items():
                    if key in label and "annualized" in label:
                        current_currency = code
                        break
                continue

            # 5-cell row after a currency label = rate values
            if current_currency and len(text) >= 5:
                parsed = []
                for val in text[:5]:
                    try:
                        parsed.append(float(val.replace(",", "").strip()))
                    except (ValueError, TypeError):
                        break

                if len(parsed) == 5 and all(0 < r < 100 for r in parsed):
                    for tenor, rate_val in zip(TENORS, parsed):
                        results.append({
                            "date": today,
                            "currency": current_currency,
                            "tenor": tenor,
                            "rate": rate_val,
                            "certificate_type": "conventional",
                            "source": "sbp",
                        })
                    current_currency = None  # consumed

        logger.info("Parsed %d NPC rate records from SBP", len(results))
        return results

    def scrape(self) -> list[dict]:
        """Scrape NPC rates (SBP primary, no fallback yet)."""
        try:
            return self.scrape_sbp()
        except Exception as e:
            logger.error("NPC rate scrape failed: %s", e)
            return []

    def sync(self, con, force: bool = False) -> int:
        """Scrape and store NPC rates. Returns count of rows written."""
        from psx_ohlcv.db.repositories.npc_rates import (
            ensure_tables, upsert_npc_rate, rates_changed,
        )

        ensure_tables(con)
        rates = self.scrape()
        if not rates:
            logger.warning("No NPC rates scraped")
            return 0

        if not force and not rates_changed(con, rates):
            logger.info("NPC rates unchanged — skipping insert")
            return 0

        ok = 0
        for r in rates:
            if upsert_npc_rate(
                con,
                date=r["date"],
                currency=r["currency"],
                tenor=r["tenor"],
                rate=r["rate"],
                certificate_type=r.get("certificate_type", "conventional"),
                effective_date=r.get("effective_date"),
                sro_reference=r.get("sro_reference"),
                source=r.get("source", "sbp"),
            ):
                ok += 1
        con.commit()
        logger.info("Stored %d NPC rate records", ok)
        return ok
