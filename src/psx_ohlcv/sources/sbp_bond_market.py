"""SBP secondary bond market data scraper.

BENCHMARK SOURCE (primary — working):
  https://www.sbp.org.pk/DFMD/msm.asp
  Sidebar has: SBP policy rate, ceiling/floor, KIBOR, MTB cutoffs,
  PIB cutoffs, overnight repo, FX reserves. Scraped daily as a
  snapshot of current benchmark rates.

SMTV PDF SOURCE (currently 404 — stubbed for future use):
  https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf
  Daily OTC bond trading volume: MTBs, PIBs, GIS with face value,
  realized value, min/max/weighted-avg yields, broken down by
  interbank + bank-to-nonbank.

ARCHIVE SOURCES (PDFs removed — 404 as of Feb 2026):
  https://www.sbp.org.pk/DFMD/SecMarBankArc.asp
  https://www.sbp.org.pk/DFMD/SecMarNonBankArc.asp
"""

import logging
import re
import sqlite3
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from psx_ohlcv.db.repositories.bond_market import (
    init_bond_market_schema,
    upsert_benchmark,
    upsert_bond_trade,
    upsert_trading_summary,
)

logger = logging.getLogger(__name__)

__all__ = ["SBPBondMarketScraper"]

MSM_URL = "https://www.sbp.org.pk/DFMD/msm.asp"
SMTV_URL = "https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf"
TIMEOUT = 30


class SBPBondMarketScraper:
    """Scrapes SBP bond market data — benchmark rates + OTC trading volume."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    # ── Benchmark snapshot (from MSM sidebar) ──────────────────

    def scrape_benchmark_snapshot(self) -> dict:
        """Scrape current benchmark rates from sbp.org.pk/DFMD/msm.asp.

        Returns dict with 'date', 'metrics': {name: value, ...}
        """
        result: dict = {"date": None, "metrics": {}, "error": None}

        try:
            resp = self.session.get(MSM_URL, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            result["error"] = str(e)
            logger.error("Failed to fetch MSM page: %s", e)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()
        today = date.today().isoformat()
        result["date"] = today

        # 1. Policy rate corridor
        self._extract_policy_rates(text, result["metrics"])

        # 2. Overnight repo rate
        self._extract_overnight_rate(text, result["metrics"])

        # 3. KIBOR rates (from sidebar tables)
        self._extract_kibor_rates(soup, result["metrics"])

        # 4. MTB cutoff yields
        self._extract_mtb_yields(soup, result["metrics"])

        # 5. PIB cutoff yields (fixed)
        self._extract_pib_yields(soup, result["metrics"])

        # 6. FX reserves
        self._extract_reserves(soup, result["metrics"])

        # 7. FX M2M rate
        self._extract_fx_rate(soup, result["metrics"])

        logger.info(
            "Benchmark snapshot: %d metrics for %s",
            len(result["metrics"]), today,
        )
        return result

    def sync_benchmark(self, con: sqlite3.Connection) -> dict:
        """Scrape and sync benchmark snapshot to DB."""
        init_bond_market_schema(con)
        snapshot = self.scrape_benchmark_snapshot()

        if snapshot["error"]:
            return {"status": "error", "error": snapshot["error"]}

        stored = 0
        for metric, value in snapshot["metrics"].items():
            if value is not None:
                ok = upsert_benchmark(con, {
                    "date": snapshot["date"],
                    "metric": metric,
                    "value": value,
                })
                if ok:
                    stored += 1

        con.commit()
        return {
            "status": "ok",
            "date": snapshot["date"],
            "metrics_stored": stored,
            "metrics_total": len(snapshot["metrics"]),
        }

    # ── SMTV PDF (currently 404 — stubbed) ─────────────────────

    def scrape_daily_smtv(self) -> dict:
        """Download and parse today's Outright SMTV PDF.

        NOTE: As of Feb 2026 the SMTV PDF URL returns 404.
        This method is stubbed and will return an error status.
        When the URL becomes available again, implement PDF parsing
        with pdfplumber.
        """
        result: dict = {
            "date": None,
            "trades": [],
            "totals": {},
            "error": None,
        }

        try:
            resp = self.session.head(SMTV_URL, timeout=10)
            if resp.status_code == 404:
                result["error"] = "SMTV PDF not available (404)"
                logger.warning("SMTV PDF at %s returns 404", SMTV_URL)
                return result
        except Exception as e:
            result["error"] = str(e)
            return result

        # TODO: when PDF becomes available:
        # 1. Download PDF with self.session.get(SMTV_URL)
        # 2. Parse with pdfplumber
        # 3. Extract date from header
        # 4. Parse interbank + bank-to-nonbank sections
        # 5. Extract per-security rows
        result["error"] = "SMTV PDF parsing not yet implemented"
        return result

    def sync_smtv(self, con: sqlite3.Connection) -> dict:
        """Scrape and sync daily SMTV to DB."""
        init_bond_market_schema(con)
        data = self.scrape_daily_smtv()

        if data["error"]:
            return {"status": "skipped", "reason": data["error"]}

        trades_stored = 0
        for trade in data.get("trades", []):
            if upsert_bond_trade(con, trade):
                trades_stored += 1

        summaries_stored = 0
        for seg, totals in data.get("totals", {}).items():
            if upsert_trading_summary(con, {
                "date": data["date"],
                "segment": seg,
                "total_face_amount": totals.get("face"),
                "total_realized_amount": totals.get("realized"),
            }):
                summaries_stored += 1

        con.commit()
        return {
            "status": "ok",
            "date": data["date"],
            "trades_stored": trades_stored,
            "summaries_stored": summaries_stored,
        }

    # ── Extractors ─────────────────────────────────────────────

    @staticmethod
    def _extract_policy_rates(text: str, metrics: dict) -> None:
        """Extract SBP policy rate corridor."""
        m = re.search(
            r"(?:SBP\s+)?Policy\s*Rate\s*(\d+\.?\d*)\s*%",
            text, re.IGNORECASE,
        )
        if m:
            metrics["policy_rate"] = float(m.group(1))

        m = re.search(
            r"Reverse\s*Repo\s*\(?\s*Ceiling\s*\)?\s*Rate\s*(\d+\.?\d*)\s*%",
            text, re.IGNORECASE,
        )
        if m:
            metrics["ceiling_rate"] = float(m.group(1))

        m = re.search(
            r"(?:Overnight\s+)?Repo\s*\(?\s*Floor\s*\)?\s*Rate\s*(\d+\.?\d*)\s*%",
            text, re.IGNORECASE,
        )
        if m:
            metrics["floor_rate"] = float(m.group(1))

    @staticmethod
    def _extract_overnight_rate(text: str, metrics: dict) -> None:
        """Extract weighted-average overnight repo rate."""
        m = re.search(
            r"Weighted[-\s]*average\s+Overnight\s+Repo\s+Rate"
            r".*?(\d+\.?\d*)\s*%",
            text, re.IGNORECASE | re.DOTALL,
        )
        if m:
            rate = float(m.group(1))
            if 0 < rate < 50:
                metrics["overnight_repo"] = rate

    @staticmethod
    def _extract_kibor_rates(soup: BeautifulSoup, metrics: dict) -> None:
        """Extract KIBOR bid/offer using regex on text."""
        text = soup.get_text()
        # Find the KIBOR data section (near "As on DD-Mon-YY"), not the
        # nav link "Daily KIBOR (Archive)"
        kibor_match = re.search(
            r"KIBOR\s*As\s+on", text, re.IGNORECASE
        )
        if not kibor_match:
            return
        section = text[kibor_match.end():kibor_match.end() + 300]
        tenor_map = {"3-M": "3m", "6-M": "6m", "12-M": "12m"}
        for label, key in tenor_map.items():
            # Values may be newline-separated: "3-M\n10.33\n10.58"
            pat = re.escape(label) + r"\s+(\d+\.?\d*)\s+(\d+\.?\d*)"
            m = re.search(pat, section)
            if m:
                try:
                    metrics[f"kibor_{key}_bid"] = float(m.group(1))
                    metrics[f"kibor_{key}_offer"] = float(m.group(2))
                except ValueError:
                    pass

    @staticmethod
    def _extract_mtb_yields(soup: BeautifulSoup, metrics: dict) -> None:
        """Extract MTB cutoff yields using regex on text."""
        text = soup.get_text()
        # Find "Cut-off Yield" section (MTBs only)
        cutoff_match = re.search(r"Cut-off\s+Yield", text, re.IGNORECASE)
        if not cutoff_match:
            return
        section = text[cutoff_match.end():cutoff_match.end() + 300]
        tenor_map = {"1-M": "1m", "3-M": "3m", "6-M": "6m", "12-M": "12m"}
        for label, key in tenor_map.items():
            pat = re.escape(label) + r"\s+(\d+\.?\d*)\s*%?"
            m = re.search(pat, section)
            if m:
                try:
                    val = float(m.group(1))
                    if 0 < val < 50:
                        metrics[f"mtb_{key}"] = val
                except ValueError:
                    pass

    @staticmethod
    def _extract_pib_yields(soup: BeautifulSoup, metrics: dict) -> None:
        """Extract fixed-rate PIB cutoff yields using regex on text.

        Must find the "Fixed-rate PIB" section specifically to avoid
        matching Floating PIB prices or GIS rental rates.
        """
        text = soup.get_text()
        pib_match = re.search(r"Fixed[-\s]*rate\s+PIB", text, re.IGNORECASE)
        if not pib_match:
            return
        # Take a limited section to avoid running into Floating PIB
        section = text[pib_match.end():pib_match.end() + 400]
        # Stop at "Floating" to prevent cross-section pollution
        floating_idx = section.lower().find("floating")
        if floating_idx > 0:
            section = section[:floating_idx]

        tenor_map = {
            "2-Y": "2y", "3-Y": "3y", "5-Y": "5y",
            "10-Y": "10y", "15-Y": "15y",
        }
        for label, key in tenor_map.items():
            pat = re.escape(label) + r"\s+(\d+\.?\d*)\s*%?"
            m = re.search(pat, section)
            if m:
                try:
                    val = float(m.group(1))
                    # PIB yields are typically 5-20%; prices > 50 are not yields
                    if 0 < val < 20:
                        metrics[f"pib_{key}"] = val
                except ValueError:
                    pass

    @staticmethod
    def _extract_reserves(soup: BeautifulSoup, metrics: dict) -> None:
        """Extract FX reserves from sidebar."""
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) >= 2:
                    label = cells[0].lower().replace("\u2019", "'")
                    val = cells[1].replace(",", "").strip()
                    try:
                        num = float(val)
                    except ValueError:
                        continue
                    if "sbp" in label and "reserve" in label:
                        metrics["sbp_reserves_m_usd"] = num
                    elif "bank" in label and "reserve" in label:
                        metrics["bank_reserves_m_usd"] = num
                    elif "total" in label and "reserve" in label:
                        metrics["total_reserves_m_usd"] = num

    @staticmethod
    def _extract_fx_rate(soup: BeautifulSoup, metrics: dict) -> None:
        """Extract M2M revaluation rate from sidebar."""
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) >= 2:
                    label = cells[0].lower()
                    val = cells[1].replace(",", "").strip()
                    if "revaluation" in label and "rate" in label:
                        try:
                            metrics["fx_m2m_rate"] = float(val)
                        except ValueError:
                            pass
                    elif "bid" in label:
                        try:
                            bid_val = val.replace("Bid:", "").strip()
                            metrics["fx_wa_bid"] = float(bid_val)
                        except ValueError:
                            pass
                    elif "offer" in label:
                        try:
                            offer_val = val.replace("Offer:", "").strip()
                            metrics["fx_wa_offer"] = float(offer_val)
                        except ValueError:
                            pass
