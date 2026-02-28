"""SBP secondary bond market data scraper.

BENCHMARK SOURCE:
  https://www.sbp.org.pk/DFMD/msm.asp
  Sidebar has: SBP policy rate, ceiling/floor, KIBOR, MTB cutoffs,
  PIB cutoffs, overnight repo, FX reserves. Scraped daily as a
  snapshot of current benchmark rates.

SMTV PDF SOURCE:
  https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf
  Daily OTC bond trading volume: MTBs, PIBs, GIS with face value,
  realized value, min/max/weighted-avg yields, broken down by
  interbank + bank-to-nonbank. Parsed with pdfplumber.
"""

import io
import logging
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pdfplumber
import requests
from bs4 import BeautifulSoup

from pakfindata.db.repositories.bond_market import (
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

    # ── SMTV PDF parsing ────────────────────────────────────────

    # Security type extracted from the PDF "Securities" column text.
    _SECURITY_TYPE_MAP = {
        "Market Treasury Bills-(MTB)": "MTB",
        "Variable Rental Rate Ijara Sukuk-(GISVRR)": "GISVRR",
        "Fixed Rental Rate GoP Ijara Sukuk-(GISFRR)": "GISFRR",
        "Fixed-Rate Pakistan Investment Bond-(PIB)": "PIB",
        "Floating-rate PIBs (Half-yearly Coupon Reset)-(PFL)": "PFL",
        "Discounted Pakistan Investment Bond-(PIB DIS)": "PIB_DIS",
        "Discounted Ijara Sukuk-(GIS DIS)": "GIS_DIS",
    }

    # MTB maturity labels → tenor bucket codes
    _MTB_TENOR_MAP = {
        "(A) Upto 14 Days": "0-14D",
        "(B) 15-91 Days": "15-91D",
        "(C) 92-182 Days": "92-182D",
        "(D) 183-364 Days": "183-364D",
    }

    # Header text → segment key
    _SEGMENT_MAP = {
        "Purchase Interbank Market": "interbank",
        "Banks Outright Purchases from Non Banks": "bank_nonbank_purchase",
        "Banks Outright Sales to Non Banks": "bank_nonbank_sale",
    }

    def scrape_daily_smtv(self) -> dict:
        """Download and parse today's Outright SMTV PDF.

        Returns:
            {
                'date': '2026-02-25',
                'trades': [{'security_type', 'tenor_bucket', 'maturity_year',
                            'segment', 'face_amount', 'realized_amount',
                            'yield_min', 'yield_max', 'yield_weighted_avg'}, ...],
                'totals': {'interbank': {'face': N, 'realized': N}, ...},
                'error': None | str
            }
        """
        result: dict = {
            "date": None,
            "trades": [],
            "totals": {},
            "error": None,
        }

        # Download PDF
        try:
            resp = self.session.get(SMTV_URL, timeout=TIMEOUT)
            if resp.status_code == 404:
                result["error"] = "SMTV PDF not available (404)"
                logger.warning("SMTV PDF at %s returns 404", SMTV_URL)
                return result
            resp.raise_for_status()
        except Exception as e:
            result["error"] = str(e)
            logger.error("Failed to download SMTV PDF: %s", e)
            return result

        pdf_bytes = resp.content
        if len(pdf_bytes) < 500:
            result["error"] = f"SMTV PDF too small ({len(pdf_bytes)} bytes)"
            return result

        # Extract date and tables from PDF
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                # Extract date from page 1 text
                page1_text = pdf.pages[0].extract_text() or ""
                result["date"] = self._extract_smtv_date(page1_text)

                # Collect all tables across all pages
                all_tables = []
                for page in pdf.pages:
                    tables = page.extract_tables()
                    all_tables.extend(tables)

        except Exception as e:
            result["error"] = f"PDF parsing failed: {e}"
            logger.error("pdfplumber error on SMTV PDF: %s", e)
            return result

        if not result["date"]:
            result["error"] = "Could not extract date from SMTV PDF"
            return result

        # Parse tables into trades
        for table in all_tables:
            self._parse_smtv_table(table, result)

        # Archive raw PDF
        self._archive_smtv_pdf(pdf_bytes, result["date"])

        logger.info(
            "SMTV parsed: %s — %d trades, totals: %s",
            result["date"], len(result["trades"]),
            {k: f"{v.get('face', 0):,.0f}M" for k, v in result["totals"].items()},
        )
        return result

    @staticmethod
    def _extract_smtv_date(text: str) -> str | None:
        """Extract date from 'Value Date As on February 25, 2026'."""
        m = re.search(
            r"Value\s+Date\s+As\s+on\s+(\w+\s+\d{1,2},?\s+\d{4})",
            text, re.IGNORECASE,
        )
        if not m:
            return None
        try:
            raw = m.group(1).replace(",", "")
            dt = datetime.strptime(raw, "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    # Track last segment across tables for page-break continuations
    _last_segment: str | None = None

    def _parse_smtv_table(self, table: list[list], result: dict) -> None:
        """Parse a single pdfplumber table into trades and totals."""
        if not table or len(table) < 2:
            return

        # Join all cells of first row to build header (handles split headers)
        header_parts = [str(c or "").replace("\n", " ").strip() for c in table[0]]
        header_text = " ".join(p for p in header_parts if p)

        # Check for grand totals table (page 2)
        if "Total Interbank" in header_text or "Grand Total" in header_text:
            self._parse_totals_table(table, result)
            return

        segment = None
        for pattern, seg_key in self._SEGMENT_MAP.items():
            if pattern.lower() in header_text.lower():
                segment = seg_key
                break

        if not segment:
            # Page-break continuation: use last segment (e.g. sales continued on page 2)
            if self._last_segment and self._match_security_type(header_text):
                segment = self._last_segment
                # No header rows to skip — data starts at row 0
                self._parse_data_rows(table, 0, segment, result)
                return
            return

        self._last_segment = segment

        # Skip header rows (typically 3: section title, column headers, sub-headers)
        self._parse_data_rows(table, 3, segment, result)

    def _parse_data_rows(
        self, table: list[list], start_row: int, segment: str, result: dict
    ) -> None:
        """Parse data rows from an SMTV table."""
        current_security = None
        for row in table[start_row:]:
            if not row or len(row) < 7:
                continue

            cell0 = (row[0] or "").replace("\n", " ").strip()
            cell1 = (row[1] or "").strip()
            cell2 = (row[2] or "").strip()

            # Skip empty rows
            if not cell1 and not cell2:
                continue

            # Check for "Total:" subtotal row
            if "total" in cell0.lower() or "total" in cell1.lower():
                face = self._parse_amount(cell2 if cell2 else (row[3] or ""))
                realized = self._parse_amount((row[3] or "") if cell2 else (row[4] or ""))
                if face and face > 0:
                    result["totals"][segment] = {
                        "face": face,
                        "realized": realized or 0,
                    }
                continue

            # Determine security type
            if cell0:
                matched_type = self._match_security_type(cell0)
                if matched_type:
                    current_security = matched_type

            if not current_security:
                continue

            # Parse maturity/tenor
            maturity_year = 0
            tenor_bucket = ""
            if current_security == "MTB":
                tenor_bucket = self._match_mtb_tenor(cell1) or ""
                if not tenor_bucket and cell0:
                    tenor_bucket = self._match_mtb_tenor(cell0) or ""
            else:
                year_match = re.search(r"\b(20\d{2})\b", cell1)
                if not year_match and cell0:
                    year_match = re.search(r"\b(20\d{2})\b", cell0)
                if year_match:
                    maturity_year = int(year_match.group(1))

            # Skip rows without identifiable maturity/tenor
            if current_security == "MTB" and not tenor_bucket:
                continue
            if current_security != "MTB" and maturity_year == 0:
                continue

            face = self._parse_amount(row[2])
            realized = self._parse_amount(row[3])
            yield_min = self._parse_yield(row[4])
            yield_max = self._parse_yield(row[5])
            yield_avg = self._parse_yield(row[6])

            if face is None and realized is None:
                continue

            result["trades"].append({
                "date": result["date"],
                "security_type": current_security,
                "maturity_year": maturity_year,
                "tenor_bucket": tenor_bucket,
                "segment": segment,
                "face_amount": face,
                "realized_amount": realized,
                "yield_min": yield_min,
                "yield_max": yield_max,
                "yield_weighted_avg": yield_avg,
            })

    @staticmethod
    def _parse_totals_table(table: list[list], result: dict) -> None:
        """Parse the grand totals table on page 2."""
        for row in table:
            if not row or len(row) < 2:
                continue
            label = (row[0] or "").replace("\n", " ").strip().lower()
            val_str = (row[1] or "").replace(",", "").strip()
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                continue

            if "interbank" in label:
                result["totals"]["interbank"] = {
                    "face": val,
                    "realized": val,
                }
            elif "non-bank" in label or "non bank" in label:
                result["totals"]["bank_nonbank"] = {
                    "face": val,
                    "realized": val,
                }
            elif "grand total" in label:
                result["totals"]["grand_total"] = {
                    "face": val,
                    "realized": val,
                }

    # Map the ticker code suffix like -(PFL), -(PIB), -(MTB) etc.
    _TICKER_CODE_MAP = {
        "MTB": "MTB",
        "GISVRR": "GISVRR",
        "GISFRR": "GISFRR",
        "PIB": "PIB",
        "PFL": "PFL",
        "PIB DIS": "PIB_DIS",
        "GIS DIS": "GIS_DIS",
    }

    @classmethod
    def _match_security_type(cls, text: str) -> str | None:
        """Match security type from cell text."""
        # Normalize: replace newlines, collapse whitespace
        clean = re.sub(r"\s+", " ", text.replace("\n", " ")).strip()

        # Primary: extract ticker code from "-(XXX)" suffix (most reliable)
        ticker_match = re.search(r"-\(([A-Z\s]+)\)\s*$", clean)
        if ticker_match:
            ticker = ticker_match.group(1).strip()
            if ticker in cls._TICKER_CODE_MAP:
                return cls._TICKER_CODE_MAP[ticker]

        # Fallback: keyword matching
        lower = clean.lower()
        if "treasury bills" in lower or "-(mtb)" in lower:
            return "MTB"
        if "variable rental" in lower and "gisvrr" in lower:
            return "GISVRR"
        if "fixed rental" in lower and ("gisfrr" in lower or "gop ijara" in lower):
            return "GISFRR"
        if "floating" in lower and ("pfl" in lower or "coupon reset" in lower):
            return "PFL"
        if "discounted" in lower and "pib" in lower:
            return "PIB_DIS"
        if "discounted" in lower and ("gis" in lower or "ijara" in lower or "sukuk" in lower):
            return "GIS_DIS"
        if "investment bond" in lower and "pib" in lower:
            return "PIB"
        return None

    @classmethod
    def _match_mtb_tenor(cls, text: str) -> str | None:
        """Match MTB tenor bucket from text."""
        for pattern, code in cls._MTB_TENOR_MAP.items():
            if pattern.lower() in text.lower():
                return code
        # Fallback regex patterns
        lower = text.lower()
        if "upto 14" in lower or "up to 14" in lower:
            return "0-14D"
        if "15-91" in lower or "15 to 91" in lower:
            return "15-91D"
        if "92-182" in lower or "92 to 182" in lower:
            return "92-182D"
        if "183-364" in lower or "183 to 364" in lower:
            return "183-364D"
        return None

    @staticmethod
    def _parse_amount(val: str | None) -> float | None:
        """Parse a PKR amount string like '78,623.03'."""
        if not val:
            return None
        clean = val.replace(",", "").strip()
        if not clean:
            return None
        try:
            return float(clean)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_yield(val: str | None) -> float | None:
        """Parse a yield percentage string."""
        if not val:
            return None
        clean = val.replace(",", "").replace("%", "").strip()
        if not clean:
            return None
        try:
            y = float(clean)
            return y if 0 < y < 50 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _archive_smtv_pdf(pdf_bytes: bytes, date_str: str) -> None:
        """Save raw PDF for archival."""
        try:
            archive_dir = Path("/mnt/e/psxdata/smtv")
            archive_dir.mkdir(parents=True, exist_ok=True)
            path = archive_dir / f"{date_str}.pdf"
            if not path.exists():
                path.write_bytes(pdf_bytes)
                logger.debug("Archived SMTV PDF: %s", path)
        except Exception as e:
            logger.debug("Could not archive SMTV PDF: %s", e)

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
