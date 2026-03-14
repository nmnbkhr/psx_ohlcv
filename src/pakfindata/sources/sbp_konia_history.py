"""SBP KONIA (Overnight Repo Rate) Historical PDF scraper.

Downloads and parses the SBP Overnight Repo Rates archive PDF to backfill
daily KONIA rates from 2015-present in one shot.

Sources:
  Archive:  sbp.org.pk/ecodata/OvernightsRepoRates_Arch2.pdf  (2015-present)
  Current:  sbp.org.pk/ecodata/overnightsreporates2.pdf        (today only)

Each page has 3 side-by-side columns of (date, rate%) pairs.
"""

import io
import json
import logging
import re
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path

import pdfplumber
import requests

from pakfindata.config import DATA_ROOT
from pakfindata.db.connection import connect, init_schema
from pakfindata.db.repositories.yield_curves import (
    init_yield_curve_schema,
    upsert_konia_rate,
)

__all__ = [
    "SBPKoniaHistoryScraper",
    "start_konia_history_sync",
    "is_konia_history_sync_running",
    "read_konia_sync_progress",
]

ARCHIVE_URL = "https://www.sbp.org.pk/ecodata/OvernightsRepoRates_Arch2.pdf"
CURRENT_URL = "https://www.sbp.org.pk/ecodata/overnightsreporates2.pdf"
TIMEOUT = 60

KONIA_SYNC_PROGRESS_FILE = DATA_ROOT / "konia_sync_progress.json"

log = logging.getLogger("pakfindata.sbp_konia_history")

# Date formats found in the PDF (varies across pages)
_DATE_PATTERNS = [
    # "25-May, 2015" or "01-June, 2015"
    (re.compile(r"(\d{1,2})-(\w+),?\s*(\d{4})"), "%d-%B-%Y"),
    # "25 May 2015" or "1 September 2020"
    (re.compile(r"(\d{1,2})\s+(\w+)\s+(\d{4})"), "%d %B %Y"),
    # "25-May-2015"
    (re.compile(r"(\d{1,2})-(\w+)-(\d{4})"), "%d-%B-%Y"),
    # "06-MAR-26" (short year)
    (re.compile(r"(\d{1,2})-([A-Z]{3})-(\d{2})$"), "%d-%b-%y"),
]


def _parse_date(raw: str) -> str | None:
    """Parse varied SBP date formats into YYYY-MM-DD."""
    raw = raw.strip().rstrip(".")
    if not raw:
        return None

    for pattern, fmt in _DATE_PATTERNS:
        m = pattern.match(raw)
        if m:
            matched_str = m.group(0).replace(",", "")
            # Normalize spacing
            matched_str = re.sub(r"\s+", " ", matched_str).strip()
            try:
                dt = datetime.strptime(matched_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

    # Fallback: try dateutil
    try:
        from dateutil import parser as _dp
        dt = _dp.parse(raw, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def _parse_rate(raw: str) -> float | None:
    """Parse rate string to float."""
    raw = raw.strip().rstrip("%")
    try:
        val = float(raw)
        if 0 < val < 50:  # sanity check
            return val
    except (ValueError, TypeError):
        pass
    return None


class SBPKoniaHistoryScraper:
    """Scrapes historical daily KONIA rates from SBP archive PDF."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

    def download_archive(self) -> bytes:
        """Download the KONIA archive PDF."""
        resp = self.session.get(ARCHIVE_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def download_current(self) -> bytes:
        """Download the single-day current rate PDF."""
        resp = self.session.get(CURRENT_URL, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def parse_archive_pdf(self, pdf_bytes: bytes) -> list[dict]:
        """Parse all (date, rate) pairs from the archive PDF.

        Returns list of {"date": "YYYY-MM-DD", "rate_pct": float}.
        """
        records: list[dict] = []
        seen: set[str] = set()

        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 2:
                        continue
                    date_str = _parse_date(str(row[0] or ""))
                    rate = _parse_rate(str(row[1] or ""))
                    if date_str and rate and date_str not in seen:
                        records.append({"date": date_str, "rate_pct": rate})
                        seen.add(date_str)

            # Also try text extraction for rows that tables miss
            text = page.extract_text()
            if text:
                for line in text.split("\n"):
                    # Match patterns like "25-May, 2015 6.58"
                    parts = line.rsplit(None, 1)
                    if len(parts) == 2:
                        date_str = _parse_date(parts[0])
                        rate = _parse_rate(parts[1])
                        if date_str and rate and date_str not in seen:
                            records.append({"date": date_str, "rate_pct": rate})
                            seen.add(date_str)

        pdf.close()
        records.sort(key=lambda r: r["date"])
        return records

    def parse_current_pdf(self, pdf_bytes: bytes) -> dict | None:
        """Parse single-day rate from current PDF."""
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.split("\n"):
                parts = line.rsplit(None, 1)
                if len(parts) == 2:
                    date_str = _parse_date(parts[0])
                    rate = _parse_rate(parts[1])
                    if date_str and rate:
                        pdf.close()
                        return {"date": date_str, "rate_pct": rate}
        pdf.close()
        return None

    def sync_konia_history(
        self,
        con: sqlite3.Connection,
        pdf_bytes: bytes | None = None,
    ) -> dict:
        """Download archive, parse, and upsert all KONIA rates.

        Returns {"total_parsed": int, "inserted": int, "skipped": int, "failed": int}.
        """
        init_yield_curve_schema(con)

        if pdf_bytes is None:
            log.info("Downloading KONIA archive PDF...")
            pdf_bytes = self.download_archive()

        log.info("Parsing KONIA archive PDF (%d bytes)...", len(pdf_bytes))
        records = self.parse_archive_pdf(pdf_bytes)
        log.info("Parsed %d KONIA records", len(records))

        # Also fetch today's rate
        try:
            current_bytes = self.download_current()
            current = self.parse_current_pdf(current_bytes)
            if current and current["date"] not in {r["date"] for r in records}:
                records.append(current)
        except Exception as e:
            log.warning("Could not fetch current KONIA rate: %s", e)

        inserted = 0
        skipped = 0
        failed = 0
        for rec in records:
            try:
                if upsert_konia_rate(con, rec):
                    inserted += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        return {
            "total_parsed": len(records),
            "inserted": inserted,
            "skipped": skipped,
            "failed": failed,
        }

    def sync_konia_history_with_progress(
        self,
        db_path: Path | str | None = None,
    ) -> None:
        """Backfill with progress file for UI polling."""
        con = connect(db_path)
        init_schema(con)
        init_yield_curve_schema(con)

        progress = {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "total_records": 0,
            "inserted": 0,
            "failed": 0,
            "phase": "downloading",
        }
        _write_progress(progress)

        try:
            pdf_bytes = self.download_archive()
            progress["phase"] = "parsing"
            _write_progress(progress)

            records = self.parse_archive_pdf(pdf_bytes)
            progress["total_records"] = len(records)
            progress["phase"] = "upserting"
            _write_progress(progress)

            # Also fetch today's rate
            try:
                current_bytes = self.download_current()
                current = self.parse_current_pdf(current_bytes)
                if current and current["date"] not in {r["date"] for r in records}:
                    records.append(current)
                    progress["total_records"] = len(records)
            except Exception:
                pass

            for i, rec in enumerate(records):
                try:
                    if upsert_konia_rate(con, rec):
                        progress["inserted"] += 1
                    else:
                        progress["failed"] += 1
                except Exception:
                    progress["failed"] += 1

                if i % 50 == 0:
                    _write_progress(progress)

        except Exception as e:
            progress["status"] = "error"
            progress["error"] = str(e)
            _write_progress(progress)
            log.exception("KONIA history sync failed")
            con.close()
            return

        progress["status"] = "completed"
        progress["finished_at"] = datetime.now().isoformat()
        _write_progress(progress)
        con.close()

        log.info(
            "KONIA history sync complete: %d records inserted",
            progress["inserted"],
        )


# ── Progress tracking ──────────────────────────────────────────

def _write_progress(data: dict) -> None:
    """Write progress dict to JSON file atomically."""
    try:
        KONIA_SYNC_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = KONIA_SYNC_PROGRESS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(KONIA_SYNC_PROGRESS_FILE)
    except OSError:
        pass


def read_konia_sync_progress() -> dict | None:
    """Read current KONIA sync progress."""
    if not KONIA_SYNC_PROGRESS_FILE.exists():
        return None
    try:
        return json.loads(KONIA_SYNC_PROGRESS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ── Background thread support ──────────────────────────────────

_konia_sync_thread: threading.Thread | None = None


def start_konia_history_sync(
    db_path: Path | str | None = None,
) -> bool:
    """Launch KONIA history sync in a background thread.

    Returns True if started, False if already running.
    """
    global _konia_sync_thread
    if _konia_sync_thread is not None and _konia_sync_thread.is_alive():
        return False

    scraper = SBPKoniaHistoryScraper()
    _konia_sync_thread = threading.Thread(
        target=scraper.sync_konia_history_with_progress,
        kwargs={"db_path": db_path},
        daemon=True,
        name="konia-history-sync",
    )
    _konia_sync_thread.start()
    return True


def is_konia_history_sync_running() -> bool:
    """Check if KONIA history sync is running."""
    return _konia_sync_thread is not None and _konia_sync_thread.is_alive()
