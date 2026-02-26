"""SBP KIBOR Historical PDF scraper.

Downloads and parses daily KIBOR PDFs from SBP (2008-present).
URL pattern: sbp.org.pk/ecodata/kibor/{YYYY}/{Mon}/Kibor-{DD}-{Mon}-{YY}.pdf

Each PDF contains a single table with:
  Tenor | Bid | Offer
for tenors: 1W, 2W, 1M, 3M, 6M, 9M, 1Y (+ 2Y, 3Y in older data).

Designed for incremental backfill with progress tracking (MUFAP pattern).
"""

import io
import json
import logging
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pdfplumber
import requests

from pakfindata.config import DATA_ROOT
from pakfindata.db.connection import connect, init_schema
from pakfindata.db.repositories.yield_curves import (
    init_yield_curve_schema,
    upsert_kibor_rate,
)

__all__ = [
    "SBPKiborHistoryScraper",
    "start_kibor_history_sync",
    "is_kibor_history_sync_running",
    "read_kibor_sync_progress",
]

KIBOR_BASE_URL = "https://www.sbp.org.pk/ecodata/kibor"
TIMEOUT = 30
REQUEST_DELAY = 0.3  # seconds between requests (be polite)

KIBOR_SYNC_PROGRESS_FILE = DATA_ROOT / "kibor_sync_progress.json"

log = logging.getLogger("pakfindata.sbp_kibor_history")

# Month name mapping for URL construction
MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# Tenor mapping from PDF labels to canonical names
TENOR_MAP = {
    "1 - Week": "1W", "1 -Week": "1W", "1-Week": "1W",
    "2 - Week": "2W", "2 -Week": "2W", "2-Week": "2W",
    "1 - Month": "1M", "1 -Month": "1M", "1-Month": "1M",
    "3 - Month": "3M", "3 -Month": "3M", "3-Month": "3M",
    "6 - Month": "6M", "6 -Month": "6M", "6-Month": "6M",
    "9 - Month": "9M", "9 -Month": "9M", "9-Month": "9M",
    "1 - Year": "1Y", "1 -Year": "1Y", "1-Year": "1Y",
    "2 - Year": "2Y", "2 -Year": "2Y", "2-Year": "2Y",
    "3 - Year": "3Y", "3 -Year": "3Y", "3-Year": "3Y", "3- Year": "3Y",
}


def _build_pdf_url(d: date) -> str:
    """Build the KIBOR PDF URL for a given date."""
    mon = MONTH_NAMES[d.month - 1]
    yy = d.strftime("%y")
    return f"{KIBOR_BASE_URL}/{d.year}/{mon}/Kibor-{d.day}-{mon}-{yy}.pdf"


def _business_days(start: date, end: date) -> list[date]:
    """Generate business days (Mon-Fri) between start and end inclusive."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon=0, Fri=4
            days.append(current)
        current += timedelta(days=1)
    return days


def _get_stored_dates(con: sqlite3.Connection) -> set[str]:
    """Get all dates already in kibor_daily table."""
    rows = con.execute("SELECT DISTINCT date FROM kibor_daily").fetchall()
    return {row[0] if isinstance(row, tuple) else row["date"] for row in rows}


class SBPKiborHistoryScraper:
    """Scrapes historical daily KIBOR PDFs from SBP."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/3.0)",
        })

    def scrape_kibor_pdf(self, d: date) -> list[dict]:
        """Download and parse a single day's KIBOR PDF.

        Returns list of dicts: {date, tenor, bid, offer}
        """
        url = _build_pdf_url(d)
        date_str = d.strftime("%Y-%m-%d")

        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            if resp.status_code == 404:
                return []  # No PDF for this date (holiday, etc.)
            resp.raise_for_status()
        except requests.RequestException:
            return []

        return self._parse_pdf(resp.content, date_str)

    @staticmethod
    def _parse_pdf(pdf_bytes: bytes, date_str: str) -> list[dict]:
        """Parse KIBOR PDF bytes into rate records."""
        records: list[dict] = []

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        except Exception:
            return records

        if not pdf.pages:
            pdf.close()
            return records

        page = pdf.pages[0]
        tables = page.extract_tables()

        if tables and len(tables[0]) > 1:
            for row in tables[0]:
                if len(row) < 3:
                    continue
                tenor_raw = (row[0] or "").strip()
                tenor = TENOR_MAP.get(tenor_raw)
                if not tenor:
                    continue
                try:
                    bid = float(row[1])
                    offer = float(row[2])
                except (ValueError, TypeError):
                    continue

                records.append({
                    "date": date_str,
                    "tenor": tenor,
                    "bid": bid,
                    "offer": offer,
                })

        pdf.close()
        return records

    def sync_kibor_history(
        self,
        con: sqlite3.Connection,
        start_year: int = 2008,
        end_date: date | None = None,
        incremental: bool = True,
    ) -> dict:
        """Backfill KIBOR history from daily PDFs.

        Args:
            con: SQLite connection
            start_year: Start year for backfill
            end_date: End date (default: today)
            incremental: If True, skip dates already in DB

        Returns counts: {dates_processed, records_inserted, skipped, failed, total_days}
        """
        init_yield_curve_schema(con)

        start = date(start_year, 1, 1)
        end = end_date or date.today()
        all_days = _business_days(start, end)

        # Get existing dates to skip
        existing: set[str] = set()
        if incremental:
            existing = _get_stored_dates(con)

        counts = {
            "dates_processed": 0,
            "records_inserted": 0,
            "skipped": 0,
            "failed": 0,
            "total_days": len(all_days),
        }

        for d in all_days:
            date_str = d.strftime("%Y-%m-%d")
            if incremental and date_str in existing:
                counts["skipped"] += 1
                continue

            records = self.scrape_kibor_pdf(d)
            if records:
                for rec in records:
                    if upsert_kibor_rate(con, rec):
                        counts["records_inserted"] += 1
                    else:
                        counts["failed"] += 1
                counts["dates_processed"] += 1
            else:
                counts["skipped"] += 1  # Holiday / no PDF

            time.sleep(REQUEST_DELAY)

        return counts

    def sync_kibor_history_with_progress(
        self,
        db_path: Path | str | None = None,
        start_year: int = 2008,
        end_date: date | None = None,
    ) -> None:
        """Backfill with progress file for UI polling. Runs in foreground."""
        con = connect(db_path)
        init_schema(con)
        init_yield_curve_schema(con)

        start = date(start_year, 1, 1)
        end = end_date or date.today()
        all_days = _business_days(start, end)
        existing = _get_stored_dates(con)

        progress = {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "total_days": len(all_days),
            "current": 0,
            "dates_processed": 0,
            "records_inserted": 0,
            "skipped": 0,
            "failed": 0,
            "current_date": "",
            "errors": [],
        }
        _write_progress(progress)

        for i, d in enumerate(all_days):
            date_str = d.strftime("%Y-%m-%d")
            progress["current"] = i + 1
            progress["current_date"] = date_str

            if date_str in existing:
                progress["skipped"] += 1
                if i % 100 == 0:
                    _write_progress(progress)
                continue

            try:
                records = self.scrape_kibor_pdf(d)
                if records:
                    for rec in records:
                        if upsert_kibor_rate(con, rec):
                            progress["records_inserted"] += 1
                        else:
                            progress["failed"] += 1
                    progress["dates_processed"] += 1
                else:
                    progress["skipped"] += 1
            except Exception as e:
                progress["failed"] += 1
                progress["errors"].append(f"{date_str}: {e}")
                progress["errors"] = progress["errors"][-20:]
                log.exception("Error scraping KIBOR for %s", date_str)

            if i % 10 == 0:
                _write_progress(progress)

            time.sleep(REQUEST_DELAY)

        progress["status"] = "completed"
        progress["finished_at"] = datetime.now().isoformat()
        _write_progress(progress)
        con.close()

        log.info(
            "KIBOR history sync complete: %d dates, %d records",
            progress["dates_processed"],
            progress["records_inserted"],
        )


# ── Progress tracking (MUFAP pattern) ──────────────────────────

def _write_progress(data: dict) -> None:
    """Write progress dict to JSON file atomically."""
    try:
        KIBOR_SYNC_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = KIBOR_SYNC_PROGRESS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(KIBOR_SYNC_PROGRESS_FILE)
    except OSError:
        pass


def read_kibor_sync_progress() -> dict | None:
    """Read current KIBOR sync progress. Returns None if no job has run."""
    if not KIBOR_SYNC_PROGRESS_FILE.exists():
        return None
    try:
        return json.loads(KIBOR_SYNC_PROGRESS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ── Background thread support ───────────────────────────────────

_kibor_sync_thread: threading.Thread | None = None


def start_kibor_history_sync(
    db_path: Path | str | None = None,
    start_year: int = 2008,
) -> bool:
    """Launch KIBOR history sync in a background thread.

    Returns True if started, False if already running.
    """
    global _kibor_sync_thread
    if _kibor_sync_thread is not None and _kibor_sync_thread.is_alive():
        return False

    scraper = SBPKiborHistoryScraper()
    _kibor_sync_thread = threading.Thread(
        target=scraper.sync_kibor_history_with_progress,
        kwargs={"db_path": db_path, "start_year": start_year},
        daemon=True,
        name="kibor-history-sync",
    )
    _kibor_sync_thread.start()
    return True


def is_kibor_history_sync_running() -> bool:
    """Check if KIBOR history sync is running."""
    return _kibor_sync_thread is not None and _kibor_sync_thread.is_alive()
