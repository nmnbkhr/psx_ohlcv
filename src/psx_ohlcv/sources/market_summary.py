"""Market Summary .Z file downloader and parser.

Downloads daily bulk market summary files from PSX DPS in compressed format,
extracts them, and parses into structured data.

Source: https://dps.psx.com.pk/download/mkt_summary/{YYYY-MM-DD}.Z
Format: UNIX compress (.Z), pipe-delimited text
"""

import logging
import subprocess
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import requests

from ..config import DATA_ROOT
from ..http import create_session, fetch_url
from ..range_utils import format_date, iter_dates

logger = logging.getLogger(__name__)

MARKET_SUMMARY_URL_TEMPLATE = (
    "https://dps.psx.com.pk/download/mkt_summary/{date}.Z"
)

# =============================================================================
# Database Schema for Tracking Downloaded Dates
# =============================================================================

MARKET_SUMMARY_TRACKING_SCHEMA = """
-- Track downloaded market summary dates for retry logic
CREATE TABLE IF NOT EXISTS downloaded_market_summary_dates (
    date TEXT PRIMARY KEY,
    status TEXT NOT NULL,  -- 'ok', 'missing', 'failed', 'skipped'
    csv_path TEXT,
    raw_path TEXT,
    extracted_path TEXT,
    row_count INTEGER DEFAULT 0,
    message TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_market_summary_status
    ON downloaded_market_summary_dates(status);
"""


def init_market_summary_tracking(con) -> None:
    """Initialize market summary tracking table."""
    con.executescript(MARKET_SUMMARY_TRACKING_SCHEMA)
    con.commit()


def upsert_download_record(
    con,
    date_str: str,
    status: str,
    csv_path: str | None = None,
    raw_path: str | None = None,
    extracted_path: str | None = None,
    row_count: int = 0,
    message: str | None = None,
) -> None:
    """Upsert a download record into the tracking table.

    Args:
        con: Database connection
        date_str: Date in YYYY-MM-DD format
        status: 'ok', 'missing', 'failed', or 'skipped'
        csv_path: Path to saved CSV (if ok)
        raw_path: Path to raw .Z file (if keep_raw)
        extracted_path: Path to extracted .txt file (if keep_raw)
        row_count: Number of records in CSV
        message: Status message or error description
    """
    from ..models import now_iso

    con.execute(
        """
        INSERT INTO downloaded_market_summary_dates
            (date, status, csv_path, raw_path, extracted_path, row_count,
             message, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            status = excluded.status,
            csv_path = excluded.csv_path,
            raw_path = excluded.raw_path,
            extracted_path = excluded.extracted_path,
            row_count = excluded.row_count,
            message = excluded.message,
            updated_at = excluded.updated_at
        """,
        (date_str, status, csv_path, raw_path, extracted_path, row_count,
         message, now_iso()),
    )
    con.commit()


def get_dates_by_status(con, status: str) -> list[str]:
    """Get list of dates with a given status.

    Args:
        con: Database connection
        status: 'ok', 'missing', 'failed', or 'skipped'

    Returns:
        List of date strings (YYYY-MM-DD)
    """
    query = (
        "SELECT date FROM downloaded_market_summary_dates "
        "WHERE status = ? ORDER BY date"
    )
    cur = con.execute(query, (status,))
    return [row[0] for row in cur.fetchall()]


def get_failed_dates(con) -> list[str]:
    """Get list of dates with status='failed'."""
    return get_dates_by_status(con, "failed")


def get_missing_dates(con) -> list[str]:
    """Get list of dates with status='missing'."""
    return get_dates_by_status(con, "missing")


def get_download_record(con, date_str: str) -> dict | None:
    """Get tracking record for a specific date.

    Args:
        con: Database connection
        date_str: Date in YYYY-MM-DD format

    Returns:
        Dict with record fields, or None if not found
    """
    cur = con.execute(
        """
        SELECT date, status, csv_path, raw_path, extracted_path,
               row_count, message, updated_at
        FROM downloaded_market_summary_dates
        WHERE date = ?
        """,
        (date_str,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "date": row[0],
        "status": row[1],
        "csv_path": row[2],
        "raw_path": row[3],
        "extracted_path": row[4],
        "row_count": row[5],
        "message": row[6],
        "updated_at": row[7],
    }


def get_all_tracking_records(con, limit: int = 500) -> list[dict]:
    """Get all tracking records, most recent first.

    Args:
        con: Database connection
        limit: Maximum records to return

    Returns:
        List of tracking record dicts
    """
    cur = con.execute(
        """
        SELECT date, status, csv_path, raw_path, extracted_path,
               row_count, message, updated_at
        FROM downloaded_market_summary_dates
        ORDER BY date DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [
        {
            "date": row[0],
            "status": row[1],
            "csv_path": row[2],
            "raw_path": row[3],
            "extracted_path": row[4],
            "row_count": row[5],
            "message": row[6],
            "updated_at": row[7],
        }
        for row in cur.fetchall()
    ]


def get_tracking_stats(con) -> dict:
    """Get summary statistics for download tracking.

    Args:
        con: Database connection

    Returns:
        Dict with total, ok, missing, failed counts and date range
    """
    init_market_summary_tracking(con)

    cur = con.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as ok,
            SUM(CASE WHEN status = 'missing' THEN 1 ELSE 0 END) as missing,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            MIN(date) as min_date,
            MAX(date) as max_date,
            SUM(row_count) as total_rows
        FROM downloaded_market_summary_dates
        """
    )
    row = cur.fetchone()
    return {
        "total": row[0] or 0,
        "ok": row[1] or 0,
        "missing": row[2] or 0,
        "failed": row[3] or 0,
        "min_date": row[4],
        "max_date": row[5],
        "total_rows": row[6] or 0,
    }

# Column schema for market summary files (actual PSX format has 13 fields)
# Fields 10-12 appear to be empty/reserved
MARKET_SUMMARY_COLUMNS = [
    "date",
    "symbol",
    "sector_code",
    "company_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "prev_close",
]

# Raw columns from file (13 fields, last 3 usually empty)
RAW_COLUMNS = [
    "date",
    "symbol",
    "sector_code",
    "company_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "prev_close",
    "_reserved1",
    "_reserved2",
    "_reserved3",
]

# Numeric columns to convert
NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume", "prev_close"]


def download_market_summary(
    date: str,
    out_dir: Path | None = None,
    session: requests.Session | None = None,
) -> Path:
    """
    Download market summary .Z file for a given date.

    Args:
        date: Date string in YYYY-MM-DD format
        out_dir: Output directory. Defaults to DATA_ROOT / "market_summary"
        session: Optional requests Session

    Returns:
        Path to downloaded .Z file

    Raises:
        requests.RequestException: On download failure
        ValueError: If date format is invalid
    """
    # Validate date format
    _validate_date_format(date)

    if out_dir is None:
        out_dir = DATA_ROOT / "market_summary"

    # Create raw directory
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    if session is None:
        session = create_session()

    url = MARKET_SUMMARY_URL_TEMPLATE.format(date=date)
    response = fetch_url(session, url, polite=True)

    # Save .Z file
    z_path = raw_dir / f"{date}.Z"
    z_path.write_bytes(response.content)

    return z_path


def extract_z_file(z_path: Path) -> Path:
    """
    Extract a .Z compressed file using system uncompress.

    Args:
        z_path: Path to .Z file

    Returns:
        Path to extracted file (same name without .Z extension)

    Raises:
        FileNotFoundError: If .Z file doesn't exist
        RuntimeError: If uncompress command fails or is not available
    """
    if not z_path.exists():
        raise FileNotFoundError(f"File not found: {z_path}")

    if not z_path.suffix == ".Z":
        raise ValueError(f"Expected .Z file, got: {z_path}")

    # Expected output path (without .Z extension)
    extracted_path = z_path.with_suffix("")

    # Remove existing extracted file if present
    if extracted_path.exists():
        extracted_path.unlink()

    # Try uncompress command
    try:
        result = subprocess.run(
            ["uncompress", "-f", str(z_path)],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            # Try gzip as fallback (gzip can decompress .Z files)
            result = subprocess.run(
                ["gzip", "-d", "-f", str(z_path)],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to extract {z_path}. "
                    f"Neither 'uncompress' nor 'gzip -d' succeeded.\n"
                    f"Error: {result.stderr}"
                )

    except FileNotFoundError:
        raise RuntimeError(
            "Neither 'uncompress' nor 'gzip' command found. "
            "Install ncompress package: sudo apt install ncompress"
        )

    if not extracted_path.exists():
        raise RuntimeError(
            f"Extraction appeared to succeed but output file not found: "
            f"{extracted_path}"
        )

    return extracted_path


def parse_market_summary(
    file_path: Path, expected_date: str | None = None
) -> pd.DataFrame:
    """
    Parse extracted market summary file into DataFrame.

    Args:
        file_path: Path to extracted (decompressed) file
        expected_date: Optional date to validate against (YYYY-MM-DD)

    Returns:
        DataFrame with columns:
        date, symbol, sector_code, company_name, open, high, low, close,
        volume, prev_close

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Read file content
    content = file_path.read_text(encoding="utf-8", errors="replace")
    lines = content.strip().split("\n")

    if not lines:
        return _empty_market_summary_df()

    # Parse each line
    rows = []
    for line_num, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue

        # Split by pipe delimiter
        fields = line.split("|")

        # Handle variable field counts (10 or 13 fields)
        # Actual PSX files have 13 fields (last 3 are empty/reserved)
        if len(fields) < 10:
            # Not enough fields, skip
            continue

        # Take only first 10 fields
        fields = fields[:10]

        # Create row dict
        row = dict(zip(MARKET_SUMMARY_COLUMNS, fields))
        rows.append(row)

    if not rows:
        return _empty_market_summary_df()

    df = pd.DataFrame(rows)

    # Convert numeric columns
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean string columns
    df["symbol"] = df["symbol"].str.strip().str.upper()
    df["company_name"] = df["company_name"].str.strip()
    df["sector_code"] = df["sector_code"].str.strip()

    # Normalize date format from PSX format (20JAN2025) to YYYY-MM-DD
    df["date"] = df["date"].str.strip()
    df["date"] = pd.to_datetime(
        df["date"], format="mixed", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Convert volume to integer
    df["volume"] = df["volume"].astype("Int64")

    # Drop rows with missing critical fields
    df = df.dropna(subset=["symbol", "close"])

    # Classify market type (REG/FUT/CONT/IDX_FUT/ODL)
    df["market_type"] = df.apply(
        lambda row: classify_market_type(
            str(row.get("sector_code", "")), str(row["symbol"])
        ),
        axis=1,
    )

    # Sort by symbol
    df = df.sort_values("symbol").reset_index(drop=True)

    return df


def save_market_summary_csv(
    df: pd.DataFrame,
    date: str,
    out_dir: Path | None = None,
) -> Path:
    """
    Save parsed market summary as CSV.

    Args:
        df: DataFrame from parse_market_summary()
        date: Date string (YYYY-MM-DD) for filename
        out_dir: Output directory. Defaults to DATA_ROOT / "market_summary"

    Returns:
        Path to saved CSV file
    """
    if out_dir is None:
        out_dir = DATA_ROOT / "market_summary"

    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_path = csv_dir / f"{date}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")

    return csv_path


def process_market_summary(
    date: str,
    out_dir: Path | None = None,
    session: requests.Session | None = None,
    keep_raw: bool = False,
) -> tuple[pd.DataFrame, Path]:
    """
    Full pipeline: download, extract, parse, save CSV.

    Args:
        date: Date string in YYYY-MM-DD format
        out_dir: Output directory
        session: Optional requests Session
        keep_raw: If True, keep the extracted raw file

    Returns:
        Tuple of (DataFrame, CSV path)
    """
    # Download
    z_path = download_market_summary(date, out_dir, session)

    # Extract
    extracted_path = extract_z_file(z_path)

    try:
        # Parse
        df = parse_market_summary(extracted_path, expected_date=date)

        # Save CSV
        csv_path = save_market_summary_csv(df, date, out_dir)

        return df, csv_path

    finally:
        # Clean up extracted file unless requested to keep
        if not keep_raw and extracted_path.exists():
            extracted_path.unlink()


def _validate_date_format(date: str) -> None:
    """Validate date string format."""
    import re

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise ValueError(
            f"Invalid date format: {date}. Expected YYYY-MM-DD"
        )


def _empty_market_summary_df() -> pd.DataFrame:
    """Return empty DataFrame with correct schema."""
    return pd.DataFrame(columns=MARKET_SUMMARY_COLUMNS + ["market_type"])


# =============================================================================
# Date Range Fetching
# =============================================================================


def fetch_day(
    d: date | str,
    out_dir: Path | None = None,
    force: bool = False,
    session: requests.Session | None = None,
    keep_raw: bool = False,
) -> dict[str, Any]:
    """Fetch, extract, parse, and save market summary for a single day.

    Args:
        d: Date to fetch (date object or YYYY-MM-DD string)
        out_dir: Output directory (default: DATA_ROOT/market_summary/)
        force: If True, re-download even if CSV exists
        session: Optional requests Session for connection reuse
        keep_raw: If True, keep the extracted raw file

    Returns:
        Dict with keys: date, status, csv_path, raw_path, extracted_path,
                       row_count, message
        status is one of: "ok", "skipped", "missing", "failed"
    """
    # Convert to string if date object
    if isinstance(d, date):
        date_str = format_date(d)
    else:
        date_str = d

    if out_dir is None:
        out_dir = DATA_ROOT / "market_summary"

    csv_dir = out_dir / "csv"
    csv_path = csv_dir / f"{date_str}.csv"

    result: dict[str, Any] = {
        "date": date_str,
        "status": "failed",
        "csv_path": None,
        "raw_path": None,
        "extracted_path": None,
        "row_count": 0,
        "message": None,
    }

    # Check if already exists (skip unless force)
    if csv_path.exists() and not force:
        try:
            df = pd.read_csv(csv_path)
            result["row_count"] = len(df)
        except Exception:
            result["row_count"] = 0
        result["status"] = "skipped"
        result["csv_path"] = str(csv_path)
        result["message"] = f"Exists with {result['row_count']} records"
        logger.info(f"{date_str}: Skipped (exists, {result['row_count']} records)")
        return result

    z_path = None
    extracted_path = None

    try:
        # Download
        z_path = download_market_summary(date_str, out_dir, session)

        # Extract
        extracted_path = extract_z_file(z_path)

        # Parse
        df = parse_market_summary(extracted_path, expected_date=date_str)

        # Save CSV
        csv_path = save_market_summary_csv(df, date_str, out_dir)

        result["status"] = "ok"
        result["csv_path"] = str(csv_path)
        result["row_count"] = len(df)
        result["message"] = f"Downloaded {len(df)} records"
        logger.info(f"{date_str}: OK ({len(df)} records)")

        # Keep raw paths if requested
        if keep_raw:
            # Note: z_path no longer exists after extraction (uncompress removes it)
            # but extracted_path exists
            if extracted_path and extracted_path.exists():
                result["extracted_path"] = str(extracted_path)
        else:
            # Clean up extracted file
            if extracted_path and extracted_path.exists():
                extracted_path.unlink()

    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            result["status"] = "missing"
            result["message"] = "No data available (404)"
            logger.info(f"{date_str}: Not found (no trading day or holiday)")
        else:
            result["status"] = "failed"
            result["message"] = f"HTTP error: {e}"
            logger.error(f"{date_str}: HTTP error: {e}")
    except requests.RequestException as e:
        result["status"] = "failed"
        result["message"] = f"Download error: {e}"
        logger.error(f"{date_str}: Download error: {e}")
    except RuntimeError as e:
        result["status"] = "failed"
        result["message"] = f"Extraction error: {e}"
        logger.error(f"{date_str}: Extraction error: {e}")
    except Exception as e:
        result["status"] = "failed"
        result["message"] = f"Error: {e}"
        logger.error(f"{date_str}: Error: {e}")

    return result


def fetch_range(
    start: date,
    end: date,
    out_dir: Path | None = None,
    skip_weekends: bool = True,
    force: bool = False,
    session: requests.Session | None = None,
    keep_raw: bool = False,
) -> Iterator[dict[str, Any]]:
    """Fetch market summaries for a date range.

    Args:
        start: Start date (inclusive)
        end: End date (inclusive)
        out_dir: Output directory (default: DATA_ROOT/market_summary/)
        skip_weekends: If True, skip Saturday and Sunday
        force: If True, re-download even if CSV exists
        session: Optional requests Session for connection reuse
        keep_raw: If True, keep the extracted raw files

    Yields:
        Result dict for each date (see fetch_day)
    """
    if session is None:
        session = create_session()

    for d in iter_dates(start, end, skip_weekends=skip_weekends):
        yield fetch_day(d, out_dir, force=force, session=session, keep_raw=keep_raw)


def fetch_range_summary(
    start: date,
    end: date,
    out_dir: Path | None = None,
    skip_weekends: bool = True,
    force: bool = False,
    keep_raw: bool = False,
) -> dict[str, Any]:
    """Fetch market summaries for a date range and return summary.

    Args:
        start: Start date (inclusive)
        end: End date (inclusive)
        out_dir: Output directory
        skip_weekends: If True, skip Saturday and Sunday
        force: If True, re-download even if CSV exists
        keep_raw: If True, keep the extracted raw files

    Returns:
        Summary dict with keys: start, end, total, ok, skipped, not_found, errors
    """
    summary: dict[str, Any] = {
        "start": format_date(start),
        "end": format_date(end),
        "total": 0,
        "ok": 0,
        "skipped": 0,
        "missing": 0,
        "failed": [],
    }

    for result in fetch_range(
        start, end, out_dir, skip_weekends, force, keep_raw=keep_raw
    ):
        summary["total"] += 1
        status = result["status"]

        if status == "ok":
            summary["ok"] += 1
        elif status == "skipped":
            summary["skipped"] += 1
        elif status == "missing":
            summary["missing"] += 1
        else:
            summary["failed"].append({
                "date": result["date"],
                "message": result.get("message"),
            })

    return summary


def fetch_day_with_tracking(
    con,
    d: date | str,
    out_dir: Path | None = None,
    force: bool = False,
    session: requests.Session | None = None,
    keep_raw: bool = False,
    retry_failed: bool = False,
    retry_missing: bool = False,
) -> dict[str, Any]:
    """Fetch market summary for a day and record result in tracking table.

    Args:
        con: Database connection for tracking
        d: Date to fetch (date object or YYYY-MM-DD string)
        out_dir: Output directory
        force: If True, re-download even if CSV exists
        session: Optional requests Session
        keep_raw: If True, keep extracted raw file
        retry_failed: If True, retry dates with status='failed'
        retry_missing: If True, retry dates with status='missing'

    Returns:
        Result dict from fetch_day
    """
    # Ensure tracking table exists
    init_market_summary_tracking(con)

    # Convert to string if date object
    if isinstance(d, date):
        date_str = format_date(d)
    else:
        date_str = d

    # Check tracking record for skip logic
    existing = get_download_record(con, date_str)
    if existing and not force:
        if existing["status"] == "ok" and existing.get("csv_path"):
            csv_path = Path(existing["csv_path"])
            if csv_path.exists():
                # Already downloaded successfully, skip
                logger.info(f"{date_str}: Skipped (already ok)")
                return {
                    "date": date_str,
                    "status": "skipped",
                    "csv_path": str(csv_path),
                    "raw_path": existing.get("raw_path"),
                    "extracted_path": existing.get("extracted_path"),
                    "row_count": existing.get("row_count", 0),
                    "message": "Already downloaded",
                }
        elif existing["status"] == "missing" and not retry_missing:
            logger.info(f"{date_str}: Skipped (missing, use --retry-missing)")
            return {
                "date": date_str,
                "status": "skipped",
                "csv_path": None,
                "raw_path": None,
                "extracted_path": None,
                "row_count": 0,
                "message": "Previously missing (use --retry-missing to retry)",
            }
        elif existing["status"] == "failed" and not retry_failed:
            logger.info(f"{date_str}: Skipped (failed, use --retry-failed)")
            return {
                "date": date_str,
                "status": "skipped",
                "csv_path": None,
                "raw_path": None,
                "extracted_path": None,
                "row_count": 0,
                "message": "Previously failed (use --retry-failed to retry)",
            }

    # Fetch the day
    result = fetch_day(d, out_dir, force=force, session=session, keep_raw=keep_raw)

    # Record result in tracking table (don't track 'skipped' status)
    if result["status"] != "skipped":
        upsert_download_record(
            con,
            result["date"],
            result["status"],
            csv_path=result.get("csv_path"),
            raw_path=result.get("raw_path"),
            extracted_path=result.get("extracted_path"),
            row_count=result.get("row_count", 0),
            message=result.get("message"),
        )

    return result


def fetch_range_with_tracking(
    con,
    start: date,
    end: date,
    out_dir: Path | None = None,
    skip_weekends: bool = True,
    force: bool = False,
    keep_raw: bool = False,
    retry_failed: bool = False,
    retry_missing: bool = False,
) -> Iterator[dict[str, Any]]:
    """Fetch market summaries for a date range with tracking.

    Args:
        con: Database connection for tracking
        start: Start date (inclusive)
        end: End date (inclusive)
        out_dir: Output directory
        skip_weekends: If True, skip Saturday and Sunday
        force: If True, re-download even if CSV exists
        keep_raw: If True, keep extracted raw files
        retry_failed: If True, retry dates with status='failed'
        retry_missing: If True, retry dates with status='missing'

    Yields:
        Result dict for each date
    """
    # Ensure tracking table exists
    init_market_summary_tracking(con)

    session = create_session()
    for d in iter_dates(start, end, skip_weekends=skip_weekends):
        yield fetch_day_with_tracking(
            con, d, out_dir, force=force, session=session, keep_raw=keep_raw,
            retry_failed=retry_failed, retry_missing=retry_missing,
        )


def retry_failed_dates(
    con,
    out_dir: Path | None = None,
    keep_raw: bool = False,
) -> dict[str, Any]:
    """Retry downloading dates that previously failed with errors.

    Args:
        con: Database connection
        out_dir: Output directory
        keep_raw: If True, keep extracted raw files

    Returns:
        Summary dict with retry results
    """
    failed_dates = get_failed_dates(con)

    summary: dict[str, Any] = {
        "total": len(failed_dates),
        "ok": 0,
        "missing": 0,
        "still_failed": 0,
        "failed": [],
    }

    if not failed_dates:
        logger.info("No failed dates to retry")
        return summary

    logger.info(f"Retrying {len(failed_dates)} failed dates")
    session = create_session()

    for date_str in failed_dates:
        result = fetch_day_with_tracking(
            con, date_str, out_dir, force=True, session=session, keep_raw=keep_raw,
            retry_failed=True,
        )
        status = result["status"]

        if status == "ok":
            summary["ok"] += 1
        elif status == "missing":
            summary["missing"] += 1
        else:
            summary["still_failed"] += 1
            summary["failed"].append({
                "date": result["date"],
                "message": result.get("message"),
            })

    return summary


def retry_missing_dates(
    con,
    out_dir: Path | None = None,
    keep_raw: bool = False,
) -> dict[str, Any]:
    """Retry downloading dates that were previously not found (404).

    Useful if PSX adds historical data or fixes data availability.

    Args:
        con: Database connection
        out_dir: Output directory
        keep_raw: If True, keep extracted raw files

    Returns:
        Summary dict with retry results
    """
    missing_dates = get_missing_dates(con)

    summary: dict[str, Any] = {
        "total": len(missing_dates),
        "ok": 0,
        "still_missing": 0,
        "failed": [],
    }

    if not missing_dates:
        logger.info("No missing dates to retry")
        return summary

    logger.info(f"Retrying {len(missing_dates)} missing dates")
    session = create_session()

    for date_str in missing_dates:
        result = fetch_day_with_tracking(
            con, date_str, out_dir, force=True, session=session, keep_raw=keep_raw,
            retry_missing=True,
        )
        status = result["status"]

        if status == "ok":
            summary["ok"] += 1
        elif status == "missing":
            summary["still_missing"] += 1
        else:
            summary["failed"].append({
                "date": result["date"],
                "message": result.get("message"),
            })

    return summary


# =============================================================================
# Market Type Classification
# =============================================================================

_MONTH_CODES = {
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
}


def _extract_month(suffix: str) -> str | None:
    """Extract month code from a suffix, handling optional B-series trailing B.

    "FEB" → "FEB", "FEBB" → "FEB", "APR" → "APR", "APRB" → "APR",
    "XYZ" → None.
    """
    s = suffix.upper()
    if s in _MONTH_CODES:
        return s
    # B-series: strip one trailing B
    if s.endswith("B") and s[:-1] in _MONTH_CODES:
        return s[:-1]
    return None


def classify_market_type(sector_code: str, symbol: str) -> str:
    """Classify a market summary row as REG/FUT/CONT/IDX_FUT/ODL.

    Args:
        sector_code: Sector code from .Z file (e.g. "40", "0807", "36").
        symbol: Full symbol (e.g. "OGDC", "OGDC-FEB", "OGDC-CFEB").

    Returns:
        One of: 'REG', 'FUT', 'CONT', 'IDX_FUT', 'ODL'.
    """
    sc = str(sector_code).strip()
    if sc == "36":
        return "ODL"
    if sc == "41":
        return "IDX_FUT"
    if sc == "40":
        # CONT: symbol has -C followed by month code (OGDC-CFEB, OGDC-CAPRB)
        if "-C" in symbol:
            parts = symbol.rsplit("-C", 1)
            if len(parts) == 2 and _extract_month(parts[1]) is not None:
                return "CONT"
        return "FUT"
    return "REG"


def parse_futures_symbol(
    symbol: str, market_type: str
) -> tuple[str, str | None]:
    """Extract base_symbol and contract_month from a futures symbol.

    Args:
        symbol: Full symbol (e.g. "OGDC-FEB", "OGDC-CFEB", "P01GIS200826").
        market_type: One of FUT, CONT, IDX_FUT, ODL.

    Returns:
        (base_symbol, contract_month). contract_month is None for ODL/REG.
    """
    if market_type in ("ODL", "REG"):
        return symbol, None

    if market_type == "CONT" and "-C" in symbol:
        parts = symbol.rsplit("-C", 1)
        if len(parts) == 2:
            month = _extract_month(parts[1])
            return parts[0], month
        return symbol, None

    # FUT or IDX_FUT: OGDC-FEB, KSE30-FEB, OGDC-FEBB
    if "-" in symbol:
        parts = symbol.rsplit("-", 1)
        month = _extract_month(parts[1])
        return parts[0], month

    return symbol, None


# =============================================================================
# Post-Close Turnover
# =============================================================================

POST_CLOSE_URL_TEMPLATE = (
    "https://dps.psx.com.pk/download/post_close/{date}.Z"
)


def download_post_close(
    d: date | str,
    session: requests.Session | None = None,
) -> bytes | None:
    """Download post_close ZIP for a date. Returns raw bytes or None."""
    date_str = d if isinstance(d, str) else format_date(d)
    url = POST_CLOSE_URL_TEMPLATE.format(date=date_str)
    sess = session or create_session()
    try:
        resp = sess.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 100:
            return resp.content
        logger.warning("post_close %s: HTTP %s (%d bytes)", date_str, resp.status_code, len(resp.content))
    except Exception as e:
        logger.warning("post_close %s download failed: %s", date_str, e)
    return None


def parse_post_close(raw_zip: bytes, date_str: str) -> list[dict]:
    """Parse post_close ZIP content into list of {symbol, date, volume, turnover}.

    The .Z file is actually a ZIP containing a pipe-delimited text file:
        symbol|company_name|volume|turnover|*
    """
    import io
    import zipfile

    records = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw_zip))
        for name in zf.namelist():
            text = zf.read(name).decode("utf-8", errors="replace")
            for line in text.strip().split("\n"):
                parts = line.split("|")
                if len(parts) < 4:
                    continue
                symbol = parts[0].strip()
                if not symbol:
                    continue
                try:
                    volume = int(parts[2].strip()) if parts[2].strip() else 0
                    turnover = float(parts[3].strip()) if parts[3].strip() else 0.0
                except (ValueError, TypeError):
                    continue
                records.append({
                    "symbol": symbol,
                    "date": date_str,
                    "company_name": parts[1].strip() if len(parts) > 1 else None,
                    "volume": volume,
                    "turnover": turnover,
                })
    except (zipfile.BadZipFile, Exception) as e:
        logger.error("parse_post_close failed: %s", e)
    return records


def fetch_post_close(
    d: date | str,
    con,
    session: requests.Session | None = None,
    save_raw: bool = True,
) -> dict[str, Any]:
    """Download post_close turnover, store in post_close_turnover table,
    and sync turnover column to eod_ohlcv/futures_eod.

    Returns dict with keys: date, status, stored, eod_updated, futures_updated, total_records.
    """
    from psx_ohlcv.db.repositories.post_close import upsert_post_close

    date_str = d if isinstance(d, str) else format_date(d)
    result = {
        "date": date_str,
        "status": "failed",
        "stored": 0,
        "eod_updated": 0,
        "futures_updated": 0,
        "total_records": 0,
    }

    raw = download_post_close(d, session=session)
    if raw is None:
        result["status"] = "missing"
        return result

    # Save raw file
    if save_raw:
        raw_dir = DATA_ROOT / "market_summary" / "post_close"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{date_str}.zip").write_bytes(raw)

    records = parse_post_close(raw, date_str)
    result["total_records"] = len(records)

    if not records:
        result["status"] = "empty"
        return result

    # 1. Store in dedicated post_close_turnover table
    result["stored"] = upsert_post_close(con, records)

    # 2. Sync turnover to eod_ohlcv and futures_eod
    eod_ok = 0
    fut_ok = 0
    for r in records:
        cur = con.execute(
            "UPDATE eod_ohlcv SET turnover = ? WHERE symbol = ? AND date = ?",
            (r["turnover"], r["symbol"], date_str),
        )
        eod_ok += cur.rowcount

        cur = con.execute(
            "UPDATE futures_eod SET turnover = ? WHERE symbol = ? AND date = ?",
            (r["turnover"], r["symbol"], date_str),
        )
        fut_ok += cur.rowcount

    con.commit()
    result["eod_updated"] = eod_ok
    result["futures_updated"] = fut_ok
    result["status"] = "ok"
    logger.info(
        "post_close %s: %d records stored, synced eod=%d futures=%d",
        date_str, len(records), eod_ok, fut_ok,
    )
    return result
