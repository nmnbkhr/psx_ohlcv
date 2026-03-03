"""Closing Rates PDF parser for PSX data.

Downloads and parses daily closing rates PDF files from PSX DPS.
This serves as a fallback when market_summary .Z files are corrupted or empty.

Source: https://dps.psx.com.pk/download/closing_rates/{YYYY-MM-DD}.pdf
Format: Multi-page PDF with tabular data
"""

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ..config import DATA_ROOT
from ..http import create_session, fetch_url
from ..range_utils import format_date

logger = logging.getLogger(__name__)

CLOSING_RATES_URL_TEMPLATE = (
    "https://dps.psx.com.pk/download/closing_rates/{date}.pdf"
)

# Columns for parsed data
CLOSING_RATES_COLUMNS = [
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "prev_close",
]


def download_closing_rates_pdf(
    date_str: str,
    out_dir: Path | None = None,
    session: requests.Session | None = None,
) -> Path:
    """Download closing rates PDF for a given date.

    Args:
        date_str: Date string in YYYY-MM-DD format
        out_dir: Output directory. Defaults to DATA_ROOT / "closing_rates"
        session: Optional requests Session

    Returns:
        Path to downloaded PDF file

    Raises:
        requests.RequestException: On download failure
    """
    if out_dir is None:
        out_dir = DATA_ROOT / "closing_rates"

    pdf_dir = out_dir / "pdf"
    pdf_dir.mkdir(parents=True, exist_ok=True)

    if session is None:
        session = create_session()

    url = CLOSING_RATES_URL_TEMPLATE.format(date=date_str)
    response = fetch_url(session, url, polite=True)

    # Check if response is actually a PDF
    if len(response.content) < 1000:
        raise ValueError(f"Response too small to be a valid PDF: {len(response.content)} bytes")

    pdf_path = pdf_dir / f"{date_str}.pdf"
    pdf_path.write_bytes(response.content)

    return pdf_path


def parse_closing_rates_pdf(pdf_path: Path, expected_date: str | None = None) -> pd.DataFrame:
    """Parse closing rates PDF into DataFrame.

    Args:
        pdf_path: Path to PDF file
        expected_date: Optional date to add to records (YYYY-MM-DD)

    Returns:
        DataFrame with columns: symbol, open, high, low, close, volume, prev_close
        If expected_date provided, also includes 'date' column

    Raises:
        FileNotFoundError: If PDF file doesn't exist
        ImportError: If pdfplumber not installed
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber package not installed. Install with: pip install pdfplumber"
        )

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    records = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.split('\n')
            for line in lines:
                record = _parse_line(line)
                if record:
                    records.append(record)

    if not records:
        return _empty_closing_rates_df(expected_date)

    df = pd.DataFrame(records)

    # Clean up
    df['symbol'] = df['symbol'].str.upper().str.strip()
    df = df.drop_duplicates(subset=['symbol'], keep='first')
    df = df.sort_values('symbol').reset_index(drop=True)

    # Add date if provided
    if expected_date:
        df['date'] = expected_date

    return df


def _parse_line(line: str) -> dict | None:
    """Parse a single line from the PDF.

    Expected format: SYMBOL CompanyName Volume PrevClose Open High Low Close Change

    Args:
        line: Text line from PDF

    Returns:
        Dict with parsed values or None if line doesn't match expected format
    """
    # Skip header and section lines
    skip_patterns = [
        'Pakistan Stock Exchange',
        'CLOSING RATE',
        'PageNo',
        'Company Name',
        'Turnover',
        '***',
        'P. Vol',
        'C. Vol',
        'Total',
        'Net Change',
        'From :',
        'Flu No',
    ]

    if any(pattern in line for pattern in skip_patterns):
        return None

    parts = line.split()
    if len(parts) < 8:
        return None

    try:
        symbol = parts[0]

        # Handle PDF extraction quirk: symbol+company merged (e.g., "LOTCHEM-CFEBLOTCHEM-CFEB")
        # Try to extract the actual symbol from doubled text
        if len(symbol) > 10 and '-' in symbol:
            # Check if it's a doubled symbol (e.g., "ABC-JANABC-JAN")
            half_len = len(symbol) // 2
            first_half = symbol[:half_len]
            second_half = symbol[half_len:half_len + len(first_half)]
            if first_half == second_half:
                symbol = first_half

        # Skip if symbol looks invalid
        # Note: Allow numeric symbols like "786" (valid PSX symbol)
        # Only skip single/double digit numbers (likely page numbers)
        if not symbol or '/' in symbol or len(symbol) > 12:
            return None
        if symbol.isdigit() and len(symbol) <= 2:
            return None

        # Skip date-like patterns
        if re.match(r'\d{4}-\d{2}-\d{2}', symbol):
            return None

        # Extract numeric values from the end of the line
        # Format: ... Volume PrevClose Open High Low Close Change
        # Note: "-" means no value (use None as placeholder, will be filled later)
        numeric_vals = []
        for p in reversed(parts):
            if p == '-':
                numeric_vals.insert(0, None)  # Placeholder for missing value
                if len(numeric_vals) == 7:
                    break
            else:
                try:
                    val = float(p.replace(',', ''))
                    numeric_vals.insert(0, val)
                    if len(numeric_vals) == 7:  # Volume + 6 numeric fields
                        break
                except ValueError:
                    if numeric_vals:  # Stop if we hit non-numeric after finding numbers
                        break

        if len(numeric_vals) < 6:
            return None

        # Parse values: volume, prev_close, open, high, low, close, change
        if len(numeric_vals) >= 7:
            volume = int(numeric_vals[0]) if numeric_vals[0] is not None else 0
            prev_close = numeric_vals[1]
            open_price = numeric_vals[2]
            high = numeric_vals[3]
            low = numeric_vals[4]
            close = numeric_vals[5]
            # change = numeric_vals[6]  # Not used

            # Fill missing high/low with open price (no intraday movement)
            if high is None:
                high = open_price if open_price is not None else close
            if low is None:
                low = open_price if open_price is not None else close
            if open_price is None:
                open_price = close
        else:
            return None

        return {
            'symbol': symbol,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'prev_close': prev_close,
        }

    except (ValueError, IndexError):
        return None


def _empty_closing_rates_df(expected_date: str | None = None) -> pd.DataFrame:
    """Return empty DataFrame with correct schema."""
    cols = CLOSING_RATES_COLUMNS.copy()
    if expected_date:
        cols.insert(0, 'date')
    return pd.DataFrame(columns=cols)


def save_closing_rates_csv(
    df: pd.DataFrame,
    date_str: str,
    out_dir: Path | None = None,
) -> Path:
    """Save parsed closing rates as CSV.

    Args:
        df: DataFrame from parse_closing_rates_pdf()
        date_str: Date string (YYYY-MM-DD) for filename
        out_dir: Output directory. Defaults to DATA_ROOT / "closing_rates"

    Returns:
        Path to saved CSV file
    """
    if out_dir is None:
        out_dir = DATA_ROOT / "closing_rates"

    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    csv_path = csv_dir / f"{date_str}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")

    return csv_path


def fetch_closing_rates(
    date_str: str,
    out_dir: Path | None = None,
    session: requests.Session | None = None,
    keep_pdf: bool = True,
) -> tuple[pd.DataFrame, Path]:
    """Full pipeline: download PDF, parse, save CSV.

    Args:
        date_str: Date string in YYYY-MM-DD format
        out_dir: Output directory
        session: Optional requests Session
        keep_pdf: If True, keep the PDF file after processing

    Returns:
        Tuple of (DataFrame, CSV path)
    """
    # Download
    pdf_path = download_closing_rates_pdf(date_str, out_dir, session)

    try:
        # Parse
        df = parse_closing_rates_pdf(pdf_path, expected_date=date_str)

        # Save CSV
        csv_path = save_closing_rates_csv(df, date_str, out_dir)

        return df, csv_path

    finally:
        # Clean up PDF if not keeping
        if not keep_pdf and pdf_path.exists():
            pdf_path.unlink()


def fetch_day(
    d: date | str,
    out_dir: Path | None = None,
    force: bool = False,
    session: requests.Session | None = None,
    keep_pdf: bool = True,
) -> dict[str, Any]:
    """Fetch closing rates PDF for a single day.

    Args:
        d: Date to fetch (date object or YYYY-MM-DD string)
        out_dir: Output directory
        force: If True, re-download even if CSV exists
        session: Optional requests Session
        keep_pdf: If True, keep PDF files

    Returns:
        Dict with keys: date, status, csv_path, pdf_path, row_count, message
        status is one of: "ok", "skipped", "missing", "failed"
    """
    # Convert to string if date object
    if isinstance(d, date):
        date_str = format_date(d)
    else:
        date_str = d

    if out_dir is None:
        out_dir = DATA_ROOT / "closing_rates"

    csv_dir = out_dir / "csv"
    csv_path = csv_dir / f"{date_str}.csv"

    result: dict[str, Any] = {
        "date": date_str,
        "status": "failed",
        "csv_path": None,
        "pdf_path": None,
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

    try:
        # Fetch and parse
        df, csv_path = fetch_closing_rates(date_str, out_dir, session, keep_pdf)

        result["status"] = "ok"
        result["csv_path"] = str(csv_path)
        result["row_count"] = len(df)
        result["message"] = f"Downloaded {len(df)} records from PDF"
        logger.info(f"{date_str}: OK ({len(df)} records from PDF)")

    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            result["status"] = "missing"
            result["message"] = "No PDF available (404)"
            logger.info(f"{date_str}: PDF not found (404)")
        else:
            result["status"] = "failed"
            result["message"] = f"HTTP error: {e}"
            logger.error(f"{date_str}: HTTP error: {e}")
    except ValueError as e:
        result["status"] = "failed"
        result["message"] = f"Invalid PDF: {e}"
        logger.error(f"{date_str}: Invalid PDF: {e}")
    except Exception as e:
        result["status"] = "failed"
        result["message"] = f"Error: {e}"
        logger.error(f"{date_str}: Error: {e}")

    return result


def ingest_closing_rates_pdf(
    con,
    date_str: str,
    out_dir: Path | None = None,
    force: bool = False,
    session=None,
) -> dict[str, Any]:
    """Fetch closing rates PDF and ingest into eod_ohlcv table.

    Args:
        con: Database connection
        date_str: Date string in YYYY-MM-DD format
        out_dir: Output directory for CSV/PDF files
        force: If True, re-ingest even if data exists
        session: Optional requests Session

    Returns:
        Dict with keys: date, status, csv_path, row_count, message, source
    """
    from ..db import upsert_eod, check_eod_date_exists, get_eod_date_count

    result = fetch_day(date_str, out_dir, force=force, session=session)

    if result["status"] != "ok":
        return result

    # Load the CSV and ingest
    csv_path = result["csv_path"]
    if csv_path:
        df = pd.read_csv(csv_path)
        if not df.empty:
            # Ensure date column
            if "date" not in df.columns:
                df["date"] = date_str

            # Upsert with source tracking
            rows = upsert_eod(con, df, source="closing_rates_pdf")
            result["rows_ingested"] = rows
            result["source"] = "closing_rates_pdf"
            result["message"] = f"Ingested {rows} rows from PDF"
            logger.info(f"{date_str}: Ingested {rows} rows from PDF")

    return result
