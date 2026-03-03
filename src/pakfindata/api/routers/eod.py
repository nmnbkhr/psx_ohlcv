"""
EOD Data API endpoints.

Provides endpoints for:
- Getting EOD table statistics
- Listing available CSV files
- Loading data into eod_ohlcv table
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ...config import DATA_ROOT, get_db_path
from ...db import (
    ingest_market_summary_csv,
    check_eod_date_exists,
    get_eod_date_count,
)

router = APIRouter()


# =============================================================================
# Pydantic Models
# =============================================================================

class EODStats(BaseModel):
    """EOD table statistics."""
    total_rows: int
    total_dates: int
    total_symbols: int
    min_date: Optional[str]
    max_date: Optional[str]
    by_source: dict
    by_processname: dict


class CSVFileInfo(BaseModel):
    """CSV file information."""
    date: str
    source: str
    exists: bool
    in_db: bool
    row_count: Optional[int] = None


class LoadRequest(BaseModel):
    """Request to load specific dates."""
    dates: list[str]
    force: bool = False


class LoadResult(BaseModel):
    """Result of loading a single date."""
    date: str
    status: str
    rows: int
    source: str
    message: Optional[str] = None


class LoadResponse(BaseModel):
    """Response from load operation."""
    ok_count: int
    total_rows: int
    results: list[LoadResult]


# =============================================================================
# Helper Functions
# =============================================================================

def get_connection():
    """Get database connection without schema init (for reads)."""
    db_path = get_db_path()
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def get_csv_dirs():
    """Get CSV directory paths."""
    csv_dir = DATA_ROOT / "market_summary" / "csv"
    pdf_csv_dir = DATA_ROOT / "closing_rates" / "csv"
    return csv_dir, pdf_csv_dir


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/stats", response_model=EODStats)
def get_eod_stats():
    """
    Get EOD OHLCV table statistics.

    Returns counts, date range, and breakdown by source/processname.
    """
    con = get_connection()
    try:
        # Total counts
        cursor = con.execute("SELECT COUNT(*) FROM eod_ohlcv")
        total_rows = cursor.fetchone()[0]

        cursor = con.execute("SELECT COUNT(DISTINCT date) FROM eod_ohlcv")
        total_dates = cursor.fetchone()[0]

        cursor = con.execute("SELECT COUNT(DISTINCT symbol) FROM eod_ohlcv")
        total_symbols = cursor.fetchone()[0]

        cursor = con.execute("SELECT MIN(date), MAX(date) FROM eod_ohlcv")
        date_range = cursor.fetchone()
        min_date = date_range[0] if date_range[0] else None
        max_date = date_range[1] if date_range[1] else None

        # By source
        cursor = con.execute(
            "SELECT source, COUNT(*) FROM eod_ohlcv GROUP BY source"
        )
        by_source = {row[0] or "unknown": row[1] for row in cursor.fetchall()}

        # By processname
        cursor = con.execute(
            "SELECT processname, COUNT(*) FROM eod_ohlcv GROUP BY processname"
        )
        by_processname = {row[0] or "unknown": row[1] for row in cursor.fetchall()}

        return EODStats(
            total_rows=total_rows,
            total_dates=total_dates,
            total_symbols=total_symbols,
            min_date=min_date,
            max_date=max_date,
            by_source=by_source,
            by_processname=by_processname,
        )
    finally:
        con.close()


@router.get("/files")
def list_csv_files(
    not_loaded_only: bool = Query(False, description="Only return files not in DB"),
    limit: int = Query(100, description="Max files to return"),
):
    """
    List available CSV files.

    Returns list of CSV files with their status (exists, in DB, etc.)
    """
    csv_dir, pdf_csv_dir = get_csv_dirs()
    con = get_connection()

    try:
        # Get all CSV files
        csv_files = sorted(csv_dir.glob("*.csv")) if csv_dir.exists() else []
        pdf_csv_files = sorted(pdf_csv_dir.glob("*.csv")) if pdf_csv_dir.exists() else []

        all_csv_dates = {}
        for f in csv_files:
            all_csv_dates[f.stem] = ("market_summary", f)
        for f in pdf_csv_files:
            if f.stem not in all_csv_dates:
                all_csv_dates[f.stem] = ("closing_rates_pdf", f)

        # Get dates in database
        cursor = con.execute("SELECT DISTINCT date FROM eod_ohlcv")
        db_dates = set(row[0] for row in cursor.fetchall())

        # Build response
        files = []
        for date_str in sorted(all_csv_dates.keys(), reverse=True):
            source, path = all_csv_dates[date_str]
            in_db = date_str in db_dates

            if not_loaded_only and in_db:
                continue

            files.append({
                "date": date_str,
                "source": source,
                "exists": path.exists(),
                "in_db": in_db,
            })

            if len(files) >= limit:
                break

        return {
            "total_csv_files": len(all_csv_dates),
            "total_in_db": len(db_dates & set(all_csv_dates.keys())),
            "total_not_loaded": len(set(all_csv_dates.keys()) - db_dates),
            "files": files,
        }
    finally:
        con.close()


@router.post("/load", response_model=LoadResponse)
def load_dates(request: LoadRequest):
    """
    Load specific dates into eod_ohlcv table.

    This is a synchronous operation for small batches.
    For large batches, use /api/tasks/start-load instead.
    """
    csv_dir, pdf_csv_dir = get_csv_dirs()
    con = get_connection()

    results = []
    ok_count = 0
    total_rows = 0

    try:
        for date_str in request.dates:
            # Find CSV file
            csv_path = csv_dir / f"{date_str}.csv"
            source = "market_summary"

            if not csv_path.exists():
                csv_path = pdf_csv_dir / f"{date_str}.csv"
                source = "closing_rates_pdf"

            if not csv_path.exists():
                results.append(LoadResult(
                    date=date_str,
                    status="not_found",
                    rows=0,
                    source="N/A",
                    message="CSV file not found",
                ))
                continue

            try:
                result = ingest_market_summary_csv(
                    con,
                    str(csv_path),
                    skip_existing=not request.force,
                    source=source,
                )

                rows = result.get("rows_inserted", 0)
                status = result.get("status", "unknown")

                if status == "ok":
                    ok_count += 1
                    total_rows += rows

                results.append(LoadResult(
                    date=date_str,
                    status=status,
                    rows=rows,
                    source=source,
                    message=result.get("message"),
                ))

            except Exception as e:
                results.append(LoadResult(
                    date=date_str,
                    status="error",
                    rows=0,
                    source=source,
                    message=str(e),
                ))

        return LoadResponse(
            ok_count=ok_count,
            total_rows=total_rows,
            results=results,
        )

    finally:
        con.close()


@router.get("/date/{date_str}")
def get_date_info(date_str: str):
    """Get information about a specific date."""
    csv_dir, pdf_csv_dir = get_csv_dirs()
    con = get_connection()

    try:
        # Check CSV exists
        csv_path = csv_dir / f"{date_str}.csv"
        source = "market_summary"
        if not csv_path.exists():
            csv_path = pdf_csv_dir / f"{date_str}.csv"
            source = "closing_rates_pdf"

        csv_exists = csv_path.exists()

        # Check DB
        in_db = check_eod_date_exists(con, date_str)
        db_count = get_eod_date_count(con, date_str) if in_db else 0

        # Get source breakdown if in DB
        source_breakdown = {}
        if in_db:
            cursor = con.execute(
                "SELECT source, COUNT(*) FROM eod_ohlcv WHERE date = ? GROUP BY source",
                (date_str,)
            )
            source_breakdown = {row[0] or "unknown": row[1] for row in cursor.fetchall()}

        return {
            "date": date_str,
            "csv_exists": csv_exists,
            "csv_source": source if csv_exists else None,
            "in_db": in_db,
            "db_row_count": db_count,
            "source_breakdown": source_breakdown,
        }

    finally:
        con.close()
