"""End-of-day OHLCV data repository."""

import sqlite3
import uuid
from pathlib import Path

import pandas as pd

from psx_ohlcv.models import now_iso


# =============================================================================
# EOD Upsert
# =============================================================================


def upsert_eod(
    con: sqlite3.Connection, df: pd.DataFrame, source: str = "unknown"
) -> int:
    """
    Upsert EOD OHLCV data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: symbol, date, open, high, low, close, volume
            Optional columns: prev_close, sector_code, company_name
        source: Data source identifier (e.g., 'market_summary', 'closing_rates_pdf', 'per_symbol_api')

    Behavior by source:
        - 'per_symbol_api': INSERT only if no data exists for symbol+date (no overwrite)
          processname = 'per_symbol_api'
        - 'market_summary', 'closing_rates_pdf', others: Full upsert (overwrite existing)
          processname = 'eodfile'

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"symbol", "date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    # Determine processname based on source
    # per_symbol_api -> processname = 'per_symbol_api'
    # market_summary, closing_rates_pdf -> processname = 'eodfile'
    if source == "per_symbol_api":
        processname = "per_symbol_api"
    else:
        processname = "eodfile"

    # per_symbol_api: INSERT OR IGNORE (don't overwrite existing data)
    # Other sources (market_summary, closing_rates_pdf): Full upsert (overwrite)
    if source == "per_symbol_api":
        for _, row in df.iterrows():
            cur = con.execute(
                """
                INSERT OR IGNORE INTO eod_ohlcv
                    (symbol, date, open, high, low, close, volume,
                     prev_close, sector_code, company_name, ingested_at, source, processname)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["symbol"],
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row.get("prev_close"),
                    row.get("sector_code"),
                    row.get("company_name"),
                    now,
                    source,
                    processname,
                ),
            )
            count += cur.rowcount
    else:
        # Full upsert for market_summary, closing_rates_pdf, etc.
        for _, row in df.iterrows():
            cur = con.execute(
                """
                INSERT INTO eod_ohlcv
                    (symbol, date, open, high, low, close, volume,
                     prev_close, sector_code, company_name, ingested_at, source, processname)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    prev_close = excluded.prev_close,
                    sector_code = excluded.sector_code,
                    company_name = excluded.company_name,
                    ingested_at = excluded.ingested_at,
                    source = excluded.source,
                    processname = excluded.processname
                """,
                (
                    row["symbol"],
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row.get("prev_close"),
                    row.get("sector_code"),
                    row.get("company_name"),
                    now,
                    source,
                    processname,
                ),
            )
            count += cur.rowcount

    con.commit()
    return count


# =============================================================================
# Sync Run Tracking
# =============================================================================


def record_sync_run_start(
    con: sqlite3.Connection, mode: str, symbols_total: int
) -> str:
    """
    Record the start of a sync run.

    Args:
        con: Database connection
        mode: Sync mode (e.g., 'full', 'incremental', 'symbols_only')
        symbols_total: Total number of symbols to sync

    Returns:
        run_id (UUID string)
    """
    run_id = str(uuid.uuid4())
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_runs (run_id, started_at, mode, symbols_total)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, now, mode, symbols_total),
    )
    con.commit()

    return run_id


def record_sync_run_end(
    con: sqlite3.Connection,
    run_id: str,
    symbols_ok: int,
    symbols_failed: int,
    rows_upserted: int,
) -> None:
    """
    Record the end of a sync run.

    Args:
        con: Database connection
        run_id: The run ID returned by record_sync_run_start
        symbols_ok: Number of symbols successfully synced
        symbols_failed: Number of symbols that failed
        rows_upserted: Total number of EOD rows upserted
    """
    now = now_iso()

    con.execute(
        """
        UPDATE sync_runs
        SET ended_at = ?,
            symbols_ok = ?,
            symbols_failed = ?,
            rows_upserted = ?
        WHERE run_id = ?
        """,
        (now, symbols_ok, symbols_failed, rows_upserted, run_id),
    )
    con.commit()


def record_failure(
    con: sqlite3.Connection,
    run_id: str,
    symbol: str,
    error_type: str,
    error_message: str | None,
) -> None:
    """
    Record a sync failure for a specific symbol.

    Args:
        con: Database connection
        run_id: The run ID
        symbol: The symbol that failed
        error_type: Type of error (e.g., 'HTTP_ERROR', 'PARSE_ERROR')
        error_message: Detailed error message
    """
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_failures
            (run_id, symbol, error_type, error_message, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, symbol, error_type, error_message, now),
    )
    con.commit()


# =============================================================================
# EOD Query Functions
# =============================================================================


def get_max_date_for_symbol(con: sqlite3.Connection, symbol: str) -> str | None:
    """
    Get the most recent date for a symbol in eod_ohlcv table.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Max date as string (YYYY-MM-DD) or None if no data exists
    """
    cur = con.execute(
        "SELECT MAX(date) as max_date FROM eod_ohlcv WHERE symbol = ?",
        (symbol,),
    )
    row = cur.fetchone()
    if row and row["max_date"]:
        return row["max_date"]
    return None


def get_date_range_for_symbol(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get date range statistics for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_date, max_date, row_count, or all None if no data
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as row_count
        FROM eod_ohlcv
        WHERE symbol = ?
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if row and row["row_count"] > 0:
        return {
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "row_count": row["row_count"],
        }
    return {"min_date": None, "max_date": None, "row_count": 0}


def get_data_coverage_summary(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Get summary of data coverage across all symbols.

    Returns:
        DataFrame with columns: symbol, min_date, max_date, row_count, days_missing
    """
    cur = con.execute(
        """
        SELECT
            symbol,
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as row_count,
            CAST(
                julianday(MAX(date)) - julianday(MIN(date)) + 1
            AS INTEGER) as days_span
        FROM eod_ohlcv
        GROUP BY symbol
        ORDER BY symbol
        """
    )

    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["symbol", "min_date", "max_date", "row_count", "days_span"]
        )

    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)

    # Calculate days with data vs expected
    df["data_coverage_pct"] = (df["row_count"] / df["days_span"] * 100).round(1)

    return df


def get_global_date_stats(con: sqlite3.Connection) -> dict:
    """
    Get global date statistics across all data.

    Returns:
        Dict with global_min_date, global_max_date, total_rows, unique_symbols
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as global_min_date,
            MAX(date) as global_max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM eod_ohlcv
        """
    )
    row = cur.fetchone()
    if row and row["total_rows"] > 0:
        return {
            "global_min_date": row["global_min_date"],
            "global_max_date": row["global_max_date"],
            "total_rows": row["total_rows"],
            "unique_symbols": row["unique_symbols"],
        }
    return {
        "global_min_date": None,
        "global_max_date": None,
        "total_rows": 0,
        "unique_symbols": 0,
    }


def get_eod_ohlcv(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Get EOD OHLCV data with optional filters.

    Args:
        con: Database connection
        symbol: Filter by stock symbol (optional)
        start_date: Start date YYYY-MM-DD (inclusive, optional)
        end_date: End date YYYY-MM-DD (inclusive, optional)
        limit: Maximum rows to return

    Returns:
        DataFrame with OHLCV data
    """
    query = """
        SELECT symbol, date, open, high, low, close, volume,
               prev_close, sector_code, company_name, ingested_at
        FROM eod_ohlcv
        WHERE 1=1
    """
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_eod_dates(con: sqlite3.Connection) -> list[str]:
    """
    Get list of all dates with EOD data.

    Args:
        con: Database connection

    Returns:
        List of date strings (YYYY-MM-DD), newest first
    """
    cur = con.execute(
        "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC"
    )
    return [row[0] for row in cur.fetchall()]


def get_eod_date_range(con: sqlite3.Connection) -> dict:
    """
    Get min/max date range and count for EOD data.

    Args:
        con: Database connection

    Returns:
        Dict with min_date, max_date, total_rows, unique_dates, unique_symbols
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM eod_ohlcv
        """
    )
    row = cur.fetchone()
    return {
        "min_date": row[0],
        "max_date": row[1],
        "total_rows": row[2] or 0,
        "unique_dates": row[3] or 0,
        "unique_symbols": row[4] or 0,
    }


def check_eod_date_exists(con: sqlite3.Connection, date: str) -> bool:
    """
    Check if EOD data exists for a specific date.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD

    Returns:
        True if data exists, False otherwise
    """
    cur = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE date = ?",
        (date,),
    )
    return cur.fetchone()[0] > 0


def get_eod_date_count(con: sqlite3.Connection, date: str) -> int:
    """
    Get count of EOD records for a specific date.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD

    Returns:
        Number of records for that date
    """
    cur = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE date = ?",
        (date,),
    )
    return cur.fetchone()[0]


# =============================================================================
# Market Summary CSV Ingestion Functions
# =============================================================================


def ingest_market_summary_csv(
    con: sqlite3.Connection,
    csv_path: str | Path,
    skip_existing: bool = True,
    source: str = "market_summary",
) -> dict:
    """
    Ingest market summary CSV file into eod_ohlcv table.

    The CSV should have columns: date, symbol, sector_code, company_name,
    open, high, low, close, volume, prev_close

    Args:
        con: Database connection
        csv_path: Path to CSV file
        skip_existing: If True, skip if date already has data
        source: Data source identifier (default: 'market_summary')

    Returns:
        Dict with status, rows_inserted, date, message
    """
    from pathlib import Path

    csv_path = Path(csv_path)

    result = {
        "csv_path": str(csv_path),
        "date": None,
        "status": "failed",
        "rows_inserted": 0,
        "rows_in_csv": 0,
        "message": None,
    }

    if not csv_path.exists():
        result["message"] = f"File not found: {csv_path}"
        return result

    try:
        df = pd.read_csv(csv_path)
        result["rows_in_csv"] = len(df)

        if df.empty:
            result["status"] = "empty"
            result["message"] = "CSV file is empty"
            return result

        # Get date from first row or filename
        if "date" in df.columns and not df["date"].isna().all():
            date_str = df["date"].iloc[0]
        else:
            # Extract date from filename (e.g., 2026-01-20.csv)
            date_str = csv_path.stem

        result["date"] = date_str

        # Check if date already exists
        if skip_existing and check_eod_date_exists(con, date_str):
            existing_count = get_eod_date_count(con, date_str)
            result["status"] = "skipped"
            result["message"] = f"Date {date_str} already has {existing_count} records"
            return result

        # Ensure required columns
        required = {"symbol", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            result["message"] = f"Missing columns: {missing}"
            return result

        # Add date column if missing
        if "date" not in df.columns:
            df["date"] = date_str

        # Classify rows by market type (REG vs FUT/CONT/IDX_FUT/ODL)
        from psx_ohlcv.sources.market_summary import (
            classify_market_type,
            parse_futures_symbol,
        )

        if "market_type" not in df.columns:
            df["market_type"] = df.apply(
                lambda row: classify_market_type(
                    str(row.get("sector_code", "")), str(row["symbol"])
                ),
                axis=1,
            )

        reg_df = df[df["market_type"] == "REG"]
        deriv_df = df[df["market_type"] != "REG"]

        # Route REG to eod_ohlcv (existing behavior)
        reg_rows = upsert_eod(con, reg_df, source=source) if not reg_df.empty else 0

        # Route FUT/CONT/IDX_FUT/ODL to futures_eod
        futures_rows = 0
        if not deriv_df.empty:
            deriv_df = deriv_df.copy()
            parsed = deriv_df.apply(
                lambda r: parse_futures_symbol(r["symbol"], r["market_type"]),
                axis=1,
                result_type="expand",
            )
            deriv_df["base_symbol"] = parsed[0]
            deriv_df["contract_month"] = parsed[1]

            from .futures import init_futures_schema, upsert_futures_eod
            init_futures_schema(con)
            futures_rows = upsert_futures_eod(con, deriv_df, source=source)

        rows = reg_rows + futures_rows
        result["rows_inserted"] = rows
        result["reg_rows"] = reg_rows
        result["futures_rows"] = futures_rows
        result["status"] = "ok"
        result["message"] = (
            f"Inserted {rows} rows for {date_str} "
            f"(REG: {reg_rows}, FUT/CONT/ODL: {futures_rows})"
        )
        result["source"] = source

    except Exception as e:
        result["message"] = f"Error: {e}"

    return result


def ingest_all_market_summary_csvs(
    con: sqlite3.Connection,
    csv_dir: str | Path | None = None,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest all market summary CSV files from a directory into eod_ohlcv table.

    Args:
        con: Database connection
        csv_dir: Directory containing CSV files. Defaults to DATA_ROOT/market_summary/csv
        skip_existing: If True, skip dates that already have data

    Returns:
        Dict with summary: total_files, ok, skipped, failed, total_rows
    """
    from pathlib import Path

    from psx_ohlcv.config import DATA_ROOT

    if csv_dir is None:
        csv_dir = DATA_ROOT / "market_summary" / "csv"
    else:
        csv_dir = Path(csv_dir)

    summary = {
        "csv_dir": str(csv_dir),
        "total_files": 0,
        "ok": 0,
        "skipped": 0,
        "empty": 0,
        "failed": 0,
        "total_rows": 0,
        "errors": [],
    }

    if not csv_dir.exists():
        summary["errors"].append(f"Directory not found: {csv_dir}")
        return summary

    # Get all CSV files sorted by name (date)
    csv_files = sorted(csv_dir.glob("*.csv"))
    summary["total_files"] = len(csv_files)

    for csv_path in csv_files:
        result = ingest_market_summary_csv(con, csv_path, skip_existing=skip_existing)

        if result["status"] == "ok":
            summary["ok"] += 1
            summary["total_rows"] += result["rows_inserted"]
        elif result["status"] == "skipped":
            summary["skipped"] += 1
        elif result["status"] == "empty":
            summary["empty"] += 1
        else:
            summary["failed"] += 1
            summary["errors"].append({
                "file": csv_path.name,
                "message": result["message"],
            })

    return summary


# =============================================================================
# EOD Source Analysis
# =============================================================================


def get_eod_source_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of EOD data by source.

    Returns:
        Dict with source counts and date ranges
    """
    cursor = con.execute("""
        SELECT
            COALESCE(source, 'unknown') as source,
            COUNT(*) as row_count,
            COUNT(DISTINCT date) as date_count,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM eod_ohlcv
        GROUP BY COALESCE(source, 'unknown')
        ORDER BY row_count DESC
    """)

    sources = {}
    for row in cursor.fetchall():
        sources[row[0]] = {
            "row_count": row[1],
            "date_count": row[2],
            "min_date": row[3],
            "max_date": row[4],
        }
    return sources


def get_eod_date_source_breakdown(con: sqlite3.Connection, date_str: str) -> dict:
    """
    Get source breakdown for a specific date.

    Args:
        con: Database connection
        date_str: Date in YYYY-MM-DD format

    Returns:
        Dict with source -> count mapping
    """
    cursor = con.execute("""
        SELECT
            COALESCE(source, 'unknown') as source,
            COUNT(*) as count
        FROM eod_ohlcv
        WHERE date = ?
        GROUP BY COALESCE(source, 'unknown')
    """, (date_str,))

    return {row[0]: row[1] for row in cursor.fetchall()}


def verify_eod_data_sources(
    con: sqlite3.Connection,
    csv_dir: str | Path | None = None,
    pdf_dir: str | Path | None = None,
) -> dict:
    """
    Verify EOD data sources and identify gaps.

    Compares DB data against available CSV and PDF files to identify:
    - Dates with data from each source
    - Dates missing from DB
    - Data source mismatches

    Args:
        con: Database connection
        csv_dir: Directory with market summary CSVs
        pdf_dir: Directory with closing rates PDFs

    Returns:
        Dict with verification results
    """
    from pathlib import Path
    from psx_ohlcv.config import DATA_ROOT

    if csv_dir is None:
        csv_dir = DATA_ROOT / "market_summary" / "csv"
    else:
        csv_dir = Path(csv_dir)

    if pdf_dir is None:
        pdf_dir = DATA_ROOT / "closing_rates" / "csv"
    else:
        pdf_dir = Path(pdf_dir)

    # Get dates from CSV files
    csv_dates = set()
    if csv_dir.exists():
        for f in csv_dir.glob("*.csv"):
            csv_dates.add(f.stem)

    # Get dates from PDF-derived CSVs
    pdf_dates = set()
    if pdf_dir.exists():
        for f in pdf_dir.glob("*.csv"):
            pdf_dates.add(f.stem)

    # Get dates and sources from DB
    cursor = con.execute("""
        SELECT date, COALESCE(source, 'unknown') as source, COUNT(*) as count
        FROM eod_ohlcv
        GROUP BY date, COALESCE(source, 'unknown')
        ORDER BY date
    """)

    db_data = {}
    for row in cursor.fetchall():
        date_str = row[0]
        source = row[1]
        count = row[2]
        if date_str not in db_data:
            db_data[date_str] = {}
        db_data[date_str][source] = count

    db_dates = set(db_data.keys())

    # Analysis
    result = {
        "csv_dates": len(csv_dates),
        "pdf_dates": len(pdf_dates),
        "db_dates": len(db_dates),
        "csv_only": sorted(csv_dates - db_dates),
        "pdf_only": sorted(pdf_dates - db_dates - csv_dates),
        "db_only": sorted(db_dates - csv_dates - pdf_dates),
        "by_date": [],
    }

    # Detailed breakdown by date
    all_dates = sorted(csv_dates | pdf_dates | db_dates)
    for date_str in all_dates:
        csv_count = 0
        pdf_count = 0
        if date_str in csv_dates and csv_dir.exists():
            try:
                csv_file = csv_dir / f"{date_str}.csv"
                if csv_file.exists():
                    csv_count = sum(1 for _ in open(csv_file)) - 1  # -1 for header
            except Exception:
                pass

        if date_str in pdf_dates and pdf_dir.exists():
            try:
                pdf_file = pdf_dir / f"{date_str}.csv"
                if pdf_file.exists():
                    pdf_count = sum(1 for _ in open(pdf_file)) - 1
            except Exception:
                pass

        db_info = db_data.get(date_str, {})
        db_total = sum(db_info.values())

        result["by_date"].append({
            "date": date_str,
            "csv_rows": csv_count,
            "pdf_rows": pdf_count,
            "db_rows": db_total,
            "db_sources": db_info,
        })

    return result


def backfill_eod_sources(con: sqlite3.Connection, dry_run: bool = True) -> dict:
    """
    Backfill source column for existing data based on available files.

    Logic:
    - If date has market_summary CSV -> set source to 'market_summary'
    - If date has closing_rates PDF CSV -> set source to 'closing_rates_pdf'
    - Otherwise -> set source to 'per_symbol_api' (historical API data)

    Args:
        con: Database connection
        dry_run: If True, only report what would be done

    Returns:
        Dict with backfill results
    """
    from pathlib import Path
    from psx_ohlcv.config import DATA_ROOT

    csv_dir = DATA_ROOT / "market_summary" / "csv"
    pdf_dir = DATA_ROOT / "closing_rates" / "csv"

    # Get dates from files
    csv_dates = set()
    if csv_dir.exists():
        for f in csv_dir.glob("*.csv"):
            csv_dates.add(f.stem)

    pdf_dates = set()
    if pdf_dir.exists():
        for f in pdf_dir.glob("*.csv"):
            pdf_dates.add(f.stem)

    # Get dates with unknown source
    cursor = con.execute("""
        SELECT DISTINCT date FROM eod_ohlcv
        WHERE source IS NULL OR source = 'unknown'
        ORDER BY date
    """)
    unknown_dates = [row[0] for row in cursor.fetchall()]

    result = {
        "total_unknown_dates": len(unknown_dates),
        "to_market_summary": [],
        "to_closing_rates_pdf": [],
        "to_per_symbol_api": [],
        "dry_run": dry_run,
    }

    for date_str in unknown_dates:
        if date_str in csv_dates:
            source = "market_summary"
            result["to_market_summary"].append(date_str)
        elif date_str in pdf_dates:
            source = "closing_rates_pdf"
            result["to_closing_rates_pdf"].append(date_str)
        else:
            source = "per_symbol_api"
            result["to_per_symbol_api"].append(date_str)

        if not dry_run:
            con.execute(
                "UPDATE eod_ohlcv SET source = ? WHERE date = ? AND (source IS NULL OR source = 'unknown')",
                (source, date_str),
            )

    if not dry_run:
        con.commit()

    return result
