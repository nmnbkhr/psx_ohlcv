"""Query helpers for PSX OHLCV data."""

import sqlite3

import pandas as pd


def get_symbols_list(
    con: sqlite3.Connection,
    limit: int | None = None,
    is_active_only: bool = True,
) -> list[str]:
    """
    Get list of symbols in sorted order.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols
        is_active_only: If True, only return active symbols

    Returns:
        List of symbol strings, sorted alphabetically
    """
    query = "SELECT symbol FROM symbols"
    if is_active_only:
        query += " WHERE is_active = 1"
    query += " ORDER BY symbol"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    cur = con.execute(query)
    return [row["symbol"] for row in cur.fetchall()]


def get_symbols_string(
    con: sqlite3.Connection,
    limit: int | None = None,
    is_active_only: bool = True,
) -> str:
    """
    Get comma-separated string of symbols.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols
        is_active_only: If True, only return active symbols

    Returns:
        Comma-separated string of symbols, sorted alphabetically
    """
    symbols = get_symbols_list(con, limit=limit, is_active_only=is_active_only)
    return ",".join(symbols)


def get_latest_close(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get latest close price for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with keys: symbol, date, open, high, low, close, volume
        Returns None if no data found.
    """
    cur = con.execute(
        """
        SELECT symbol, date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "symbol": row["symbol"],
        "date": row["date"],
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": row["volume"],
    }


def get_ohlcv_range(
    con: sqlite3.Connection,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Get OHLCV data for a symbol within a date range.

    Args:
        con: Database connection
        symbol: Stock symbol
        start_date: Start date (YYYY-MM-DD), inclusive. None for no lower bound.
        end_date: End date (YYYY-MM-DD), inclusive. None for no upper bound.

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
        Sorted by date ascending.
    """
    query = """
        SELECT symbol, date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if start_date is not None:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date is not None:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date ASC"

    df = pd.read_sql_query(query, con, params=params)
    return df


def get_ohlcv_stats(con: sqlite3.Connection) -> dict:
    """
    Get overall statistics for OHLCV data.

    Args:
        con: Database connection

    Returns:
        Dict with min_date, max_date, total_rows, unique_symbols
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM eod_ohlcv
        """
    )
    row = cur.fetchone()
    if row and row[2] > 0:
        return {
            "min_date": row[0],
            "max_date": row[1],
            "total_rows": row[2],
            "unique_symbols": row[3],
        }
    return {"min_date": None, "max_date": None, "total_rows": 0, "unique_symbols": 0}


def get_ohlcv_market_daily(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """
    Get daily market aggregates from OHLCV data.

    Computes gainers/losers/volume per day across all symbols.

    Args:
        con: Database connection
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        limit: Maximum rows to return

    Returns:
        DataFrame with columns: date, total_symbols, gainers, losers, unchanged,
                               total_volume, avg_change_pct
    """
    query = """
        SELECT
            date,
            COUNT(*) as total_symbols,
            SUM(CASE WHEN close > open THEN 1 ELSE 0 END) as gainers,
            SUM(CASE WHEN close < open THEN 1 ELSE 0 END) as losers,
            SUM(CASE WHEN close = open THEN 1 ELSE 0 END) as unchanged,
            SUM(volume) as total_volume,
            AVG(CASE WHEN open > 0 THEN (close - open) / open * 100 ELSE 0 END)
                as avg_change_pct
        FROM eod_ohlcv
        WHERE 1=1
    """
    params: list = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " GROUP BY date ORDER BY date DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_ohlcv_symbol_stats(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get OHLCV statistics for a specific symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_date, max_date, total_rows, avg_volume
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_rows,
            AVG(volume) as avg_volume
        FROM eod_ohlcv
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row[2] > 0:
        return {
            "min_date": row[0],
            "max_date": row[1],
            "total_rows": row[2],
            "avg_volume": row[3],
        }
    return {"min_date": None, "max_date": None, "total_rows": 0, "avg_volume": 0}


# =============================================================================
# Intraday Query Helpers
# =============================================================================


def get_intraday_range(
    con: sqlite3.Connection,
    symbol: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get intraday bars for a symbol within a time range.

    Args:
        con: Database connection
        symbol: Stock symbol
        start_ts: Optional start timestamp (inclusive)
        end_ts: Optional end timestamp (inclusive)
        limit: Maximum rows to return (default 2000)

    Returns:
        DataFrame with columns: symbol, ts, ts_epoch, open, high, low, close, volume
        Sorted by ts_epoch ascending (oldest first).
    """
    from .db import _parse_ts_to_epoch

    query = """
        SELECT symbol, ts, ts_epoch, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if start_ts:
        query += " AND ts_epoch >= ?"
        params.append(_parse_ts_to_epoch(start_ts))

    if end_ts:
        query += " AND ts_epoch <= ?"
        params.append(_parse_ts_to_epoch(end_ts))

    query += " ORDER BY ts_epoch DESC LIMIT ?"
    params.append(limit)

    df = pd.read_sql_query(query, con, params=params)

    # Sort ascending for display
    if not df.empty:
        df = df.sort_values("ts_epoch").reset_index(drop=True)

    return df


def get_intraday_latest(
    con: sqlite3.Connection, symbol: str, limit: int = 500
) -> pd.DataFrame:
    """
    Get the most recent intraday bars for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return (default 500)

    Returns:
        DataFrame with columns: symbol, ts, ts_epoch, open, high, low, close, volume
        Sorted by ts_epoch ascending (oldest first).
    """
    query = """
        SELECT symbol, ts, ts_epoch, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol = ?
        ORDER BY ts_epoch DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, con, params=[symbol.upper(), limit])

    # Sort ascending for display
    if not df.empty:
        df = df.sort_values("ts_epoch").reset_index(drop=True)

    return df


def get_intraday_stats(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get statistics for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_ts, max_ts, row_count
    """
    cur = con.execute(
        """
        SELECT
            MIN(ts) as min_ts,
            MAX(ts) as max_ts,
            COUNT(*) as row_count
        FROM intraday_bars
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row["row_count"] > 0:
        return {
            "min_ts": row["min_ts"],
            "max_ts": row["max_ts"],
            "row_count": row["row_count"],
        }
    return {"min_ts": None, "max_ts": None, "row_count": 0}


# =============================================================================
# History Query Helpers
# =============================================================================


def get_time_range_bounds(range_key: str) -> tuple[str | None, str | None]:
    """
    Convert quick range key to ISO timestamp bounds.

    Uses Asia/Karachi timezone for PSX market hours.

    Args:
        range_key: One of 'last_1h', 'last_3h', 'today', 'last_5d', 'all'

    Returns:
        Tuple of (start_ts, end_ts) as ISO strings, or (None, None) for 'all'
    """
    from datetime import datetime, timedelta

    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore

    tz = ZoneInfo("Asia/Karachi")
    now = datetime.now(tz)

    if range_key == "all":
        return None, None

    if range_key == "last_1h":
        start = now - timedelta(hours=1)
    elif range_key == "last_3h":
        start = now - timedelta(hours=3)
    elif range_key == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range_key == "last_5d":
        start = now - timedelta(days=5)
    else:
        return None, None

    # Format as ISO strings
    start_ts = start.strftime("%Y-%m-%dT%H:%M:%S")
    end_ts = now.strftime("%Y-%m-%dT%H:%M:%S")

    return start_ts, end_ts


def get_market_history(
    con: sqlite3.Connection,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get market analytics history from analytics_market_snapshot.

    Args:
        con: Database connection
        start_ts: Optional start timestamp (inclusive)
        end_ts: Optional end timestamp (inclusive)
        limit: Maximum rows to return

    Returns:
        DataFrame with columns: ts, gainers_count, losers_count, unchanged_count,
        total_symbols, total_volume, top_gainer_symbol, top_loser_symbol, computed_at
        Sorted by ts ascending.
    """
    query = """
        SELECT ts, gainers_count, losers_count, unchanged_count,
               total_symbols, total_volume, top_gainer_symbol,
               top_loser_symbol, computed_at
        FROM analytics_market_snapshot
        WHERE 1=1
    """
    params: list = []

    if start_ts:
        query += " AND ts >= ?"
        params.append(start_ts)

    if end_ts:
        query += " AND ts <= ?"
        params.append(end_ts)

    query += " ORDER BY ts ASC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_symbol_snapshot_history(
    con: sqlite3.Connection,
    symbol: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 5000,
) -> pd.DataFrame:
    """
    Get snapshot history for a symbol from regular_market_snapshots.

    Args:
        con: Database connection
        symbol: Stock symbol
        start_ts: Optional start timestamp (inclusive)
        end_ts: Optional end timestamp (inclusive)
        limit: Maximum rows to return

    Returns:
        DataFrame with columns: symbol, ts, status, sector_code, listed_in,
        ldcp, open, high, low, current, change, change_pct, volume
        Sorted by ts ascending.
    """
    query = """
        SELECT symbol, ts, status, sector_code, listed_in,
               ldcp, open, high, low, current, change, change_pct, volume
        FROM regular_market_snapshots
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if start_ts:
        query += " AND ts >= ?"
        params.append(start_ts)

    if end_ts:
        query += " AND ts <= ?"
        params.append(end_ts)

    query += " ORDER BY ts ASC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_sector_history(
    con: sqlite3.Connection,
    sector_code: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get sector rollup history from analytics_sector_snapshot.

    Args:
        con: Database connection
        sector_code: Sector code (e.g., '0807')
        start_ts: Optional start timestamp (inclusive)
        end_ts: Optional end timestamp (inclusive)
        limit: Maximum rows to return

    Returns:
        DataFrame with columns: ts, sector_code, sector_name, symbols_count,
        avg_change_pct, sum_volume, top_symbol
        Sorted by ts ascending.
    """
    query = """
        SELECT ts, sector_code, sector_name, symbols_count,
               avg_change_pct, sum_volume, top_symbol
        FROM analytics_sector_snapshot
        WHERE sector_code = ?
    """
    params: list = [sector_code]

    if start_ts:
        query += " AND ts >= ?"
        params.append(start_ts)

    if end_ts:
        query += " AND ts <= ?"
        params.append(end_ts)

    query += " ORDER BY ts ASC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_sector_list_from_analytics(con: sqlite3.Connection) -> list[dict[str, str]]:
    """
    Get unique sectors from analytics_sector_snapshot.

    Args:
        con: Database connection

    Returns:
        List of dicts with sector_code and sector_name
    """
    cur = con.execute(
        """
        SELECT DISTINCT sector_code, sector_name
        FROM analytics_sector_snapshot
        WHERE sector_code IS NOT NULL AND sector_code != ''
        ORDER BY sector_name
        """
    )
    return [{"sector_code": row[0], "sector_name": row[1]} for row in cur.fetchall()]


def get_market_history_stats(con: sqlite3.Connection) -> dict:
    """
    Get statistics about market history data availability.

    Args:
        con: Database connection

    Returns:
        Dict with min_ts, max_ts, snapshot_count
    """
    cur = con.execute(
        """
        SELECT
            MIN(ts) as min_ts,
            MAX(ts) as max_ts,
            COUNT(*) as snapshot_count
        FROM analytics_market_snapshot
        """
    )
    row = cur.fetchone()
    if row and row[2] > 0:
        return {
            "min_ts": row[0],
            "max_ts": row[1],
            "snapshot_count": row[2],
        }
    return {"min_ts": None, "max_ts": None, "snapshot_count": 0}


def get_symbol_history_stats(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get statistics about symbol snapshot history.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_ts, max_ts, snapshot_count
    """
    cur = con.execute(
        """
        SELECT
            MIN(ts) as min_ts,
            MAX(ts) as max_ts,
            COUNT(*) as snapshot_count
        FROM regular_market_snapshots
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row[2] > 0:
        return {
            "min_ts": row[0],
            "max_ts": row[1],
            "snapshot_count": row[2],
        }
    return {"min_ts": None, "max_ts": None, "snapshot_count": 0}


# =============================================================================
# Company Query Helpers
# =============================================================================


def get_company_profile(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get company profile for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with profile fields, or None if not found.
    """
    cur = con.execute(
        """
        SELECT symbol, company_name, sector_name, business_description,
               address, website, registrar, auditor, fiscal_year_end,
               updated_at, source_url
        FROM company_profile
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "symbol": row[0],
        "company_name": row[1],
        "sector_name": row[2],
        "business_description": row[3],
        "address": row[4],
        "website": row[5],
        "registrar": row[6],
        "auditor": row[7],
        "fiscal_year_end": row[8],
        "updated_at": row[9],
        "source_url": row[10],
    }


def get_company_people(con: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    """
    Get key people for a company.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        DataFrame with columns: symbol, role, name, updated_at
    """
    return pd.read_sql_query(
        """
        SELECT symbol, role, name, updated_at
        FROM company_key_people
        WHERE symbol = ?
        ORDER BY role
        """,
        con,
        params=[symbol.upper()],
    )


def get_company_quotes(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 200,
) -> pd.DataFrame:
    """
    Get recent quote snapshots for a company.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with quote snapshot columns, sorted by ts descending.
    """
    return pd.read_sql_query(
        """
        SELECT symbol, ts, as_of, price, change, change_pct,
               open, high, low, volume,
               day_range_low, day_range_high,
               wk52_low, wk52_high,
               circuit_low, circuit_high,
               market_mode, raw_hash, ingested_at
        FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        con,
        params=[symbol.upper(), limit],
    )


def get_company_latest_quote(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get the most recent quote snapshot for a company.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with quote fields, or None if not found.
    """
    cur = con.execute(
        """
        SELECT symbol, ts, as_of, price, change, change_pct,
               open, high, low, volume,
               day_range_low, day_range_high,
               wk52_low, wk52_high,
               circuit_low, circuit_high,
               market_mode, raw_hash, ingested_at
        FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "symbol": row[0],
        "ts": row[1],
        "as_of": row[2],
        "price": row[3],
        "change": row[4],
        "change_pct": row[5],
        "open": row[6],
        "high": row[7],
        "low": row[8],
        "volume": row[9],
        "day_range_low": row[10],
        "day_range_high": row[11],
        "wk52_low": row[12],
        "wk52_high": row[13],
        "circuit_low": row[14],
        "circuit_high": row[15],
        "market_mode": row[16],
        "raw_hash": row[17],
        "ingested_at": row[18],
    }


def get_company_latest_signals(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get the most recent signals for a company.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict of signal_key -> signal_value for the latest timestamp.
        Empty dict if no signals found.
    """
    # First get the latest timestamp
    cur = con.execute(
        """
        SELECT ts FROM company_signal_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return {}

    latest_ts = row[0]

    # Get all signals for that timestamp
    cur = con.execute(
        """
        SELECT signal_key, signal_value
        FROM company_signal_snapshots
        WHERE symbol = ? AND ts = ?
        """,
        (symbol.upper(), latest_ts),
    )
    result = {"ts": latest_ts}
    for row in cur.fetchall():
        result[row[0]] = row[1]
    return result


def get_company_signals_history(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get signal history for a company.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with columns: ts, signal_key, signal_value
        Sorted by ts descending.
    """
    return pd.read_sql_query(
        """
        SELECT ts, signal_key, signal_value
        FROM company_signal_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        con,
        params=[symbol.upper(), limit],
    )


def get_company_quote_stats(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get statistics about company quote snapshots.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_ts, max_ts, snapshot_count
    """
    cur = con.execute(
        """
        SELECT
            MIN(ts) as min_ts,
            MAX(ts) as max_ts,
            COUNT(*) as snapshot_count
        FROM company_quote_snapshots
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row[2] > 0:
        return {
            "min_ts": row[0],
            "max_ts": row[1],
            "snapshot_count": row[2],
        }
    return {"min_ts": None, "max_ts": None, "snapshot_count": 0}


def get_symbols_with_profiles(con: sqlite3.Connection) -> list[dict]:
    """
    Get list of symbols that have company profiles.

    Args:
        con: Database connection

    Returns:
        List of dicts with symbol and company_name.
    """
    cur = con.execute(
        """
        SELECT symbol, company_name
        FROM company_profile
        ORDER BY symbol
        """
    )
    return [{"symbol": row[0], "company_name": row[1]} for row in cur.fetchall()]
