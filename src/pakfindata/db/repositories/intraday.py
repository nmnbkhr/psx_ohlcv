"""Intraday bar data repository."""

import sqlite3
from datetime import datetime

import pandas as pd

from pakfindata.models import now_iso


def _parse_ts_to_epoch(ts: str) -> int:
    """
    Parse a timestamp string to Unix epoch (seconds).

    Handles formats:
    - YYYY-MM-DD HH:MM:SS
    - YYYY-MM-DDTHH:MM:SS
    - ISO format with timezone

    Args:
        ts: Timestamp string

    Returns:
        Unix epoch in seconds
    """
    from datetime import datetime

    ts = str(ts).strip()

    # Try common formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts[:19], fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    # Fallback: try pandas
    try:
        dt = pd.to_datetime(ts)
        return int(dt.timestamp())
    except Exception:
        # Last resort: return 0
        return 0


def upsert_intraday(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert intraday bars data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: symbol, ts, open, high, low, close, volume
            Optionally ts_epoch (will be computed if missing)

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"symbol", "ts"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        # Compute ts_epoch if not provided
        ts_epoch = row.get("ts_epoch")
        if ts_epoch is None or pd.isna(ts_epoch):
            ts_epoch = _parse_ts_to_epoch(row["ts"])

        cur = con.execute(
            """
            INSERT OR IGNORE INTO intraday_bars
                (symbol, ts, ts_epoch, open, high, low, close, volume,
                 interval, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'int', ?)
            """,
            (
                row["symbol"],
                row["ts"],
                int(ts_epoch),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_intraday_sync_state(
    con: sqlite3.Connection, symbol: str
) -> tuple[str | None, int | None]:
    """
    Get the last synced timestamp for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Tuple of (last_ts string, last_ts_epoch integer) or (None, None)
    """
    cur = con.execute(
        "SELECT last_ts, last_ts_epoch FROM intraday_sync_state WHERE symbol = ?",
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row["last_ts"]:
        return row["last_ts"], row["last_ts_epoch"]
    return None, None


def update_intraday_sync_state(
    con: sqlite3.Connection, symbol: str, last_ts: str, last_ts_epoch: int | None = None
) -> None:
    """
    Update the sync state for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol
        last_ts: Latest timestamp that was synced
        last_ts_epoch: Unix epoch of last_ts (computed if not provided)
    """
    if last_ts_epoch is None:
        last_ts_epoch = _parse_ts_to_epoch(last_ts)

    now = now_iso()
    con.execute(
        """
        INSERT INTO intraday_sync_state (symbol, last_ts, last_ts_epoch, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            last_ts = excluded.last_ts,
            last_ts_epoch = excluded.last_ts_epoch,
            updated_at = excluded.updated_at
        """,
        (symbol.upper(), last_ts, last_ts_epoch, now),
    )
    con.commit()


def get_intraday_range(
    con: sqlite3.Connection,
    symbol: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    start_epoch: int | None = None,
    end_epoch: int | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get intraday bars for a symbol within a time range.

    Args:
        con: Database connection
        symbol: Stock symbol
        start_ts: Optional start timestamp string (inclusive)
        end_ts: Optional end timestamp string (inclusive)
        start_epoch: Optional start epoch (takes precedence over start_ts)
        end_epoch: Optional end epoch (takes precedence over end_ts)
        limit: Maximum rows to return (default 2000)

    Returns:
        DataFrame with columns: symbol, ts, ts_epoch, open, high, low, close, volume
        Sorted by ts_epoch ascending (oldest first).
    """
    query = """
        SELECT symbol, ts, ts_epoch, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    # Use epoch for filtering if provided, otherwise convert ts to epoch
    if start_epoch is not None:
        query += " AND ts_epoch >= ?"
        params.append(start_epoch)
    elif start_ts:
        query += " AND ts_epoch >= ?"
        params.append(_parse_ts_to_epoch(start_ts))

    if end_epoch is not None:
        query += " AND ts_epoch <= ?"
        params.append(end_epoch)
    elif end_ts:
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


def promote_intraday_to_eod(
    con: sqlite3.Connection, date: str | None = None
) -> int:
    """Aggregate intraday_bars into eod_ohlcv with REAL high/low from actual trades.

    For each symbol on the given date:
    - open  = close price of the FIRST tick (earliest ts_epoch)
    - high  = MAX(close) across all ticks
    - low   = MIN(close) across all ticks
    - close = close price of the LAST tick (latest ts_epoch)
    - volume = MAX(volume) (cumulative, so max = final)

    Only promotes symbols with >= 2 ticks to avoid single-tick noise.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD. Defaults to today.

    Returns:
        Number of rows promoted to eod_ohlcv.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    now = datetime.now().isoformat()
    old_factory = con.row_factory
    con.row_factory = sqlite3.Row

    # Step 1: aggregate intraday_bars into OHLCV per symbol
    ts_start = f"{date} 00:00:00"
    ts_end = f"{date} 23:59:59"
    rows = con.execute(
        """
        WITH ranked AS (
            SELECT
                symbol,
                close,
                volume,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY ts_epoch ASC
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY ts_epoch DESC
                ) AS rn_last
            FROM intraday_bars
            WHERE ts BETWEEN ? AND ?
        )
        SELECT
            symbol,
            MAX(CASE WHEN rn_first = 1 THEN close END) AS open,
            MAX(close)                                  AS high,
            MIN(close)                                  AS low,
            MAX(CASE WHEN rn_last  = 1 THEN close END) AS close,
            MAX(volume)                                 AS volume
        FROM ranked
        GROUP BY symbol
        HAVING COUNT(*) >= 2
        """,
        (ts_start, ts_end),
    ).fetchall()

    if not rows:
        return 0

    # Step 2: get prev_close, sector_code, company_name from prior date
    prev_close_map = {}
    sector_map = {}
    name_map = {}
    prev_rows = con.execute(
        """
        SELECT symbol, close, sector_code, company_name
        FROM eod_ohlcv
        WHERE date = (
            SELECT MAX(date) FROM eod_ohlcv WHERE date < ?
        )
        """,
        (date,),
    ).fetchall()
    def _pad_sector(code):
        """Normalize sector code to 4-digit zero-padded (e.g. '807' → '0807')."""
        if code and code.isdigit() and len(code) < 4:
            return code.zfill(4)
        return code

    for pr in prev_rows:
        prev_close_map[pr["symbol"]] = pr["close"]
        if pr["sector_code"]:
            sector_map[pr["symbol"]] = _pad_sector(pr["sector_code"])
        if pr["company_name"]:
            name_map[pr["symbol"]] = pr["company_name"]

    # Fill gaps from symbols table (already uses 4-digit codes)
    sym_rows = con.execute(
        "SELECT symbol, sector, name FROM symbols WHERE sector IS NOT NULL"
    ).fetchall()
    for sr in sym_rows:
        if sr["symbol"] not in sector_map and sr["sector"]:
            sector_map[sr["symbol"]] = _pad_sector(sr["sector"])
        if sr["symbol"] not in name_map and sr["name"]:
            name_map[sr["symbol"]] = sr["name"]

    # Step 3: upsert each aggregated row into eod_ohlcv
    count = 0
    for r in rows:
        sym = r["symbol"]
        prev_close = prev_close_map.get(sym)
        sector_code = sector_map.get(sym)
        company_name = name_map.get(sym)
        con.execute(
            """
            INSERT INTO eod_ohlcv
                (symbol, date, open, high, low, close, volume,
                 prev_close, sector_code, company_name,
                 ingested_at, source, processname)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'intraday_aggregation', 'sync_timeseries')
            ON CONFLICT(symbol, date) DO UPDATE SET
                open         = excluded.open,
                high         = excluded.high,
                low          = excluded.low,
                close        = excluded.close,
                volume       = excluded.volume,
                prev_close   = excluded.prev_close,
                sector_code  = COALESCE(excluded.sector_code, eod_ohlcv.sector_code),
                company_name = COALESCE(excluded.company_name, eod_ohlcv.company_name),
                turnover     = COALESCE(eod_ohlcv.turnover, excluded.turnover),
                ingested_at  = excluded.ingested_at,
                source       = excluded.source,
                processname  = excluded.processname
            """,
            (sym, date, r["open"], r["high"], r["low"],
             r["close"], r["volume"], prev_close, sector_code,
             company_name, now),
        )
        count += 1

    con.commit()
    con.row_factory = old_factory
    return count


def get_intraday_dates(con: sqlite3.Connection) -> list[str]:
    """Get distinct dates available in intraday_bars, newest first.

    Returns:
        List of date strings (YYYY-MM-DD).
    """
    old_factory = con.row_factory
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT DISTINCT DATE(ts) AS d FROM intraday_bars ORDER BY d DESC"
    ).fetchall()
    con.row_factory = old_factory
    return [r["d"] for r in rows if r["d"]]
