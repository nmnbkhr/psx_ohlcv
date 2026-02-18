"""Tick data repository — raw tick storage and OHLCV aggregation from live market-watch polling."""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

__all__ = [
    "init_tick_schema",
    "insert_ticks_batch",
    "upsert_tick_ohlcv",
    "get_ticks_for_symbol_today",
    "get_tick_ohlcv_today",
    "get_tick_ohlcv_symbol",
    "promote_tick_ohlcv_to_eod",
    "cleanup_old_ticks",
]

TICK_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS tick_data (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol            TEXT NOT NULL,
    timestamp         INTEGER NOT NULL,
    price             REAL NOT NULL,
    change            REAL DEFAULT 0,
    change_pct        REAL DEFAULT 0,
    cumulative_volume INTEGER DEFAULT 0,
    mw_high           REAL DEFAULT 0,
    mw_low            REAL DEFAULT 0,
    mw_open           REAL DEFAULT 0,
    UNIQUE(symbol, timestamp, price)
);

CREATE INDEX IF NOT EXISTS idx_tick_symbol_ts ON tick_data(symbol, timestamp);

CREATE TABLE IF NOT EXISTS tick_ohlcv (
    symbol        TEXT NOT NULL,
    date          TEXT NOT NULL,
    open          REAL NOT NULL,
    high          REAL NOT NULL,
    low           REAL NOT NULL,
    close         REAL NOT NULL,
    volume        INTEGER DEFAULT 0,
    tick_count    INTEGER DEFAULT 0,
    first_tick_ts INTEGER,
    last_tick_ts  INTEGER,
    source        TEXT DEFAULT 'tick_collector',
    PRIMARY KEY(symbol, date)
);
"""


def init_tick_schema(con: sqlite3.Connection) -> None:
    """Create tick tables if they don't exist."""
    con.executescript(TICK_SCHEMA_SQL)
    con.commit()


def insert_ticks_batch(
    con: sqlite3.Connection, ticks: list[dict]
) -> int:
    """Insert a batch of tick records.

    Args:
        con: Database connection
        ticks: List of dicts with keys: symbol, timestamp, price,
               change, change_pct, cumulative_volume, mw_high, mw_low, mw_open

    Returns:
        Number of rows inserted (duplicates silently ignored).
    """
    if not ticks:
        return 0

    count = 0
    for t in ticks:
        try:
            con.execute(
                """INSERT OR IGNORE INTO tick_data
                   (symbol, timestamp, price, change, change_pct,
                    cumulative_volume, mw_high, mw_low, mw_open)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    t["symbol"],
                    t["timestamp"],
                    t["price"],
                    t.get("change", 0),
                    t.get("change_pct", 0),
                    t.get("cumulative_volume", 0),
                    t.get("mw_high", 0),
                    t.get("mw_low", 0),
                    t.get("mw_open", 0),
                ),
            )
            count += con.total_changes  # approximate
        except sqlite3.IntegrityError:
            pass

    con.commit()
    return count


def upsert_tick_ohlcv(
    con: sqlite3.Connection, ohlcv_rows: list[dict]
) -> int:
    """Upsert running OHLCV aggregations into tick_ohlcv table.

    Args:
        con: Database connection
        ohlcv_rows: List of dicts with keys: symbol, date, open, high, low,
                    close, volume, tick_count, first_tick_ts, last_tick_ts

    Returns:
        Number of rows upserted.
    """
    if not ohlcv_rows:
        return 0

    count = 0
    for row in ohlcv_rows:
        con.execute(
            """INSERT INTO tick_ohlcv
               (symbol, date, open, high, low, close, volume,
                tick_count, first_tick_ts, last_tick_ts, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'tick_collector')
               ON CONFLICT(symbol, date) DO UPDATE SET
                   open = excluded.open,
                   high = excluded.high,
                   low  = excluded.low,
                   close = excluded.close,
                   volume = excluded.volume,
                   tick_count = excluded.tick_count,
                   first_tick_ts = excluded.first_tick_ts,
                   last_tick_ts  = excluded.last_tick_ts""",
            (
                row["symbol"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row.get("volume", 0),
                row.get("tick_count", 0),
                row.get("first_tick_ts"),
                row.get("last_tick_ts"),
            ),
        )
        count += 1

    con.commit()
    return count


def get_ticks_for_symbol_today(
    con: sqlite3.Connection, symbol: str, date: str | None = None
) -> pd.DataFrame:
    """Get all raw ticks for a symbol on a given date.

    Args:
        con: Database connection
        symbol: Stock symbol
        date: Date string YYYY-MM-DD. Defaults to today.

    Returns:
        DataFrame of tick records sorted by timestamp.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    # Convert date to epoch range
    day_start = int(datetime.strptime(date, "%Y-%m-%d").timestamp())
    day_end = day_start + 86400

    df = pd.read_sql_query(
        """SELECT * FROM tick_data
           WHERE symbol = ? AND timestamp >= ? AND timestamp < ?
           ORDER BY timestamp""",
        con,
        params=(symbol, day_start, day_end),
    )
    return df


def get_tick_ohlcv_today(
    con: sqlite3.Connection, date: str | None = None
) -> pd.DataFrame:
    """Get tick-built OHLCV for all symbols on a given date.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD. Defaults to today.

    Returns:
        DataFrame of tick_ohlcv records.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    df = pd.read_sql_query(
        "SELECT * FROM tick_ohlcv WHERE date = ? ORDER BY symbol",
        con,
        params=(date,),
    )
    return df


def get_tick_ohlcv_symbol(
    con: sqlite3.Connection, symbol: str, limit: int = 90
) -> pd.DataFrame:
    """Get tick-built OHLCV history for a specific symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Max rows to return

    Returns:
        DataFrame of tick_ohlcv records for the symbol, newest first.
    """
    df = pd.read_sql_query(
        "SELECT * FROM tick_ohlcv WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        con,
        params=(symbol, limit),
    )
    return df


def promote_tick_ohlcv_to_eod(
    con: sqlite3.Connection, date: str | None = None
) -> int:
    """Copy tick-built OHLCV into eod_ohlcv table with source='tick_aggregation'.

    This solves the fake H/L problem: tick-collected OHLCV gives REAL high/low.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD. Defaults to today.

    Returns:
        Number of rows promoted.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    now = datetime.now().isoformat()
    cur = con.execute(
        """INSERT INTO eod_ohlcv
           (symbol, date, open, high, low, close, volume, ingested_at, source, processname)
           SELECT symbol, date, open, high, low, close, volume, ?, 'tick_aggregation', 'tick_collector'
           FROM tick_ohlcv
           WHERE date = ?
           ON CONFLICT(symbol, date) DO UPDATE SET
               open   = excluded.open,
               high   = excluded.high,
               low    = excluded.low,
               close  = excluded.close,
               volume = excluded.volume,
               ingested_at = excluded.ingested_at,
               source      = excluded.source,
               processname = excluded.processname""",
        (now, date),
    )
    con.commit()
    return cur.rowcount


def cleanup_old_ticks(con: sqlite3.Connection, days: int = 7) -> int:
    """Delete raw tick_data older than N days to control DB growth.

    Args:
        con: Database connection
        days: Number of days to keep (default: 7)

    Returns:
        Number of rows deleted.
    """
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
    cur = con.execute("DELETE FROM tick_data WHERE timestamp < ?", (cutoff,))
    con.commit()
    return cur.rowcount
