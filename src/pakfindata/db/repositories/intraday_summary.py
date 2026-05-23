"""Intraday summary tables — precomputed per-symbol / per-minute / per-hour metrics
from the cloud JSONL tick logs at /mnt/e/psxdata/tick_logs_cloud/ticks_{date}.jsonl.

The Intraday page reads these instead of scanning raw `intraday_bars` on every render.
Aggregation uses in-memory DuckDB to parse JSONL; output is written to psx.sqlite.
No persistent DuckDB file is touched — `:memory:` only.

Populate via button on Intraday Sync tab. Sync logic is untouched.
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger("pakfindata.intraday_summary")

JSONL_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
PER_SYMBOL_DIR = Path("/mnt/e/psxdata/intraday")

_CREATE_SQL = """\
CREATE TABLE IF NOT EXISTS intraday_daily_summary (
    date        TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL,
    day_open    REAL,
    day_high    REAL,
    day_low     REAL,
    day_close   REAL,
    prev_close  REAL,
    day_volume  INTEGER DEFAULT 0,
    day_trades  INTEGER DEFAULT 0,
    turnover    REAL DEFAULT 0,
    vwap        REAL,
    tick_count  INTEGER DEFAULT 0,
    first_ts    TEXT,
    last_ts     TEXT,
    change      REAL DEFAULT 0,
    change_pct  REAL DEFAULT 0,
    PRIMARY KEY (date, symbol, market)
);
CREATE INDEX IF NOT EXISTS idx_ids_date   ON intraday_daily_summary(date);
CREATE INDEX IF NOT EXISTS idx_ids_sym    ON intraday_daily_summary(symbol, date);
CREATE INDEX IF NOT EXISTS idx_ids_market ON intraday_daily_summary(market, date);

CREATE TABLE IF NOT EXISTS intraday_minute_breadth (
    date          TEXT NOT NULL,
    minute        TEXT NOT NULL,
    market        TEXT NOT NULL DEFAULT 'REG',
    advancing     INTEGER DEFAULT 0,
    declining     INTEGER DEFAULT 0,
    unchanged     INTEGER DEFAULT 0,
    total_symbols INTEGER DEFAULT 0,
    net_ticks     INTEGER DEFAULT 0,
    PRIMARY KEY (date, minute, market)
);
CREATE INDEX IF NOT EXISTS idx_imb_date ON intraday_minute_breadth(date);

CREATE TABLE IF NOT EXISTS intraday_hourly_summary (
    date         TEXT NOT NULL,
    hour         INTEGER NOT NULL,
    market       TEXT NOT NULL DEFAULT 'REG',
    tick_count   INTEGER DEFAULT 0,
    symbol_count INTEGER DEFAULT 0,
    PRIMARY KEY (date, hour, market)
);
CREATE INDEX IF NOT EXISTS idx_ihs_date ON intraday_hourly_summary(date);

CREATE TABLE IF NOT EXISTS intraday_index_minute (
    date       TEXT NOT NULL,
    minute     TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    last_value REAL,
    PRIMARY KEY (date, symbol, minute)
);
CREATE INDEX IF NOT EXISTS idx_iim_date_sym ON intraday_index_minute(date, symbol);
"""

_tables_ready = False


def ensure_tables(con: sqlite3.Connection) -> None:
    """Create intraday summary tables if missing. Caller commits via pakfindata.db.safe_writer.

    Uses split execute() per statement instead of executescript() so the call
    is safe to invoke inside a safe_writer's BEGIN IMMEDIATE transaction.
    """
    global _tables_ready
    if _tables_ready:
        return
    for stmt in _CREATE_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    _tables_ready = True


def jsonl_path(date_str: str) -> Path:
    return JSONL_DIR / f"ticks_{date_str}.jsonl"


def per_symbol_dir(date_str: str) -> Path:
    return PER_SYMBOL_DIR / date_str


def source_available(date_str: str) -> str | None:
    """Return 'jsonl', 'per_symbol', or None."""
    if jsonl_path(date_str).exists():
        return "jsonl"
    if per_symbol_dir(date_str).is_dir() and any(per_symbol_dir(date_str).glob("*.json")):
        return "per_symbol"
    return None


# ─── Aggregation via in-memory DuckDB (no file locks) ─────────────────────────


def _memory_duck():
    import duckdb
    return duckdb.connect(":memory:")


def _compute_daily_from_jsonl(date_str: str) -> list[tuple]:
    """Aggregate daily OHLC/volume per (symbol, market) from JSONL."""
    jf = str(jsonl_path(date_str))
    dcon = _memory_duck()
    try:
        df = dcon.execute(
            f"""
            SELECT
                symbol,
                market,
                FIRST(open ORDER BY _ts)           AS day_open,
                MAX(high)                           AS day_high,
                MIN(low)                            AS day_low,
                LAST(price ORDER BY _ts)           AS day_close,
                LAST(previousClose ORDER BY _ts)   AS prev_close,
                MAX(volume)                         AS day_volume,
                MAX(trades)                         AS day_trades,
                MAX(value)                          AS turnover,
                COUNT(*)                            AS tick_count,
                FIRST(_ts ORDER BY _ts)            AS first_ts,
                LAST(_ts ORDER BY _ts)             AS last_ts
            FROM read_json_auto('{jf}', ignore_errors=true)
            GROUP BY symbol, market
            """
        ).df()
    finally:
        dcon.close()

    rows = []
    for _, r in df.iterrows():
        day_open = r["day_open"] if r["day_open"] else r["day_close"]
        day_close = r["day_close"]
        prev_close = r["prev_close"] if r["prev_close"] else day_open
        volume = int(r["day_volume"] or 0)
        turnover = float(r["turnover"] or 0)
        vwap = turnover / volume if volume > 0 else (day_close or 0)
        change = (day_close - prev_close) if prev_close else 0
        change_pct = (change / prev_close * 100) if prev_close else 0
        rows.append((
            date_str, r["symbol"], r["market"],
            day_open, r["day_high"], r["day_low"], day_close, prev_close,
            volume, int(r["day_trades"] or 0), turnover, vwap,
            int(r["tick_count"]), r["first_ts"], r["last_ts"],
            change, change_pct,
        ))
    return rows


def _compute_minute_breadth_from_jsonl(date_str: str, market: str = "REG") -> list[tuple]:
    """Per-minute advance/decline counts. Direction = sign(last_price_this_min - last_price_prev_min)."""
    jf = str(jsonl_path(date_str))
    dcon = _memory_duck()
    try:
        df = dcon.execute(
            f"""
            WITH per_min AS (
                SELECT symbol,
                       SUBSTR(_ts, 1, 16) AS minute,
                       LAST(price ORDER BY _ts) AS close_price
                FROM read_json_auto('{jf}', ignore_errors=true)
                WHERE market = '{market}'
                GROUP BY symbol, SUBSTR(_ts, 1, 16)
            ),
            ranked AS (
                SELECT symbol, minute, close_price,
                       LAG(close_price) OVER (PARTITION BY symbol ORDER BY minute) AS prev_price
                FROM per_min
            )
            SELECT minute,
                   SUM(CASE WHEN close_price > prev_price THEN 1 ELSE 0 END) AS advancing,
                   SUM(CASE WHEN close_price < prev_price THEN 1 ELSE 0 END) AS declining,
                   SUM(CASE WHEN close_price = prev_price THEN 1 ELSE 0 END) AS unchanged,
                   COUNT(DISTINCT symbol) AS total_symbols,
                   SUM(CASE WHEN close_price > prev_price THEN 1
                            WHEN close_price < prev_price THEN -1
                            ELSE 0 END) AS net_ticks
            FROM ranked
            WHERE prev_price IS NOT NULL
            GROUP BY minute
            ORDER BY minute
            """
        ).df()
    finally:
        dcon.close()

    rows = [
        (date_str, r["minute"], market,
         int(r["advancing"]), int(r["declining"]), int(r["unchanged"]),
         int(r["total_symbols"]), int(r["net_ticks"]))
        for _, r in df.iterrows()
    ]
    return rows


def _compute_hourly_from_jsonl(date_str: str, market: str = "REG") -> list[tuple]:
    """Per-hour tick counts and distinct symbol counts."""
    jf = str(jsonl_path(date_str))
    dcon = _memory_duck()
    try:
        df = dcon.execute(
            f"""
            SELECT
                CAST(SUBSTR(_ts, 12, 2) AS INTEGER) AS hour,
                COUNT(*) AS tick_count,
                COUNT(DISTINCT symbol) AS symbol_count
            FROM read_json_auto('{jf}', ignore_errors=true)
            WHERE market = '{market}'
            GROUP BY CAST(SUBSTR(_ts, 12, 2) AS INTEGER)
            ORDER BY hour
            """
        ).df()
    finally:
        dcon.close()

    return [
        (date_str, int(r["hour"]), market,
         int(r["tick_count"]), int(r["symbol_count"]))
        for _, r in df.iterrows()
    ]


def _compute_index_minute_from_jsonl(date_str: str) -> list[tuple]:
    """Per-minute last value per IDX symbol. Powers the KSE-100 overlay chart."""
    jf = str(jsonl_path(date_str))
    dcon = _memory_duck()
    try:
        df = dcon.execute(
            f"""
            SELECT
                symbol,
                REPLACE(SUBSTR(_ts, 1, 16), 'T', ' ') AS minute,
                LAST(price ORDER BY _ts) AS last_value
            FROM read_json_auto('{jf}', ignore_errors=true)
            WHERE market = 'IDX'
            GROUP BY symbol, REPLACE(SUBSTR(_ts, 1, 16), 'T', ' ')
            ORDER BY symbol, minute
            """
        ).df()
    finally:
        dcon.close()

    return [
        (date_str, r["minute"], r["symbol"], float(r["last_value"]))
        for _, r in df.iterrows()
    ]


# ─── Public API ────────────────────────────────────────────────────────────────


def compute_daily_summary(con: sqlite3.Connection, date_str: str) -> int:
    """Caller commits via pakfindata.db.safe_writer."""
    ensure_tables(con)
    if not jsonl_path(date_str).exists():
        logger.warning("JSONL missing for %s", date_str)
        return 0
    rows = _compute_daily_from_jsonl(date_str)
    if not rows:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO intraday_daily_summary
           (date, symbol, market, day_open, day_high, day_low, day_close, prev_close,
            day_volume, day_trades, turnover, vwap, tick_count, first_ts, last_ts,
            change, change_pct)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def compute_minute_breadth(con: sqlite3.Connection, date_str: str, market: str = "REG") -> int:
    """Caller commits via pakfindata.db.safe_writer."""
    ensure_tables(con)
    if not jsonl_path(date_str).exists():
        return 0
    rows = _compute_minute_breadth_from_jsonl(date_str, market=market)
    if not rows:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO intraday_minute_breadth
           (date, minute, market, advancing, declining, unchanged, total_symbols, net_ticks)
           VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def compute_hourly_summary(con: sqlite3.Connection, date_str: str, market: str = "REG") -> int:
    """Caller commits via pakfindata.db.safe_writer."""
    ensure_tables(con)
    if not jsonl_path(date_str).exists():
        return 0
    rows = _compute_hourly_from_jsonl(date_str, market=market)
    if not rows:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO intraday_hourly_summary
           (date, hour, market, tick_count, symbol_count)
           VALUES (?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def compute_index_minute(con: sqlite3.Connection, date_str: str) -> int:
    """Caller commits via pakfindata.db.safe_writer."""
    ensure_tables(con)
    if not jsonl_path(date_str).exists():
        return 0
    rows = _compute_index_minute_from_jsonl(date_str)
    if not rows:
        return 0
    con.executemany(
        """INSERT OR REPLACE INTO intraday_index_minute
           (date, minute, symbol, last_value)
           VALUES (?,?,?,?)""",
        rows,
    )
    return len(rows)


def compute_all(con: sqlite3.Connection, date_str: str) -> dict:
    """Run all four aggregations for a date. Returns per-table row counts + timings."""
    import time
    ensure_tables(con)
    result: dict[str, Any] = {"date": date_str, "source": source_available(date_str)}
    if result["source"] != "jsonl":
        result["error"] = f"No JSONL for {date_str}"
        return result

    t0 = time.perf_counter()
    result["daily"] = compute_daily_summary(con, date_str)
    t1 = time.perf_counter()
    result["minute_breadth"] = compute_minute_breadth(con, date_str)
    t2 = time.perf_counter()
    result["hourly"] = compute_hourly_summary(con, date_str)
    t3 = time.perf_counter()
    result["index_minute"] = compute_index_minute(con, date_str)
    t4 = time.perf_counter()
    result["timings"] = {
        "daily_s":          round(t1 - t0, 2),
        "minute_breadth_s": round(t2 - t1, 2),
        "hourly_s":         round(t3 - t2, 2),
        "index_minute_s":   round(t4 - t3, 2),
        "total_s":          round(t4 - t0, 2),
    }
    return result


def get_index_minute(con: sqlite3.Connection, date_str: str, symbols: list[str]):
    import pandas as pd
    ensure_tables(con)
    placeholders = ",".join("?" * len(symbols))
    return pd.read_sql_query(
        f"SELECT minute, symbol, last_value AS value "
        f"FROM intraday_index_minute "
        f"WHERE date = ? AND symbol IN ({placeholders}) "
        f"ORDER BY minute",
        con, params=[date_str] + list(symbols),
    )


def get_summary_dates(con: sqlite3.Connection) -> list[str]:
    ensure_tables(con)
    rows = con.execute(
        "SELECT DISTINCT date FROM intraday_daily_summary ORDER BY date DESC"
    ).fetchall()
    return [r[0] for r in rows]


def get_daily_summary(con: sqlite3.Connection, date_str: str):
    import pandas as pd
    ensure_tables(con)
    return pd.read_sql_query(
        "SELECT * FROM intraday_daily_summary WHERE date = ? ORDER BY day_volume DESC",
        con, params=(date_str,),
    )


def get_minute_breadth(con: sqlite3.Connection, date_str: str, market: str = "REG"):
    import pandas as pd
    ensure_tables(con)
    return pd.read_sql_query(
        "SELECT * FROM intraday_minute_breadth WHERE date = ? AND market = ? ORDER BY minute",
        con, params=(date_str, market),
    )


def get_hourly_summary(con: sqlite3.Connection, date_str: str, market: str = "REG"):
    import pandas as pd
    ensure_tables(con)
    return pd.read_sql_query(
        "SELECT * FROM intraday_hourly_summary WHERE date = ? AND market = ? ORDER BY hour",
        con, params=(date_str, market),
    )


def get_stats(con: sqlite3.Connection) -> dict:
    ensure_tables(con)
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT date), MIN(date), MAX(date) "
        "FROM intraday_daily_summary"
    ).fetchone()
    return {
        "total_rows": row[0],
        "dates": row[1],
        "first_date": row[2],
        "last_date": row[3],
    }
