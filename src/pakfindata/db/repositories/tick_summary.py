"""Tick daily summary — precomputed per-symbol metrics from tick_bars.db.

Reads ohlcv_5s + raw_ticks from tick_bars.db, computes daily aggregates,
and stores in psx.sqlite tick_daily_summary table. The UI reads only
this small summary table instead of scanning millions of raw rows.

Run incrementally: only computes dates not already in the summary table.
"""

import sqlite3
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger("pakfindata.tick_summary")

TICK_BARS_DB = Path("/mnt/e/psxdata/tick_bars.db")

_CREATE_SQL = """\
CREATE TABLE IF NOT EXISTS tick_daily_summary (
    date        TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL DEFAULT 'REG',
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      INTEGER DEFAULT 0,
    trades      INTEGER DEFAULT 0,
    turnover    REAL DEFAULT 0,
    vwap        REAL,
    bar_count   INTEGER DEFAULT 0,
    tick_count  INTEGER DEFAULT 0,
    first_ts    TEXT,
    last_ts     TEXT,
    change      REAL DEFAULT 0,
    change_pct  REAL DEFAULT 0,
    avg_spread_bps REAL DEFAULT 0,
    med_spread_bps REAL DEFAULT 0,
    vol_realized   REAL DEFAULT 0,
    PRIMARY KEY (date, symbol, market)
);
CREATE INDEX IF NOT EXISTS idx_tds_date ON tick_daily_summary(date);
CREATE INDEX IF NOT EXISTS idx_tds_sym  ON tick_daily_summary(symbol, date);
"""

_table_ready = False


def ensure_summary_table(con: sqlite3.Connection) -> None:
    global _table_ready
    if _table_ready:
        return
    con.executescript(_CREATE_SQL)
    _table_ready = True


def get_summary_dates(con: sqlite3.Connection) -> list[str]:
    """Get dates already in the summary table."""
    ensure_summary_table(con)
    rows = con.execute("SELECT DISTINCT date FROM tick_daily_summary ORDER BY date DESC").fetchall()
    return [r[0] for r in rows]


def get_available_dates(con: sqlite3.Connection) -> list[str]:
    """Get all dates available in ohlcv_5s. DuckDB primary, tick_bars.db fallback."""
    # DuckDB primary — instant columnar DISTINCT
    try:
        from pakfindata.db.connections import has_duckdb
        if has_duckdb():
            import duckdb
            from pakfindata.db.duckdb_manager import DUCKDB_PATH
            dcon = duckdb.connect(str(DUCKDB_PATH), read_only=True)
            rows = dcon.execute(
                "SELECT DISTINCT SUBSTR(ts, 1, 10) AS d FROM ohlcv_5s ORDER BY d DESC"
            ).fetchall()
            dcon.close()
            return [r[0] for r in rows]
    except Exception:
        pass

    # SQLite fallback
    if not TICK_BARS_DB.exists():
        return []
    try:
        tcon = sqlite3.connect(f"file:{TICK_BARS_DB}?mode=ro", uri=True, timeout=5)
        for probe in ("OGDC", "PPL", "HBL", "ENGRO", "FFC", "MCB"):
            rows = tcon.execute(
                "SELECT DISTINCT SUBSTR(ts, 1, 10) FROM ohlcv_5s WHERE symbol = ? ORDER BY 1 DESC",
                (probe,),
            ).fetchall()
            if rows:
                tcon.close()
                return [r[0] for r in rows]
        tcon.close()
    except Exception:
        pass
    return []


def compute_daily_summary(con: sqlite3.Connection, date_str: str) -> int:
    """Compute daily summary for a single date. DuckDB primary, tick_bars.db fallback.

    Returns number of symbols summarized.
    """
    ensure_summary_table(con)

    # Try DuckDB first — single vectorized query instead of per-symbol loop
    try:
        from pakfindata.db.connections import has_duckdb
        if has_duckdb():
            rows_to_insert = _compute_summary_duckdb(date_str)
            if rows_to_insert is not None:
                if rows_to_insert:
                    con.executemany(
                        """INSERT OR REPLACE INTO tick_daily_summary
                           (date, symbol, market, open, high, low, close,
                            volume, trades, turnover, vwap, bar_count, tick_count,
                            first_ts, last_ts, change, change_pct,
                            avg_spread_bps, med_spread_bps, vol_realized)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        rows_to_insert,
                    )
                    con.commit()
                count = len(rows_to_insert)
                logger.info("tick_daily_summary (DuckDB): %s — %d symbols", date_str, count)
                return count
    except Exception as e:
        logger.warning("DuckDB summary failed for %s: %s — falling back to SQLite", date_str, e)

    # SQLite fallback
    return _compute_summary_sqlite(con, date_str)


def _compute_summary_duckdb(date_str: str) -> list[tuple] | None:
    """Compute daily summary from DuckDB ohlcv_5s — single vectorized query."""
    import duckdb
    from pakfindata.db.duckdb_manager import DUCKDB_PATH

    dcon = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    ts_prefix = date_str

    # Single query: aggregate all symbols at once
    df = dcon.execute("""
        SELECT
            symbol, market,
            FIRST(o ORDER BY ts) AS day_open,
            MAX(h) AS day_high,
            MIN(l) AS day_low,
            LAST(c ORDER BY ts) AS day_close,
            SUM(v) AS day_volume,
            SUM(trades) AS day_trades,
            SUM(c * v) AS turnover,
            COUNT(*) AS bar_count,
            FIRST(ts ORDER BY ts) AS first_ts,
            LAST(ts ORDER BY ts) AS last_ts
        FROM ohlcv_5s
        WHERE ts LIKE ? || '%'
        GROUP BY symbol, market
        HAVING COUNT(*) >= 1
    """, [ts_prefix]).df()
    dcon.close()

    if df.empty:
        # Fallback: aggregate directly from tick_logs
        dcon = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        src_file = f"ticks_{date_str}.jsonl"
        df = dcon.execute("""
            SELECT
                symbol, market,
                FIRST(price ORDER BY _ts) AS day_open,
                MAX(high) AS day_high,
                MIN(low) AS day_low,
                LAST(price ORDER BY _ts) AS day_close,
                MAX(volume) AS day_volume,
                MAX(trades) AS day_trades,
                MAX(value) AS turnover,
                COUNT(*) AS bar_count,
                FIRST(_ts ORDER BY _ts) AS first_ts,
                LAST(_ts ORDER BY _ts) AS last_ts
            FROM tick_logs
            WHERE source_file = ? AND market != 'IDX'
            GROUP BY symbol, market
            HAVING COUNT(*) >= 1
        """, [src_file]).df()
        dcon.close()

        if df.empty:
            return None  # No data — let fallback try

    rows_to_insert = []
    for _, r in df.iterrows():
        day_open = r["day_open"] or r["day_close"]
        day_close = r["day_close"]
        change = day_close - day_open if day_open else 0
        change_pct = (change / day_open * 100) if day_open else 0
        vwap = r["turnover"] / max(r["day_volume"], 1) if r["day_volume"] else 0

        rows_to_insert.append((
            date_str, r["symbol"], r["market"],
            day_open, r["day_high"], r["day_low"], day_close,
            int(r["day_volume"]), int(r["day_trades"]),
            r["turnover"], vwap,
            int(r["bar_count"]), 0,
            r["first_ts"], r["last_ts"],
            change, change_pct,
            0.0, 0.0, 0.0,
        ))

    return rows_to_insert


def _compute_summary_sqlite(con: sqlite3.Connection, date_str: str) -> int:
    """Compute daily summary from tick_bars.db SQLite (fallback)."""
    if not TICK_BARS_DB.exists():
        return 0

    ensure_summary_table(con)
    ts_start = f"{date_str}T00:00:00"
    ts_end = f"{date_str}T23:59:59"

    tcon = sqlite3.connect(f"file:{TICK_BARS_DB}?mode=ro", uri=True, timeout=10)
    tcon.execute("PRAGMA cache_size=-20000")

    all_syms = [r[0] for r in tcon.execute(
        "SELECT DISTINCT symbol FROM ohlcv_5s"
    ).fetchall()]

    count = 0
    rows_to_insert = []

    for sym in all_syms:
        bars = tcon.execute(
            """SELECT market, ts, o, h, l, c, v, trades
               FROM ohlcv_5s
               WHERE symbol = ? AND ts >= ? AND ts <= ?
               ORDER BY ts""",
            (sym, ts_start, ts_end),
        ).fetchall()

        if not bars:
            continue

        market = bars[0][0]
        opens = [b[2] for b in bars if b[2]]
        highs = [b[3] for b in bars if b[3]]
        lows = [b[4] for b in bars if b[4]]
        closes = [b[5] for b in bars if b[5]]
        volumes = [b[6] or 0 for b in bars]
        trade_counts = [b[7] or 0 for b in bars]

        if not closes:
            continue

        day_open = opens[0] if opens else closes[0]
        day_high = max(highs) if highs else closes[0]
        day_low = min(lows) if lows else closes[0]
        day_close = closes[-1]
        day_volume = sum(volumes)
        day_trades = sum(trade_counts)
        turnover = sum(c * v for c, v in zip(closes, volumes))
        vwap = turnover / max(day_volume, 1)
        change = day_close - day_open
        change_pct = (change / day_open * 100) if day_open else 0
        first_ts = bars[0][1]
        last_ts = bars[-1][1]

        vol_realized = 0.0
        if len(closes) > 1:
            returns = [
                np.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i] > 0 and closes[i - 1] > 0
            ]
            if returns:
                vol_realized = float(np.std(returns)) * np.sqrt(len(returns))

        rows_to_insert.append((
            date_str, sym, market,
            day_open, day_high, day_low, day_close,
            day_volume, day_trades, turnover, vwap,
            len(bars), 0,
            first_ts, last_ts,
            change, change_pct,
            0.0, 0.0, vol_realized,
        ))
        count += 1

    tcon.close()

    if rows_to_insert:
        con.executemany(
            """INSERT OR REPLACE INTO tick_daily_summary
               (date, symbol, market, open, high, low, close,
                volume, trades, turnover, vwap, bar_count, tick_count,
                first_ts, last_ts, change, change_pct,
                avg_spread_bps, med_spread_bps, vol_realized)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows_to_insert,
        )
        con.commit()

    logger.info("tick_daily_summary (SQLite): %s — %d symbols", date_str, count)
    return count


def compute_missing_summaries(con: sqlite3.Connection) -> dict:
    """Compute summaries for all dates not yet in the table.

    Returns dict with: dates_computed, symbols_total
    """
    ensure_summary_table(con)
    available = set(get_available_dates(con))
    existing = set(get_summary_dates(con))
    missing = sorted(available - existing)

    if not missing:
        return {"dates_computed": 0, "symbols_total": 0}

    total_syms = 0
    for d in missing:
        n = compute_daily_summary(con, d)
        total_syms += n

    return {"dates_computed": len(missing), "symbols_total": total_syms}


def get_daily_summary(con: sqlite3.Connection, date_str: str) -> pd.DataFrame:
    """Get the precomputed daily summary for a date. Fast: indexed read."""
    ensure_summary_table(con)
    return pd.read_sql_query(
        "SELECT * FROM tick_daily_summary WHERE date = ? ORDER BY symbol",
        con,
        params=(date_str,),
    )


def get_multi_day_summary(con: sqlite3.Connection, dates: list[str]) -> pd.DataFrame:
    """Get summary for multiple dates. Used for cross-day analysis."""
    ensure_summary_table(con)
    placeholders = ",".join("?" * len(dates))
    return pd.read_sql_query(
        f"SELECT * FROM tick_daily_summary WHERE date IN ({placeholders}) ORDER BY date, symbol",
        con,
        params=dates,
    )


def get_summary_stats(con: sqlite3.Connection) -> dict:
    """Quick stats about the summary table."""
    ensure_summary_table(con)
    try:
        row = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT date), MIN(date), MAX(date) FROM tick_daily_summary"
        ).fetchone()
        return {
            "total_rows": row[0],
            "dates": row[1],
            "first_date": row[2],
            "last_date": row[3],
        }
    except Exception:
        return {"total_rows": 0, "dates": 0, "first_date": None, "last_date": None}
