"""Market-summary queries and pre-computed summary tables.

Overview
--------
The functions in this module power the Dashboard and Market Pulse pages.
Historically every page load re-scanned ``eod_ohlcv`` to derive breadth,
top movers, volume/value leaders, change distribution, and sector roll-ups.
That duplicated logic across the UI and caused subtle drift between pages.

This module now supports a two-tier model:

1. **Pre-computed summary tables** populated per-date after EOD ingest:
     - ``eod_symbol_summary``  — one row per (date, symbol) with derived
       columns (change_pct, turnover, is_gainer/loser) and dense ranks
       (change desc/asc, volume, turnover).
     - ``eod_market_summary``  — one row per date (breadth rollup).
     - ``eod_sector_summary``  — one row per (date, sector_name).
2. **Raw-query fallback**: readers prefer the summary tables but fall back
   to the original ``eod_ohlcv`` queries if summaries haven't been built
   for that date yet. UI pages don't care which path served the data.

ETL
---
Call :func:`refresh_eod_summary` after ``eod_ohlcv`` has been upserted for
a date, or :func:`refresh_eod_summary_range` to (re)build history.

Reads
-----
``get_latest_full_trading_day``, ``get_eod_breadth``, ``get_top_movers``,
``get_volume_leaders``, ``get_value_leaders``, ``get_52w_extremes``,
``get_change_distribution``, ``get_sector_performance``,
``get_recent_announcements`` — all safe to call before summaries exist.
"""

from __future__ import annotations

import sqlite3

import pandas as pd


# =============================================================================
# Schema
# =============================================================================

_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS eod_symbol_summary (
    date              TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    sector_name       TEXT,
    close             REAL,
    prev_close        REAL,
    change_pct        REAL,
    volume            INTEGER,
    turnover          REAL,
    is_gainer         INTEGER NOT NULL DEFAULT 0,
    is_loser          INTEGER NOT NULL DEFAULT 0,
    rank_change_desc  INTEGER,
    rank_change_asc   INTEGER,
    rank_volume       INTEGER,
    rank_turnover     INTEGER,
    ingested_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_ess_date_chg_desc ON eod_symbol_summary(date, rank_change_desc);
CREATE INDEX IF NOT EXISTS idx_ess_date_chg_asc  ON eod_symbol_summary(date, rank_change_asc);
CREATE INDEX IF NOT EXISTS idx_ess_date_vol      ON eod_symbol_summary(date, rank_volume);
CREATE INDEX IF NOT EXISTS idx_ess_date_turn     ON eod_symbol_summary(date, rank_turnover);
CREATE INDEX IF NOT EXISTS idx_ess_symbol        ON eod_symbol_summary(symbol);

CREATE TABLE IF NOT EXISTS eod_market_summary (
    date          TEXT PRIMARY KEY,
    total         INTEGER,
    gainers       INTEGER,
    losers        INTEGER,
    unchanged     INTEGER,
    avg_change    REAL,
    total_volume  INTEGER,
    total_value   REAL,
    ingested_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS eod_sector_summary (
    date          TEXT NOT NULL,
    sector_name   TEXT NOT NULL,
    stocks        INTEGER,
    avg_change    REAL,
    total_volume  INTEGER,
    up            INTEGER,
    down          INTEGER,
    ingested_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (date, sector_name)
);

CREATE INDEX IF NOT EXISTS idx_sec_date ON eod_sector_summary(date);
"""


def init_eod_summary_schema(con: sqlite3.Connection) -> None:
    """Create the three summary tables and their indexes (idempotent)."""
    con.executescript(_SCHEMA_DDL)


# =============================================================================
# ETL — populate summary tables from eod_ohlcv
# =============================================================================

def refresh_eod_summary(con: sqlite3.Connection, date: str) -> int:
    """Rebuild ``eod_{symbol,market,sector}_summary`` rows for a single date.

    Reads raw rows from ``eod_ohlcv`` (joined to ``symbols.sector_name``),
    computes derived columns and dense ranks in pandas, then replaces any
    existing rows for ``date`` atomically.

    Returns the number of symbol-rows written. Returns 0 if no rows in
    ``eod_ohlcv`` for ``date`` have a positive ``prev_close``.
    """
    init_eod_summary_schema(con)

    df = pd.read_sql_query(
        """SELECT e.date, e.symbol, e.close, e.prev_close, e.volume,
                  COALESCE(s.sector_name, 'Unknown') AS sector_name
           FROM eod_ohlcv e
           LEFT JOIN symbols s ON e.symbol = s.symbol
           WHERE e.date = ? AND e.prev_close > 0""",
        con,
        params=(date,),
    )
    if df.empty:
        return 0

    df["change_pct"] = ((df["close"] - df["prev_close"]) / df["prev_close"] * 100).round(2)
    df["turnover"] = (df["close"] * df["volume"]).astype(float)
    df["is_gainer"] = (df["close"] > df["prev_close"]).astype(int)
    df["is_loser"] = (df["close"] < df["prev_close"]).astype(int)

    df["rank_change_desc"] = df["change_pct"].rank(method="dense", ascending=False).astype("Int64")
    df["rank_change_asc"]  = df["change_pct"].rank(method="dense", ascending=True).astype("Int64")
    df["rank_volume"]      = df["volume"].rank(method="dense", ascending=False).astype("Int64")
    df["rank_turnover"]    = df["turnover"].rank(method="dense", ascending=False).astype("Int64")

    # ---- symbol_summary ----
    con.execute("DELETE FROM eod_symbol_summary WHERE date = ?", (date,))
    symbol_cols = [
        "date", "symbol", "sector_name", "close", "prev_close", "change_pct",
        "volume", "turnover", "is_gainer", "is_loser",
        "rank_change_desc", "rank_change_asc", "rank_volume", "rank_turnover",
    ]
    rows = []
    for r in df[symbol_cols].to_dict(orient="records"):
        rows.append((
            r["date"], r["symbol"], r["sector_name"],
            None if pd.isna(r["close"]) else float(r["close"]),
            None if pd.isna(r["prev_close"]) else float(r["prev_close"]),
            None if pd.isna(r["change_pct"]) else float(r["change_pct"]),
            None if pd.isna(r["volume"]) else int(r["volume"]),
            None if pd.isna(r["turnover"]) else float(r["turnover"]),
            int(r["is_gainer"]), int(r["is_loser"]),
            None if pd.isna(r["rank_change_desc"]) else int(r["rank_change_desc"]),
            None if pd.isna(r["rank_change_asc"])  else int(r["rank_change_asc"]),
            None if pd.isna(r["rank_volume"])      else int(r["rank_volume"]),
            None if pd.isna(r["rank_turnover"])    else int(r["rank_turnover"]),
        ))
    con.executemany(
        """INSERT INTO eod_symbol_summary
           (date, symbol, sector_name, close, prev_close, change_pct, volume,
            turnover, is_gainer, is_loser,
            rank_change_desc, rank_change_asc, rank_volume, rank_turnover)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )

    # ---- market_summary (one row) ----
    total = len(df)
    gainers = int(df["is_gainer"].sum())
    losers = int(df["is_loser"].sum())
    unchanged = total - gainers - losers
    avg_change = round(float(df["change_pct"].mean()), 2) if total else 0.0
    total_volume = int(df["volume"].fillna(0).sum())
    total_value = float(df["turnover"].fillna(0).sum())
    con.execute(
        """INSERT OR REPLACE INTO eod_market_summary
           (date, total, gainers, losers, unchanged, avg_change,
            total_volume, total_value, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
        (date, total, gainers, losers, unchanged, avg_change, total_volume, total_value),
    )

    # ---- sector_summary ----
    sec = (
        df.groupby("sector_name")
          .agg(stocks=("symbol", "count"),
               avg_change=("change_pct", "mean"),
               total_volume=("volume", "sum"),
               up=("is_gainer", "sum"),
               down=("is_loser", "sum"))
          .reset_index()
    )
    sec["avg_change"] = sec["avg_change"].round(2)
    con.execute("DELETE FROM eod_sector_summary WHERE date = ?", (date,))
    con.executemany(
        """INSERT INTO eod_sector_summary
           (date, sector_name, stocks, avg_change, total_volume, up, down)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (date, row["sector_name"], int(row["stocks"]),
             float(row["avg_change"]) if not pd.isna(row["avg_change"]) else None,
             int(row["total_volume"]) if not pd.isna(row["total_volume"]) else 0,
             int(row["up"]), int(row["down"]))
            for _, row in sec.iterrows()
        ],
    )

    con.commit()
    return total


def refresh_eod_summary_range(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
    only_missing: bool = False,
) -> dict:
    """Rebuild summaries for every trading date in ``eod_ohlcv`` within the
    range ``[start_date, end_date]`` (both inclusive; both optional — defaults
    walk the entire table).

    Args:
        con: Database connection.
        start_date: Inclusive lower bound (``YYYY-MM-DD``) or None for earliest.
        end_date:   Inclusive upper bound (``YYYY-MM-DD``) or None for latest.
        only_missing: When True, skip dates that already have a row in
            ``eod_market_summary`` — useful for backfill without recompute.

    Returns a dict with ``dates_processed``, ``rows_written``, ``skipped``,
    and the first/last processed dates.
    """
    init_eod_summary_schema(con)

    q = "SELECT DISTINCT date FROM eod_ohlcv WHERE 1=1"
    params: list = []
    if start_date:
        q += " AND date >= ?"
        params.append(start_date)
    if end_date:
        q += " AND date <= ?"
        params.append(end_date)
    q += " ORDER BY date"
    dates = [r[0] for r in con.execute(q, params).fetchall() if r[0]]

    if only_missing:
        existing = {r[0] for r in con.execute("SELECT date FROM eod_market_summary").fetchall()}
        dates = [d for d in dates if d not in existing]

    processed = 0
    rows_written = 0
    for d in dates:
        n = refresh_eod_summary(con, d)
        rows_written += n
        if n:
            processed += 1

    return {
        "dates_processed": processed,
        "dates_considered": len(dates),
        "rows_written": rows_written,
        "skipped": len(dates) - processed,
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
    }


def summary_coverage(con: sqlite3.Connection) -> dict:
    """Quick diagnostics: row counts and date ranges for the summary tables.

    Useful for UI "is this built out?" checks before running a rebuild.
    """
    out: dict = {}
    for t in ("eod_symbol_summary", "eod_market_summary", "eod_sector_summary"):
        try:
            row = con.execute(
                f"SELECT COUNT(*) AS n, MIN(date) AS min_d, MAX(date) AS max_d FROM {t}"
            ).fetchone()
            out[t] = {"rows": row[0], "min_date": row[1], "max_date": row[2]}
        except Exception:
            out[t] = {"rows": 0, "min_date": None, "max_date": None}
    try:
        row = con.execute(
            "SELECT COUNT(DISTINCT date) AS n, MIN(date) AS min_d, MAX(date) AS max_d FROM eod_ohlcv"
        ).fetchone()
        out["eod_ohlcv"] = {"dates": row[0], "min_date": row[1], "max_date": row[2]}
    except Exception:
        out["eod_ohlcv"] = {"dates": 0, "min_date": None, "max_date": None}
    return out


# =============================================================================
# Reads — prefer summary tables, fall back to raw eod_ohlcv
# =============================================================================

def _summary_has_date(con: sqlite3.Connection, date: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM eod_market_summary WHERE date = ? LIMIT 1", (date,)
        ).fetchone()
        return row is not None
    except Exception:
        return False


def get_latest_full_trading_day(
    con: sqlite3.Connection,
    min_symbols: int = 100,
) -> str | None:
    """Most recent date in ``eod_ohlcv`` with at least ``min_symbols`` distinct
    symbols traded. Used as the canonical "today" on overview pages so a
    partially-loaded day doesn't produce misleading breadth.
    """
    try:
        row = con.execute(
            """SELECT date FROM eod_ohlcv
               GROUP BY date HAVING COUNT(DISTINCT symbol) >= ?
               ORDER BY date DESC LIMIT 1""",
            (min_symbols,),
        ).fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_eod_breadth(
    con: sqlite3.Connection,
    date: str | None = None,
    min_symbols: int = 100,
) -> dict | None:
    """Return breadth counts for a given EOD date.

    Prefers ``eod_market_summary`` (1-row lookup). Falls back to the
    ``eod_ohlcv`` fast path (``prev_close``-aware), and then to a CTE-based
    comparison against the previous trading day.

    Returns dict with ``total, gainers, losers, unchanged, avg_change,
    total_volume, total_value, date`` — or None if no data.
    """
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return None

    # Preferred path: pre-computed summary row
    try:
        row = con.execute(
            """SELECT date, total, gainers, losers, unchanged,
                      avg_change, total_volume, total_value
               FROM eod_market_summary WHERE date = ?""",
            (date,),
        ).fetchone()
        if row:
            return dict(row)
    except Exception:
        pass

    return _get_eod_breadth_raw(con, date)


def _get_eod_breadth_raw(con: sqlite3.Connection, date: str) -> dict | None:
    # Fast path: row-local prev_close comparison
    try:
        row = con.execute(
            """SELECT
                 COUNT(*)                                                         AS total,
                 SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END)               AS gainers,
                 SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END)               AS losers,
                 SUM(CASE WHEN close = prev_close OR prev_close IS NULL OR prev_close = 0
                          THEN 1 ELSE 0 END)                                        AS unchanged,
                 ROUND(AVG(CASE WHEN prev_close > 0
                                THEN (close - prev_close) / prev_close * 100 END), 2) AS avg_change,
                 SUM(volume)                                                       AS total_volume,
                 SUM(close * volume)                                               AS total_value
               FROM eod_ohlcv
               WHERE date = ? AND prev_close > 0""",
            (date,),
        ).fetchone()
        if row and row["total"] and row["total"] > 0:
            d = dict(row)
            d["date"] = date
            return d
    except Exception:
        pass

    # Fallback: CTE against previous trading day
    try:
        row = con.execute(
            """WITH today AS (
                 SELECT symbol, close, volume FROM eod_ohlcv WHERE date = ?
               ),
               prev AS (
                 SELECT symbol, close AS prev_close FROM eod_ohlcv
                 WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < ?)
               ),
               changes AS (
                 SELECT t.symbol, t.volume, t.close,
                        CASE WHEN p.prev_close > 0
                             THEN (t.close - p.prev_close) / p.prev_close * 100
                             ELSE 0 END AS change_pct
                 FROM today t LEFT JOIN prev p ON t.symbol = p.symbol
               )
               SELECT
                 COUNT(*)                                                  AS total,
                 SUM(CASE WHEN change_pct > 0.01 THEN 1 ELSE 0 END)         AS gainers,
                 SUM(CASE WHEN change_pct < -0.01 THEN 1 ELSE 0 END)        AS losers,
                 SUM(CASE WHEN change_pct BETWEEN -0.01 AND 0.01 THEN 1 ELSE 0 END) AS unchanged,
                 ROUND(AVG(change_pct), 2)                                  AS avg_change,
                 SUM(volume)                                                AS total_volume,
                 SUM(close * volume)                                        AS total_value
               FROM changes""",
            (date, date),
        ).fetchone()
        if row:
            d = dict(row)
            d["date"] = date
            return d
    except Exception:
        pass

    return None


def get_top_movers(
    con: sqlite3.Connection,
    direction: str = "gainers",
    date: str | None = None,
    limit: int = 10,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """Top-N gainers or losers for a date.

    Columns: ``symbol, close, prev_close, change_pct, volume``.
    """
    if direction not in ("gainers", "losers"):
        raise ValueError("direction must be 'gainers' or 'losers'")
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return pd.DataFrame()

    if _summary_has_date(con, date):
        rank_col = "rank_change_desc" if direction == "gainers" else "rank_change_asc"
        try:
            return pd.read_sql_query(
                f"""SELECT symbol, close, prev_close, change_pct, volume
                    FROM eod_symbol_summary
                    WHERE date = ? AND is_{'gainer' if direction == 'gainers' else 'loser'} = 1
                    ORDER BY {rank_col} ASC LIMIT ?""",
                con,
                params=(date, limit),
            )
        except Exception:
            pass
    return _get_top_movers_raw(con, direction, date, limit)


def _get_top_movers_raw(con: sqlite3.Connection, direction: str, date: str, limit: int) -> pd.DataFrame:
    cmp_op = ">" if direction == "gainers" else "<"
    order = "DESC" if direction == "gainers" else "ASC"
    try:
        return pd.read_sql_query(
            f"""SELECT symbol, close, prev_close,
                       ROUND((close - prev_close) / prev_close * 100, 2) AS change_pct,
                       volume
                FROM eod_ohlcv
                WHERE date = ? AND prev_close > 0 AND close {cmp_op} prev_close
                ORDER BY change_pct {order} LIMIT ?""",
            con,
            params=(date, limit),
        )
    except Exception:
        return pd.DataFrame()


def get_volume_leaders(
    con: sqlite3.Connection,
    date: str | None = None,
    limit: int = 10,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """Top-N by share volume for a date.

    Columns: ``symbol, close, volume, change_pct``.
    """
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return pd.DataFrame()

    if _summary_has_date(con, date):
        try:
            return pd.read_sql_query(
                """SELECT symbol, close, volume, change_pct
                   FROM eod_symbol_summary
                   WHERE date = ? AND volume > 0
                   ORDER BY rank_volume ASC LIMIT ?""",
                con,
                params=(date, limit),
            )
        except Exception:
            pass
    try:
        return pd.read_sql_query(
            """SELECT symbol, close, volume,
                      ROUND((close - prev_close) / prev_close * 100, 2) AS change_pct
               FROM eod_ohlcv
               WHERE date = ? AND volume > 0 AND prev_close > 0
               ORDER BY volume DESC LIMIT ?""",
            con,
            params=(date, limit),
        )
    except Exception:
        return pd.DataFrame()


def get_value_leaders(
    con: sqlite3.Connection,
    date: str | None = None,
    limit: int = 10,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """Top-N by turnover (close × volume) for a date.

    Columns: ``symbol, close, volume, value, change_pct``.
    """
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return pd.DataFrame()

    if _summary_has_date(con, date):
        try:
            return pd.read_sql_query(
                """SELECT symbol, close, volume, turnover AS value, change_pct
                   FROM eod_symbol_summary
                   WHERE date = ? AND volume > 0
                   ORDER BY rank_turnover ASC LIMIT ?""",
                con,
                params=(date, limit),
            )
        except Exception:
            pass
    try:
        return pd.read_sql_query(
            """SELECT symbol, close, volume, close * volume AS value,
                      ROUND((close - prev_close) / prev_close * 100, 2) AS change_pct
               FROM eod_ohlcv
               WHERE date = ? AND volume > 0 AND prev_close > 0
               ORDER BY value DESC LIMIT ?""",
            con,
            params=(date, limit),
        )
    except Exception:
        return pd.DataFrame()


def get_52w_extremes(
    con: sqlite3.Connection,
    near: str = "high",
    limit: int = 3,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """Symbols nearest the 52-week high (or low) from ``trading_sessions``.

    Not summary-backed — ``trading_sessions`` already stores the 52w columns
    pre-computed, so an additional summary adds no speed.

    Columns: ``symbol, pos_pct``. ``pos_pct`` is 100 at 52w high, 0 at low.
    """
    if near not in ("high", "low"):
        raise ValueError("near must be 'high' or 'low'")
    order = "DESC" if near == "high" else "ASC"
    try:
        return pd.read_sql_query(
            f"""WITH best_date AS (
                 SELECT session_date FROM trading_sessions
                 WHERE market_type='REG' AND week_52_high > 0 AND week_52_low > 0
                 GROUP BY session_date HAVING COUNT(DISTINCT symbol) >= ?
                 ORDER BY session_date DESC LIMIT 1
               )
               SELECT symbol,
                      CASE WHEN (week_52_high - week_52_low) > 0
                           THEN ROUND((COALESCE(close, high, ldcp) - week_52_low)
                                      / (week_52_high - week_52_low) * 100, 1)
                           ELSE 50 END AS pos_pct
               FROM trading_sessions
               WHERE session_date = (SELECT session_date FROM best_date)
                 AND market_type='REG'
                 AND week_52_high > 0 AND week_52_low > 0
                 AND COALESCE(close, high, ldcp) > 0
               ORDER BY pos_pct {order} LIMIT ?""",
            con,
            params=(min_symbols, limit),
        )
    except Exception:
        return pd.DataFrame()


def get_change_distribution(
    con: sqlite3.Connection,
    date: str | None = None,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """One row per symbol with rounded daily change% — for histograms.

    Columns: ``chg_pct``.
    """
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return pd.DataFrame()

    if _summary_has_date(con, date):
        try:
            return pd.read_sql_query(
                """SELECT ROUND(change_pct, 1) AS chg_pct
                   FROM eod_symbol_summary WHERE date = ?""",
                con,
                params=(date,),
            )
        except Exception:
            pass
    try:
        return pd.read_sql_query(
            """SELECT ROUND((close - prev_close) / prev_close * 100, 1) AS chg_pct
               FROM eod_ohlcv
               WHERE date = ? AND prev_close > 0""",
            con,
            params=(date,),
        )
    except Exception:
        return pd.DataFrame()


def get_sector_performance(
    con: sqlite3.Connection,
    date: str | None = None,
    min_stocks: int = 3,
    min_symbols: int = 100,
) -> pd.DataFrame:
    """Per-sector roll-up for a date.

    Columns: ``sector, stocks, avg_chg, total_vol, up, down``. Sectors with
    fewer than ``min_stocks`` names are filtered out.
    """
    if date is None:
        date = get_latest_full_trading_day(con, min_symbols=min_symbols)
    if not date:
        return pd.DataFrame()

    if _summary_has_date(con, date):
        try:
            return pd.read_sql_query(
                """SELECT sector_name AS sector, stocks,
                          avg_change AS avg_chg, total_volume AS total_vol,
                          up, down
                   FROM eod_sector_summary
                   WHERE date = ? AND stocks >= ?
                   ORDER BY avg_change DESC""",
                con,
                params=(date, min_stocks),
            )
        except Exception:
            pass
    # Fallback: compute on the fly via symbols.sector_name (sector_map was
    # the legacy join target but doesn't exist in current schemas).
    try:
        return pd.read_sql_query(
            """SELECT COALESCE(s.sector_name, 'Unknown')                      AS sector,
                      COUNT(*)                                                 AS stocks,
                      ROUND(AVG((e.close - e.prev_close) / e.prev_close * 100), 2) AS avg_chg,
                      SUM(e.volume)                                            AS total_vol,
                      SUM(CASE WHEN e.close > e.prev_close THEN 1 ELSE 0 END)  AS up,
                      SUM(CASE WHEN e.close < e.prev_close THEN 1 ELSE 0 END)  AS down
               FROM eod_ohlcv e
               LEFT JOIN symbols s ON e.symbol = s.symbol
               WHERE e.date = ? AND e.prev_close > 0
               GROUP BY COALESCE(s.sector_name, 'Unknown')
               HAVING COUNT(*) >= ?
               ORDER BY avg_chg DESC""",
            con,
            params=(date, min_stocks),
        )
    except Exception:
        return pd.DataFrame()


def get_recent_announcements(
    con: sqlite3.Connection,
    limit: int = 8,
) -> pd.DataFrame:
    """Recent corporate announcements with a fallback table.

    Returns rows from ``company_announcements`` when it has data, otherwise
    falls back to ``corporate_announcements``. Columns: ``symbol, date,
    subject``.
    """
    try:
        df = pd.read_sql_query(
            """SELECT symbol, announcement_date AS date, title AS subject
               FROM company_announcements
               ORDER BY announcement_date DESC LIMIT ?""",
            con,
            params=(limit,),
        )
        if not df.empty:
            return df
    except Exception:
        pass
    try:
        return pd.read_sql_query(
            """SELECT symbol, announcement_date AS date, title AS subject
               FROM corporate_announcements
               ORDER BY announcement_date DESC LIMIT ?""",
            con,
            params=(limit,),
        )
    except Exception:
        return pd.DataFrame()
