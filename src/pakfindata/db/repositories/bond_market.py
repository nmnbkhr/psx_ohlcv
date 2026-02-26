"""Bond market repository — OTC trading volumes + benchmark rate snapshots.

Tables:
  sbp_bond_trading_daily   — per-security OTC trade data from SMTV PDF
  sbp_bond_trading_summary — daily aggregate totals
  sbp_benchmark_snapshot   — daily benchmark rates from SBP MSM sidebar
"""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_bond_market_schema",
    "upsert_bond_trade",
    "upsert_trading_summary",
    "upsert_benchmark",
    "get_bond_trading",
    "get_trading_volume_trend",
    "get_benchmark_snapshot",
    "get_benchmark_history",
    "get_bond_market_status",
]

BOND_MARKET_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS sbp_bond_trading_daily (
    date TEXT NOT NULL,
    security_type TEXT NOT NULL,
    maturity_year INTEGER NOT NULL DEFAULT 0,
    tenor_bucket TEXT NOT NULL DEFAULT '',
    segment TEXT NOT NULL,
    face_amount REAL,
    realized_amount REAL,
    yield_min REAL,
    yield_max REAL,
    yield_weighted_avg REAL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, security_type, maturity_year, tenor_bucket, segment)
);

CREATE INDEX IF NOT EXISTS idx_bond_trading_date
    ON sbp_bond_trading_daily(date);
CREATE INDEX IF NOT EXISTS idx_bond_trading_type
    ON sbp_bond_trading_daily(security_type);

CREATE TABLE IF NOT EXISTS sbp_bond_trading_summary (
    date TEXT NOT NULL,
    segment TEXT NOT NULL,
    total_face_amount REAL,
    total_realized_amount REAL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, segment)
);

CREATE TABLE IF NOT EXISTS sbp_benchmark_snapshot (
    date TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, metric)
);

CREATE INDEX IF NOT EXISTS idx_benchmark_date
    ON sbp_benchmark_snapshot(date);
"""


def init_bond_market_schema(con: sqlite3.Connection) -> None:
    """Create bond market tables if they don't exist."""
    con.executescript(BOND_MARKET_SCHEMA_SQL)
    con.commit()


# ── upsert functions ───────────────────────────────────────────


def upsert_bond_trade(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a single bond trade record."""
    try:
        con.execute(
            """INSERT INTO sbp_bond_trading_daily
               (date, security_type, maturity_year, tenor_bucket,
                segment, face_amount, realized_amount,
                yield_min, yield_max, yield_weighted_avg)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT DO UPDATE SET
                 face_amount = excluded.face_amount,
                 realized_amount = excluded.realized_amount,
                 yield_min = excluded.yield_min,
                 yield_max = excluded.yield_max,
                 yield_weighted_avg = excluded.yield_weighted_avg,
                 scraped_at = datetime('now')""",
            (
                data["date"],
                data["security_type"],
                data.get("maturity_year") or 0,
                data.get("tenor_bucket") or "",
                data["segment"],
                data.get("face_amount"),
                data.get("realized_amount"),
                data.get("yield_min"),
                data.get("yield_max"),
                data.get("yield_weighted_avg"),
            ),
        )
        return True
    except Exception:
        return False


def upsert_trading_summary(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a daily trading summary record."""
    try:
        con.execute(
            """INSERT INTO sbp_bond_trading_summary
               (date, segment, total_face_amount, total_realized_amount)
               VALUES (?, ?, ?, ?)
               ON CONFLICT DO UPDATE SET
                 total_face_amount = excluded.total_face_amount,
                 total_realized_amount = excluded.total_realized_amount,
                 scraped_at = datetime('now')""",
            (
                data["date"],
                data["segment"],
                data.get("total_face_amount"),
                data.get("total_realized_amount"),
            ),
        )
        return True
    except Exception:
        return False


def upsert_benchmark(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a single benchmark metric."""
    try:
        con.execute(
            """INSERT INTO sbp_benchmark_snapshot (date, metric, value)
               VALUES (?, ?, ?)
               ON CONFLICT DO UPDATE SET
                 value = excluded.value,
                 scraped_at = datetime('now')""",
            (data["date"], data["metric"], data["value"]),
        )
        return True
    except Exception:
        return False


# ── query functions ────────────────────────────────────────────


def get_bond_trading(
    con: sqlite3.Connection,
    date: str | None = None,
    security_type: str | None = None,
    segment: str | None = None,
) -> pd.DataFrame:
    """Get bond trading data with optional filters."""
    sql = "SELECT * FROM sbp_bond_trading_daily WHERE 1=1"
    params: list = []

    if date:
        sql += " AND date = ?"
        params.append(date)
    if security_type:
        sql += " AND security_type = ?"
        params.append(security_type)
    if segment:
        sql += " AND segment = ?"
        params.append(segment)

    sql += " ORDER BY date DESC, security_type, segment"
    return pd.read_sql_query(sql, con, params=params)


def get_trading_volume_trend(
    con: sqlite3.Connection,
    n_days: int = 30,
    security_type: str | None = None,
) -> pd.DataFrame:
    """Get daily aggregate trading volume for the last N days."""
    sql = """
        SELECT date, segment,
               SUM(face_amount) AS total_face,
               SUM(realized_amount) AS total_realized,
               AVG(yield_weighted_avg) AS avg_yield
        FROM sbp_bond_trading_daily
        WHERE date >= date('now', ? || ' days')
    """
    params: list = [str(-n_days)]

    if security_type:
        sql += " AND security_type = ?"
        params.append(security_type)

    sql += " GROUP BY date, segment ORDER BY date"
    return pd.read_sql_query(sql, con, params=params)


def get_benchmark_snapshot(
    con: sqlite3.Connection, date: str | None = None
) -> dict:
    """Get benchmark snapshot for a date (defaults to latest)."""
    if date:
        sql = """SELECT metric, value FROM sbp_benchmark_snapshot
                 WHERE date = ? ORDER BY metric"""
        rows = con.execute(sql, (date,)).fetchall()
    else:
        sql = """SELECT metric, value FROM sbp_benchmark_snapshot
                 WHERE date = (SELECT MAX(date) FROM sbp_benchmark_snapshot)
                 ORDER BY metric"""
        rows = con.execute(sql).fetchall()

    result: dict = {}
    for metric, value in rows:
        result[metric] = value

    # Add the date
    if rows:
        date_row = con.execute(
            "SELECT MAX(date) FROM sbp_benchmark_snapshot"
        ).fetchone()
        result["_date"] = date_row[0] if date_row else None

    return result


def get_benchmark_history(
    con: sqlite3.Connection,
    metric: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get historical values for a specific benchmark metric."""
    sql = "SELECT date, value FROM sbp_benchmark_snapshot WHERE metric = ?"
    params: list = [metric]

    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY date"
    return pd.read_sql_query(sql, con, params=params)


def get_bond_market_status(con: sqlite3.Connection) -> dict:
    """Get bond market data coverage summary."""
    status: dict = {}

    try:
        row = con.execute(
            """SELECT COUNT(*), MIN(date), MAX(date),
                      COUNT(DISTINCT date)
               FROM sbp_bond_trading_daily"""
        ).fetchone()
        status["trading_rows"] = row[0]
        status["trading_earliest"] = row[1]
        status["trading_latest"] = row[2]
        status["trading_days"] = row[3]
    except Exception:
        status["trading_rows"] = 0

    try:
        row = con.execute(
            """SELECT COUNT(DISTINCT date), MIN(date), MAX(date)
               FROM sbp_benchmark_snapshot"""
        ).fetchone()
        status["benchmark_days"] = row[0]
        status["benchmark_earliest"] = row[1]
        status["benchmark_latest"] = row[2]
    except Exception:
        status["benchmark_days"] = 0

    return status
