"""Post-close turnover data repository."""

import sqlite3

import pandas as pd

from pakfindata.models import now_iso

__all__ = [
    "init_post_close_schema",
    "upsert_post_close",
    "get_post_close",
    "get_post_close_dates",
    "get_post_close_stats",
    "get_dates_missing_turnover",
]

# =============================================================================
# Schema
# =============================================================================

POST_CLOSE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS post_close_turnover (
    symbol        TEXT NOT NULL,
    date          TEXT NOT NULL,
    company_name  TEXT,
    volume        INTEGER,
    turnover      REAL,
    ingested_at   TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_pc_turnover_date
    ON post_close_turnover(date);
CREATE INDEX IF NOT EXISTS idx_pc_turnover_symbol
    ON post_close_turnover(symbol);
"""


def init_post_close_schema(con: sqlite3.Connection) -> None:
    """Create post_close_turnover table and indexes."""
    con.executescript(POST_CLOSE_SCHEMA)


# =============================================================================
# Upsert
# =============================================================================


def upsert_post_close(
    con: sqlite3.Connection,
    records: list[dict],
) -> int:
    """Insert or update post_close_turnover rows.

    Each record: {symbol, date, company_name, volume, turnover}.
    Returns number of rows upserted.
    """
    if not records:
        return 0

    now = now_iso()
    count = 0
    for r in records:
        con.execute(
            """
            INSERT INTO post_close_turnover
                (symbol, date, company_name, volume, turnover, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                company_name = excluded.company_name,
                volume       = excluded.volume,
                turnover     = excluded.turnover,
                ingested_at  = excluded.ingested_at
            """,
            (
                r["symbol"], r["date"], r.get("company_name"),
                r.get("volume", 0), r.get("turnover", 0.0), now,
            ),
        )
        count += 1
    con.commit()
    return count


# =============================================================================
# Queries
# =============================================================================


def get_post_close(
    con: sqlite3.Connection,
    date: str | None = None,
    symbol: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """Query post_close_turnover with optional filters."""
    query = "SELECT * FROM post_close_turnover WHERE 1=1"
    params: list = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    query += " ORDER BY date DESC, turnover DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_post_close_dates(con: sqlite3.Connection) -> list[str]:
    """Get distinct dates with post_close data, newest first."""
    rows = con.execute(
        "SELECT DISTINCT date FROM post_close_turnover ORDER BY date DESC"
    ).fetchall()
    return [r[0] for r in rows]


def get_post_close_stats(con: sqlite3.Connection) -> dict:
    """Get summary statistics for post_close_turnover."""
    row = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT date) as total_dates,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM post_close_turnover
    """).fetchone()
    return {
        "total_rows": row[0] or 0,
        "total_dates": row[1] or 0,
        "unique_symbols": row[2] or 0,
        "min_date": row[3],
        "max_date": row[4],
    }


def get_dates_missing_turnover(
    con: sqlite3.Connection,
    since: str = "2024-01-01",
) -> list[str]:
    """Find dates that have eod_ohlcv data but no post_close_turnover rows."""
    rows = con.execute(
        """
        SELECT DISTINCT e.date
        FROM eod_ohlcv e
        LEFT JOIN (
            SELECT DISTINCT date FROM post_close_turnover
        ) pc ON e.date = pc.date
        WHERE pc.date IS NULL AND e.date >= ?
        ORDER BY e.date DESC
        """,
        (since,),
    ).fetchall()
    return [r[0] for r in rows]
