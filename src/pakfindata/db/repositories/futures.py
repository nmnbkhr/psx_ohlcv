"""Futures, contracts, and odd-lot EOD data repository."""

import sqlite3

import pandas as pd

from pakfindata.models import now_iso

__all__ = [
    "init_futures_schema",
    "upsert_futures_eod",
    "get_futures_eod",
    "get_futures_dates",
    "get_futures_stats",
    "get_contract_comparison",
    "get_most_active_futures",
    "migrate_from_eod_ohlcv",
    "get_odl_symbols",
    "get_odl_history",
    "get_odl_stats",
]

# =============================================================================
# Schema
# =============================================================================

FUTURES_EOD_SCHEMA = """\
CREATE TABLE IF NOT EXISTS futures_eod (
    symbol          TEXT NOT NULL,
    date            TEXT NOT NULL,
    market_type     TEXT NOT NULL,
    base_symbol     TEXT NOT NULL,
    contract_month  TEXT,
    sector_code     TEXT,
    company_name    TEXT,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          INTEGER,
    turnover        REAL,
    prev_close      REAL,
    change_value    REAL,
    change_pct      REAL,
    ingested_at     TEXT NOT NULL,
    source          TEXT DEFAULT 'market_summary',
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_futures_eod_date
    ON futures_eod(date);
CREATE INDEX IF NOT EXISTS idx_futures_eod_base
    ON futures_eod(base_symbol);
CREATE INDEX IF NOT EXISTS idx_futures_eod_type
    ON futures_eod(market_type);
CREATE INDEX IF NOT EXISTS idx_futures_eod_month
    ON futures_eod(contract_month);
CREATE INDEX IF NOT EXISTS idx_futures_eod_type_base
    ON futures_eod(market_type, base_symbol);
CREATE INDEX IF NOT EXISTS idx_futures_eod_type_sym_date
    ON futures_eod(market_type, symbol, date);
"""


def init_futures_schema(con: sqlite3.Connection) -> None:
    """Create futures_eod table and indexes if they don't exist."""
    con.executescript(FUTURES_EOD_SCHEMA)


# =============================================================================
# Upsert
# =============================================================================


def upsert_futures_eod(
    con: sqlite3.Connection,
    df: pd.DataFrame,
    source: str = "market_summary",
) -> int:
    """Upsert futures/contract/odd-lot EOD data.

    Expected DataFrame columns:
        symbol, date, market_type, base_symbol, contract_month,
        sector_code, company_name, open, high, low, close,
        volume, prev_close

    Returns:
        Number of rows upserted.
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    for _, row in df.iterrows():
        close = row.get("close") or 0
        prev = row.get("prev_close") or 0
        change_val = (close - prev) if prev else None
        change_pct = ((close - prev) / prev * 100) if prev and prev != 0 else None

        con.execute(
            """
            INSERT INTO futures_eod
                (symbol, date, market_type, base_symbol, contract_month,
                 sector_code, company_name, open, high, low, close,
                 volume, prev_close, change_value, change_pct,
                 ingested_at, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                market_type    = excluded.market_type,
                base_symbol    = excluded.base_symbol,
                contract_month = excluded.contract_month,
                sector_code    = excluded.sector_code,
                company_name   = excluded.company_name,
                open           = excluded.open,
                high           = excluded.high,
                low            = excluded.low,
                close          = excluded.close,
                volume         = excluded.volume,
                prev_close     = excluded.prev_close,
                change_value   = excluded.change_value,
                change_pct     = excluded.change_pct,
                ingested_at    = excluded.ingested_at,
                source         = excluded.source
            """,
            (
                row["symbol"], row["date"], row["market_type"],
                row["base_symbol"], row.get("contract_month"),
                row.get("sector_code"), row.get("company_name"),
                row.get("open"), row.get("high"), row.get("low"),
                row.get("close"), row.get("volume"), row.get("prev_close"),
                change_val, change_pct, now, source,
            ),
        )
        count += 1

    con.commit()
    return count


# =============================================================================
# Queries
# =============================================================================


def get_futures_eod(
    con: sqlite3.Connection,
    date: str | None = None,
    base_symbol: str | None = None,
    market_type: str | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """Query futures_eod with optional filters."""
    query = "SELECT * FROM futures_eod WHERE 1=1"
    params: list = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if base_symbol:
        query += " AND base_symbol = ?"
        params.append(base_symbol.upper())
    if market_type:
        query += " AND market_type = ?"
        params.append(market_type)

    query += " ORDER BY date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_futures_dates(con: sqlite3.Connection) -> list[str]:
    """Get distinct dates with futures data, newest first."""
    rows = con.execute(
        "SELECT DISTINCT date FROM futures_eod ORDER BY date DESC"
    ).fetchall()
    return [r[0] for r in rows]


def get_futures_stats(con: sqlite3.Connection) -> dict:
    """Get summary statistics for futures_eod."""
    row = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT date) as total_dates,
            COUNT(DISTINCT base_symbol) as unique_base,
            MIN(date) as min_date,
            MAX(date) as max_date,
            SUM(CASE WHEN market_type = 'FUT' THEN 1 ELSE 0 END),
            SUM(CASE WHEN market_type = 'CONT' THEN 1 ELSE 0 END),
            SUM(CASE WHEN market_type = 'IDX_FUT' THEN 1 ELSE 0 END),
            SUM(CASE WHEN market_type = 'ODL' THEN 1 ELSE 0 END)
        FROM futures_eod
    """).fetchone()
    return {
        "total_rows": row[0] or 0,
        "total_dates": row[1] or 0,
        "unique_base_symbols": row[2] or 0,
        "min_date": row[3],
        "max_date": row[4],
        "fut_rows": row[5] or 0,
        "cont_rows": row[6] or 0,
        "idx_fut_rows": row[7] or 0,
        "odl_rows": row[8] or 0,
    }


def get_contract_comparison(
    con: sqlite3.Connection,
    base_symbol: str,
    date: str,
) -> pd.DataFrame:
    """Get FUT vs CONT prices for a base symbol on a date."""
    return pd.read_sql_query(
        """
        SELECT symbol, market_type, contract_month, close, volume,
               prev_close, change_value, change_pct
        FROM futures_eod
        WHERE base_symbol = ? AND date = ?
        ORDER BY market_type, contract_month
        """,
        con,
        params=(base_symbol.upper(), date),
    )


def get_most_active_futures(
    con: sqlite3.Connection,
    date: str,
    market_type: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """Get most actively traded futures for a date."""
    query = """
        SELECT symbol, base_symbol, market_type, contract_month,
               close, volume, change_pct, prev_close
        FROM futures_eod
        WHERE date = ? AND volume > 0
    """
    params: list = [date]
    if market_type:
        query += " AND market_type = ?"
        params.append(market_type)
    query += " ORDER BY volume DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# ODL (Odd-Lot) Queries
# =============================================================================


def get_odl_symbols(con: sqlite3.Connection) -> pd.DataFrame:
    """Get distinct ODL symbols with their latest price data."""
    return pd.read_sql_query("""
        SELECT f.symbol, f.company_name, f.close, f.volume,
               f.prev_close, f.change_value, f.change_pct, f.date
        FROM futures_eod f
        INNER JOIN (
            SELECT symbol, MAX(date) as max_date
            FROM futures_eod WHERE market_type = 'ODL'
            GROUP BY symbol
        ) latest ON f.symbol = latest.symbol AND f.date = latest.max_date
        WHERE f.market_type = 'ODL'
        ORDER BY f.symbol
    """, con)


def get_odl_history(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 365,
) -> pd.DataFrame:
    """Get price history for a specific ODL symbol."""
    return pd.read_sql_query(
        """SELECT date, open, high, low, close, volume,
                  prev_close, change_value, change_pct
           FROM futures_eod
           WHERE symbol = ? AND market_type = 'ODL'
           ORDER BY date DESC LIMIT ?""",
        con,
        params=(symbol, limit),
    )


def get_odl_stats(con: sqlite3.Connection) -> dict:
    """Get ODL summary: distinct symbols, total rows, date range."""
    row = con.execute("""
        SELECT COUNT(DISTINCT symbol), COUNT(*),
               MIN(date), MAX(date)
        FROM futures_eod WHERE market_type = 'ODL'
    """).fetchone()
    return {
        "distinct_symbols": row[0] or 0,
        "total_rows": row[1] or 0,
        "min_date": row[2],
        "max_date": row[3],
    }


# =============================================================================
# Migration from eod_ohlcv
# =============================================================================


def migrate_from_eod_ohlcv(
    con: sqlite3.Connection,
    dry_run: bool = True,
) -> dict:
    """Migrate FUT/CONT/IDX_FUT/ODL rows from eod_ohlcv to futures_eod.

    Args:
        con: Database connection.
        dry_run: If True, only report counts without modifying data.

    Returns:
        Dict with migration statistics.
    """
    from pakfindata.sources.market_summary import (
        classify_market_type,
        parse_futures_symbol,
    )

    # Count eligible rows
    total = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE sector_code IN ('40', '41', '36')"
    ).fetchone()[0]

    result = {
        "total_eligible": total,
        "migrated": 0,
        "deleted_from_eod": 0,
        "dry_run": dry_run,
    }

    if dry_run or total == 0:
        return result

    # Read all derivative rows
    df = pd.read_sql_query(
        "SELECT * FROM eod_ohlcv WHERE sector_code IN ('40', '41', '36')",
        con,
    )

    # Classify and parse
    df["market_type"] = df.apply(
        lambda r: classify_market_type(str(r.get("sector_code", "")), r["symbol"]),
        axis=1,
    )
    parsed = df.apply(
        lambda r: parse_futures_symbol(r["symbol"], r["market_type"]),
        axis=1,
        result_type="expand",
    )
    df["base_symbol"] = parsed[0]
    df["contract_month"] = parsed[1]

    migrated = upsert_futures_eod(con, df, source="migrated_from_eod")

    # Delete from eod_ohlcv
    con.execute("DELETE FROM eod_ohlcv WHERE sector_code IN ('40', '41', '36')")
    con.commit()

    result["migrated"] = migrated
    result["deleted_from_eod"] = total
    return result
