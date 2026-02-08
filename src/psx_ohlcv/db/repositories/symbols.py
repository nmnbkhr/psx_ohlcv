"""Symbol/ticker repository — CRUD operations for stock symbols."""

import sqlite3

import pandas as pd

from psx_ohlcv.models import now_iso


def upsert_symbols(con: sqlite3.Connection, symbols: list[dict]) -> int:
    """
    Upsert symbols into the symbols table.

    Args:
        con: Database connection
        symbols: List of dicts with keys: symbol, name (optional), sector (optional)

    Returns:
        Number of rows inserted or updated
    """
    if not symbols:
        return 0

    now = now_iso()
    count = 0

    for sym in symbols:
        symbol = sym.get("symbol")
        if not symbol:
            continue

        name = sym.get("name")
        sector = sym.get("sector")

        # Try insert, on conflict update
        cur = con.execute(
            """
            INSERT INTO symbols
                (symbol, name, sector, is_active, discovered_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = COALESCE(excluded.name, symbols.name),
                sector = COALESCE(excluded.sector, symbols.sector),
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (symbol, name, sector, now, now),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_symbols_list(con: sqlite3.Connection, limit: int | None = None) -> list[str]:
    """
    Get list of active symbols in sorted order.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        List of symbol strings, sorted alphabetically
    """
    query = "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    cur = con.execute(query)
    return [row["symbol"] for row in cur.fetchall()]


def get_symbols_string(con: sqlite3.Connection, limit: int | None = None) -> str:
    """
    Get comma-separated string of active symbols.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        Comma-separated string of symbols, sorted alphabetically
    """
    symbols = get_symbols_list(con, limit)
    return ",".join(symbols)


def get_unified_symbols_list(con: sqlite3.Connection) -> list[str]:
    """Get list of symbols available in Deep Data tables."""
    cur = con.execute(
        "SELECT DISTINCT symbol FROM company_snapshots ORDER BY symbol"
    )
    return [row[0] for row in cur.fetchall()]


def get_unified_symbol_count(con: sqlite3.Connection) -> int:
    """Get count of symbols in Deep Data tables."""
    cur = con.execute("SELECT COUNT(DISTINCT symbol) FROM company_snapshots")
    return cur.fetchone()[0]


def get_sector_map(con: sqlite3.Connection) -> dict[str, str]:
    """
    Get a mapping of sector_code -> sector_name.

    Args:
        con: Database connection

    Returns:
        Dict mapping sector codes to sector names
    """
    cur = con.execute("SELECT sector_code, sector_name FROM sectors")
    return {row["sector_code"]: row["sector_name"] for row in cur.fetchall()}


def get_sector_name(con: sqlite3.Connection, sector_code: str) -> str | None:
    """
    Get sector name for a given sector code.

    Args:
        con: Database connection
        sector_code: Sector code (e.g., '0101')

    Returns:
        Sector name or None if not found
    """
    cur = con.execute(
        "SELECT sector_name FROM sectors WHERE sector_code = ?",
        (sector_code,),
    )
    row = cur.fetchone()
    return row["sector_name"] if row else None


def get_sectors(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Get all sectors from the database.

    Args:
        con: Database connection

    Returns:
        DataFrame with columns: sector_code, sector_name, updated_at, source
    """
    query = """
        SELECT sector_code, sector_name, updated_at, source
        FROM sectors
        ORDER BY sector_code
    """
    return pd.read_sql_query(query, con)


def upsert_sectors(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert sectors data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: sector_code, sector_name

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"sector_code", "sector_name"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        cur = con.execute(
            """
            INSERT INTO sectors (sector_code, sector_name, updated_at, source)
            VALUES (?, ?, ?, 'DPS_SECTOR_SUMMARY')
            ON CONFLICT(sector_code) DO UPDATE SET
                sector_name = excluded.sector_name,
                updated_at = excluded.updated_at
            """,
            (row["sector_code"], row["sector_name"], now),
        )
        count += cur.rowcount

    con.commit()
    return count


def sync_sector_names_from_company_profile(con: sqlite3.Connection) -> int:
    """
    Sync sector_name from company_profile to symbols table.

    Updates the symbols.sector_name column with values from company_profile
    for any symbols where company_profile has a sector_name.

    Args:
        con: Database connection

    Returns:
        Number of rows updated
    """
    now = now_iso()
    cur = con.execute(
        """
        UPDATE symbols
        SET sector_name = (
            SELECT company_profile.sector_name
            FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        ),
        updated_at = ?
        WHERE EXISTS (
            SELECT 1 FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        )
        AND (
            symbols.sector_name IS NULL
            OR symbols.sector_name = ''
            OR symbols.sector_name != (
                SELECT company_profile.sector_name
                FROM company_profile
                WHERE company_profile.symbol = symbols.symbol
            )
        )
        """,
        (now,),
    )
    con.commit()
    return cur.rowcount


def get_symbol_activity(
    con: sqlite3.Connection,
    symbol: str,
    days: int = 30,
) -> dict:
    """
    Get activity statistics for a specific symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        days: Number of days to look back

    Returns:
        Dict with symbol-specific stats
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    symbol = symbol.upper()

    stats: dict = {
        "symbol": symbol,
        "total_views": 0,
        "unique_sessions": 0,
        "action_breakdown": {},
        "recent_activity": [],
    }

    # Total views and unique sessions
    cur = con.execute(
        """
        SELECT COUNT(*) as total, COUNT(DISTINCT session_id) as sessions
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        """,
        (cutoff, symbol),
    )
    row = cur.fetchone()
    if row:
        stats["total_views"] = row[0]
        stats["unique_sessions"] = row[1]

    # Action breakdown
    cur = con.execute(
        """
        SELECT action_type, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        GROUP BY action_type
        """,
        (cutoff, symbol),
    )
    stats["action_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

    # Recent activity
    cur = con.execute(
        """
        SELECT timestamp, action_type, action_detail
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (cutoff, symbol),
    )
    stats["recent_activity"] = [
        {"timestamp": row[0], "action": row[1], "detail": row[2]}
        for row in cur.fetchall()
    ]

    return stats
