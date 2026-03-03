"""Financial instruments repository (futures, options, etc.)."""

import sqlite3

import pandas as pd


# =============================================================================
# Instrument CRUD Functions
# =============================================================================


def upsert_instrument(con: sqlite3.Connection, instrument: dict) -> bool:
    """
    Insert or update an instrument record.

    Args:
        con: Database connection
        instrument: Dict with instrument_id, symbol, name, instrument_type, etc.

    Returns:
        True if successful, False otherwise
    """
    try:
        con.execute("""
            INSERT INTO instruments (
                instrument_id, symbol, name, instrument_type,
                exchange, currency, is_active, source,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                symbol = excluded.symbol,
                name = excluded.name,
                instrument_type = excluded.instrument_type,
                exchange = excluded.exchange,
                currency = excluded.currency,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            instrument.get("name"),
            instrument.get("instrument_type"),
            instrument.get("exchange", "PSX"),
            instrument.get("currency", "PKR"),
            instrument.get("is_active", 1),
            instrument.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def upsert_instruments_batch(con: sqlite3.Connection, instruments: list[dict]) -> dict:
    """
    Batch upsert multiple instruments.

    Args:
        con: Database connection
        instruments: List of instrument dicts

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    counts = {"inserted": 0, "updated": 0, "failed": 0}

    for inst in instruments:
        try:
            # Check if exists
            cur = con.execute(
                "SELECT 1 FROM instruments WHERE instrument_id = ?",
                (inst.get("instrument_id"),)
            )
            exists = cur.fetchone() is not None

            if upsert_instrument(con, inst):
                if exists:
                    counts["updated"] += 1
                else:
                    counts["inserted"] += 1
            else:
                counts["failed"] += 1
        except Exception:
            counts["failed"] += 1

    return counts


def get_instruments(
    con: sqlite3.Connection,
    instrument_type: str | None = None,
    active_only: bool = True
) -> list[dict]:
    """
    Get instruments, optionally filtered by type.

    Args:
        con: Database connection
        instrument_type: Filter by type ('EQUITY', 'ETF', 'REIT', 'INDEX'), or None for all
        active_only: If True, only return active instruments

    Returns:
        List of instrument dicts
    """
    try:
        query = "SELECT * FROM instruments WHERE 1=1"
        params = []

        if instrument_type:
            query += " AND instrument_type = ?"
            params.append(instrument_type)

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY instrument_type, symbol"

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_instrument_by_id(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single instrument by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_instrument_by_symbol(
    con: sqlite3.Connection,
    symbol: str,
    exchange: str = "PSX"
) -> dict | None:
    """Get instrument by symbol and exchange."""
    try:
        cur = con.execute(
            "SELECT * FROM instruments WHERE symbol = ? AND exchange = ?",
            (symbol, exchange)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def resolve_instrument_id(exchange: str, symbol: str) -> str:
    """
    Generate a standardized instrument ID.

    Args:
        exchange: Exchange code (e.g., 'PSX', 'IDX')
        symbol: Instrument symbol

    Returns:
        Standardized ID like "PSX:HBL" or "IDX:KSE100"
    """
    return f"{exchange}:{symbol}"


# =============================================================================
# OHLCV Instrument Functions
# =============================================================================


def upsert_ohlcv_instrument(con: sqlite3.Connection, instrument_id: str, df: pd.DataFrame) -> int:
    """
    Upsert OHLCV data for an instrument.

    Args:
        con: Database connection
        instrument_id: Instrument ID
        df: DataFrame with date, open, high, low, close, volume columns

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO ohlcv_instruments (
                    instrument_id, date, open, high, low, close, volume, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(instrument_id, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    ingested_at = datetime('now')
            """, (
                instrument_id,
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            ))
            count += 1
        except Exception:
            pass

    con.commit()
    return count


def get_ohlcv_instrument(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> pd.DataFrame:
    """
    Get OHLCV data for an instrument.

    Args:
        con: Database connection
        instrument_id: Instrument ID
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        limit: Max rows to return

    Returns:
        DataFrame with date, open, high, low, close, volume
    """
    query = "SELECT date, open, high, low, close, volume FROM ohlcv_instruments WHERE instrument_id = ?"
    params = [instrument_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


def get_instrument_latest_date(con: sqlite3.Connection, instrument_id: str) -> str | None:
    """Get the latest OHLCV date for an instrument."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM ohlcv_instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


# =============================================================================
# Instrument Ranking Functions
# =============================================================================


def upsert_instrument_ranking(con: sqlite3.Connection, ranking: dict) -> bool:
    """
    Insert or update an instrument ranking.

    Args:
        con: Database connection
        ranking: Dict with as_of_date, instrument_id, instrument_type, and metrics

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO instrument_rankings (
                as_of_date, instrument_id, instrument_type,
                return_1m, return_3m, return_6m, return_1y,
                volatility_30d, relative_strength, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(as_of_date, instrument_id) DO UPDATE SET
                instrument_type = excluded.instrument_type,
                return_1m = excluded.return_1m,
                return_3m = excluded.return_3m,
                return_6m = excluded.return_6m,
                return_1y = excluded.return_1y,
                volatility_30d = excluded.volatility_30d,
                relative_strength = excluded.relative_strength,
                computed_at = datetime('now')
        """, (
            ranking.get("as_of_date"),
            ranking.get("instrument_id"),
            ranking.get("instrument_type"),
            ranking.get("return_1m"),
            ranking.get("return_3m"),
            ranking.get("return_6m"),
            ranking.get("return_1y"),
            ranking.get("volatility_30d"),
            ranking.get("relative_strength"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_instrument_rankings(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    instrument_type: str | None = None,
    top_n: int = 10
) -> list[dict]:
    """
    Get instrument rankings.

    Args:
        con: Database connection
        as_of_date: Date for rankings (default: most recent)
        instrument_type: Filter by type
        top_n: Number of top rankings to return

    Returns:
        List of ranking dicts with instrument details
    """
    if as_of_date:
        date_clause = "r.as_of_date = ?"
        params = [as_of_date]
    else:
        date_clause = "r.as_of_date = (SELECT MAX(as_of_date) FROM instrument_rankings)"
        params = []

    query = f"""
        SELECT r.*, i.symbol, i.name
        FROM instrument_rankings r
        JOIN instruments i ON r.instrument_id = i.instrument_id
        WHERE {date_clause}
    """

    if instrument_type:
        query += " AND r.instrument_type = ?"
        params.append(instrument_type)

    query += " ORDER BY r.return_1m DESC NULLS LAST LIMIT ?"
    params.append(top_n)

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_ranking_date(con: sqlite3.Connection) -> str | None:
    """Get the latest date for which rankings exist."""
    try:
        cur = con.execute("SELECT MAX(as_of_date) FROM instrument_rankings")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


# =============================================================================
# Instruments Sync Run Functions
# =============================================================================


def create_instruments_sync_run(con: sqlite3.Connection, run_id: str, instrument_types: str) -> bool:
    """Create a new sync run record."""
    try:
        con.execute("""
            INSERT INTO instruments_sync_runs (run_id, started_at, instrument_types)
            VALUES (?, datetime('now'), ?)
        """, (run_id, instrument_types))
        con.commit()
        return True
    except Exception:
        return False


def sync_index_membership(con: sqlite3.Connection) -> dict:
    """Populate instrument_membership from regular_market_current.listed_in.

    Parses the comma-separated listed_in column (e.g. "ALLSHR,KSE100,KSE100PR")
    and creates parent→child rows in instrument_membership for each index→equity pair.

    Uses REPLACE to refresh all memberships each run (clear + insert pattern).

    Returns:
        Dict with 'indices' (count of indices with members),
        'memberships' (total rows inserted), 'skipped' (symbols not in instruments).
    """
    from datetime import date

    today = date.today().isoformat()

    # Get all equity symbols with listed_in data
    rows = con.execute("""
        SELECT symbol, listed_in FROM regular_market_current
        WHERE listed_in IS NOT NULL AND listed_in != ''
    """).fetchall()

    if not rows:
        return {"indices": 0, "memberships": 0, "skipped": 0}

    # Build lookup: symbol → instrument_id for equities
    equity_map = {}
    for r in con.execute(
        "SELECT instrument_id, symbol FROM instruments WHERE instrument_type = 'EQUITY'"
    ).fetchall():
        equity_map[r[0] if isinstance(r, tuple) else r["symbol"]] = (
            r[1] if isinstance(r, tuple) else r["instrument_id"]
        )
    # Fix: use dict(symbol → instrument_id)
    equity_map = {}
    for r in con.execute(
        "SELECT symbol, instrument_id FROM instruments WHERE instrument_type = 'EQUITY'"
    ).fetchall():
        equity_map[r[0]] = r[1]

    # Build lookup: index code → instrument_id for indices
    index_map = {}
    for r in con.execute(
        "SELECT symbol, instrument_id FROM instruments WHERE instrument_type = 'INDEX'"
    ).fetchall():
        index_map[r[0]] = r[1]

    # Clear existing memberships for today
    con.execute("DELETE FROM instrument_membership WHERE effective_date = ?", (today,))

    memberships = 0
    skipped = 0
    indices_seen = set()

    for row in rows:
        symbol = row[0]
        listed_in = row[1]

        child_id = equity_map.get(symbol)
        if not child_id:
            skipped += 1
            continue

        for idx_code in listed_in.split(","):
            idx_code = idx_code.strip()
            parent_id = index_map.get(idx_code)
            if not parent_id:
                continue

            con.execute("""
                INSERT OR IGNORE INTO instrument_membership
                    (parent_instrument_id, child_instrument_id, weight, effective_date, source)
                VALUES (?, ?, NULL, ?, 'market_watch_listed_in')
            """, (parent_id, child_id, today))
            memberships += 1
            indices_seen.add(idx_code)

    con.commit()
    return {
        "indices": len(indices_seen),
        "memberships": memberships,
        "skipped": skipped,
    }


def get_index_constituents(
    con: sqlite3.Connection,
    index_symbol: str,
    effective_date: str | None = None,
) -> list[dict]:
    """Get constituent symbols for an index.

    Args:
        con: Database connection
        index_symbol: Index symbol (e.g. 'KSE100')
        effective_date: Date for membership (default: latest)

    Returns:
        List of dicts with symbol, name, instrument_id.
    """
    if effective_date is None:
        # Use the latest effective date
        row = con.execute("""
            SELECT MAX(effective_date) FROM instrument_membership im
            JOIN instruments i ON im.parent_instrument_id = i.instrument_id
            WHERE i.symbol = ?
        """, (index_symbol,)).fetchone()
        effective_date = row[0] if row and row[0] else None

    if not effective_date:
        return []

    cur = con.execute("""
        SELECT c.symbol, c.name, c.instrument_id
        FROM instrument_membership im
        JOIN instruments p ON im.parent_instrument_id = p.instrument_id
        JOIN instruments c ON im.child_instrument_id = c.instrument_id
        WHERE p.symbol = ? AND im.effective_date = ?
        ORDER BY c.symbol
    """, (index_symbol, effective_date))

    return [dict(r) for r in cur.fetchall()]


def update_instruments_sync_run(con: sqlite3.Connection, run_id: str, stats: dict) -> bool:
    """Update a sync run with final stats."""
    try:
        con.execute("""
            UPDATE instruments_sync_runs SET
                ended_at = datetime('now'),
                instruments_total = ?,
                instruments_ok = ?,
                instruments_failed = ?,
                instruments_no_data = ?,
                rows_upserted = ?
            WHERE run_id = ?
        """, (
            stats.get("total", 0),
            stats.get("ok", 0),
            stats.get("failed", 0),
            stats.get("no_data", 0),
            stats.get("rows", 0),
            run_id,
        ))
        con.commit()
        return True
    except Exception:
        return False
