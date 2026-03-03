"""Analytics computation module for PSX market data.

Computes and stores analytics from regular market data:
- Market breadth (gainers/losers/unchanged)
- Top gainers/losers/volume lists
- Sector rollups
"""

import sqlite3
from typing import Any

import pandas as pd

from .models import now_iso

# =============================================================================
# Schema for analytics tables
# =============================================================================

ANALYTICS_SCHEMA_SQL = """
-- Market-level analytics per snapshot timestamp
CREATE TABLE IF NOT EXISTS analytics_market_snapshot (
    ts TEXT PRIMARY KEY,
    gainers_count INTEGER NOT NULL DEFAULT 0,
    losers_count INTEGER NOT NULL DEFAULT 0,
    unchanged_count INTEGER NOT NULL DEFAULT 0,
    total_symbols INTEGER NOT NULL DEFAULT 0,
    total_volume REAL DEFAULT 0,
    top_gainer_symbol TEXT,
    top_loser_symbol TEXT,
    computed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Top-N rankings per snapshot timestamp
CREATE TABLE IF NOT EXISTS analytics_symbol_snapshot (
    ts TEXT NOT NULL,
    rank_type TEXT NOT NULL,  -- 'gainers', 'losers', 'volume'
    rank INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    company_name TEXT,
    sector_name TEXT,
    current REAL,
    change_pct REAL,
    volume REAL,
    PRIMARY KEY (ts, rank_type, rank)
);

CREATE INDEX IF NOT EXISTS idx_analytics_symbol_ts
    ON analytics_symbol_snapshot(ts);
CREATE INDEX IF NOT EXISTS idx_analytics_symbol_type
    ON analytics_symbol_snapshot(rank_type);

-- Sector rollups per snapshot timestamp
CREATE TABLE IF NOT EXISTS analytics_sector_snapshot (
    ts TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    symbols_count INTEGER NOT NULL DEFAULT 0,
    avg_change_pct REAL,
    sum_volume REAL,
    top_symbol TEXT,
    PRIMARY KEY (ts, sector_code)
);

CREATE INDEX IF NOT EXISTS idx_analytics_sector_ts
    ON analytics_sector_snapshot(ts);
"""


def init_analytics_schema(con: sqlite3.Connection) -> None:
    """Initialize analytics tables if they don't exist."""
    con.executescript(ANALYTICS_SCHEMA_SQL)
    con.commit()


# =============================================================================
# Market Analytics Computation
# =============================================================================


def compute_market_analytics(con: sqlite3.Connection, ts: str) -> dict[str, Any]:
    """Compute market-level analytics for a given timestamp.

    Uses data from regular_market_current table.

    Args:
        con: Database connection
        ts: Timestamp for this snapshot

    Returns:
        Dict with computed analytics:
        - gainers_count: Symbols with change_pct > 0
        - losers_count: Symbols with change_pct < 0
        - unchanged_count: Symbols with change_pct == 0 or NULL
        - total_symbols: Total count
        - total_volume: Sum of volume
        - top_gainer_symbol: Symbol with max change_pct
        - top_loser_symbol: Symbol with min change_pct
    """
    # Query current market data
    df = pd.read_sql_query(
        "SELECT symbol, change_pct, volume FROM regular_market_current",
        con,
    )

    if df.empty:
        return {
            "ts": ts,
            "gainers_count": 0,
            "losers_count": 0,
            "unchanged_count": 0,
            "total_symbols": 0,
            "total_volume": 0.0,
            "top_gainer_symbol": None,
            "top_loser_symbol": None,
        }

    # Calculate counts
    gainers = df[df["change_pct"] > 0]
    losers = df[df["change_pct"] < 0]
    unchanged = df[(df["change_pct"] == 0) | (df["change_pct"].isna())]

    # Get top gainer/loser
    top_gainer = None
    top_loser = None
    if not gainers.empty:
        top_gainer = gainers.loc[gainers["change_pct"].idxmax(), "symbol"]
    if not losers.empty:
        top_loser = losers.loc[losers["change_pct"].idxmin(), "symbol"]

    # Total volume
    total_volume = df["volume"].sum() if "volume" in df.columns else 0.0
    if pd.isna(total_volume):
        total_volume = 0.0

    return {
        "ts": ts,
        "gainers_count": len(gainers),
        "losers_count": len(losers),
        "unchanged_count": len(unchanged),
        "total_symbols": len(df),
        "total_volume": float(total_volume),
        "top_gainer_symbol": top_gainer,
        "top_loser_symbol": top_loser,
    }


def store_market_analytics(con: sqlite3.Connection, analytics: dict[str, Any]) -> None:
    """Store market analytics to database.

    Args:
        con: Database connection
        analytics: Dict from compute_market_analytics
    """
    now = now_iso()
    con.execute(
        """
        INSERT INTO analytics_market_snapshot (
            ts, gainers_count, losers_count, unchanged_count,
            total_symbols, total_volume, top_gainer_symbol, top_loser_symbol,
            computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ts) DO UPDATE SET
            gainers_count = excluded.gainers_count,
            losers_count = excluded.losers_count,
            unchanged_count = excluded.unchanged_count,
            total_symbols = excluded.total_symbols,
            total_volume = excluded.total_volume,
            top_gainer_symbol = excluded.top_gainer_symbol,
            top_loser_symbol = excluded.top_loser_symbol,
            computed_at = excluded.computed_at
        """,
        (
            analytics["ts"],
            analytics["gainers_count"],
            analytics["losers_count"],
            analytics["unchanged_count"],
            analytics["total_symbols"],
            analytics["total_volume"],
            analytics["top_gainer_symbol"],
            analytics["top_loser_symbol"],
            now,
        ),
    )
    con.commit()


# =============================================================================
# Top Lists Computation
# =============================================================================


def compute_top_lists(
    con: sqlite3.Connection,
    ts: str,
    top_n: int = 10,
) -> dict[str, pd.DataFrame]:
    """Compute top gainers, losers, and volume lists.

    Joins with sectors table to get sector_name and symbols table for company_name.

    Args:
        con: Database connection
        ts: Timestamp for this snapshot
        top_n: Number of symbols in each list

    Returns:
        Dict with keys 'gainers', 'losers', 'volume', each containing a DataFrame
    """
    # Query current market data with join to sectors and symbols
    query = """
        SELECT
            rm.symbol,
            rm.current,
            rm.change_pct,
            rm.volume,
            COALESCE(sec.sector_name, s.sector_name, rm.sector_code) as sector_name,
            s.name as company_name
        FROM regular_market_current rm
        LEFT JOIN sectors sec ON rm.sector_code = sec.sector_code
        LEFT JOIN symbols s ON rm.symbol = s.symbol
    """
    df = pd.read_sql_query(query, con)

    results: dict[str, pd.DataFrame] = {}

    # Top gainers (highest change_pct)
    gainers = df[df["change_pct"].notna() & (df["change_pct"] > 0)]
    gainers = gainers.nlargest(top_n, "change_pct").reset_index(drop=True)
    gainers["rank"] = range(1, len(gainers) + 1)
    gainers["ts"] = ts
    gainers["rank_type"] = "gainers"
    results["gainers"] = gainers

    # Top losers (lowest change_pct)
    losers = df[df["change_pct"].notna() & (df["change_pct"] < 0)]
    losers = losers.nsmallest(top_n, "change_pct").reset_index(drop=True)
    losers["rank"] = range(1, len(losers) + 1)
    losers["ts"] = ts
    losers["rank_type"] = "losers"
    results["losers"] = losers

    # Top volume
    volume_df = df[df["volume"].notna() & (df["volume"] > 0)]
    volume_df = volume_df.nlargest(top_n, "volume").reset_index(drop=True)
    volume_df["rank"] = range(1, len(volume_df) + 1)
    volume_df["ts"] = ts
    volume_df["rank_type"] = "volume"
    results["volume"] = volume_df

    return results


def store_top_lists(
    con: sqlite3.Connection,
    ts: str,
    top_lists: dict[str, pd.DataFrame],
) -> int:
    """Store top lists to database.

    Args:
        con: Database connection
        ts: Timestamp
        top_lists: Dict from compute_top_lists

    Returns:
        Number of rows inserted
    """
    # Delete existing entries for this ts
    con.execute(
        "DELETE FROM analytics_symbol_snapshot WHERE ts = ?",
        (ts,),
    )

    count = 0
    for rank_type, df in top_lists.items():
        for _, row in df.iterrows():
            con.execute(
                """
                INSERT INTO analytics_symbol_snapshot (
                    ts, rank_type, rank, symbol, company_name, sector_name,
                    current, change_pct, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    rank_type,
                    row["rank"],
                    row["symbol"],
                    row.get("company_name"),
                    row.get("sector_name"),
                    row.get("current"),
                    row.get("change_pct"),
                    row.get("volume"),
                ),
            )
            count += 1

    con.commit()
    return count


# =============================================================================
# Sector Rollups Computation
# =============================================================================


def compute_sector_rollups(con: sqlite3.Connection, ts: str) -> pd.DataFrame:
    """Compute sector-level rollups.

    Groups by sector and computes:
    - symbols_count: Number of symbols in sector
    - avg_change_pct: Average change percentage
    - sum_volume: Total volume
    - top_symbol: Symbol with max change_pct in sector

    Args:
        con: Database connection
        ts: Timestamp for this snapshot

    Returns:
        DataFrame with sector rollups
    """
    # Query with join to sectors table for sector names
    query = """
        SELECT
            rm.sector_code,
            COALESCE(sec.sector_name, s.sector_name, rm.sector_code) as sector_name,
            rm.symbol,
            rm.change_pct,
            rm.volume
        FROM regular_market_current rm
        LEFT JOIN sectors sec ON rm.sector_code = sec.sector_code
        LEFT JOIN symbols s ON rm.symbol = s.symbol
        WHERE rm.sector_code IS NOT NULL
          AND rm.sector_code != ''
    """
    df = pd.read_sql_query(query, con)

    if df.empty:
        return pd.DataFrame(columns=[
            "ts", "sector_code", "sector_name", "symbols_count",
            "avg_change_pct", "sum_volume", "top_symbol"
        ])

    # Group by sector
    grouped = df.groupby(["sector_code", "sector_name"], dropna=False)

    rollups = []
    for (sector_code, sector_name), group in grouped:
        # Skip if no sector_code
        if not sector_code:
            continue

        symbols_count = len(group)
        avg_change_pct = group["change_pct"].mean()
        if pd.isna(avg_change_pct):
            avg_change_pct = None
        sum_volume = group["volume"].sum()
        if pd.isna(sum_volume):
            sum_volume = None

        # Top symbol by change_pct in this sector
        top_symbol = None
        valid_changes = group[group["change_pct"].notna()]
        if not valid_changes.empty:
            top_idx = valid_changes["change_pct"].idxmax()
            top_symbol = valid_changes.loc[top_idx, "symbol"]

        rollups.append({
            "ts": ts,
            "sector_code": sector_code,
            "sector_name": sector_name or sector_code,
            "symbols_count": symbols_count,
            "avg_change_pct": avg_change_pct,
            "sum_volume": sum_volume,
            "top_symbol": top_symbol,
        })

    return pd.DataFrame(rollups)


def store_sector_rollups(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Store sector rollups to database.

    Args:
        con: Database connection
        df: DataFrame from compute_sector_rollups

    Returns:
        Number of rows inserted
    """
    if df.empty:
        return 0

    # Delete existing entries for this ts
    ts = df["ts"].iloc[0] if not df.empty else None
    if ts:
        con.execute(
            "DELETE FROM analytics_sector_snapshot WHERE ts = ?",
            (ts,),
        )

    count = 0
    for _, row in df.iterrows():
        con.execute(
            """
            INSERT INTO analytics_sector_snapshot (
                ts, sector_code, sector_name, symbols_count,
                avg_change_pct, sum_volume, top_symbol
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["ts"],
                row["sector_code"],
                row["sector_name"],
                row["symbols_count"],
                row.get("avg_change_pct"),
                row.get("sum_volume"),
                row.get("top_symbol"),
            ),
        )
        count += 1

    con.commit()
    return count


# =============================================================================
# Full Analytics Pipeline
# =============================================================================


def compute_all_analytics(
    con: sqlite3.Connection,
    ts: str,
    top_n: int = 10,
) -> dict[str, Any]:
    """Run the full analytics pipeline for a timestamp.

    Computes and stores:
    - Market analytics (breadth)
    - Top lists (gainers/losers/volume)
    - Sector rollups

    Args:
        con: Database connection
        ts: Timestamp for this snapshot
        top_n: Number of symbols in top lists

    Returns:
        Summary dict with counts
    """
    # Ensure analytics tables exist
    init_analytics_schema(con)

    # 1. Market analytics
    market_analytics = compute_market_analytics(con, ts)
    store_market_analytics(con, market_analytics)

    # 2. Top lists
    top_lists = compute_top_lists(con, ts, top_n=top_n)
    top_lists_count = store_top_lists(con, ts, top_lists)

    # 3. Sector rollups
    sector_rollups = compute_sector_rollups(con, ts)
    sectors_count = store_sector_rollups(con, sector_rollups)

    return {
        "ts": ts,
        "market_analytics": market_analytics,
        "top_lists_count": top_lists_count,
        "sectors_count": sectors_count,
    }


# =============================================================================
# Query Functions
# =============================================================================


def get_latest_market_analytics(con: sqlite3.Connection) -> dict[str, Any] | None:
    """Get the most recent market analytics.

    Args:
        con: Database connection

    Returns:
        Dict with analytics or None if no data
    """
    cur = con.execute(
        """
        SELECT * FROM analytics_market_snapshot
        ORDER BY ts DESC LIMIT 1
        """
    )
    row = cur.fetchone()
    if row:
        return dict(row)
    return None


def get_top_list(
    con: sqlite3.Connection,
    rank_type: str,
    ts: str | None = None,
    limit: int = 10,
) -> pd.DataFrame:
    """Get a top list (gainers/losers/volume).

    Args:
        con: Database connection
        rank_type: 'gainers', 'losers', or 'volume'
        ts: Optional timestamp (latest if None)
        limit: Maximum rows to return

    Returns:
        DataFrame with ranked symbols
    """
    if ts is None:
        # Get latest ts
        cur = con.execute(
            "SELECT MAX(ts) as max_ts FROM analytics_symbol_snapshot"
        )
        row = cur.fetchone()
        ts = row[0] if row else None

    if not ts:
        return pd.DataFrame()

    return pd.read_sql_query(
        """
        SELECT * FROM analytics_symbol_snapshot
        WHERE ts = ? AND rank_type = ?
        ORDER BY rank
        LIMIT ?
        """,
        con,
        params=[ts, rank_type, limit],
    )


def get_sector_leaderboard(
    con: sqlite3.Connection,
    ts: str | None = None,
    sort_by: str = "avg_change_pct",
    ascending: bool = False,
) -> pd.DataFrame:
    """Get sector rollups as a leaderboard.

    Args:
        con: Database connection
        ts: Optional timestamp (latest if None)
        sort_by: Column to sort by
        ascending: Sort order

    Returns:
        DataFrame with sector rollups
    """
    if ts is None:
        # Get latest ts
        cur = con.execute(
            "SELECT MAX(ts) as max_ts FROM analytics_sector_snapshot"
        )
        row = cur.fetchone()
        ts = row[0] if row else None

    if not ts:
        return pd.DataFrame()

    df = pd.read_sql_query(
        "SELECT * FROM analytics_sector_snapshot WHERE ts = ?",
        con,
        params=[ts],
    )

    if not df.empty and sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    return df


def get_analytics_history(
    con: sqlite3.Connection,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Get market analytics history.

    Args:
        con: Database connection
        start_ts: Optional start timestamp
        end_ts: Optional end timestamp
        limit: Maximum rows

    Returns:
        DataFrame with analytics history
    """
    query = "SELECT * FROM analytics_market_snapshot WHERE 1=1"
    params: list = []

    if start_ts:
        query += " AND ts >= ?"
        params.append(start_ts)

    if end_ts:
        query += " AND ts <= ?"
        params.append(end_ts)

    query += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_current_market_with_sectors(con: sqlite3.Connection) -> pd.DataFrame:
    """Get current market data joined with sector names from sectors table.

    Args:
        con: Database connection

    Returns:
        DataFrame with market data including sector_name and company_name
    """
    query = """
        SELECT
            rm.symbol,
            rm.ts,
            rm.status,
            rm.sector_code,
            COALESCE(sec.sector_name, s.sector_name, rm.sector_code) as sector_name,
            s.name as company_name,
            rm.listed_in,
            rm.ldcp,
            rm.open,
            rm.high,
            rm.low,
            rm.current,
            rm.change,
            rm.change_pct,
            rm.volume,
            rm.row_hash,
            rm.updated_at
        FROM regular_market_current rm
        LEFT JOIN sectors sec ON rm.sector_code = sec.sector_code
        LEFT JOIN symbols s ON rm.symbol = s.symbol
        ORDER BY rm.symbol
    """
    return pd.read_sql_query(query, con)
