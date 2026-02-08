"""User interaction tracking and activity analytics repository."""

import json
import sqlite3

import pandas as pd

from psx_ohlcv.models import now_iso


# =============================================================================
# Interaction Logging Functions
# =============================================================================


def log_interaction(
    con: sqlite3.Connection,
    session_id: str,
    action_type: str,
    page_name: str | None = None,
    symbol: str | None = None,
    action_detail: str | None = None,
    metadata: dict | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> int:
    """
    Log a user interaction to the database.

    Args:
        con: Database connection
        session_id: Unique session identifier
        action_type: Type of action ('page_visit', 'search', 'button_click', 'refresh', 'download')
        page_name: Which page the action was on
        symbol: Stock symbol if applicable
        action_detail: Additional details (button name, search query, etc.)
        metadata: Dict with extra data (will be JSON serialized)
        ip_address: Optional user IP
        user_agent: Optional browser/client info

    Returns:
        ID of the inserted row
    """
    now = now_iso()
    metadata_json = json.dumps(metadata) if metadata else None

    cur = con.execute(
        """
        INSERT INTO user_interactions (
            session_id, timestamp, action_type, page_name,
            symbol, action_detail, metadata, ip_address, user_agent
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            now,
            action_type,
            page_name,
            symbol.upper() if symbol else None,
            action_detail,
            metadata_json,
            ip_address,
            user_agent,
        ),
    )
    con.commit()
    return cur.lastrowid


def get_session_interactions(
    con: sqlite3.Connection,
    session_id: str,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get interactions for a specific session.

    Args:
        con: Database connection
        session_id: Session identifier
        limit: Maximum rows to return

    Returns:
        DataFrame with interaction history
    """
    query = """
        SELECT id, session_id, timestamp, action_type, page_name,
               symbol, action_detail, metadata
        FROM user_interactions
        WHERE session_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """
    return pd.read_sql_query(query, con, params=[session_id, limit])


def get_recent_interactions(
    con: sqlite3.Connection,
    action_type: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get recent interactions across all sessions.

    Args:
        con: Database connection
        action_type: Filter by action type
        symbol: Filter by symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with recent interactions
    """
    query = """
        SELECT id, session_id, timestamp, action_type, page_name,
               symbol, action_detail, metadata
        FROM user_interactions
        WHERE 1=1
    """
    params: list = []

    if action_type:
        query += " AND action_type = ?"
        params.append(action_type)

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Interaction Analytics Functions
# =============================================================================


def get_interaction_stats(
    con: sqlite3.Connection,
    days: int = 7,
) -> dict:
    """
    Get interaction statistics for analytics.

    Args:
        con: Database connection
        days: Number of days to look back

    Returns:
        Dict with stats: total_interactions, unique_sessions, top_pages,
                        top_symbols, action_breakdown
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    stats: dict = {
        "total_interactions": 0,
        "unique_sessions": 0,
        "top_pages": [],
        "top_symbols": [],
        "action_breakdown": {},
    }

    # Total interactions and unique sessions
    cur = con.execute(
        """
        SELECT COUNT(*) as total, COUNT(DISTINCT session_id) as sessions
        FROM user_interactions
        WHERE timestamp >= ?
        """,
        (cutoff,),
    )
    row = cur.fetchone()
    if row:
        stats["total_interactions"] = row[0]
        stats["unique_sessions"] = row[1]

    # Top pages
    cur = con.execute(
        """
        SELECT page_name, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND page_name IS NOT NULL
        GROUP BY page_name
        ORDER BY count DESC
        LIMIT 10
        """,
        (cutoff,),
    )
    stats["top_pages"] = [{"page": row[0], "count": row[1]} for row in cur.fetchall()]

    # Top symbols
    cur = con.execute(
        """
        SELECT symbol, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND symbol IS NOT NULL
        GROUP BY symbol
        ORDER BY count DESC
        LIMIT 10
        """,
        (cutoff,),
    )
    stats["top_symbols"] = [{"symbol": row[0], "count": row[1]} for row in cur.fetchall()]

    # Action breakdown
    cur = con.execute(
        """
        SELECT action_type, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ?
        GROUP BY action_type
        ORDER BY count DESC
        """,
        (cutoff,),
    )
    stats["action_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

    return stats


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
