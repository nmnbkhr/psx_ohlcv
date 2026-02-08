"""Market data API endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_all_latest_indices,
    get_latest_index,
    get_index_history,
    get_latest_market_stats,
)

router = APIRouter()


@router.get("/indices")
def latest_indices():
    """Get latest values for all indices (KSE-100, KSE-30, etc.)."""
    con = connect()
    init_schema(con)
    indices = get_all_latest_indices(con)
    return {"count": len(indices), "indices": indices}


@router.get("/indices/{index_code}")
def index_detail(
    index_code: str,
    days: int = Query(30, description="Number of days of history"),
):
    """Get latest value and history for a specific index."""
    con = connect()
    init_schema(con)
    latest = get_latest_index(con, index_code.upper())
    history = get_index_history(con, index_code.upper(), days=days)
    return {"latest": latest, "history": history}


@router.get("/breadth")
def market_breadth():
    """Get market breadth — gainers, losers, unchanged counts."""
    con = connect()
    init_schema(con)
    try:
        row = con.execute("""
            SELECT
                COUNT(CASE WHEN change_pct > 0 THEN 1 END) as gainers,
                COUNT(CASE WHEN change_pct < 0 THEN 1 END) as losers,
                COUNT(CASE WHEN change_pct = 0 OR change_pct IS NULL THEN 1 END) as unchanged,
                COUNT(*) as total
            FROM regular_market_current
        """).fetchone()
        return dict(row) if row else {"gainers": 0, "losers": 0, "unchanged": 0, "total": 0}
    except Exception:
        return {"gainers": 0, "losers": 0, "unchanged": 0, "total": 0, "note": "No live data available"}


@router.get("/live")
def live_market(
    limit: int = Query(50, description="Number of symbols"),
):
    """Get current regular market data."""
    con = connect()
    init_schema(con)
    try:
        rows = con.execute(
            "SELECT * FROM regular_market_current ORDER BY symbol LIMIT ?",
            (limit,),
        ).fetchall()
        return {"count": len(rows), "data": [dict(r) for r in rows]}
    except Exception:
        return {"count": 0, "data": [], "note": "No live data available"}


@router.get("/stats")
def market_stats():
    """Get latest market statistics."""
    con = connect()
    init_schema(con)
    stats = get_latest_market_stats(con)
    return stats if stats else {"note": "No market stats available"}
