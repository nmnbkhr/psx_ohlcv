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
    get_latest_kse100,
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


@router.get("/snapshot")
def market_snapshot():
    """Full market snapshot — KSE-100, breadth, top movers."""
    con = connect()
    init_schema(con)
    kse100 = get_latest_kse100(con)
    # Breadth
    try:
        brow = con.execute("""
            SELECT
                COUNT(CASE WHEN change_pct > 0 THEN 1 END) as gainers,
                COUNT(CASE WHEN change_pct < 0 THEN 1 END) as losers,
                COUNT(CASE WHEN change_pct = 0 OR change_pct IS NULL THEN 1 END) as unchanged,
                COUNT(*) as total
            FROM regular_market_current
        """).fetchone()
        breadth = dict(brow) if brow else {}
    except Exception:
        breadth = {}
    # Top movers from EOD
    try:
        max_date = con.execute("SELECT MAX(date) as d FROM eod_ohlcv").fetchone()
        date_val = max_date["d"] if max_date else None
        if date_val:
            prev_row = con.execute(
                "SELECT MAX(date) as d FROM eod_ohlcv WHERE date < ?", (date_val,)
            ).fetchone()
            prev_date = prev_row["d"] if prev_row else None
            if prev_date:
                movers_sql = """
                    SELECT t.symbol, t.close, p.close as prev_close, t.volume,
                           ROUND((t.close - p.close) / p.close * 100, 2) as change_pct
                    FROM eod_ohlcv t
                    INNER JOIN eod_ohlcv p ON t.symbol = p.symbol AND p.date = ?
                    WHERE t.date = ? AND p.close > 0 AND t.close > 0
                    ORDER BY change_pct DESC LIMIT 5
                """
                top_gainers = [dict(r) for r in con.execute(movers_sql, (prev_date, date_val)).fetchall()]
                movers_sql_losers = movers_sql.replace("DESC", "ASC")
                top_losers = [dict(r) for r in con.execute(movers_sql_losers, (prev_date, date_val)).fetchall()]
            else:
                top_gainers, top_losers = [], []
        else:
            top_gainers, top_losers = [], []
    except Exception:
        top_gainers, top_losers = [], []
    return {
        "kse100": kse100,
        "breadth": breadth,
        "top_gainers": top_gainers,
        "top_losers": top_losers,
    }


@router.get("/movers")
def top_movers(
    direction: str = Query("gainers", description="'gainers' or 'losers'"),
    limit: int = Query(10, description="Number of results"),
):
    """Get top gainers or losers from latest trading day."""
    con = connect()
    init_schema(con)
    try:
        max_date = con.execute("SELECT MAX(date) as d FROM eod_ohlcv").fetchone()
        date_val = max_date["d"] if max_date else None
        if not date_val:
            return {"count": 0, "movers": []}
        prev_row = con.execute(
            "SELECT MAX(date) as d FROM eod_ohlcv WHERE date < ?", (date_val,)
        ).fetchone()
        prev_date = prev_row["d"] if prev_row else None
        if not prev_date:
            return {"count": 0, "movers": []}
        order = "DESC" if direction == "gainers" else "ASC"
        rows = con.execute(
            f"""SELECT t.symbol, t.close, p.close as prev_close, t.volume,
                       ROUND((t.close - p.close) / p.close * 100, 2) as change_pct
                FROM eod_ohlcv t
                INNER JOIN eod_ohlcv p ON t.symbol = p.symbol AND p.date = ?
                WHERE t.date = ? AND p.close > 0 AND t.close > 0
                ORDER BY change_pct {order} LIMIT ?""",
            (prev_date, date_val, limit),
        ).fetchall()
        return {"date": date_val, "direction": direction, "count": len(rows), "movers": [dict(r) for r in rows]}
    except Exception:
        return {"count": 0, "movers": []}


@router.get("/sectors")
def sector_summary():
    """Get sector summary — count of symbols and average change per sector."""
    con = connect()
    init_schema(con)
    try:
        max_date = con.execute("SELECT MAX(date) as d FROM eod_ohlcv").fetchone()
        date_val = max_date["d"] if max_date else None
        if not date_val:
            return {"count": 0, "sectors": []}
        prev_row = con.execute(
            "SELECT MAX(date) as d FROM eod_ohlcv WHERE date < ?", (date_val,)
        ).fetchone()
        prev_date = prev_row["d"] if prev_row else None
        if not prev_date:
            return {"count": 0, "sectors": []}
        rows = con.execute(
            """SELECT s.sector,
                      COUNT(*) as symbol_count,
                      ROUND(AVG((t.close - p.close) / p.close * 100), 2) as avg_change_pct,
                      SUM(t.volume) as total_volume
               FROM eod_ohlcv t
               INNER JOIN eod_ohlcv p ON t.symbol = p.symbol AND p.date = ?
               INNER JOIN symbols s ON t.symbol = s.symbol
               WHERE t.date = ? AND p.close > 0 AND t.close > 0
                     AND s.sector IS NOT NULL
               GROUP BY s.sector
               ORDER BY avg_change_pct DESC""",
            (prev_date, date_val),
        ).fetchall()
        return {"date": date_val, "count": len(rows), "sectors": [dict(r) for r in rows]}
    except Exception:
        return {"count": 0, "sectors": []}
