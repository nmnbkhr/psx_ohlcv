"""Symbols API endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import connect, init_schema, get_symbols_list, get_sector_map, get_sectors

router = APIRouter()


@router.get("/")
def list_symbols(
    active: bool = True,
    sector: Optional[str] = Query(None, description="Filter by sector code"),
    limit: Optional[int] = Query(None, description="Limit number of results"),
):
    """List all symbols with optional filters."""
    con = connect()
    init_schema(con)
    symbols = get_symbols_list(con, limit=limit)

    if sector:
        rows = con.execute(
            "SELECT symbol FROM symbols WHERE sector = ? AND is_active = ?",
            (sector, 1 if active else 0),
        ).fetchall()
        symbols = [r["symbol"] for r in rows]
    elif active:
        rows = con.execute(
            "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
        ).fetchall()
        symbols = [r["symbol"] for r in rows]
        if limit:
            symbols = symbols[:limit]

    return {"count": len(symbols), "symbols": symbols}


@router.get("/sectors")
def list_sectors():
    """List all sectors with codes and names."""
    con = connect()
    init_schema(con)
    sector_map = get_sector_map(con)
    return {"count": len(sector_map), "sectors": sector_map}


@router.get("/{symbol}")
def get_symbol(symbol: str):
    """Get detail for a specific symbol."""
    con = connect()
    init_schema(con)
    row = con.execute(
        "SELECT * FROM symbols WHERE symbol = ?", (symbol.upper(),)
    ).fetchone()
    if row is None:
        return {"error": f"Symbol {symbol.upper()} not found"}
    return dict(row)
