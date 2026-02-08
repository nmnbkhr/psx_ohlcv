"""Instruments API endpoints (ETFs, REITs, indices)."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_instruments,
    get_instrument_by_id,
    get_ohlcv_instrument,
)

router = APIRouter()


@router.get("/")
def list_instruments(
    instrument_type: Optional[str] = Query(None, description="Filter by type (ETF, REIT, INDEX)"),
    active_only: bool = Query(True, description="Only active instruments"),
):
    """List all instruments."""
    con = connect()
    init_schema(con)
    instruments = get_instruments(con, instrument_type=instrument_type, active_only=active_only)
    return {"count": len(instruments), "instruments": instruments}


@router.get("/{instrument_id}")
def instrument_detail(instrument_id: str):
    """Get detail for a specific instrument."""
    con = connect()
    init_schema(con)
    instrument = get_instrument_by_id(con, instrument_id)
    if instrument is None:
        return {"error": f"Instrument {instrument_id} not found"}
    return instrument


@router.get("/{instrument_id}/ohlcv")
def instrument_ohlcv(
    instrument_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(None, description="Limit rows"),
):
    """Get OHLCV data for an instrument."""
    con = connect()
    init_schema(con)
    df = get_ohlcv_instrument(
        con, instrument_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if df.empty:
        return {"count": 0, "data": []}
    return {"count": len(df), "data": df.to_dict(orient="records")}
