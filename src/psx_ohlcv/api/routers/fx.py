"""FX microservice passthrough API endpoints."""

from fastapi import APIRouter, HTTPException, Query

from ...sources.fx_client import FXClient
from ...sources.fx_sync import sync_fx_rates, backfill_fx_history
from ...db import connect, init_schema

router = APIRouter()
_fx = FXClient()


@router.get("/snapshot")
def fx_snapshot():
    """Get FX snapshot (rates + KIBOR + signals) from FX microservice."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    data = _fx.get_snapshot()
    if not data:
        raise HTTPException(502, "FX service returned empty response")
    return data


@router.get("/regime")
def fx_regime():
    """Get FX-equity regime signal with sector exposure guide."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    return _fx.get_regime() or {}


@router.get("/intervention")
def fx_intervention():
    """Get SBP intervention report (FXIM published data + statistical)."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    return _fx.get_intervention() or {}


@router.get("/health")
def fx_health():
    """Check if FX microservice is reachable."""
    return {"fx_service": "up" if _fx.is_healthy() else "down"}


@router.post("/sync")
def fx_sync():
    """Sync today's FX snapshot (rates + KIBOR) into local DB."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    con = connect()
    init_schema(con)
    return sync_fx_rates(con)


@router.post("/backfill")
def fx_backfill(
    from_date: str = Query("2024-01-01", description="Start date YYYY-MM-DD"),
    to_date: str = Query(None, description="End date YYYY-MM-DD, defaults to today"),
):
    """Backfill historical FX rates from FX microservice into local DB.

    Call this once on first setup, or anytime to fill date gaps.
    Uses INSERT OR IGNORE — only fills gaps, never overwrites.
    """
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    con = connect()
    init_schema(con)
    return backfill_fx_history(con, from_date, to_date)
