"""Global reference rates API endpoints — SOFR, EFFR, SONIA, EUSTR, TONA."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from ...db import connect, init_schema
from ...db.repositories import global_rates as gr_repo

router = APIRouter()


@router.get("/latest")
def get_latest(rate_name: Optional[str] = Query(None, description="Filter by rate name (e.g. SOFR, EFFR)")):
    """Get latest values of all global reference rates."""
    con = connect()
    init_schema(con)
    gr_repo.ensure_tables(con)
    df = gr_repo.get_all_latest_rates(con)
    if rate_name:
        df = df[df["rate_name"] == rate_name.upper()]
    return {"count": len(df), "rates": df.to_dict(orient="records")}


@router.get("/sofr")
def get_sofr(days: int = Query(30, ge=1, le=1000)):
    """Get SOFR history."""
    con = connect()
    init_schema(con)
    gr_repo.ensure_tables(con)
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = gr_repo.get_rate_history(con, rate_name="SOFR", tenor="ON", start_date=start, limit=0)
    return {"count": len(df), "history": df.to_dict(orient="records")}


@router.get("/effr")
def get_effr(days: int = Query(30, ge=1, le=1000)):
    """Get EFFR history."""
    con = connect()
    init_schema(con)
    gr_repo.ensure_tables(con)
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = gr_repo.get_rate_history(con, rate_name="EFFR", tenor="ON", start_date=start, limit=0)
    return {"count": len(df), "history": df.to_dict(orient="records")}


@router.get("/spread/sofr-kibor")
def get_sofr_kibor_spread(days: int = Query(30, ge=1, le=365)):
    """Get SOFR vs KIBOR spread for FX swap pricing analysis."""
    con = connect()
    init_schema(con)
    gr_repo.ensure_tables(con)
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = gr_repo.get_sofr_kibor_spread(con, start_date=start)
    return {"count": len(df), "spread": df.to_dict(orient="records")}


@router.get("/comparison")
def get_rate_comparison(date: Optional[str] = Query(None, description="Date (YYYY-MM-DD), latest if omitted")):
    """Compare all rates (SOFR, KIBOR, KONIA, EFFR, etc.) for a given date."""
    con = connect()
    init_schema(con)
    gr_repo.ensure_tables(con)
    return gr_repo.get_rate_comparison(con, date=date)
