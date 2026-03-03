"""Naya Pakistan Certificate (NPC) rate API endpoints."""

from typing import Optional

from fastapi import APIRouter, Query

from ...db import connect, init_schema
from ...db.repositories import npc_rates as npc_repo

router = APIRouter()


@router.get("/latest")
def get_latest(
    currency: Optional[str] = Query(None, description="Filter by currency (USD, GBP, EUR, PKR)"),
    certificate_type: str = Query("conventional", description="conventional or islamic"),
):
    """Latest NPC rates, optionally filtered by currency."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    df = npc_repo.get_latest_npc_rates(con, currency=currency, certificate_type=certificate_type)
    return {"count": len(df), "rates": df.to_dict(orient="records")}


@router.get("/history/{currency}/{tenor}")
def get_history(
    currency: str,
    tenor: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Historical NPC rate for a currency+tenor pair."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    df = npc_repo.get_npc_rate_history(
        con, currency=currency, tenor=tenor,
        start_date=start_date, end_date=end_date,
    )
    return {"count": len(df), "history": df.to_dict(orient="records")}


@router.get("/yield-curve/{currency}")
def yield_curve(
    currency: str,
    date: Optional[str] = Query(None, description="Date (YYYY-MM-DD), latest if omitted"),
):
    """NPC yield curve for a currency (all tenors)."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    curve = npc_repo.get_npc_yield_curve(con, currency=currency, date=date)
    return curve or {}


@router.get("/spread/rfr")
def rfr_spread(
    currency: Optional[str] = Query(None, description="Filter by currency"),
):
    """NPC premium over global risk-free rates (SOFR, SONIA, EUSTR)."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    df = npc_repo.get_npc_vs_rfr_spread(con, currency=currency)
    return {"count": len(df), "spread": df.to_dict(orient="records")}


@router.get("/spread/carry")
def carry_trade(
    currency: str = Query("USD", description="NPC currency for carry trade analysis"),
):
    """NPC vs KIBOR carry trade analysis."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    df = npc_repo.get_carry_trade_analysis(con, currency=currency)
    return {"count": len(df), "carry": df.to_dict(orient="records")}


@router.get("/dashboard")
def multicurrency_dashboard(
    date: Optional[str] = Query(None, description="Date (YYYY-MM-DD)"),
):
    """Comprehensive multi-currency view: NPC + RFR + KIBOR + FX."""
    con = connect()
    init_schema(con)
    npc_repo.ensure_tables(con)
    df = npc_repo.get_multicurrency_dashboard(con, date=date)
    return {"count": len(df), "dashboard": df.to_dict(orient="records")}


@router.post("/sync")
def sync_rates(force: bool = Query(False, description="Store even if rates unchanged")):
    """Trigger NPC rate scrape from SBP."""
    from ...sources.npc_rates_scraper import NPCRatesScraper

    con = connect()
    init_schema(con)
    scraper = NPCRatesScraper()
    count = scraper.sync(con, force=force)
    return {"stored": count}
