"""Bond market API endpoints — OTC trading volume + benchmark rates."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import connect, init_schema
from ...db.repositories.bond_market import (
    init_bond_market_schema,
    get_bond_trading,
    get_trading_volume_trend,
    get_benchmark_snapshot,
    get_benchmark_history,
    get_bond_market_status,
)

router = APIRouter()


@router.get("/benchmark")
def benchmark_snapshot(
    date: Optional[str] = Query(None, description="Date (YYYY-MM-DD), defaults to latest"),
):
    """Get benchmark rate snapshot (policy rate, KIBOR, MTB/PIB cutoffs)."""
    con = connect()
    init_bond_market_schema(con)
    snap = get_benchmark_snapshot(con, date=date)
    return {"date": snap.pop("_date", None), "metrics": snap}


@router.get("/benchmark/history/{metric}")
def benchmark_metric_history(
    metric: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get historical values for a specific benchmark metric."""
    con = connect()
    init_bond_market_schema(con)
    df = get_benchmark_history(con, metric, start_date=start_date, end_date=end_date)
    return {"metric": metric, "count": len(df), "data": df.to_dict(orient="records")}


@router.get("/trading")
def bond_trading(
    date: Optional[str] = Query(None),
    security_type: Optional[str] = Query(None),
    segment: Optional[str] = Query(None),
):
    """Get OTC bond trading data with optional filters."""
    con = connect()
    init_bond_market_schema(con)
    df = get_bond_trading(con, date=date, security_type=security_type, segment=segment)
    return {"count": len(df), "trades": df.to_dict(orient="records")}


@router.get("/volume-trend")
def volume_trend(
    days: int = Query(30, description="Number of days"),
    security_type: Optional[str] = Query(None),
):
    """Get daily aggregate trading volume trend."""
    con = connect()
    init_bond_market_schema(con)
    df = get_trading_volume_trend(con, n_days=days, security_type=security_type)
    return {"days": days, "count": len(df), "data": df.to_dict(orient="records")}


@router.get("/status")
def bond_market_status():
    """Get bond market data coverage summary."""
    con = connect()
    init_bond_market_schema(con)
    return get_bond_market_status(con)


@router.post("/sync/benchmark")
def sync_benchmark():
    """Scrape and store SBP benchmark snapshot."""
    from ...sources.sbp_bond_market import SBPBondMarketScraper

    con = connect()
    init_bond_market_schema(con)
    scraper = SBPBondMarketScraper()
    return scraper.sync_benchmark(con)
