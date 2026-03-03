"""Fund API endpoints — Mutual Funds, ETFs, VPS pension funds."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_mutual_funds,
    get_mutual_fund,
    get_mf_nav,
    get_mf_latest_nav,
    get_mf_data_summary,
    get_etf_list,
    get_etf_detail,
    get_etf_nav_history,
    get_all_etf_latest_nav,
    get_vps_funds,
    get_vps_nav_history,
    compare_vps_performance,
    get_vps_summary,
)

router = APIRouter()


# ── Mutual Funds ──────────────────────────────────────────────────

@router.get("/mutual")
def list_mutual_funds(
    category: Optional[str] = Query(None, description="Fund category filter"),
    amc: Optional[str] = Query(None, description="AMC name filter"),
    fund_type: Optional[str] = Query(None, description="Fund type (OPEN_END, CLOSED_END, VPS)"),
    active_only: bool = Query(True, description="Only active funds"),
    limit: int = Query(100, description="Max results"),
):
    """List mutual funds with filters."""
    con = connect()
    init_schema(con)
    funds = get_mutual_funds(
        con, category=category, amc_code=amc,
        fund_type=fund_type, active_only=active_only, limit=limit,
    )
    return {"count": len(funds), "funds": funds}


@router.get("/mutual/summary")
def mutual_fund_summary():
    """Get mutual fund data summary statistics."""
    con = connect()
    init_schema(con)
    return get_mf_data_summary(con)


@router.get("/mutual/{fund_id:path}/nav")
def mutual_fund_nav(
    fund_id: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(365, description="Max NAV records"),
):
    """Get NAV history for a mutual fund."""
    con = connect()
    init_schema(con)
    df = get_mf_nav(con, fund_id=fund_id, start_date=start_date, end_date=end_date, limit=limit)
    return {"fund_id": fund_id, "count": len(df), "nav": df.to_dict(orient="records")}


@router.get("/mutual/{fund_id:path}")
def mutual_fund_detail(fund_id: str):
    """Get detail for a specific mutual fund."""
    con = connect()
    init_schema(con)
    fund = get_mutual_fund(con, fund_id)
    if fund is None:
        return {"error": f"Fund {fund_id} not found"}
    latest_nav = get_mf_latest_nav(con, fund_id)
    return {"fund": fund, "latest_nav": latest_nav}


# ── ETFs ──────────────────────────────────────────────────────────

@router.get("/etf")
def list_etfs():
    """List all ETFs."""
    con = connect()
    init_schema(con)
    etfs = get_etf_list(con)
    return {"count": len(etfs), "etfs": etfs}


@router.get("/etf/latest")
def etf_latest_navs():
    """Get latest NAV for all ETFs."""
    con = connect()
    init_schema(con)
    df = get_all_etf_latest_nav(con)
    return {"count": len(df), "etfs": df.to_dict(orient="records")}


@router.get("/etf/{symbol}")
def etf_detail(symbol: str):
    """Get ETF detail and latest NAV."""
    con = connect()
    init_schema(con)
    etf = get_etf_detail(con, symbol.upper())
    if etf is None:
        return {"error": f"ETF {symbol} not found"}
    return etf


@router.get("/etf/{symbol}/nav")
def etf_nav_history(
    symbol: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(365),
):
    """Get NAV history for an ETF."""
    con = connect()
    init_schema(con)
    df = get_etf_nav_history(con, symbol.upper(), start_date=start_date, end_date=end_date, limit=limit)
    return {"symbol": symbol.upper(), "count": len(df), "nav": df.to_dict(orient="records")}


# ── VPS Pension ───────────────────────────────────────────────────

@router.get("/vps")
def list_vps_funds():
    """List all VPS pension funds."""
    con = connect()
    init_schema(con)
    df = get_vps_funds(con)
    return {"count": len(df), "funds": df.to_dict(orient="records")}


@router.get("/vps/summary")
def vps_summary():
    """Get VPS data summary."""
    con = connect()
    init_schema(con)
    return get_vps_summary(con)


@router.get("/vps/compare")
def vps_compare(
    days: int = Query(365, description="Period in days"),
):
    """Compare VPS fund performance."""
    con = connect()
    init_schema(con)
    df = compare_vps_performance(con, days=days)
    return {"period_days": days, "count": len(df), "funds": df.to_dict(orient="records")}


@router.get("/vps/{fund_id:path}/nav")
def vps_nav_history(
    fund_id: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get NAV history for a VPS fund."""
    con = connect()
    init_schema(con)
    df = get_vps_nav_history(con, fund_id, start_date=start_date, end_date=end_date)
    return {"fund_id": fund_id, "count": len(df), "nav": df.to_dict(orient="records")}
