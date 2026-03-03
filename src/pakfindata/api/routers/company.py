"""Company data API endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_company_profile,
    get_company_fundamentals,
    get_company_financials,
    get_quote_snapshots,
    get_company_unified,
)

router = APIRouter()


@router.get("/{symbol}")
def company_overview(symbol: str):
    """Get unified company data (profile, fundamentals, latest quote)."""
    con = connect()
    init_schema(con)
    data = get_company_unified(con, symbol.upper())
    if data is None:
        return {"error": f"No data for {symbol.upper()}"}
    return data


@router.get("/{symbol}/profile")
def company_profile(symbol: str):
    """Get company profile."""
    con = connect()
    init_schema(con)
    profile = get_company_profile(con, symbol.upper())
    if profile is None:
        return {"error": f"No profile for {symbol.upper()}"}
    return profile


@router.get("/{symbol}/fundamentals")
def company_fundamentals(symbol: str):
    """Get company fundamentals (market cap, P/E, EPS, etc.)."""
    con = connect()
    init_schema(con)
    data = get_company_fundamentals(con, symbol.upper())
    if data is None:
        return {"error": f"No fundamentals for {symbol.upper()}"}
    return data


@router.get("/{symbol}/financials")
def company_financials(
    symbol: str,
    period_type: Optional[str] = Query(None, description="annual or quarterly"),
    limit: int = Query(20, description="Number of records"),
):
    """Get company financial statements."""
    con = connect()
    init_schema(con)
    df = get_company_financials(con, symbol.upper(), period_type=period_type, limit=limit)
    if df.empty:
        return {"count": 0, "data": []}
    return {"count": len(df), "data": df.to_dict(orient="records")}


@router.get("/{symbol}/quotes")
def company_quotes(
    symbol: str,
    limit: int = Query(100, description="Number of snapshots"),
):
    """Get historical quote snapshots."""
    con = connect()
    init_schema(con)
    df = get_quote_snapshots(con, symbol.upper(), limit=limit)
    if df.empty:
        return {"count": 0, "data": []}
    return {"count": len(df), "data": df.to_dict(orient="records")}
