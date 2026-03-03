"""Fixed Income API endpoints (bonds, sukuk, yield curves, FX rates)."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_bonds,
    get_bond,
    get_sukuk_list,
    get_sukuk,
    get_latest_yield_curve,
    get_fx_pairs,
    get_fx_latest_rate,
    get_fx_ohlcv,
    get_latest_policy_rate,
    get_latest_kibor_rates,
)

router = APIRouter()


@router.get("/bonds")
def list_bonds(
    bond_type: Optional[str] = Query(None, description="Filter by type (PIB, T-Bill, etc.)"),
    active_only: bool = Query(True),
):
    """List all bonds."""
    con = connect()
    init_schema(con)
    bonds = get_bonds(con, bond_type=bond_type, active_only=active_only)
    return {"count": len(bonds), "bonds": bonds}


@router.get("/bonds/{bond_id}")
def bond_detail(bond_id: str):
    """Get detail for a specific bond."""
    con = connect()
    init_schema(con)
    bond = get_bond(con, bond_id)
    if bond is None:
        return {"error": f"Bond {bond_id} not found"}
    return bond


@router.get("/sukuk")
def list_sukuk(
    category: Optional[str] = Query(None, description="Filter by category"),
    active_only: bool = Query(True),
):
    """List all sukuk instruments."""
    con = connect()
    init_schema(con)
    sukuk = get_sukuk_list(con, category=category, active_only=active_only)
    return {"count": len(sukuk), "sukuk": sukuk}


@router.get("/sukuk/{instrument_id}")
def sukuk_detail(instrument_id: str):
    """Get detail for a specific sukuk."""
    con = connect()
    init_schema(con)
    s = get_sukuk(con, instrument_id)
    if s is None:
        return {"error": f"Sukuk {instrument_id} not found"}
    return s


@router.get("/yield-curve/{curve_type}")
def yield_curve(
    curve_type: str = "PIB",
):
    """Get latest yield curve for a bond type."""
    con = connect()
    init_schema(con)
    date, points = get_latest_yield_curve(con, bond_type=curve_type.upper())
    return {"curve_type": curve_type.upper(), "date": date, "points": points}


@router.get("/fx-rates")
def fx_rates():
    """Get latest FX rates for all pairs."""
    con = connect()
    init_schema(con)
    pairs = get_fx_pairs(con, active_only=True)
    rates = []
    for p in pairs:
        pair_code = p.get("pair") or p.get("symbol", "")
        rate = get_fx_latest_rate(con, pair_code)
        if rate:
            rates.append(rate)
    return {"count": len(rates), "rates": rates}


@router.get("/fx-rates/{pair}")
def fx_rate_detail(
    pair: str,
    limit: Optional[int] = Query(30, description="Number of historical records"),
):
    """Get FX rate detail and history for a pair."""
    con = connect()
    init_schema(con)
    latest = get_fx_latest_rate(con, pair.upper())
    df = get_fx_ohlcv(con, pair.upper(), limit=limit)
    history = df.to_dict(orient="records") if not df.empty else []
    return {"pair": pair.upper(), "latest": latest, "history": history}


@router.get("/policy-rate")
def policy_rate():
    """Get latest SBP policy rate."""
    con = connect()
    init_schema(con)
    rate = get_latest_policy_rate(con)
    return rate if rate else {"note": "No policy rate data available"}


@router.get("/kibor")
def kibor_rates():
    """Get latest KIBOR rates."""
    con = connect()
    init_schema(con)
    rates = get_latest_kibor_rates(con)
    return {"count": len(rates), "rates": rates}
