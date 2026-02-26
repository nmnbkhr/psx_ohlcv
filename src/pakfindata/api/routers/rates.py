"""Rates API endpoints — FX rates, KIBOR, KONIA, policy rate."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_fx_rate,
    get_fx_history,
    get_all_fx_latest,
    get_fx_spread,
    get_kibor_history,
    get_konia_history,
    get_latest_konia,
    get_latest_policy_rate,
    get_latest_kibor_rates,
)

router = APIRouter()


# ── FX Rates ──────────────────────────────────────────────────────

@router.get("/fx")
def fx_rates(
    currency: Optional[str] = Query(None, description="Currency code (e.g. USD, EUR)"),
    source: str = Query("interbank", description="Source: interbank, open_market, kerb"),
):
    """Get latest FX rates. Filter by currency and source."""
    con = connect()
    init_schema(con)
    if currency:
        rate = get_fx_rate(con, currency, source=source)
        return {"currency": currency.upper(), "source": source, "rate": rate}
    df = get_all_fx_latest(con, source=source)
    return {"source": source, "count": len(df), "rates": df.to_dict(orient="records")}


@router.get("/fx/spread/{currency}")
def fx_spread(
    currency: str,
    date: Optional[str] = Query(None, description="Date (YYYY-MM-DD), latest if omitted"),
):
    """Get FX spread across all sources for a currency."""
    con = connect()
    init_schema(con)
    return get_fx_spread(con, currency, date=date)


@router.get("/fx/history/{currency}")
def fx_history(
    currency: str,
    source: str = Query("interbank"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get FX rate history for a currency."""
    con = connect()
    init_schema(con)
    df = get_fx_history(con, currency, source=source, start_date=start_date, end_date=end_date)
    return {
        "currency": currency.upper(),
        "source": source,
        "count": len(df),
        "history": df.to_dict(orient="records"),
    }


# ── KIBOR ─────────────────────────────────────────────────────────

@router.get("/kibor")
def kibor_rates(
    tenor: Optional[str] = Query(None, description="Tenor filter (e.g. '1W', '1M', '3M')"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get KIBOR rate history. Without date filters, returns latest rates."""
    con = connect()
    init_schema(con)
    if not start_date and not end_date and not tenor:
        rates = get_latest_kibor_rates(con)
        return {"count": len(rates), "rates": rates}
    df = get_kibor_history(con, tenor=tenor, start_date=start_date, end_date=end_date)
    return {"count": len(df), "rates": df.to_dict(orient="records")}


# ── KONIA ─────────────────────────────────────────────────────────

@router.get("/konia")
def konia_rates(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get KONIA rate history. Without date filters, returns latest rate."""
    con = connect()
    init_schema(con)
    if not start_date and not end_date:
        rate = get_latest_konia(con)
        return {"latest": rate}
    df = get_konia_history(con, start_date=start_date, end_date=end_date)
    return {"count": len(df), "rates": df.to_dict(orient="records")}


# ── Policy Rate ───────────────────────────────────────────────────

@router.get("/policy")
def policy_rate():
    """Get latest SBP policy rate."""
    con = connect()
    init_schema(con)
    rate = get_latest_policy_rate(con)
    return rate if rate else {"note": "No policy rate data available"}
