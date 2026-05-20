"""Market-overview composite endpoints under /v1/market and /v1/rates.

Denormalized views: each endpoint maps 1:1 to a Dashboard widget so
the client gets a widget's data in a single round-trip. Wraps existing
repo functions in ``market_summary`` / ``rates_strip`` / ``market``.

Route ownership:
    GET /v1/market/kse100              — hero quote + breadth + 52w range
    GET /v1/market/top-gainers
    GET /v1/market/top-losers
    GET /v1/market/volume-leaders
    GET /v1/market/52w-extremes        — bundle (Q3)
    GET /v1/market/sector-leaderboard
    GET /v1/rates/strip                — KIBOR + PKRV + policy + FX
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.market import (
    FXRow,
    FiftyTwoWeekExtremes,
    KSE100Hero,
    Mover,
    RatesStrip,
    SectorRow,
)
from pakfindata.db.repositories import market as market_repo
from pakfindata.db.repositories import market_summary as ms_repo
from pakfindata.db.repositories import rates_strip as rates_repo

market_router = APIRouter(prefix="/v1/market", tags=["market"])
rates_router = APIRouter(prefix="/v1/rates", tags=["rates"])

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


# ---------------------------------------------------------------- /v1/market

@market_router.get("/kse100", response_model=KSE100Hero)
def kse100_hero(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    as_of: Annotated[
        Optional[str],
        Query(description="Override the latest snapshot date", pattern=DATE_RE),
    ] = None,
) -> KSE100Hero:
    """Denormalized hero — KSE-100 quote, 52w range, and breadth."""
    kse = market_repo.get_latest_kse100(con)
    if kse is None:
        raise HTTPException(
            status_code=503, detail="no KSE-100 row in psx_indices"
        )

    target = as_of or kse.get("index_date")
    # psx_indices can run a day ahead of eod_market_summary; if breadth
    # isn't available on the KSE date, fall back to latest available so
    # the widget still has values. The repo returns a dict with total=0
    # (not None) when the date has no rows, so test the total explicitly.
    breadth = ms_repo.get_eod_breadth(con, date=target, min_symbols=100)
    if not breadth or not (breadth.get("total") or 0):
        breadth = ms_repo.get_eod_breadth(con, date=None, min_symbols=100) or {}

    return KSE100Hero(
        as_of=target,
        value=kse["value"],
        change=kse.get("change"),
        change_pct=kse.get("change_pct"),
        ytd_change_pct=kse.get("ytd_change_pct"),
        one_year_change_pct=kse.get("one_year_change_pct"),
        week_52_high=kse.get("week_52_high"),
        week_52_low=kse.get("week_52_low"),
        advancers=breadth.get("gainers"),
        decliners=breadth.get("losers"),
        unchanged=breadth.get("unchanged"),
    )


@market_router.get("/top-gainers", response_model=list[Mover])
def top_gainers(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> list[dict]:
    df = ms_repo.get_top_movers(con, direction="gainers", limit=limit)
    return df_to_records(df)


@market_router.get("/top-losers", response_model=list[Mover])
def top_losers(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> list[dict]:
    df = ms_repo.get_top_movers(con, direction="losers", limit=limit)
    return df_to_records(df)


@market_router.get("/volume-leaders", response_model=list[Mover])
def volume_leaders(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> list[dict]:
    df = ms_repo.get_volume_leaders(con, limit=limit)
    return df_to_records(df)


@market_router.get("/52w-extremes", response_model=FiftyTwoWeekExtremes)
def fifty_two_week_extremes(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[int, Query(ge=1, le=50)] = 5,
) -> FiftyTwoWeekExtremes:
    """Bundle of symbols near 52w high and near 52w low (Q3)."""
    high = df_to_records(ms_repo.get_52w_extremes(con, near="high", limit=limit))
    low = df_to_records(ms_repo.get_52w_extremes(con, near="low", limit=limit))
    return FiftyTwoWeekExtremes(near_high=high, near_low=low)


@market_router.get("/sector-leaderboard", response_model=list[SectorRow])
def sector_leaderboard(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[dict]:
    df = ms_repo.get_sector_performance(con)
    return df_to_records(df)


# ---------------------------------------------------------------- /v1/rates

@rates_router.get("/strip", response_model=RatesStrip)
def rates_strip(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> RatesStrip:
    """Macro rates strip (SBP policy, KIBOR-3M, T-Bill-3M, PKRV-10Y) + FX.

    Missing data points return ``null`` for that field — never fabricated,
    never carried-forward.
    """
    strip = rates_repo.get_rates_strip(con)
    fx_tuples = rates_repo.get_fx_strip(con)

    policy = strip.get("policy") or (None, None)
    kibor = strip.get("kibor3m") or (None, None, None)
    tbill = strip.get("tbill3m") or (None, None)
    pkrv = strip.get("pkrv10y") or (None, None)

    return RatesStrip(
        sbp_policy_rate=policy[0],
        sbp_policy_date=policy[1],
        kibor_3m_bid=kibor[0],
        kibor_3m_offer=kibor[1],
        kibor_3m_date=kibor[2],
        tbill_3m_cutoff=tbill[0],
        tbill_3m_date=tbill[1],
        pkrv_10y_yield=pkrv[0],
        pkrv_10y_date=pkrv[1],
        fx=[FXRow(currency=c, selling=s, as_of=d) for c, s, d in fx_tuples],
    )
