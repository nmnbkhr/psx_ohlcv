"""Fixed-Income endpoints — /v1/treasury, /v1/yield-curves,
/v1/curve, /v1/bonds, /v1/benchmark, /v1/rates/policy,
/v1/rates/npc (extras), /v1/rates/global (extras), /v1/alm, /v1/fi.

Backs the Group B pages (rates_overview, treasury_dashboard,
fixed_income, curve_analytics, bond_market, debt_terminal,
alm_dashboard, global_rates, npc_rates). All read-only, all
auth-gated by the global Bearer middleware.

ALM and NPC-view routes delegate to repository functions; this
preserves graceful empty-result handling when the underlying tables
or views are absent (e.g. v_npc_carry_trade is unbuilt — repo returns
an empty DataFrame).
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.fixed_income import (
    AlmFtpRow,
    AlmLiquidityLadderRow,
    AlmPositionRow,
    AlmProductRow,
    AlmRepricingGapRow,
    AlmSensitivityRow,
    BenchmarkSnapshot,
    BenchmarkSnapshotRow,
    BondTradingDailyRow,
    FiInstrumentRow,
    FiQuoteRow,
    GenericRow,
    GisAuctionRow,
    GenericRow as _Gen,  # noqa: F401  re-export typing hint
    PibAuctionRow,
    PkfrvRow,
    PkisrvRow,
    PkrvRow,
    PolicyRateRow,
    SovereignCurveRow,
    TbillAuctionRow,
)
from pakfindata.db.repositories import (
    alm as alm_repo,
    bond_market as bond_repo,
    global_rates as gr_repo,
    npc_rates as npc_repo,
)

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"

treasury_router = APIRouter(prefix="/v1/treasury", tags=["treasury"])
yield_curves_router = APIRouter(prefix="/v1/yield-curves", tags=["yield-curves"])
curve_router = APIRouter(prefix="/v1/curve", tags=["curve"])
bonds_router = APIRouter(prefix="/v1/bonds", tags=["bonds"])
benchmark_router = APIRouter(prefix="/v1/benchmark", tags=["benchmark"])
fi_router = APIRouter(prefix="/v1/fi", tags=["fi"])
alm_router = APIRouter(prefix="/v1/alm", tags=["alm"])
# Extras attach to existing prefixes from fx.py / market.py
rates_policy_router = APIRouter(prefix="/v1/rates/policy", tags=["rates"])
rates_npc_extras_router = APIRouter(prefix="/v1/rates/npc", tags=["rates"])
rates_global_extras_router = APIRouter(prefix="/v1/rates/global", tags=["rates"])


# ── /v1/treasury ────────────────────────────────────────────────────


@treasury_router.get("/tbill", response_model=list[TbillAuctionRow])
def get_tbill_auctions(
    tenor: Annotated[Optional[str], Query(description="Filter by tenor (e.g. 3M, 6M, 12M)")] = None,
    from_: Annotated[
        Optional[str],
        Query(alias="from", description="Range start (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    to: Annotated[
        Optional[str],
        Query(description="Range end (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Treasury Bill auction history, newest first."""
    where = ["1=1"]
    params: list = []
    if tenor:
        where.append("tenor = ?")
        params.append(tenor.upper())
    if from_:
        where.append("auction_date >= ?")
        params.append(from_)
    if to:
        where.append("auction_date <= ?")
        params.append(to)
    cur = con.execute(
        f"SELECT * FROM tbill_auctions WHERE {' AND '.join(where)} "
        f"ORDER BY auction_date DESC LIMIT ?",
        params + [limit],
    )
    return [dict(r) for r in cur.fetchall()]


@treasury_router.get("/tbill/latest-per-tenor", response_model=list[TbillAuctionRow])
def get_tbill_latest_per_tenor(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Most recent T-Bill auction for each tenor, sorted 3M→6M→12M."""
    cur = con.execute(
        """WITH ranked AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY tenor ORDER BY auction_date DESC) AS rn
              FROM tbill_auctions
           )
           SELECT * FROM ranked WHERE rn = 1
           ORDER BY CASE tenor WHEN '3M' THEN 1 WHEN '6M' THEN 2
                               WHEN '12M' THEN 3 ELSE 4 END"""
    )
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r.pop("rn", None)
    return rows


@treasury_router.get("/pib", response_model=list[PibAuctionRow])
def get_pib_auctions(
    tenor: Annotated[Optional[str], Query(description="Filter by tenor (2Y/3Y/5Y/10Y/15Y/20Y/30Y)")] = None,
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    to: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """PIB auction history, newest first."""
    where = ["1=1"]
    params: list = []
    if tenor:
        where.append("tenor = ?")
        params.append(tenor.upper())
    if from_:
        where.append("auction_date >= ?")
        params.append(from_)
    if to:
        where.append("auction_date <= ?")
        params.append(to)
    cur = con.execute(
        f"SELECT * FROM pib_auctions WHERE {' AND '.join(where)} "
        f"ORDER BY auction_date DESC LIMIT ?",
        params + [limit],
    )
    return [dict(r) for r in cur.fetchall()]


@treasury_router.get("/pib/latest-per-tenor", response_model=list[PibAuctionRow])
def get_pib_latest_per_tenor(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Most recent PIB auction for each tenor, sorted 2Y→30Y."""
    cur = con.execute(
        """WITH ranked AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY tenor ORDER BY auction_date DESC) AS rn
              FROM pib_auctions
           )
           SELECT * FROM ranked WHERE rn = 1
           ORDER BY CASE tenor
                      WHEN '2Y' THEN 1 WHEN '3Y' THEN 2 WHEN '5Y' THEN 3
                      WHEN '10Y' THEN 4 WHEN '15Y' THEN 5 WHEN '20Y' THEN 6
                      WHEN '30Y' THEN 7 ELSE 8 END"""
    )
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r.pop("rn", None)
    return rows


@treasury_router.get("/gis", response_model=list[GisAuctionRow])
def get_gis_auctions(
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """GIS (Government Ijara Sukuk) auction history, newest first."""
    cur = con.execute(
        "SELECT * FROM gis_auctions ORDER BY auction_date DESC LIMIT ?",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/yield-curves ────────────────────────────────────────────────


@yield_curves_router.get("/pkrv", response_model=list[PkrvRow])
def get_pkrv(
    date: Annotated[Optional[str], Query(description="YYYY-MM-DD; defaults to latest", pattern=DATE_RE)] = None,
    days: Annotated[int, Query(ge=1, le=5000)] = 1,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """PKRV (conventional sovereign curve) — by date or recent N days.

    With ``date``: returns all tenors for that day, sorted by tenor_months.
    Without ``date``: returns the last ``days`` distinct trading days
    across all tenors, newest first.
    """
    if date:
        cur = con.execute(
            "SELECT date, tenor_months, yield_pct, change_bps, source "
            "FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
            (date,),
        )
    else:
        if days == 1:
            cur = con.execute(
                """SELECT date, tenor_months, yield_pct, change_bps, source
                   FROM pkrv_daily
                   WHERE date = (SELECT MAX(date) FROM pkrv_daily)
                   ORDER BY tenor_months"""
            )
        else:
            cur = con.execute(
                """SELECT date, tenor_months, yield_pct, change_bps, source
                   FROM pkrv_daily
                   WHERE date IN (SELECT DISTINCT date FROM pkrv_daily
                                  ORDER BY date DESC LIMIT ?)
                   ORDER BY date DESC, tenor_months""",
                (days,),
            )
    return [dict(r) for r in cur.fetchall()]


@yield_curves_router.get("/pkisrv", response_model=list[PkisrvRow])
def get_pkisrv(
    date: Annotated[Optional[str], Query(description="YYYY-MM-DD; defaults to latest", pattern=DATE_RE)] = None,
    days: Annotated[int, Query(ge=1, le=5000)] = 1,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """PKISRV (Islamic sovereign curve) — by date or recent N days."""
    if date:
        cur = con.execute(
            "SELECT date, tenor, yield_pct, source FROM pkisrv_daily "
            "WHERE date = ? ORDER BY tenor",
            (date,),
        )
    else:
        if days == 1:
            cur = con.execute(
                """SELECT date, tenor, yield_pct, source
                   FROM pkisrv_daily
                   WHERE date = (SELECT MAX(date) FROM pkisrv_daily)
                   ORDER BY tenor"""
            )
        else:
            cur = con.execute(
                """SELECT date, tenor, yield_pct, source
                   FROM pkisrv_daily
                   WHERE date IN (SELECT DISTINCT date FROM pkisrv_daily
                                  ORDER BY date DESC LIMIT ?)
                   ORDER BY date DESC, tenor""",
                (days,),
            )
    return [dict(r) for r in cur.fetchall()]


@yield_curves_router.get("/pkfrv", response_model=list[PkfrvRow])
def get_pkfrv(
    date: Annotated[Optional[str], Query(description="YYYY-MM-DD; defaults to latest", pattern=DATE_RE)] = None,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """PKFRV (floating-rate PIB valuations) — per-bond, latest or
    by date."""
    if date:
        cur = con.execute(
            """SELECT date, bond_code, issue_date, maturity_date,
                      coupon_frequency, fma_price, source
               FROM pkfrv_daily WHERE date = ?
               ORDER BY maturity_date LIMIT ?""",
            (date, limit),
        )
    else:
        cur = con.execute(
            """SELECT date, bond_code, issue_date, maturity_date,
                      coupon_frequency, fma_price, source
               FROM pkfrv_daily
               WHERE date = (SELECT MAX(date) FROM pkfrv_daily)
               ORDER BY maturity_date LIMIT ?""",
            (limit,),
        )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/curve/sovereign ─────────────────────────────────────────────


@curve_router.get("/sovereign/sources", response_model=list[str])
def get_sovereign_sources(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Distinct source codes present in ``sovereign_curve``."""
    cur = con.execute(
        "SELECT DISTINCT source FROM sovereign_curve ORDER BY source"
    )
    return [r[0] for r in cur.fetchall()]


@curve_router.get("/sovereign/dates", response_model=list[str])
def get_sovereign_dates(
    source: Annotated[
        Optional[str],
        Query(description="Restrict to a single source code"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Distinct trading dates with curve data, newest first."""
    if source:
        cur = con.execute(
            "SELECT DISTINCT date FROM sovereign_curve "
            "WHERE source = ? ORDER BY date DESC LIMIT ?",
            (source.upper(), limit),
        )
    else:
        cur = con.execute(
            "SELECT DISTINCT date FROM sovereign_curve "
            "ORDER BY date DESC LIMIT ?",
            (limit,),
        )
    return [r[0] for r in cur.fetchall()]


@curve_router.get("/sovereign/tenor-history", response_model=list[SovereignCurveRow])
def get_sovereign_tenor_history(
    tenor: Annotated[str, Query(description="Tenor label (e.g. 10Y, _RMSE)")],
    sources: Annotated[
        Optional[str],
        Query(description="Comma-separated source codes; omit for all"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """History of (date, yield_pct, …) for a fixed tenor across one or
    more sources.

    Backs the Curve Analytics history + NSS-RMSE charts.
    Special tenor ``_RMSE`` returns the per-source NSS fit error.
    """
    where = ["tenor = ?"]
    params: list = [tenor]
    if sources:
        keys = [s.strip().upper() for s in sources.split(",") if s.strip()]
        placeholders = ",".join("?" * len(keys))
        where.append(f"source IN ({placeholders})")
        params.extend(keys)
    cur = con.execute(
        f"""SELECT date, source, tenor, days, yield_pct, bid, offer
            FROM sovereign_curve WHERE {' AND '.join(where)}
            ORDER BY date DESC LIMIT ?""",
        params + [limit],
    )
    return [dict(r) for r in cur.fetchall()]


@curve_router.get("/sovereign", response_model=list[SovereignCurveRow])
def get_sovereign_curve(
    date: Annotated[Optional[str], Query(description="YYYY-MM-DD; defaults to latest", pattern=DATE_RE)] = None,
    source: Annotated[
        Optional[str],
        Query(description="Filter by source code (PKRV/PKISRV/MTB/PIB/KIBOR/POLICY/*_SYN)"),
    ] = None,
    include_synthetic: Annotated[
        bool,
        Query(description="Include *_SYN spline/NSS sources (default True; required by Curve Analytics)"),
    ] = True,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Sovereign curve points from the consolidated ``sovereign_curve``
    table.

    Returns the day's full set across sources by default; pass
    ``source`` to restrict to one. ``include_synthetic=False`` filters
    out the spline/NSS fits and their RMSE metadata rows.
    """
    where = []
    params: list = []
    if date:
        where.append("date = ?")
        params.append(date)
    else:
        where.append("date = (SELECT MAX(date) FROM sovereign_curve)")
    if source:
        where.append("source = ?")
        params.append(source.upper())
    if not include_synthetic:
        where.append("source NOT LIKE '%\\_SYN' ESCAPE '\\'")

    sql = (
        "SELECT date, source, tenor, days, yield_pct, bid, offer "
        f"FROM sovereign_curve WHERE {' AND '.join(where)} ORDER BY source, days"
    )
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


# ── /v1/bonds ───────────────────────────────────────────────────────


@bonds_router.get("/trading-daily", response_model=list[BondTradingDailyRow])
def get_bond_trading_daily(
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    to: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    security_type: Annotated[Optional[str], Query(description="MTB or PIB")] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """``sbp_bond_trading_daily`` (SMTV) — daily bond-market trading
    aggregates, newest first.
    """
    where = ["1=1"]
    params: list = []
    if from_:
        where.append("date >= ?")
        params.append(from_)
    if to:
        where.append("date <= ?")
        params.append(to)
    if security_type:
        where.append("security_type = ?")
        params.append(security_type.upper())
    cur = con.execute(
        f"""SELECT date, security_type, maturity_year, tenor_bucket, segment,
                   face_amount, realized_amount, yield_min, yield_max,
                   yield_weighted_avg, scraped_at
            FROM sbp_bond_trading_daily WHERE {' AND '.join(where)}
            ORDER BY date DESC LIMIT ?""",
        params + [limit],
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/benchmark ───────────────────────────────────────────────────


@benchmark_router.get("/snapshot/history", response_model=list[BenchmarkSnapshotRow])
def get_benchmark_history(
    metric: Annotated[str, Query(description="Metric name (e.g. policy_rate)")],
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    to: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """History of a single ``sbp_benchmark_snapshot`` metric, oldest first."""
    where = ["metric = ?"]
    params: list = [metric]
    if from_:
        where.append("date >= ?")
        params.append(from_)
    if to:
        where.append("date <= ?")
        params.append(to)
    cur = con.execute(
        f"SELECT date, metric, value FROM sbp_benchmark_snapshot "
        f"WHERE {' AND '.join(where)} ORDER BY date",
        params,
    )
    return [dict(r) for r in cur.fetchall()]


@benchmark_router.get("/status", response_model=GenericRow)
def get_bond_market_status(
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """``bond_market`` repo's status dict (used to gate the Trading
    Volume section)."""
    try:
        return bond_repo.get_bond_market_status(con) or {}
    except Exception:
        return {}


@benchmark_router.get("/snapshot", response_model=BenchmarkSnapshot)
def get_benchmark_snapshot(
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Latest ``sbp_benchmark_snapshot`` collapsed to metric→value dict.

    Returns ``{date: None, metrics: {}}`` if the table is empty or
    absent (catalog status note rather than 404 — page renders
    "—" tiles).
    """
    try:
        row = con.execute(
            "SELECT MAX(date) AS d FROM sbp_benchmark_snapshot"
        ).fetchone()
    except sqlite3.OperationalError:
        return {"date": None, "metrics": {}}

    if not row or not row["d"]:
        return {"date": None, "metrics": {}}

    date = row["d"]
    cur = con.execute(
        "SELECT metric, value FROM sbp_benchmark_snapshot WHERE date = ?",
        (date,),
    )
    metrics = {r["metric"]: r["value"] for r in cur.fetchall() if r["value"] is not None}
    return {"date": date, "metrics": metrics}


# ── /v1/rates/policy ────────────────────────────────────────────────


@rates_policy_router.get("/history", response_model=list[PolicyRateRow])
def get_policy_history(
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """SBP policy-rate history, newest first."""
    cur = con.execute(
        """SELECT rate_date, policy_rate, ceiling_rate, floor_rate,
                  overnight_repo_rate, source, ingested_at
           FROM sbp_policy_rates
           ORDER BY rate_date DESC LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/rates/npc extras ────────────────────────────────────────────


@rates_npc_extras_router.get("/spread", response_model=list[GenericRow])
def get_npc_vs_rfr_spread(
    currency: Annotated[Optional[str], Query(description="Filter to one currency")] = None,
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """NPC-vs-RFR spread (from ``v_npc_vs_rfr_spread`` view).

    The view may be absent (Phase 0.5 known coverage gap) — endpoint
    returns ``[]`` rather than 500.
    """
    try:
        df = npc_repo.get_npc_vs_rfr_spread(con, currency=currency, start_date=from_)
    except Exception:
        return []
    return df_to_records(df)


@rates_npc_extras_router.get("/carry", response_model=list[GenericRow])
def get_npc_carry(
    currency: Annotated[str, Query(description="Currency to analyze")] = "USD",
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """NPC carry-trade analysis (from ``v_npc_carry_trade`` view)."""
    try:
        df = npc_repo.get_carry_trade_analysis(con, currency=currency, start_date=from_)
    except Exception:
        return []
    return df_to_records(df)


@rates_npc_extras_router.get("/multicurrency", response_model=list[GenericRow])
def get_npc_multicurrency(
    date: Annotated[Optional[str], Query(description="YYYY-MM-DD; defaults to latest", pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Multi-currency dashboard view (from ``v_multicurrency_dashboard``)."""
    try:
        df = npc_repo.get_multicurrency_dashboard(con, date=date)
    except Exception:
        return []
    return df_to_records(df)


@rates_npc_extras_router.get("/yield-curve", response_model=GenericRow)
def get_npc_yield_curve(
    currency: Annotated[str, Query()] = "USD",
    date: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """NPC yield curve (all tenors) for a currency on a date.

    The underlying view ``v_npc_yield_curve`` may be absent (Phase 0.5
    coverage gap) — endpoint returns ``{}`` rather than 500.
    """
    try:
        payload = npc_repo.get_npc_yield_curve(con, currency=currency, date=date)
    except Exception:
        return {}
    return payload or {}


# ── /v1/rates/global extras ─────────────────────────────────────────


@rates_global_extras_router.get("/spread/sofr-kibor", response_model=list[GenericRow])
def get_sofr_kibor_spread(
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """SOFR vs KIBOR 3M daily spread (from ``v_sofr_kibor_spread`` view)."""
    try:
        df = gr_repo.get_sofr_kibor_spread(con, start_date=from_)
    except Exception:
        return []
    return df_to_records(df)


@rates_global_extras_router.get("/latest", response_model=list[GenericRow])
def get_global_rates_latest(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest row for every (rate_name, tenor) combination in
    ``global_reference_rates``."""
    try:
        df = gr_repo.get_all_latest_rates(con)
    except Exception:
        return []
    return df_to_records(df)


@rates_global_extras_router.get("/history", response_model=list[GenericRow])
def get_global_rate_history(
    rate_name: Annotated[str, Query(description="SOFR / EFFR / SONIA / EUSTR / TONA")],
    tenor: Annotated[str, Query(description="Typically 'ON' for overnight rates")] = "ON",
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE)] = None,
    limit: Annotated[int, Query(ge=0, le=5000, description="0 = no limit")] = 0,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """History for a single (rate_name, tenor) — date-ascending."""
    try:
        df = gr_repo.get_rate_history(
            con, rate_name=rate_name, tenor=tenor,
            start_date=from_, limit=limit,
        )
    except Exception:
        return []
    return df_to_records(df)


@rates_global_extras_router.get("/comparison", response_model=GenericRow)
def get_rate_comparison(
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Latest single-row snapshot: SOFR/EFFR/KIBOR/KONIA/policy_rate.

    Repo returns a flat dict (not a row set) — schema returns one
    :class:`GenericRow` so JSON shape is ``{SOFR: …, EFFR: …, …}``.
    """
    try:
        payload = gr_repo.get_rate_comparison(con)
    except Exception:
        return {}
    return payload or {}


# ── /v1/alm ─────────────────────────────────────────────────────────


def _alm_safe(call) -> list[dict]:
    """Defensive wrapper for ALM repo calls.

    The ALM tables include some pre-existing data corruption (e.g.
    ``alm_liquidity_ladder`` rows are column-shifted with non-numeric
    values landing in numeric columns). Pydantic validation would
    raise on those; we'd rather surface an empty result and let the
    page render "—" tiles than 500 the whole endpoint.
    """
    try:
        return df_to_records(call())
    except Exception:
        return []


@alm_router.get("/products", response_model=list[AlmProductRow])
def get_alm_products(
    active_only: Annotated[bool, Query()] = True,
    asset_liability: Annotated[Optional[str], Query(description="A or L filter")] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    return _alm_safe(lambda: alm_repo.get_alm_products(
        con, active_only=active_only, asset_liability=asset_liability))


@alm_router.get("/positions", response_model=list[AlmPositionRow])
def get_alm_positions(
    as_of: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    return _alm_safe(lambda: alm_repo.get_alm_positions(con, as_of_date=as_of))


@alm_router.get("/repricing-gap", response_model=list[AlmRepricingGapRow])
def get_repricing_gap(
    as_of: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    return _alm_safe(lambda: alm_repo.get_repricing_gap(con, as_of_date=as_of))


@alm_router.get("/ftp-rates", response_model=list[AlmFtpRow])
def get_ftp_rates(
    as_of: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    return _alm_safe(lambda: alm_repo.get_ftp_rates(con, as_of_date=as_of))


@alm_router.get("/sensitivity", response_model=list[AlmSensitivityRow])
def get_sensitivity(
    as_of: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    return _alm_safe(lambda: alm_repo.get_sensitivity(con, as_of_date=as_of))


@alm_router.get("/liquidity-ladder", response_model=list[GenericRow])
def get_liquidity_ladder(
    as_of: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Liquidity-ladder endpoint uses :class:`GenericRow` because the
    backing ``alm_liquidity_ladder`` table is currently column-shifted
    (Group B finding) — rows do not validate against the strict
    schema. Page consumers should treat non-numeric values defensively.
    """
    return _alm_safe(lambda: alm_repo.get_liquidity_ladder(con, as_of_date=as_of))


# ── /v1/fi ──────────────────────────────────────────────────────────


@fi_router.get("/instruments", response_model=list[FiInstrumentRow])
def get_fi_instruments(
    active_only: Annotated[bool, Query()] = True,
    category: Annotated[Optional[str], Query(description="Filter by category (e.g. PIB, SUKUK, CORP)")] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """``fi_instruments`` — bond / sukuk universe."""
    where = ["1=1"]
    params: list = []
    if active_only:
        where.append("is_active = 1")
    if category:
        where.append("category = ?")
        params.append(category.upper())
    cur = con.execute(
        f"""SELECT instrument_id, isin, name, category, maturity_date,
                   coupon_rate, coupon_frequency, face_value,
                   shariah_compliant, issue_date, issuer, currency,
                   day_count, is_active, source, created_at, updated_at,
                   denomination_currency, reference_rate, spread_bps
            FROM fi_instruments WHERE {' AND '.join(where)}
            ORDER BY maturity_date LIMIT ?""",
        params + [limit],
    )
    return [dict(r) for r in cur.fetchall()]


@fi_router.get("/fcy-instruments", response_model=list[GenericRow])
def get_fcy_instruments(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """FCY-denominated instruments combined across ``fi_instruments``,
    ``bonds_master``, and ``sukuk_master``.

    Each source table contributes a ``source_table`` discriminator
    column. Any table that doesn't exist or has no FCY rows is
    silently skipped (graceful degradation when, e.g., bonds_master
    is unbuilt).
    """
    dfs = []
    queries = [
        ("fi_instruments", """SELECT name, category, maturity_date, coupon_rate,
                                     denomination_currency, reference_rate, spread_bps
                              FROM fi_instruments
                              WHERE denomination_currency IS NOT NULL
                                AND denomination_currency != 'PKR'"""),
        ("bonds_master", """SELECT symbol, issuer AS name, bond_type AS category,
                                   maturity_date, coupon_rate,
                                   denomination_currency, reference_rate, spread_bps
                            FROM bonds_master
                            WHERE denomination_currency IS NOT NULL
                              AND denomination_currency != 'PKR'"""),
        ("sukuk_master", """SELECT name, category, maturity_date, coupon_rate,
                                   denomination_currency, reference_rate, spread_bps
                            FROM sukuk_master
                            WHERE denomination_currency IS NOT NULL
                              AND denomination_currency != 'PKR'"""),
    ]
    for source_table, sql in queries:
        try:
            df = pd.read_sql_query(sql, con)
            if not df.empty:
                df["source_table"] = source_table
                dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return []
    return df_to_records(pd.concat(dfs, ignore_index=True))


@fi_router.get("/quotes/latest", response_model=list[FiQuoteRow])
def get_fi_quotes_latest(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest ``fi_quotes`` row per instrument."""
    cur = con.execute(
        """WITH ranked AS (
              SELECT *, ROW_NUMBER() OVER (
                          PARTITION BY instrument_id
                          ORDER BY quote_date DESC) AS rn
              FROM fi_quotes
           )
           SELECT instrument_id, quote_date, clean_price, ytm, bid, ask,
                  volume, source, ingested_at
           FROM ranked WHERE rn = 1"""
    )
    return [dict(r) for r in cur.fetchall()]


@fi_router.get(
    "/quotes/{instrument_id}/history",
    response_model=list[FiQuoteRow],
)
def get_fi_quotes_history(
    instrument_id: Annotated[str, Path()],
    days: Annotated[int, Query(ge=1, le=3650)] = 60,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Price/volume history for one FI instrument, oldest first."""
    cur = con.execute(
        """SELECT instrument_id, quote_date, clean_price, ytm, bid, ask,
                  volume, source, ingested_at
           FROM fi_quotes
           WHERE instrument_id = ?
             AND quote_date >= date('now', ?)
           ORDER BY quote_date""",
        (instrument_id, f"-{days} days"),
    )
    return [dict(r) for r in cur.fetchall()]
