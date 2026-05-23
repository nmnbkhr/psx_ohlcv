"""Funds + ETFs endpoints — /v1/funds, /v1/etfs.

Backs Group G.2: ``fund_explorer.py`` (~2000 LOC, ~40 reads) and
``funds.py`` (621 LOC, 11 reads). All read-only.

Note on data coverage: ``mutual_fund_nav`` is sparse — the MUFAP
sync historically only completed ~140 of 1,270 funds. Pages
rendering "no NAV data" for most funds is correct behavior, not
a migration bug. The /v1 surface is structurally complete regardless
of underlying NAV coverage.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.funds import (
    AmcRow,
    EtfNavRow,
    EtfRow,
    FundCalendarReturnRow,
    FundCategorySummaryRow,
    FundNavLatestRow,
    FundNavRow,
    FundPerformanceRow,
    FundRiskRow,
    FundRow,
)

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"

funds_router = APIRouter(prefix="/v1/funds", tags=["funds"])
etfs_router = APIRouter(prefix="/v1/etfs", tags=["etfs"])


# ── /v1/funds (collection routes — registered before {fund_id}) ──


@funds_router.get(
    "/category-summary", response_model=list[FundCategorySummaryRow]
)
def list_fund_category_summary(
    con: sqlite3.Connection = Depends(get_read_db),
    min_daily_count: Annotated[int, Query(ge=10, le=10_000)] = 100,
) -> list[dict]:
    """Aggregate daily NAV change + AUM per category.

    Phase-1.2-shaped, single-domain. NOT a composite endpoint —
    reads only from `mutual_fund_nav` + `mutual_funds`. Came out of
    the 2.A.4 audit because market_research.py's
    `_load_fund_category_summary` had no existing equivalent. Added
    here so the page's eventual migration to /v1 has a clean target.

    The CTE identifies the two most recent dates with at least
    `min_daily_count` NAVs each, computes per-fund daily change
    between them, then averages by category and sums AUM.
    """
    cur = con.execute(
        """
        WITH date_counts AS (
            SELECT date, COUNT(*) AS cnt
              FROM mutual_fund_nav
             WHERE nav > 0
             GROUP BY date
             ORDER BY date DESC
             LIMIT 10
        ),
        latest_dates AS (
            SELECT date FROM date_counts
             WHERE cnt >= ?
             ORDER BY date DESC
             LIMIT 2
        ),
        ranked AS (
            SELECT MAX(date) AS d1, MIN(date) AS d0 FROM latest_dates
        ),
        changes AS (
            SELECT n1.fund_id,
                   ROUND((n1.nav - n0.nav) / n0.nav * 100, 2) AS daily_chg,
                   n1.aum
              FROM mutual_fund_nav n1
              JOIN ranked r ON n1.date = r.d1
              JOIN mutual_fund_nav n0
                ON n0.fund_id = n1.fund_id AND n0.date = r.d0
             WHERE n1.nav > 0 AND n0.nav > 0
        )
        SELECT mf.category,
               COUNT(*) AS funds,
               ROUND(AVG(c.daily_chg), 2) AS avg_daily_chg,
               ROUND(SUM(c.aum) / 1e6, 0) AS total_aum_m
          FROM changes c
          JOIN mutual_funds mf ON c.fund_id = mf.fund_id
         WHERE mf.is_active = 1
           AND mf.category IS NOT NULL
         GROUP BY mf.category
         ORDER BY avg_daily_chg DESC
        """,
        (min_daily_count,),
    )
    return [dict(r) for r in cur.fetchall()]


@funds_router.get("/categories", response_model=list[str])
def list_fund_categories(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Distinct category values from mutual_funds (Equity, Money Market, ...)."""
    cur = con.execute(
        "SELECT DISTINCT category FROM mutual_funds "
        "WHERE category IS NOT NULL ORDER BY category"
    )
    return [r["category"] for r in cur.fetchall()]


@funds_router.get("/amcs", response_model=list[AmcRow])
def list_amcs(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Distinct AMC code + name with fund count."""
    cur = con.execute(
        """SELECT amc_code, amc_name, COUNT(*) AS fund_count
             FROM mutual_funds
            WHERE amc_code IS NOT NULL
            GROUP BY amc_code, amc_name
            ORDER BY amc_name, amc_code"""
    )
    return [dict(r) for r in cur.fetchall()]


@funds_router.get("/nav-latest", response_model=list[FundNavLatestRow])
def list_funds_nav_latest(
    limit: Annotated[int, Query(ge=1, le=2000)] = 2000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest NAV per fund (cross-fund table).

    Uses a window function over ``mutual_fund_nav`` to pick the most
    recent row per fund_id; left-joins ``mutual_funds`` for the
    display name.
    """
    cur = con.execute(
        """WITH ranked AS (
              SELECT fund_id, date, nav, nav_change_pct,
                     ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY date DESC) AS rn
                FROM mutual_fund_nav
           )
           SELECT r.fund_id, mf.fund_name, r.date, r.nav, r.nav_change_pct
             FROM ranked r
             LEFT JOIN mutual_funds mf ON mf.fund_id = r.fund_id
            WHERE r.rn = 1
            ORDER BY r.fund_id
            LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


@funds_router.get(
    "/performance/leaders", response_model=list[FundPerformanceRow]
)
def list_performance_leaders(
    metric: Annotated[
        str,
        Query(
            description="Performance column to rank by",
            pattern=r"^return_(ytd|mtd|1d|15d|30d|90d|180d|270d|365d|2y|3y)$",
        ),
    ] = "return_365d",
    category: Annotated[Optional[str], Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    direction: Annotated[
        str, Query(description="Top performers vs worst", pattern=r"^(top|bottom)$")
    ] = "top",
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Top (or bottom) N funds by a given pre-computed return metric.

    ``metric`` is allowlisted via the regex pattern to prevent
    SQL-identifier injection. ``category`` filter is parameterized.
    """
    sql = (
        f"SELECT * FROM fund_performance_latest "
        f"WHERE {metric} IS NOT NULL"
    )
    params: list = []
    if category:
        sql += " AND category = ?"
        params.append(category)
    sql += f" ORDER BY {metric} {'DESC' if direction == 'top' else 'ASC'} LIMIT ?"
    params.append(limit)
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


@funds_router.get("", response_model=list[FundRow])
def list_funds(
    category: Annotated[Optional[str], Query()] = None,
    amc_code: Annotated[Optional[str], Query()] = None,
    fund_type: Annotated[
        Optional[str], Query(description="OPEN_END | VPS | ETF")
    ] = None,
    is_shariah: Annotated[Optional[int], Query(ge=0, le=1)] = None,
    active_only: Annotated[bool, Query()] = True,
    limit: Annotated[int, Query(ge=1, le=5000)] = 2000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """List funds from mutual_funds with optional filters."""
    where = []
    params: list = []
    if active_only:
        where.append("is_active = 1")
    if category:
        where.append("category = ?")
        params.append(category)
    if amc_code:
        where.append("amc_code = ?")
        params.append(amc_code)
    if fund_type:
        where.append("fund_type = ?")
        params.append(fund_type)
    if is_shariah is not None:
        where.append("is_shariah = ?")
        params.append(is_shariah)
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    params.append(limit)
    cur = con.execute(
        f"""SELECT fund_id, symbol, fund_name, amc_code, amc_name, fund_type,
                   category, is_shariah, launch_date, expense_ratio,
                   management_fee, is_active, risk_profile, benchmark,
                   rating, trustee, fund_manager, aum, sector, psx_ticker
              FROM mutual_funds {where_sql}
             ORDER BY fund_name LIMIT ?""",
        params,
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/funds/{fund_id} (per-fund detail routes) ──


@funds_router.get("/{fund_id}", response_model=FundRow)
def get_fund(
    fund_id: Annotated[str, Path(description="Fund ID e.g. MUFAP:ABL-ISF")],
    con: sqlite3.Connection = Depends(get_read_db),
) -> FundRow:
    """Single fund metadata."""
    row = con.execute(
        """SELECT fund_id, symbol, fund_name, amc_code, amc_name, fund_type,
                  category, is_shariah, launch_date, expense_ratio,
                  management_fee, is_active, risk_profile, benchmark,
                  rating, trustee, fund_manager, aum, sector, psx_ticker
             FROM mutual_funds WHERE fund_id = ?""",
        (fund_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown fund_id {fund_id!r}")
    return FundRow(**dict(row))


@funds_router.get("/{fund_id}/nav", response_model=list[FundNavRow])
def get_fund_nav(
    fund_id: Annotated[str, Path()],
    from_: Annotated[
        Optional[str], Query(alias="from", pattern=DATE_RE)
    ] = None,
    to: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 2000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """NAV history for one fund."""
    sql = (
        "SELECT fund_id, date, nav, offer_price, redemption_price, aum, "
        "nav_change_pct FROM mutual_fund_nav WHERE fund_id = ?"
    )
    params: list = [fund_id]
    if from_:
        sql += " AND date >= ?"
        params.append(from_)
    if to:
        sql += " AND date <= ?"
        params.append(to)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


@funds_router.get(
    "/{fund_id}/performance", response_model=Optional[FundPerformanceRow]
)
def get_fund_performance(
    fund_id: Annotated[str, Path()],
    con: sqlite3.Connection = Depends(get_read_db),
) -> Optional[dict]:
    """Pre-computed performance metrics for one fund (1 row or None)."""
    row = con.execute(
        "SELECT * FROM fund_performance_latest WHERE fund_id = ?",
        (fund_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


@funds_router.get(
    "/{fund_id}/risk", response_model=Optional[FundRiskRow]
)
def get_fund_risk(
    fund_id: Annotated[str, Path()],
    con: sqlite3.Connection = Depends(get_read_db),
) -> Optional[dict]:
    """Risk metrics for one fund (1 row or None)."""
    row = con.execute(
        "SELECT * FROM fund_risk_metrics WHERE fund_id = ?",
        (fund_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


@funds_router.get(
    "/{fund_id}/calendar-returns", response_model=list[FundCalendarReturnRow]
)
def get_fund_calendar_returns(
    fund_id: Annotated[str, Path()],
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Yearly returns for one fund."""
    cur = con.execute(
        """SELECT fund_id, year, return_pct, first_nav, last_nav, trading_days
             FROM fund_calendar_returns
            WHERE fund_id = ?
            ORDER BY year DESC""",
        (fund_id,),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/etfs ───────────────────────────────────────────────────────


@etfs_router.get("", response_model=list[EtfRow])
def list_etfs(con: sqlite3.Connection = Depends(get_read_db)) -> list[dict]:
    """List all ETFs from etf_master."""
    cur = con.execute(
        """SELECT symbol, name, amc, benchmark_index, inception_date,
                  expense_ratio, management_fee, shariah_compliant,
                  trustee, fiscal_year_end
             FROM etf_master ORDER BY symbol"""
    )
    return [dict(r) for r in cur.fetchall()]


@etfs_router.get("/{symbol}", response_model=EtfRow)
def get_etf(
    symbol: Annotated[str, Path()],
    con: sqlite3.Connection = Depends(get_read_db),
) -> EtfRow:
    """Single ETF metadata."""
    row = con.execute(
        """SELECT symbol, name, amc, benchmark_index, inception_date,
                  expense_ratio, management_fee, shariah_compliant,
                  trustee, fiscal_year_end
             FROM etf_master WHERE symbol = ?""",
        (symbol.upper(),),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown ETF {symbol!r}")
    return EtfRow(**dict(row))


@etfs_router.get("/{symbol}/nav", response_model=list[EtfNavRow])
def get_etf_nav(
    symbol: Annotated[str, Path()],
    from_: Annotated[
        Optional[str], Query(alias="from", pattern=DATE_RE)
    ] = None,
    to: Annotated[Optional[str], Query(pattern=DATE_RE)] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 2000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """NAV history for one ETF."""
    sql = (
        "SELECT symbol, date, nav, market_price, premium_discount, "
        "aum_millions, outstanding_units FROM etf_nav WHERE symbol = ?"
    )
    params: list = [symbol.upper()]
    if from_:
        sql += " AND date >= ?"
        params.append(from_)
    if to:
        sql += " AND date <= ?"
        params.append(to)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]
