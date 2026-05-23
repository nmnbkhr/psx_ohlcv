"""Funds endpoint response models.

Backs the Group G.2 pages: ``fund_explorer.py`` (the dragon) and
``funds.py``. Covers mutual funds (open-end + VPS + ETF via
``mutual_funds.fund_type``) and the separate ``etf_master`` /
``etf_nav`` tables for PSX-listed ETFs specifically.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class FundCategorySummaryRow(BaseModel):
    """Aggregate daily change + AUM per category, computed from the
    two most recent mutual_fund_nav dates with ≥ 100 NAV rows each.
    Backs market_research.py's _load_fund_category_summary section.
    Phase-1.2-shaped (single-domain) endpoint added during 2.A.4.3b.
    """

    category: str
    funds: int
    avg_daily_chg: Optional[float] = None
    total_aum_m: Optional[float] = None


class FundRow(BaseModel):
    """One row from ``mutual_funds`` (fund master)."""

    fund_id: str
    symbol: str
    fund_name: str
    amc_code: str
    amc_name: Optional[str] = None
    fund_type: str
    category: str
    is_shariah: Optional[int] = None
    launch_date: Optional[str] = None
    expense_ratio: Optional[float] = None
    management_fee: Optional[float] = None
    is_active: Optional[int] = None
    risk_profile: Optional[str] = None
    benchmark: Optional[str] = None
    rating: Optional[str] = None
    trustee: Optional[str] = None
    fund_manager: Optional[str] = None
    aum: Optional[float] = None
    sector: Optional[str] = None
    psx_ticker: Optional[str] = None


class FundNavRow(BaseModel):
    """One row from ``mutual_fund_nav``."""

    fund_id: str
    date: str
    nav: float
    offer_price: Optional[float] = None
    redemption_price: Optional[float] = None
    aum: Optional[float] = None
    nav_change_pct: Optional[float] = None


class FundNavLatestRow(BaseModel):
    """Latest NAV for one fund (cross-fund table)."""

    fund_id: str
    fund_name: Optional[str] = None
    date: str
    nav: float
    nav_change_pct: Optional[float] = None


class FundPerformanceRow(BaseModel):
    """One row from ``fund_performance_latest`` — pre-computed returns."""

    model_config = ConfigDict(extra="ignore")

    fund_id: Optional[str] = None
    fund_name: str
    sector: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[str] = None
    validity_date: Optional[str] = None
    nav: Optional[float] = None
    return_ytd: Optional[float] = None
    return_mtd: Optional[float] = None
    return_1d: Optional[float] = None
    return_15d: Optional[float] = None
    return_30d: Optional[float] = None
    return_90d: Optional[float] = None
    return_180d: Optional[float] = None
    return_270d: Optional[float] = None
    return_365d: Optional[float] = None
    return_2y: Optional[float] = None
    return_3y: Optional[float] = None


class FundRiskRow(BaseModel):
    """One row from ``fund_risk_metrics``."""

    model_config = ConfigDict(extra="ignore")

    fund_id: str
    fund_name: Optional[str] = None
    category: Optional[str] = None
    return_1m: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_1y: Optional[float] = None
    return_2y: Optional[float] = None
    return_3y: Optional[float] = None
    return_5y: Optional[float] = None
    return_ytd: Optional[float] = None
    return_since_inception: Optional[float] = None
    volatility_1y: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    treynor_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    max_drawdown_start: Optional[str] = None
    max_drawdown_end: Optional[str] = None
    beta: Optional[float] = None
    alpha: Optional[float] = None
    r_squared: Optional[float] = None
    var_95: Optional[float] = None
    cvar_95: Optional[float] = None
    information_ratio: Optional[float] = None
    tracking_error: Optional[float] = None
    up_capture: Optional[float] = None
    down_capture: Optional[float] = None
    nav_count: Optional[int] = None
    first_nav_date: Optional[str] = None
    last_nav_date: Optional[str] = None
    computed_at: Optional[str] = None


class FundCalendarReturnRow(BaseModel):
    """One row from ``fund_calendar_returns`` — yearly return per fund."""

    fund_id: str
    year: int
    return_pct: Optional[float] = None
    first_nav: Optional[float] = None
    last_nav: Optional[float] = None
    trading_days: Optional[int] = None


class AmcRow(BaseModel):
    """Distinct AMC from mutual_funds."""

    amc_code: str
    amc_name: Optional[str] = None
    fund_count: Optional[int] = None


class EtfRow(BaseModel):
    """One row from ``etf_master``."""

    symbol: str
    name: str
    amc: Optional[str] = None
    benchmark_index: Optional[str] = None
    inception_date: Optional[str] = None
    expense_ratio: Optional[float] = None
    management_fee: Optional[str] = None
    shariah_compliant: Optional[int] = None
    trustee: Optional[str] = None
    fiscal_year_end: Optional[str] = None


class EtfNavRow(BaseModel):
    """One row from ``etf_nav``."""

    symbol: str
    date: str
    nav: Optional[float] = None
    market_price: Optional[float] = None
    premium_discount: Optional[float] = None
    aum_millions: Optional[float] = None
    outstanding_units: Optional[int] = None
