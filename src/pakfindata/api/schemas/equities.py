"""Pydantic schemas for /v1/symbols/*, /v1/sectors/*, /v1/companies/*,
and /v1/factors/* endpoints.

Schemas mirror the source SQLite tables column-for-column where
possible:

- ``company_financials`` — wide row covering both bank and non-bank
  income statement + balance sheet fields; :class:`CompanyFinancialsRow`.
- ``company_profile`` + ``company_key_people`` — joined into
  :class:`CompanyProfileExtras`.
- ``corporate_announcements`` — :class:`CompanyAnnouncement`.
- ``dividend_payouts`` — fallback feed used by Company Deep when the
  smart-client ``payouts_df`` is empty.
- ``company_snapshots`` JSON columns (``trading_data``, ``equity_data``,
  ``financials_data``, ``ratios_data``) — exploded into flat
  :class:`FactorRow` fields for the Factor Analysis page.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


# ── /v1/symbols/screener ────────────────────────────────────────────


class ScreenerRow(BaseModel):
    """One row of the Stock Screener result set.

    Sourced from ``symbols`` LEFT JOINed with ``regular_market_current``,
    ``company_fundamentals``, ``company_profile``, and the latest
    ``eod_ohlcv`` row. Any field can be NULL when the underlying source
    is missing — the UI renders ``—`` rather than a synthetic value.
    """

    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    price: Optional[float] = None
    pe_ratio: Optional[float] = None
    market_cap: Optional[float] = None
    free_float_pct: Optional[float] = None
    last_volume: Optional[float] = None
    turnover: Optional[float] = None
    change_pct: Optional[float] = None


# ── /v1/sectors/* ───────────────────────────────────────────────────


class SectorPerformanceRow(BaseModel):
    """One row of per-sector aggregates from a single trading day.

    Computed by GROUP BY over ``eod_ohlcv`` joined with ``sectors``.
    Sector names with fewer than 2 stocks for the date are filtered
    out at the source (sector_analysis page convention).
    """

    sector: str
    stocks: int
    avg_change: Optional[float] = None
    total_volume: Optional[int] = None
    gainers: int
    losers: int


class SectorSymbolMapRow(BaseModel):
    """One symbol→sector_name mapping for a given trading day.

    Backs the index-weight treemap; left-joined with the constituent
    XLS file on the page side.
    """

    symbol: str
    sector: Optional[str] = None


# ── /v1/companies/* ─────────────────────────────────────────────────


class CompanyFinancialsRow(BaseModel):
    """One row from ``company_financials`` — wide schema covering both
    non-financial (sales / gross_profit / …) and banking (markup_earned
    / net_interest_income / provisions / …) layouts.

    The Symbol Financials page renders different tabs based on
    ``is_bank`` (derived server-side from markup_earned presence) and
    on which columns are populated for the symbol.
    """

    symbol: str
    period_end: str
    period_type: str
    sales: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_profit: Optional[float] = None
    profit_before_tax: Optional[float] = None
    profit_after_tax: Optional[float] = None
    eps: Optional[float] = None
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    total_equity: Optional[float] = None
    currency: Optional[str] = None
    updated_at: Optional[str] = None
    markup_earned: Optional[float] = None
    markup_expensed: Optional[float] = None
    cost_of_sales: Optional[float] = None
    operating_expenses: Optional[float] = None
    finance_cost: Optional[float] = None
    other_income: Optional[float] = None
    taxation: Optional[float] = None
    net_interest_income: Optional[float] = None
    non_markup_income: Optional[float] = None
    total_income: Optional[float] = None
    provisions: Optional[float] = None
    current_assets: Optional[float] = None
    non_current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    non_current_liabilities: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    share_capital: Optional[float] = None
    source: Optional[str] = None
    currency_scale: Optional[str] = None
    parsed_at: Optional[str] = None


class CompanyFinancialsResponse(BaseModel):
    """Financials envelope including the derived ``is_bank`` flag.

    ``is_bank`` is True if any of the symbol's financial rows has a
    non-NULL ``markup_earned`` — the same probe the legacy page used.
    """

    symbol: str
    is_bank: bool
    rows: list[CompanyFinancialsRow]


class SectorValuation(BaseModel):
    """Symbol P/E vs sector peers from latest ``company_snapshots`` rows.

    Reads ``trading_data.REG.pe_ratio_ttm`` from the JSON column
    (NB: not ``snapshot_json.fundamentals.pe_ratio`` — that path was
    referenced in the legacy page but the column never existed, so the
    feature was silently dead. /v1 now reads the actual JSON path).

    ``pe_percentile`` is 0..100 representing what fraction of the
    sector is cheaper than this symbol (lower = symbol is cheap).
    None when sector has fewer than 3 peers with valid P/E.
    """

    symbol: str
    symbol_pe: Optional[float] = None
    sector_code: Optional[str] = None
    sector_name: Optional[str] = None
    sector_count: int = 0
    sector_avg_pe: Optional[float] = None
    sector_min_pe: Optional[float] = None
    sector_max_pe: Optional[float] = None
    pe_percentile: Optional[float] = None


class CompanyKeyPerson(BaseModel):
    """One row from ``company_key_people``."""

    name: str
    role: str


class CompanyProfile(BaseModel):
    """One row from ``company_profile`` — deep-scraped metadata."""

    symbol: str
    company_name: Optional[str] = None
    sector_name: Optional[str] = None
    business_description: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    registrar: Optional[str] = None
    auditor: Optional[str] = None
    fiscal_year_end: Optional[str] = None
    updated_at: Optional[str] = None
    source_url: Optional[str] = None
    listing_status: Optional[str] = None


class CompanyProfileExtras(BaseModel):
    """Profile + key-people bundle used by Company Deep.

    Either field may be empty; the page handles missing data
    individually rather than treating the bundle as all-or-nothing.
    """

    profile: Optional[CompanyProfile] = None
    key_people: list[CompanyKeyPerson] = []


class CompanyAnnouncement(BaseModel):
    """One row from ``corporate_announcements``."""

    announcement_date: str
    title: str
    announcement_type: Optional[str] = None
    category: Optional[str] = None
    document_url: Optional[str] = None
    document_type: Optional[str] = None
    summary: Optional[str] = None


class CompanyDividendPayout(BaseModel):
    """One row from ``dividend_payouts`` — global-scraper fallback feed.

    Used by Company Deep's Payouts tab when the smart-client
    ``payouts_df`` (per-symbol cash/bonus history) is empty.
    """

    announcement_date: str
    dividend_percent: Optional[float] = None
    dividend_type: Optional[str] = None
    dividend_number: Optional[str] = None
    fiscal_period: Optional[str] = None
    book_closure_from: Optional[str] = None
    book_closure_to: Optional[str] = None


# ── /v1/factors/* ───────────────────────────────────────────────────


class FactorRow(BaseModel):
    """One symbol's flattened factor-data row.

    Reads from the latest ``company_snapshots`` per symbol + the
    latest 60 trading days of ``eod_ohlcv``. Field names mirror the
    legacy ``_load_factor_data`` SELECT aliases so the page-side
    scoring math doesn't need to change.
    """

    symbol: str
    snapshot_date: Optional[str] = None
    company_name: Optional[str] = None
    sector_code: Optional[str] = None
    price: Optional[float] = None
    ldcp: Optional[float] = None
    volume: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    wk52_low: Optional[float] = None
    wk52_high: Optional[float] = None
    pe_ratio: Optional[float] = None
    ytd_change: Optional[float] = None
    year_1_change: Optional[float] = None
    market_cap: Optional[float] = None
    outstanding_shares: Optional[float] = None
    free_float_pct: Optional[float] = None
    eps: Optional[float] = None
    net_margin: Optional[float] = None
    eps_growth: Optional[float] = None
    latest_close: Optional[float] = None
    close_20d_ago: Optional[float] = None
    close_60d_ago: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    return_20d: Optional[float] = None
    return_60d: Optional[float] = None


class FactorRawData(BaseModel):
    """Factor-data envelope with the snapshot-coverage count.

    ``snapshot_count`` (distinct symbols in ``company_snapshots``) is
    used by the page to warn if coverage is too thin for meaningful
    rankings (<10 companies).
    """

    rows: list[FactorRow]
    snapshot_count: int


class FactorRiskRow(BaseModel):
    """One symbol's 90-day price-range stats from ``eod_ohlcv``.

    ``range_pct`` = (max_close - min_close) / avg_close * 100 — the
    legacy page's volatility proxy. Days = number of trading days in
    the window with a row for this symbol.
    """

    symbol: str
    trading_days: int
    avg_price: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    range_pct: Optional[float] = None
