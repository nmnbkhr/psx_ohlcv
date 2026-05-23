"""Commodities + khistocks + PMEX-portal response models.

Backs Group G.3 page ``commodities.py`` (~1,160 LOC, ~21 read sites).

Three table families served from the main psx.sqlite:
- Global commodity prices (``commodity_eod``, ``commodity_fx_rates``,
  ``commodity_pkr``, ``commodity_monthly``, ``commodity_symbols``,
  ``commodity_sync_runs``) — yfinance / FRED / World Bank / computed PKR.
- Pakistan local markets (``khistocks_prices``) — PMEX + Sarafa + Mandi
  + LME feeds scraped from khistocks.com.
- PMEX portal market watch (``pmex_market_watch``) — direct snapshots
  from the PMEX portal.

The separate ``commod.db`` (pmex_ohlc, pmex_margins, pmex_intraday_snapshots)
backing ``pmex.py`` + ``pmex_analytics_page.py`` is NOT exposed here —
those pages are scraper-maintenance owners of their own DB and are
intentionally skipped in Phase 1.7.G.3.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


# ── /v1/commodities (global) ───────────────────────────────────────────


class CommodityEodRow(BaseModel):
    """One row from ``commodity_eod`` (or ``commodity_fx_rates`` fallback)."""

    symbol: str
    date: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None


class CommodityLatestRow(BaseModel):
    """Latest + previous close for one commodity (powers KPI cards)."""

    symbol: str
    date: str
    close: Optional[float] = None
    open: Optional[float] = None
    prev_close: Optional[float] = None
    source: str  # "commodity_eod" | "commodity_fx_rates"


class CommoditySectorPerfRow(BaseModel):
    category: str
    avg_chg: Optional[float] = None


class CommodityPkrRow(BaseModel):
    """One row from ``commodity_pkr``."""

    symbol: str
    date: str
    pkr_price: Optional[float] = None
    pk_unit: Optional[str] = None
    usd_price: Optional[float] = None
    usd_pkr: Optional[float] = None
    source: Optional[str] = None


class CommodityCategoryLatestRow(BaseModel):
    """Latest row per symbol JOIN ``commodity_symbols`` for category browse."""

    symbol: str
    date: str
    close: Optional[float] = None
    open: Optional[float] = None
    volume: Optional[int] = None
    name: Optional[str] = None
    category: Optional[str] = None
    unit: Optional[str] = None
    pk_relevance: Optional[str] = None


class CommodityHasData(BaseModel):
    """Composite gate for the empty-state render in ``commodities.py``."""

    commodity_eod: int
    khistocks_prices: int
    pmex_market_watch: int
    has_any: bool


class CommoditySyncRunRow(BaseModel):
    """One row from ``commodity_sync_runs`` (page renders raw schema)."""

    model_config = ConfigDict(extra="allow")

    started_at: Optional[str] = None
    source: Optional[str] = None
    symbols_total: Optional[int] = None
    symbols_ok: Optional[int] = None
    rows_upserted: Optional[int] = None
    status: Optional[str] = None


# ── /v1/khistocks ──────────────────────────────────────────────────────


class KhistocksRow(BaseModel):
    """One row from ``khistocks_prices``.

    Schema is wide because the table merges 5 different feeds with
    feed-specific columns (LME uses cash_buyer/seller; Mandi uses
    rate+quotation; etc.). All extra fields are tolerated.
    """

    model_config = ConfigDict(extra="allow")

    symbol: str
    date: str
    feed: str
    name: Optional[str] = None
    quotation: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    rate: Optional[float] = None
    cash_buyer: Optional[float] = None
    cash_seller: Optional[float] = None
    three_month_buyer: Optional[float] = None
    three_month_seller: Optional[float] = None
    net_change: Optional[float] = None
    change_pct: Optional[str] = None


# ── /v1/pmex-portal ────────────────────────────────────────────────────


class PmexMarketWatchRow(BaseModel):
    """One row from ``pmex_market_watch``."""

    contract: str
    snapshot_date: str
    category: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    open: Optional[float] = None
    close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    last_price: Optional[float] = None
    last_vol: Optional[int] = None
    total_vol: Optional[int] = None
    total_volume: Optional[int] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None
    bid_diff: Optional[float] = None
    ask_diff: Optional[float] = None
    state: Optional[str] = None
    snapshot_ts: Optional[str] = None
    source: Optional[str] = None
