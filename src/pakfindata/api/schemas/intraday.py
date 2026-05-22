"""Intraday + Turnover endpoint response models.

Backs the Group E pages (intraday.py Dashboard/Charts/Market Pulse,
post_close.py). All read-only.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class IntradaySummaryRow(BaseModel):
    """One row from ``intraday_daily_summary`` — per-symbol day OHLCV."""

    date: str
    symbol: str
    market: str
    day_open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    day_close: Optional[float] = None
    prev_close: Optional[float] = None
    day_volume: Optional[int] = None
    day_trades: Optional[int] = None
    turnover: Optional[float] = None
    vwap: Optional[float] = None
    tick_count: Optional[int] = None
    first_ts: Optional[str] = None
    last_ts: Optional[str] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None


class IntradayBarRow(BaseModel):
    """One row from ``intraday_bars`` — single-symbol tick bar."""

    symbol: str
    market: str
    date: str
    ts: str
    ts_epoch: int
    interval: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    value: Optional[float] = None
    trade_count: Optional[int] = None
    vwap: Optional[float] = None


class IntradayMinuteBreadthRow(BaseModel):
    """One row from ``intraday_minute_breadth``."""

    date: str
    minute: str
    market: str
    advancing: Optional[int] = None
    declining: Optional[int] = None
    unchanged: Optional[int] = None
    total_symbols: Optional[int] = None
    net_ticks: Optional[int] = None


class IntradayHourlyBreadthRow(BaseModel):
    """One row from ``intraday_hourly_summary``."""

    date: str
    hour: int
    market: str
    tick_count: Optional[int] = None
    symbol_count: Optional[int] = None


class IntradayIndexMinuteRow(BaseModel):
    """One row from ``intraday_index_minute`` — per-minute index value."""

    date: str
    minute: str
    symbol: str
    value: Optional[float] = None


class PostCloseRow(BaseModel):
    """One row from ``post_close_turnover``."""

    symbol: str
    date: str
    company_name: Optional[str] = None
    volume: Optional[int] = None
    turnover: Optional[float] = None
    ingested_at: Optional[str] = None


class PostCloseStats(BaseModel):
    """Aggregate stats from ``post_close_turnover``."""

    total_rows: int = 0
    total_dates: int = 0
    unique_symbols: int = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None
