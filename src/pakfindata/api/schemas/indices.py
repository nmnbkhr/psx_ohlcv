"""Indices endpoint response models."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class IndexLatest(BaseModel):
    """Latest row from ``psx_indices`` for a single index code."""

    index_code: str
    index_date: str
    index_time: Optional[str] = None
    value: float
    change: Optional[float] = None
    change_pct: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[int] = None
    previous_close: Optional[float] = None
    ytd_change_pct: Optional[float] = None
    one_year_change_pct: Optional[float] = None
    week_52_low: Optional[float] = None
    week_52_high: Optional[float] = None
    trades: Optional[int] = None
    market_cap: Optional[float] = None
    turnover: Optional[float] = None


class IndexHistoryRow(BaseModel):
    """One day of history for an index."""

    index_date: str
    value: float
    change: Optional[float] = None
    change_pct: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[int] = None


class IndexConstituent(BaseModel):
    """Membership row for an index."""

    symbol: str
    weight: Optional[float] = None
    shares: Optional[int] = None
    effective_date: Optional[str] = None
