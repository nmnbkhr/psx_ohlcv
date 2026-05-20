"""Market-overview composite response models.

These map 1:1 to Dashboard widgets — each model bundles everything one
UI tile needs so the client gets a widget's data in a single round-trip.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class KSE100Hero(BaseModel):
    """Dashboard's hero widget — KSE-100 quote + breadth + 52w range."""

    as_of: str
    value: float
    change: Optional[float] = None
    change_pct: Optional[float] = None
    ytd_change_pct: Optional[float] = None
    one_year_change_pct: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    advancers: Optional[int] = None
    decliners: Optional[int] = None
    unchanged: Optional[int] = None


class Mover(BaseModel):
    """One row in the gainers / losers / volume-leaders lists."""

    symbol: str
    company_name: Optional[str] = None
    sector: Optional[str] = None
    close: Optional[float] = None
    change: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    turnover: Optional[float] = None


class FiftyTwoWeekRow(BaseModel):
    """One symbol near its 52-week high or low."""

    symbol: str
    company_name: Optional[str] = None
    close: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    distance_pct: Optional[float] = None


class FiftyTwoWeekExtremes(BaseModel):
    """Bundle near-high + near-low (one widget, one round-trip)."""

    as_of: Optional[str] = None
    near_high: list[FiftyTwoWeekRow]
    near_low: list[FiftyTwoWeekRow]


class SectorRow(BaseModel):
    """One row in the sector leaderboard."""

    sector_code: Optional[str] = None
    sector_name: Optional[str] = None
    symbols: Optional[int] = None
    avg_change_pct: Optional[float] = None
    total_volume: Optional[int] = None
    total_turnover: Optional[float] = None
    advancers: Optional[int] = None
    decliners: Optional[int] = None


class FXRow(BaseModel):
    currency: str
    rate: Optional[float] = None
    as_of: Optional[str] = None


class RatesStrip(BaseModel):
    """Dashboard top-bar strip — key rates + FX, single round-trip."""

    sbp_policy: Optional[float] = None
    sbp_policy_date: Optional[str] = None
    kibor_3m: Optional[float] = None
    kibor_3m_date: Optional[str] = None
    tbill_3m: Optional[float] = None
    tbill_3m_date: Optional[str] = None
    pkrv_10y: Optional[float] = None
    pkrv_10y_date: Optional[str] = None
    fx: list[FXRow]
