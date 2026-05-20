"""Market-overview composite response models.

Each model maps 1:1 to a Dashboard widget — fields mirror the columns
returned by ``db/repositories/market_summary`` and ``rates_strip`` so
``model_validate(row_dict)`` round-trips without renames.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class KSE100Hero(BaseModel):
    """Dashboard's hero widget — KSE-100 quote + breadth + 52w range.

    Fields drawn from ``psx_indices`` (via ``get_latest_kse100``) plus
    breadth from ``eod_market_summary`` (via ``get_eod_breadth``).
    """

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
    """Row shape returned by get_top_movers / get_volume_leaders."""

    symbol: str
    close: Optional[float] = None
    prev_close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None


class FiftyTwoWeekRow(BaseModel):
    """Row shape from get_52w_extremes: ``symbol`` + position percent.

    ``pos_pct`` is 100 at the 52-week high, 0 at the 52-week low.
    """

    symbol: str
    pos_pct: Optional[float] = None


class FiftyTwoWeekExtremes(BaseModel):
    """Bundle near-high + near-low — one widget, one round-trip."""

    near_high: list[FiftyTwoWeekRow]
    near_low: list[FiftyTwoWeekRow]


class SectorRow(BaseModel):
    """Row shape from get_sector_performance."""

    sector: Optional[str] = None
    stocks: Optional[int] = None
    avg_chg: Optional[float] = None
    total_vol: Optional[int] = None
    up: Optional[int] = None
    down: Optional[int] = None


class FXRow(BaseModel):
    """One currency's latest selling rate."""

    currency: str
    selling: Optional[float] = None
    as_of: Optional[str] = None


class RatesStrip(BaseModel):
    """Dashboard top-bar strip — key rates + FX, single round-trip.

    Field shapes:
        sbp_policy_rate / _date              ← sbp_policy_rates
        kibor_3m_bid / _offer / _date        ← kibor_daily (tenor=3M)
        tbill_3m_cutoff / _date              ← tbill_auctions (tenor=3M)
        pkrv_10y_yield / _date               ← pkrv_daily (tenor_months=120)

    A missing point leaves both value and date None — no fabrication.
    """

    sbp_policy_rate: Optional[float] = None
    sbp_policy_date: Optional[str] = None
    kibor_3m_bid: Optional[float] = None
    kibor_3m_offer: Optional[float] = None
    kibor_3m_date: Optional[str] = None
    tbill_3m_cutoff: Optional[float] = None
    tbill_3m_date: Optional[str] = None
    pkrv_10y_yield: Optional[float] = None
    pkrv_10y_date: Optional[str] = None
    fx: list[FXRow]
