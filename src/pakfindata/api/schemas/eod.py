"""EOD endpoint response models."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class EodRow(BaseModel):
    """One row of daily OHLCV from ``eod_ohlcv``."""

    symbol: str
    date: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    prev_close: Optional[float] = None
    sector_code: Optional[str] = None
    company_name: Optional[str] = None
    ingested_at: Optional[str] = None


class EodBreadth(BaseModel):
    """Advancers / decliners / unchanged for a single date."""

    date: str
    gainers: int
    losers: int
    unchanged: int
    total: int
    total_volume: Optional[int] = None
    total_value: Optional[float] = None
    avg_change: Optional[float] = None
