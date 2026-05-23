"""Pydantic response schemas for /v1/research/* composites."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel


class DataQualityEntry(BaseModel):
    """Per-source freshness signal. See composite_aggregator_pattern §7."""

    status: Literal["ok", "stale", "failed", "unknown", "not_available"]
    last_row_date: Optional[str] = None
    days_stale: Optional[int] = None
    row_count: Optional[int] = None


class MoverEnrichedRow(BaseModel):
    symbol: str
    close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    turnover: Optional[float] = None
    sector_name: Optional[str] = None
    # Enrichment columns from trading_sessions (may be NULL when LEFT JOIN
    # finds no match for a symbol on the latest REG session_date).
    pe_ratio_ttm: Optional[float] = None
    ytd_change: Optional[float] = None
    year_1_change: Optional[float] = None


class MoversEnriched(BaseModel):
    """Composite response for /v1/research/movers-enriched.

    `data_quality.trading_sessions.days_stale` is the headline signal —
    UI renders a banner whenever it's > 7 (or status != 'ok').
    """

    as_of: Optional[str]
    direction: Literal["gainers", "losers", "volume", "value"]
    rows: list[MoverEnrichedRow]
    data_quality: dict[str, DataQualityEntry]
