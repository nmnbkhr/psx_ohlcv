"""Pydantic response schemas for /v1/derivatives/* composites."""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel


class DerivativesDataQualityEntry(BaseModel):
    """Per-source freshness for the derivatives composite. The OI entry
    uses status='not_available' with source_path_pattern; futures_eod
    and eod_ohlcv use last_row_date.
    """

    status: Literal["ok", "stale", "failed", "unknown", "not_available"]
    last_row_date: Optional[str] = None
    source_path_pattern: Optional[str] = None


class DerivativesSummary(BaseModel):
    futures_count: int
    premium_count: int
    discount_count: int
    flat_count: int
    avg_basis_pct: Optional[float] = None
    total_futures_volume: int


class BasisRow(BaseModel):
    base_symbol: str
    contract_month: Optional[str] = None
    fut_close: Optional[float] = None
    spot_close: Optional[float] = None
    basis: Optional[float] = None
    basis_pct: Optional[float] = None
    fut_volume: Optional[int] = None


class DerivativesOverview(BaseModel):
    """Composite response for /v1/derivatives/overview.

    OI section is deliberately absent — `data_quality.oi.status` is
    'not_available' because OI lives only in disk XLS today. Adoption
    into the DB is deferred (likely Phase 2.A.5 alongside the other
    scraper work).
    """

    as_of: Optional[str]
    summary: DerivativesSummary
    basis_premium: list[BasisRow]
    basis_discount: list[BasisRow]
    data_quality: dict[str, DerivativesDataQualityEntry]
