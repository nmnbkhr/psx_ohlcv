"""NCCPL flows response models.

Backs Group G.4.5 page ``nccpl_flows.py``. Four base tables:

- ``nccpl_fipi`` — daily foreign investor flows.
- ``nccpl_lipi`` — daily local institutional / retail flows.
- ``nccpl_fipi_sector`` — sector-level foreign flows per date.
- ``nccpl_flows_derived`` — pre-computed regime + smart/dumb ratio.

Data freshness depends on the BRecorder/KhiStocks fetchers — NCCPL's
own portal has been Cloudflare-blocked since 2025-Q3, so the page
sync surface relies on those mirror sources.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict


class NccplCoverage(BaseModel):
    """Row counts + date range — backs the data-coverage strip."""

    fipi_count: int
    lipi_count: int
    sector_count: int
    derived_count: int
    date_min: Optional[str] = None
    date_max: Optional[str] = None
    has_any: bool


class NccplFipiRow(BaseModel):
    """One row from ``nccpl_fipi``."""

    date: str
    fpi_buy: Optional[float] = None
    fpi_sell: Optional[float] = None
    fpi_net: Optional[float] = None
    fpi_foreign_individual_net: Optional[float] = None
    fpi_foreign_corporate_net: Optional[float] = None
    fpi_overseas_pak_net: Optional[float] = None


class NccplLipiRow(BaseModel):
    """One row from ``nccpl_lipi``."""

    model_config = ConfigDict(extra="allow")

    date: str
    mf_net: Optional[float] = None
    insurance_net: Optional[float] = None
    bank_net: Optional[float] = None
    retail_net: Optional[float] = None
    corporate_net: Optional[float] = None
    broker_net: Optional[float] = None


class NccplSectorRow(BaseModel):
    """One sector row from ``nccpl_fipi_sector``."""

    date: str
    sector: str
    fpi_buy: Optional[float] = None
    fpi_sell: Optional[float] = None
    fpi_net: Optional[float] = None


class NccplDerivedRow(BaseModel):
    """One row from ``nccpl_flows_derived`` (pre-computed signals)."""

    date: str
    fpi_net_4w: Optional[float] = None
    mf_net_4w: Optional[float] = None
    retail_net_4w: Optional[float] = None
    bank_net_4w: Optional[float] = None
    smart_money_net: Optional[float] = None
    dumb_money_net: Optional[float] = None
    smart_dumb_ratio: Optional[float] = None
    institutional_consensus: Optional[int] = None
    foreign_domestic_divergence: Optional[int] = None
    flow_regime_signal: Optional[str] = None
