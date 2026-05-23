"""Pydantic schemas for /v1/treasury/*, /v1/yield-curves/*, /v1/curve/*,
/v1/bonds/*, /v1/benchmark/*, /v1/rates/policy/*, /v1/rates/npc/* extras,
/v1/rates/global/* extras, /v1/alm/*, and /v1/fi/* endpoints.

Schemas mirror the source SQLite tables column-for-column. For endpoints
that wrap repository functions returning DataFrames with variable column
sets (ALM, NPC views), schemas keep every field Optional so absence of
a column doesn't break the response.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


# ── /v1/treasury/* ──────────────────────────────────────────────────


class TbillAuctionRow(BaseModel):
    """One row from ``tbill_auctions``."""

    auction_date: str
    tenor: str
    target_amount_billions: Optional[float] = None
    bids_received_billions: Optional[float] = None
    amount_accepted_billions: Optional[float] = None
    cutoff_yield: Optional[float] = None
    cutoff_price: Optional[float] = None
    weighted_avg_yield: Optional[float] = None
    maturity_date: Optional[str] = None
    settlement_date: Optional[str] = None
    scraped_at: Optional[str] = None


class PibAuctionRow(BaseModel):
    """One row from ``pib_auctions``."""

    auction_date: str
    tenor: str
    pib_type: Optional[str] = None
    target_amount_billions: Optional[float] = None
    bids_received_billions: Optional[float] = None
    amount_accepted_billions: Optional[float] = None
    cutoff_yield: Optional[float] = None
    cutoff_price: Optional[float] = None
    coupon_rate: Optional[float] = None
    maturity_date: Optional[str] = None
    scraped_at: Optional[str] = None


class GisAuctionRow(BaseModel):
    """One row from ``gis_auctions``."""

    auction_date: str
    gis_type: str
    tenor: Optional[str] = None
    target_amount_billions: Optional[float] = None
    amount_accepted_billions: Optional[float] = None
    cutoff_rental_rate: Optional[float] = None
    maturity_date: Optional[str] = None
    scraped_at: Optional[str] = None


# ── /v1/yield-curves/* ──────────────────────────────────────────────


class PkrvRow(BaseModel):
    """One row from ``pkrv_daily``. Note: tenor is months (INTEGER)."""

    date: str
    tenor_months: int
    yield_pct: Optional[float] = None
    change_bps: Optional[float] = None
    source: Optional[str] = None


class PkisrvRow(BaseModel):
    """One row from ``pkisrv_daily``. Tenor is a text label (e.g. '3M', '6M')."""

    date: str
    tenor: str
    yield_pct: Optional[float] = None
    source: Optional[str] = None


class PkfrvRow(BaseModel):
    """One row from ``pkfrv_daily``. Per-bond floating-rate valuations."""

    date: str
    bond_code: str
    issue_date: Optional[str] = None
    maturity_date: Optional[str] = None
    coupon_frequency: Optional[str] = None
    fma_price: Optional[float] = None
    source: Optional[str] = None


# ── /v1/curve/sovereign ─────────────────────────────────────────────


class SovereignCurveRow(BaseModel):
    """One row from ``sovereign_curve``.

    Includes synthetic rows (``source`` ending in ``_SYN``) with their
    associated ``_RMSE`` metadata — Curve Analytics depends on these.
    """

    date: str
    source: str
    tenor: str
    days: int
    yield_pct: Optional[float] = None
    bid: Optional[float] = None
    offer: Optional[float] = None


# ── /v1/bonds/* ─────────────────────────────────────────────────────


class BondTradingDailyRow(BaseModel):
    """One row from ``sbp_bond_trading_daily`` (SBP SMTV)."""

    date: str
    security_type: str
    maturity_year: int
    tenor_bucket: str
    segment: str
    face_amount: Optional[float] = None
    realized_amount: Optional[float] = None
    yield_min: Optional[float] = None
    yield_max: Optional[float] = None
    yield_weighted_avg: Optional[float] = None
    scraped_at: Optional[str] = None


# ── /v1/benchmark/* ─────────────────────────────────────────────────


class BenchmarkSnapshotRow(BaseModel):
    """One ``(metric, value)`` from ``sbp_benchmark_snapshot``."""

    date: str
    metric: str
    value: Optional[float] = None


class BenchmarkSnapshot(BaseModel):
    """Latest benchmark snapshot as a metric→value dict + the date."""

    date: Optional[str] = None
    metrics: dict[str, float] = {}


# ── /v1/rates/policy/* ──────────────────────────────────────────────


class PolicyRateRow(BaseModel):
    """One row from ``sbp_policy_rates``.

    ``rate_date`` is the column actually populated; ``effective_date``
    alias kept for legacy callers.
    """

    rate_date: Optional[str] = None
    policy_rate: Optional[float] = None
    ceiling_rate: Optional[float] = None
    floor_rate: Optional[float] = None
    overnight_repo_rate: Optional[float] = None
    source: Optional[str] = None
    ingested_at: Optional[str] = None


# ── /v1/fi/* ────────────────────────────────────────────────────────


class FiInstrumentRow(BaseModel):
    """One row from ``fi_instruments``."""

    instrument_id: Optional[str] = None
    isin: Optional[str] = None
    issuer: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    currency: Optional[str] = None
    issue_date: Optional[str] = None
    maturity_date: Optional[str] = None
    coupon_rate: Optional[float] = None
    coupon_frequency: Optional[int] = None
    day_count: Optional[str] = None
    face_value: Optional[float] = None
    shariah_compliant: Optional[int] = None
    is_active: Optional[int] = None
    source: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    denomination_currency: Optional[str] = None
    reference_rate: Optional[str] = None
    spread_bps: Optional[str] = None


class FiQuoteRow(BaseModel):
    """One row from ``fi_quotes``."""

    instrument_id: str
    quote_date: str
    clean_price: Optional[float] = None
    ytm: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    source: Optional[str] = None
    ingested_at: Optional[str] = None


# ── /v1/alm/* (variable shapes via repo wrappers) ───────────────────


class AlmProductRow(BaseModel):
    """One row from ``alm_products``."""

    product_code: Optional[str] = None
    product_name: Optional[str] = None
    product_type: Optional[str] = None
    asset_liability: Optional[str] = None
    rate_type: Optional[str] = None
    reference_rate: Optional[str] = None
    spread_bps: Optional[int] = None
    repricing_freq_months: Optional[int] = None
    contractual_maturity_months: Optional[int] = None
    behavioral_maturity_months: Optional[int] = None
    currency: Optional[str] = None
    is_islamic: Optional[int] = None
    liq_premium_bps: Optional[int] = None
    optionality_cost_bps: Optional[int] = None
    core_pct: Optional[float] = None
    core_tenor_months: Optional[int] = None
    volatile_tenor_months: Optional[int] = None
    category: Optional[str] = None
    is_active: Optional[int] = None
    created_at: Optional[str] = None


class AlmPositionRow(BaseModel):
    """One row from ``alm_positions``."""

    as_of_date: Optional[str] = None
    product_code: Optional[str] = None
    bucket: Optional[str] = None
    outstanding_mn: Optional[float] = None
    weighted_avg_rate: Optional[float] = None
    num_accounts: Optional[int] = None
    avg_remaining_mat_months: Optional[float] = None
    source: Optional[str] = None
    ingested_at: Optional[str] = None


class AlmRepricingGapRow(BaseModel):
    """One row from the repricing-gap analytics."""

    bucket: Optional[str] = None
    assets_mn: Optional[float] = None
    liabilities_mn: Optional[float] = None
    gap_mn: Optional[float] = None
    cumulative_gap_mn: Optional[float] = None


class AlmFtpRow(BaseModel):
    """One row from the FTP-rates analytics (per product, per as-of-date)."""

    as_of_date: Optional[str] = None
    product_code: Optional[str] = None
    product_name: Optional[str] = None
    product_type: Optional[str] = None
    asset_liability: Optional[str] = None
    ftp_curve: Optional[str] = None
    ftp_tenor_months: Optional[float] = None
    ftp_base_rate: Optional[float] = None
    liq_premium_bps: Optional[float] = None
    credit_spread_bps: Optional[float] = None
    optionality_bps: Optional[float] = None
    total_ftp_rate: Optional[float] = None
    customer_rate: Optional[float] = None
    ftp_margin_bps: Optional[float] = None
    outstanding_mn: Optional[float] = None
    daily_nii_mn: Optional[float] = None
    computed_at: Optional[str] = None


class AlmSensitivityRow(BaseModel):
    """One row from ``alm_sensitivity``."""

    as_of_date: Optional[str] = None
    scenario: Optional[str] = None
    shock_bps: Optional[int] = None
    nii_base_mn: Optional[float] = None
    nii_shocked_mn: Optional[float] = None
    nii_impact_mn: Optional[float] = None
    nii_pct_change: Optional[float] = None
    eve_base_mn: Optional[float] = None
    eve_shocked_mn: Optional[float] = None
    eve_impact_mn: Optional[float] = None
    eve_pct_change: Optional[float] = None
    duration_gap: Optional[float] = None
    computed_at: Optional[str] = None


class AlmLiquidityLadderRow(BaseModel):
    """One row from ``alm_liquidity_ladder``."""

    as_of_date: Optional[str] = None
    bucket: Optional[str] = None
    inflows_mn: Optional[float] = None
    outflows_mn: Optional[float] = None
    net_gap_mn: Optional[float] = None
    cumulative_gap_mn: Optional[float] = None
    hqla_mn: Optional[float] = None
    lcr_pct: Optional[float] = None
    computed_at: Optional[str] = None


# ── /v1/rates/npc/* extras + /v1/rates/global/* extras ──────────────


class GenericRow(BaseModel):
    """Flexible row used by view-backed endpoints whose column shape
    depends on missing-or-present views (NPC carry, NPC spread,
    multicurrency dashboard, SOFR–KIBOR spread).

    Every field is Optional + arbitrary keys are dropped at serialize
    time — caller is responsible for handling absence.
    """

    model_config = {"extra": "allow"}
