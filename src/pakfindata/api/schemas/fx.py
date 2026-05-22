"""Pydantic schemas for /v1/fx/* and /v1/rates/{konia,npc} endpoints.

Schemas mirror the source SQLite tables column-for-column where
possible:

- ``sbp_fx_interbank`` / ``forex_kerb`` / ``sbp_fx_open_market`` —
  same shape (currency, date, buying, selling); :class:`FXRateRow`.
- ``fx_ohlcv`` — daily OHLC for FX pairs; :class:`FXOhlcvRow`.
- ``commodity_fx_rates`` — global pairs (USD/EUR etc.); same shape as
  :class:`FXOhlcvCloseRow` but distinct source.
- ``npc_rates`` / ``konia_daily`` — small lookup rows.
- ``fx_sync_runs`` — operational view of recent sync history.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class FXRateRow(BaseModel):
    """One row from ``sbp_fx_interbank`` / ``forex_kerb`` /
    ``sbp_fx_open_market`` — same shape, different source table.

    ``spread`` is derived (``selling - buying``) and only set when both
    legs are present.
    """

    currency: str
    date: str
    buying: Optional[float] = None
    selling: Optional[float] = None
    spread: Optional[float] = None


class FXOhlcvRow(BaseModel):
    """Daily OHLC for an FX pair (``fx_ohlcv`` table)."""

    date: str
    pair: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None


class FXSpreadRow(BaseModel):
    """One row of the interbank-vs-kerb spread heatmap.

    ``spread`` = ``kerb.selling - interbank.selling`` for the same
    (currency, date).
    """

    currency: str
    date: str
    spread: Optional[float] = None


class FXSyncRunRow(BaseModel):
    """One row from ``fx_sync_runs`` — operational sync history."""

    run_id: Optional[int] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    mode: Optional[str] = None
    rows_upserted: Optional[int] = None
    status: Optional[str] = None
    error: Optional[str] = None


class KoniaRow(BaseModel):
    """One row from ``konia_daily`` — overnight rate."""

    date: str
    rate_pct: Optional[float] = None


class NPCRatesRow(BaseModel):
    """One row from ``npc_rates`` — non-prepayable carry rates.

    Schema fields mirror the SQLite ``npc_rates`` table column-for-column:
    ``date | effective_date | currency | tenor | rate | certificate_type | source``.
    The companion ``npc_carry_*`` views are missing per Phase 0.5
    coverage gaps; the base table itself has data.
    """

    date: str
    effective_date: Optional[str] = None
    currency: str
    tenor: str
    rate: Optional[float] = None
    certificate_type: Optional[str] = None
    source: Optional[str] = None
