"""Sync-runs / job-tracking response models.

The Phase-0 ``sync_runs`` table is the system-of-record for batch
data-load runs (per-symbol API fetches, market_summary ingest, etc.).
Phase 1.4-1.6 introduces a richer ``jobs`` table; until then the
``sync_runs`` shape is what the Dashboard's footer widget reads.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class SyncRun(BaseModel):
    """One row from ``sync_runs``."""

    run_id: str
    started_at: str
    ended_at: Optional[str] = None
    mode: str
    symbols_total: Optional[int] = None
    symbols_ok: Optional[int] = None
    symbols_failed: Optional[int] = None
    rows_upserted: Optional[int] = None
