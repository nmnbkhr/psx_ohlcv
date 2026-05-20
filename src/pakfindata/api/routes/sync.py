"""Sync-side observability endpoints under /v1/sync.

Surfaces the Phase-0 ``sync_runs`` history for Dashboard's footer
widget. Future Phase 1.4-1.6 will expand this surface with the richer
``jobs`` table.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.sync import SyncRun

router = APIRouter(prefix="/v1/sync", tags=["sync"])


@router.get("/runs", response_model=list[SyncRun])
def sync_runs(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[int, Query(ge=1, le=500)] = 10,
) -> list[SyncRun]:
    """Most recent ``sync_runs`` rows, newest first."""
    cur = con.execute(
        """SELECT run_id, started_at, ended_at, mode,
                  symbols_total, symbols_ok, symbols_failed, rows_upserted
             FROM sync_runs
             ORDER BY started_at DESC
             LIMIT ?""",
        (limit,),
    )
    return [SyncRun.model_validate(dict(r)) for r in cur.fetchall()]
