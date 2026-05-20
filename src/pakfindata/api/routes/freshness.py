"""Freshness endpoints — surface of the ``data_freshness`` catalog.

The catalog is Phase 0.2's single source of truth for "when was each
dataset last updated and is it healthy". UI pages render staleness
badges from this; Phase 1.3 will re-point those badges to /v1/freshness
instead of opening SQLite directly.

Routes:
    GET /v1/freshness           — all 39 datasets, ordered by recency
    GET /v1/freshness/{domain}  — one dataset, 404 if unknown
"""

from __future__ import annotations

import sqlite3
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import FreshnessRow

router = APIRouter(prefix="/v1/freshness", tags=["freshness"])


@router.get("", response_model=list[FreshnessRow])
def list_freshness(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[FreshnessRow]:
    """Return every row in ``data_freshness``, freshest first."""
    cur = con.execute(
        "SELECT * FROM data_freshness ORDER BY last_sync_at DESC NULLS LAST"
    )
    return [FreshnessRow.model_validate(dict(r)) for r in cur.fetchall()]


@router.get("/{domain}", response_model=FreshnessRow)
def get_freshness(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    domain: Annotated[str, Path(description="data_freshness.domain PK")],
) -> FreshnessRow:
    """Return one row by PK; 404 if no such domain."""
    row = con.execute(
        "SELECT * FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"unknown freshness domain: {domain!r}"
        )
    return FreshnessRow.model_validate(dict(row))
