"""Indices endpoints under /v1/indices.

Thin wrappers around ``db/repositories/market.py`` and
``db/repositories/instruments.py``. Index codes are uppercased before
the query — clients can send any case.

Route order (specific paths first):
    GET /v1/indices
    GET /v1/indices/{code}/constituents
    GET /v1/indices/{code}
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.indices import (
    IndexConstituent,
    IndexHistoryRow,
    IndexLatest,
)
from pakfindata.db.repositories import instruments as inst_repo
from pakfindata.db.repositories import market as market_repo

router = APIRouter(prefix="/v1/indices", tags=["indices"])

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


@router.get("", response_model=list[IndexLatest])
def list_indices(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[IndexLatest]:
    """Latest snapshot for every index code in ``psx_indices``."""
    rows = market_repo.get_all_latest_indices(con)
    return [IndexLatest.model_validate(r) for r in rows]


@router.get("/{code}/constituents", response_model=list[IndexConstituent])
def index_constituents(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    code: Annotated[str, Path(description="Index code (case-insensitive)")],
) -> list[IndexConstituent]:
    """Membership for an index at its latest effective date.

    Returns 404 if the index code is unknown OR has no membership row
    seeded yet (sync hasn't run for it).
    """
    sym = code.upper()
    rows = inst_repo.get_index_constituents(con, index_symbol=sym)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"no constituents for index {sym!r}",
        )
    return [IndexConstituent.model_validate(r) for r in rows]


@router.get("/{code}", response_model=list[IndexHistoryRow])
def index_history(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    code: Annotated[str, Path(description="Index code (case-insensitive)")],
    from_: Annotated[
        Optional[str],
        Query(alias="from", description="Range start (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    to: Annotated[
        Optional[str],
        Query(description="Range end (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
) -> list[IndexHistoryRow]:
    """Daily history for one index, newest first.

    Returns 404 if the index code is unknown (zero rows). Empty range
    within a known code returns 200 with an empty list.
    """
    sym = code.upper()
    if from_ is not None and to is not None and from_ > to:
        raise HTTPException(
            status_code=400, detail=f"from ({from_}) must be <= to ({to})"
        )
    if market_repo.get_latest_index(con, index_code=sym) is None:
        raise HTTPException(
            status_code=404, detail=f"unknown index code {sym!r}"
        )
    rows = market_repo.get_index_history_range(
        con, index_code=sym, start_date=from_, end_date=to, limit=5000
    )
    return [IndexHistoryRow.model_validate(r) for r in rows]
