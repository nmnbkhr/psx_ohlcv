"""/v1/derivatives/* composite-aggregator endpoints.

Second prototype of the pattern documented in
``docs/architecture/composite_aggregator_pattern.md`` (2.A.4.1).

Route ownership:
    GET /v1/derivatives/overview — basis premium / discount snapshot
                                   + futures summary for the latest
                                   (or specified) trading date.

Note: OI is intentionally NOT in this composite — see pattern §8.
The response's data_quality.oi field surfaces that it's disk-only
(`status='not_available'`). When OI is ingested into DB (Phase 2.A.5+
expected), the endpoint adds the section — non-breaking change.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.derivatives import DerivativesOverview
from pakfindata.db.repositories.composites import derivatives as derivatives_repo

router = APIRouter(prefix="/v1/derivatives", tags=["derivatives"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@router.get("/overview", response_model=DerivativesOverview)
def derivatives_overview(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    date: Annotated[
        Optional[str],
        Query(
            description="Trading date (YYYY-MM-DD); latest futures_eod date if omitted",
            pattern=r"^\d{4}-\d{2}-\d{2}$",
        ),
    ] = None,
    top_n: Annotated[
        int,
        Query(ge=1, le=50, description="Rows in basis_premium / basis_discount each"),
    ] = 10,
) -> DerivativesOverview:
    """Basis premium / discount snapshot + summary.

    Reads near-month FUT/CONT from `futures_eod`, joins on spot close
    from `eod_ohlcv`, returns top-N premium + top-N discount + counts.
    OI absent by design (pattern §8). data_quality.oi reports
    `status='not_available'` with the source_path_pattern.
    """
    if date is not None and not _DATE_RE.match(date):
        # Defensive — FastAPI's pattern already rejects, but explicit
        # here keeps the error consistent if someone calls the repo
        # directly later.
        raise HTTPException(status_code=400, detail="invalid date format")
    return derivatives_repo.get_derivatives_overview(
        con, date=date, top_n=top_n,
    )
