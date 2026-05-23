"""/v1/research/* composite-aggregator endpoints.

First prototype of the pattern documented in
``docs/architecture/composite_aggregator_pattern.md`` (2.A.4.1).

Route ownership:
    GET /v1/research/movers-enriched — movers (gainers/losers/volume/value)
                                       enriched with sector name + P/E +
                                       YTD + 1y change from trading_sessions.
                                       Surfaces trading_sessions staleness
                                       via the data_quality field.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Depends, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.research import MoversEnriched
from pakfindata.db.repositories.composites import research as research_repo

router = APIRouter(prefix="/v1/research", tags=["research"])


@router.get("/movers-enriched", response_model=MoversEnriched)
def movers_enriched(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    direction: Annotated[
        Literal["gainers", "losers", "volume", "value"],
        Query(description="Ranking strategy"),
    ] = "gainers",
    top_n: Annotated[int, Query(ge=1, le=100)] = 15,
    sector: Annotated[Optional[str], Query(max_length=64)] = None,
    pe_max: Annotated[
        Optional[float],
        Query(ge=0, le=1000, description="Cap on ts.pe_ratio_ttm (value direction)"),
    ] = None,
    min_volume: Annotated[int, Query(ge=0, le=10_000_000)] = 50_000,
) -> MoversEnriched:
    """Movers with sector / P/E / YTD / 1y-change enrichment.

    The P/E + YTD + 1y-change columns come from `trading_sessions`,
    which is currently 55 days stale. Staleness is honest-surfaced in
    `data_quality.trading_sessions` so clients can render a per-section
    banner (see composite_aggregator_pattern §7).
    """
    return research_repo.get_movers_enriched(
        con,
        direction=direction,
        top_n=top_n,
        sector=sector,
        pe_max=pe_max,
        min_volume=min_volume,
    )
