"""Futures endpoints — /v1/futures.

Scope deliberately tiny — only the latest-active-contract lookup
needed by signal_dashboard's intelligence brief (futures-basis tile).
Broader futures coverage (history, deep contract chains) is not part
of Phase 1.7; if the futures terminal page ever migrates, it should
extend this router rather than re-implementing reads inline.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.futures import FuturesContractRow

futures_router = APIRouter(prefix="/v1/futures", tags=["futures"])


@futures_router.get(
    "/{base_symbol}/latest", response_model=FuturesContractRow
)
def get_latest_futures(
    base_symbol: Annotated[
        str, Path(description="Underlying equity symbol (case-insensitive)")
    ],
    con: sqlite3.Connection = Depends(get_read_db),
) -> FuturesContractRow:
    """Most-recent active futures contract for an underlying.

    Filters market_type IN ('FUT', 'CONT'), close > 0,
    contract_month NOT NULL. Returns the single newest row by
    (date, contract_month) — i.e. the front-month contract on the
    latest date that has data.
    """
    row = con.execute(
        """SELECT base_symbol, symbol, date, market_type, contract_month,
                  close, volume
             FROM futures_eod
            WHERE base_symbol = ?
              AND market_type IN ('FUT', 'CONT')
              AND close > 0
              AND contract_month IS NOT NULL
            ORDER BY date DESC, contract_month
            LIMIT 1""",
        (base_symbol.upper(),),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no active futures contract for {base_symbol!r}",
        )
    return FuturesContractRow(**dict(row))
