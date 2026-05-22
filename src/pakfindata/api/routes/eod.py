"""EOD endpoints under /v1/eod.

Thin wrappers around ``db/repositories/eod.py`` +
``db/repositories/market_summary.py``. All read-only, all auth-gated by
the global Bearer middleware.

Route order (FastAPI is sensitive to this — specific paths first):
    GET /v1/eod/latest
    GET /v1/eod/breadth
    GET /v1/eod
    GET /v1/eod/{symbol}

Wave A semantics chosen at Step-0 approval:
    Q1 — unknown symbol → 404 with detail (typo case is common,
         empty list hides typos).
    Q2 — symbol history default window = 90 days when no from/to.
    Q4 — /eod/latest supports ?as_of=YYYY-MM-DD for reproducibility.
"""

from __future__ import annotations

import sqlite3
from datetime import date as date_cls, timedelta
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.eod import EodBreadth, EodRow
from pakfindata.db.repositories import eod as eod_repo
from pakfindata.db.repositories import market_summary as ms_repo

router = APIRouter(prefix="/v1/eod", tags=["eod"])

# YYYY-MM-DD; ISO-8601 calendar dates only.
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


def _parse_date(s: str) -> date_cls:
    """Validate + return a date; HTTPException(400) on malformed input."""
    try:
        return date_cls.fromisoformat(s)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid date {s!r}: {exc}")


@router.get("/latest", response_model=list[EodRow])
def eod_latest(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    as_of: Annotated[
        Optional[str],
        Query(description="Override the auto-detected latest trading day (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
) -> list[dict]:
    """All symbols' OHLCV for the latest available trading day.

    If ``as_of`` is provided, returns rows for that specific date.
    Otherwise the server picks the latest day with ≥100 symbols.
    """
    if as_of is not None:
        target = as_of
    else:
        target = ms_repo.get_latest_full_trading_day(con, min_symbols=100)
        if target is None:
            raise HTTPException(
                status_code=503, detail="no recent trading day in eod_ohlcv"
            )
    df = eod_repo.get_eod_ohlcv(
        con, start_date=target, end_date=target, limit=2000
    )
    return df_to_records(df)


@router.get("/breadth", response_model=EodBreadth)
def eod_breadth(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    date: Annotated[
        Optional[str],
        Query(description="Date (YYYY-MM-DD); defaults to latest", pattern=DATE_RE),
    ] = None,
) -> EodBreadth:
    """Advancers / decliners / unchanged for a single date."""
    payload = ms_repo.get_eod_breadth(con, date=date, min_symbols=100)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"no breadth available for {date or 'latest'}",
        )
    return EodBreadth.model_validate(payload)


@router.get("", response_model=list[EodRow])
def eod_for_date(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    date: Annotated[
        str,
        Query(description="Trading date (YYYY-MM-DD)", pattern=DATE_RE),
    ],
) -> list[dict]:
    """All symbols' OHLCV for a specific date."""
    df = eod_repo.get_eod_ohlcv(
        con, start_date=date, end_date=date, limit=2000
    )
    return df_to_records(df)


@router.get("/{symbol}", response_model=list[EodRow])
def eod_for_symbol(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    from_: Annotated[
        Optional[str],
        Query(alias="from", description="Range start (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    to: Annotated[
        Optional[str],
        Query(description="Range end (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    limit: Annotated[
        Optional[int],
        Query(
            ge=1, le=10000,
            description=(
                "Trading-day count semantics. When supplied without "
                "explicit `from`, the from-bound is dropped and the last "
                "N rows are returned (date-descending). Combine with `to` "
                "to anchor the right edge."
            ),
        ),
    ] = None,
) -> list[dict]:
    """OHLCV history for a single symbol.

    Default window when neither ``from``, ``to``, nor ``limit`` is
    supplied: last 90 days ending at the symbol's most recent row.
    404 if the symbol has zero rows at all (typo).
    """
    sym = symbol.upper()
    sym_max = eod_repo.get_max_date_for_symbol(con, sym)
    if sym_max is None:
        raise HTTPException(
            status_code=404, detail=f"unknown symbol {sym!r} (no rows in eod_ohlcv)"
        )

    if to is None:
        end = sym_max
    else:
        _parse_date(to)  # validation only
        end = to

    if from_ is None and limit is None:
        # Both omitted → keep the historical default of 90 days back.
        start = (date_cls.fromisoformat(end) - timedelta(days=90)).isoformat()
    elif from_ is None:
        # limit supplied without from → drop the lower bound; repo's
        # built-in ordering + LIMIT will return the last N rows.
        start = None
    else:
        _parse_date(from_)
        start = from_

    if start is not None and start > end:
        raise HTTPException(
            status_code=400, detail=f"from ({start}) must be <= to ({end})"
        )

    df = eod_repo.get_eod_ohlcv(
        con, symbol=sym, start_date=start, end_date=end,
        limit=limit if limit is not None else 10_000,
    )
    return df_to_records(df)
