"""Tick-logs endpoints — /v1/tick-logs.

First /v1 route using ``get_analytics_con`` (DuckDB in-memory + Parquet/
JSONL views) instead of ``get_read_db`` (SQLite mode=ro). The
``tick_logs`` view is exposed by the analytics layer via DuckDB's
``read_json_auto`` over ``/mnt/e/psxdata/tick_logs_cloud/*.jsonl``.

Scope is intentionally narrow — only the trivial DISTINCT-dates lookup
that Group F's strategy pages (strategy_ofi, strategy_orderbook) need
for their date pickers. Heavy tick-level compute (VPIN, OFI bars, RL
training) stays in the engine layer per CLAUDE.md documented exception
— do NOT add ``/v1/tick-logs/bars`` etc. that block for seconds.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path

from pakfindata.api.deps import get_analytics_con

tick_logs_router = APIRouter(prefix="/v1/tick-logs", tags=["tick-logs"])


@tick_logs_router.get("/dates/{symbol}", response_model=list[str])
def get_tick_logs_dates(
    symbol: Annotated[str, Path(description="Symbol (case-insensitive)")],
    con=Depends(get_analytics_con),
) -> list[str]:
    """Distinct dates with tick data for a symbol, newest first.

    Single ``SELECT DISTINCT date`` against the DuckDB ``tick_logs``
    view. Sub-second on the cached connection.
    """
    rows = con.execute(
        "SELECT DISTINCT date FROM tick_logs WHERE symbol = ? ORDER BY date DESC",
        [symbol.upper()],
    ).fetchall()
    return [r[0] for r in rows]
