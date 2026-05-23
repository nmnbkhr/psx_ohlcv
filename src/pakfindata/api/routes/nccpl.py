"""NCCPL flow endpoints — /v1/nccpl/*.

Backs Group G.4.5 page ``nccpl_flows.py`` (3 tabs: Flow Dashboard,
Sector Flows, Sync & Backfill). All read-only.

Sync surface (BRecorder Tier 2a + KhiStocks JSON backfill) remains in
``pakfindata.sources.nccpl_*`` and is invoked from the page directly —
not exposed via /v1 because the upstream sources require live-domain
state (Cloudflare nonces, JSON pagination) that doesn't map cleanly to
a stateless endpoint.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.nccpl import (
    NccplCoverage,
    NccplDerivedRow,
    NccplFipiRow,
    NccplLipiRow,
    NccplSectorRow,
)

router = APIRouter(prefix="/v1/nccpl", tags=["nccpl"])

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


@router.get("/coverage", response_model=NccplCoverage)
def get_nccpl_coverage(
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Counts + date range for FIPI/LIPI/sector/derived tables."""
    def _count(table: str) -> int:
        try:
            return con.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        except sqlite3.Error:
            return 0

    fipi = _count("nccpl_fipi")
    lipi = _count("nccpl_lipi")
    sector = _count("nccpl_fipi_sector")
    derived = _count("nccpl_flows_derived")

    date_min = date_max = None
    if fipi > 0:
        try:
            row = con.execute(
                "SELECT MIN(date) AS dmin, MAX(date) AS dmax FROM nccpl_fipi"
            ).fetchone()
            date_min, date_max = row["dmin"], row["dmax"]
        except sqlite3.Error:
            pass

    return {
        "fipi_count": fipi,
        "lipi_count": lipi,
        "sector_count": sector,
        "derived_count": derived,
        "date_min": date_min,
        "date_max": date_max,
        "has_any": (fipi + lipi + sector + derived) > 0,
    }


@router.get("/fipi", response_model=list[NccplFipiRow])
def list_nccpl_fipi(
    limit: Annotated[int, Query(ge=1, le=5000)] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Recent daily FIPI rows (date-desc)."""
    try:
        cur = con.execute(
            "SELECT * FROM nccpl_fipi ORDER BY date DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@router.get("/lipi", response_model=list[NccplLipiRow])
def list_nccpl_lipi(
    limit: Annotated[int, Query(ge=1, le=5000)] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Recent daily LIPI rows (date-desc) — mf/insurance/bank/retail/etc."""
    try:
        cur = con.execute(
            "SELECT * FROM nccpl_lipi ORDER BY date DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@router.get("/sector-dates", response_model=list[str])
def list_nccpl_sector_dates(
    limit: Annotated[int, Query(ge=1, le=500)] = 60,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Distinct trading dates with sector flow data (date-desc)."""
    try:
        cur = con.execute(
            "SELECT DISTINCT date FROM nccpl_fipi_sector ORDER BY date DESC LIMIT ?",
            (limit,),
        )
        return [r["date"] for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@router.get("/sector", response_model=list[NccplSectorRow])
def list_nccpl_sector(
    date: Annotated[str, Query(pattern=DATE_RE)],
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Per-sector foreign flows for one trading date (sorted by fpi_net desc)."""
    try:
        cur = con.execute(
            """SELECT date, sector, fpi_buy, fpi_sell, fpi_net
                 FROM nccpl_fipi_sector
                WHERE date = ?
                ORDER BY fpi_net DESC""",
            (date,),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@router.get("/sector-heatmap", response_model=list[NccplSectorRow])
def get_nccpl_sector_heatmap(
    days: Annotated[int, Query(ge=1, le=120)] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Last N days of sector flows for the heatmap viz.

    Returns long-format rows; the page pivots to a (sector × date)
    matrix for plotly Heatmap.
    """
    try:
        cur = con.execute(
            """SELECT date, sector, fpi_buy, fpi_sell, fpi_net
                 FROM nccpl_fipi_sector
                WHERE date >= (
                    SELECT date FROM nccpl_fipi_sector
                     ORDER BY date DESC LIMIT 1 OFFSET ?
                )
                ORDER BY date, sector""",
            (max(days - 1, 0),),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@router.get("/flows-derived", response_model=list[NccplDerivedRow])
def list_nccpl_flows_derived(
    limit: Annotated[int, Query(ge=1, le=10000)] = 1000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Full derived signal series (date-asc — order matches page expectation)."""
    try:
        cur = con.execute(
            """SELECT * FROM (
                   SELECT * FROM nccpl_flows_derived ORDER BY date DESC LIMIT ?
                ) ORDER BY date""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
