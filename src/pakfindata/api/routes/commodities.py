"""Commodities + khistocks + PMEX-portal endpoints.

Backs Group G.3 page ``commodities.py`` (7 tabs: Dashboard, Charts,
Categories, Pakistan View, Local Markets, PMEX Portal, Export +
sync controls). All read-only.

Three routers because the URL shape is intentionally distinct per
domain:

- ``/v1/commodities/*`` — global commodity OHLCV (yfinance), PKR-
  converted prices, sector performance, sync-run ledger, bulk export.
- ``/v1/khistocks/*`` — Pakistan local markets (PMEX, Sarafa, Mandi,
  Bullion, LME) from the wide ``khistocks_prices`` table.
- ``/v1/pmex-portal/*`` — PMEX portal market watch snapshots
  (``pmex_market_watch``).

NOT exposed here: the separate ``commod.db`` (pmex_ohlc, pmex_margins,
pmex_intraday_snapshots) backing ``pmex.py`` + ``pmex_analytics_page.py``.
Those pages are scraper-maintenance owners of their own DB and are
intentionally skipped in Phase 1.7.G.3 (architectural unification is
out of scope for this phase).
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.commodities import (
    CommodityCategoryLatestRow,
    CommodityEodRow,
    CommodityHasData,
    CommodityLatestRow,
    CommodityPkrRow,
    CommoditySectorPerfRow,
    CommoditySyncRunRow,
    KhistocksRow,
    PmexMarketWatchRow,
)

commodities_router = APIRouter(prefix="/v1/commodities", tags=["commodities"])
khistocks_router = APIRouter(prefix="/v1/khistocks", tags=["khistocks"])
pmex_portal_router = APIRouter(prefix="/v1/pmex-portal", tags=["pmex-portal"])


# Allowlist for the bulk-export endpoint. The dataset name is
# interpolated as part of a SQL table identifier (cannot be
# parameterized), so it MUST be a fixed enum.
_EXPORT_TABLES: dict[str, str] = {
    "eod": "commodity_eod",
    "monthly": "commodity_monthly",
    "pkr": "commodity_pkr",
    "fx": "commodity_fx_rates",
    "khistocks": "khistocks_prices",
    "pmex_market_watch": "pmex_market_watch",
}


# ── /v1/commodities ────────────────────────────────────────────────────


@commodities_router.get("/has-data", response_model=CommodityHasData)
def get_has_data(con: sqlite3.Connection = Depends(get_read_db)) -> dict:
    """Composite gate — does ANY of the 3 commodity tables have rows.

    Powers the empty-state guard in ``render_commodities``. Returns
    individual counts so the page can show a tighter message if only
    one feed is missing.
    """
    def _safe_count(table: str) -> int:
        try:
            return con.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        except sqlite3.Error:
            return 0

    eod = _safe_count("commodity_eod")
    khi = _safe_count("khistocks_prices")
    pmex = _safe_count("pmex_market_watch")
    return {
        "commodity_eod": eod,
        "khistocks_prices": khi,
        "pmex_market_watch": pmex,
        "has_any": (eod + khi + pmex) > 0,
    }


@commodities_router.get("/symbols", response_model=list[str])
def list_commodity_symbols(
    source: Annotated[
        Optional[str], Query(description="Filter by source e.g. yfinance")
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """DISTINCT symbol from ``commodity_eod``."""
    if source:
        cur = con.execute(
            "SELECT DISTINCT symbol FROM commodity_eod WHERE source = ? ORDER BY symbol",
            (source,),
        )
    else:
        cur = con.execute(
            "SELECT DISTINCT symbol FROM commodity_eod ORDER BY symbol"
        )
    return [r["symbol"] for r in cur.fetchall()]


@commodities_router.get("/fx-pairs", response_model=list[str])
def list_commodity_fx_pairs(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """DISTINCT pair from ``commodity_fx_rates``."""
    cur = con.execute(
        "SELECT DISTINCT pair FROM commodity_fx_rates ORDER BY pair"
    )
    return [r["pair"] for r in cur.fetchall()]


@commodities_router.get("/latest", response_model=list[CommodityLatestRow])
def get_commodity_latest(
    symbols: Annotated[
        str,
        Query(description="Comma-separated symbol list e.g. GOLD,BRENT,USD_PKR"),
    ],
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest + previous close per symbol with FX fallback.

    For each requested symbol, returns the latest row from
    ``commodity_eod`` (yfinance source). If no row is found, falls back
    to ``commodity_fx_rates`` (where the FX-style symbol e.g.
    USD_PKR lives). ``prev_close`` is the close from the previous row,
    or the same row's ``open`` if only one row exists. Symbols with no
    data in either table are omitted from the response.
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return []
    out: list[dict] = []
    for sym in sym_list:
        rows = con.execute(
            """SELECT symbol, date, close, open FROM commodity_eod
                WHERE symbol = ? AND source = 'yfinance'
                ORDER BY date DESC LIMIT 2""",
            (sym,),
        ).fetchall()
        src = "commodity_eod"
        if not rows:
            rows = con.execute(
                """SELECT pair AS symbol, date, close, open FROM commodity_fx_rates
                    WHERE pair = ? ORDER BY date DESC LIMIT 2""",
                (sym,),
            ).fetchall()
            src = "commodity_fx_rates"
        if not rows:
            continue
        cur = dict(rows[0])
        prev_close = dict(rows[1])["close"] if len(rows) > 1 else cur.get("open")
        out.append(
            {
                "symbol": cur["symbol"],
                "date": cur["date"],
                "close": cur.get("close"),
                "open": cur.get("open"),
                "prev_close": prev_close,
                "source": src,
            }
        )
    return out


@commodities_router.get(
    "/sector-performance", response_model=list[CommoditySectorPerfRow]
)
def get_commodity_sector_performance(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Average daily change by ``commodity_symbols.category`` (latest session)."""
    cur = con.execute(
        """SELECT cs.category, AVG((e.close - e.open) / NULLIF(e.open, 0) * 100) AS avg_chg
             FROM commodity_eod e
             JOIN commodity_symbols cs ON e.symbol = cs.symbol
             JOIN (SELECT symbol, MAX(date) AS md FROM commodity_eod
                    WHERE source = 'yfinance' GROUP BY symbol) latest
               ON e.symbol = latest.symbol AND e.date = latest.md
            WHERE e.source = 'yfinance'
            GROUP BY cs.category
            ORDER BY avg_chg"""
    )
    return [dict(r) for r in cur.fetchall()]


@commodities_router.get("/pkr-latest", response_model=list[CommodityPkrRow])
def get_commodity_pkr_latest(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest PKR-converted price per symbol from ``commodity_pkr``."""
    cur = con.execute(
        """SELECT cp.symbol, cp.date, cp.pkr_price, cp.pk_unit,
                  cp.usd_price, cp.usd_pkr, cp.source
             FROM commodity_pkr cp
             INNER JOIN (SELECT symbol, MAX(date) AS max_date
                           FROM commodity_pkr GROUP BY symbol) latest
               ON cp.symbol = latest.symbol AND cp.date = latest.max_date
            ORDER BY cp.symbol"""
    )
    return [dict(r) for r in cur.fetchall()]


@commodities_router.get(
    "/categories-latest", response_model=list[CommodityCategoryLatestRow]
)
def get_commodity_categories_latest(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest yfinance row per symbol with category metadata JOIN.

    Powers the Categories tab table + change heatmap. Rows missing a
    ``commodity_symbols`` entry still appear (LEFT JOIN), with NULL
    category/name/unit.
    """
    cur = con.execute(
        """SELECT e.symbol, e.date, e.close, e.open, e.volume,
                  cs.name, cs.category, cs.unit, cs.pk_relevance
             FROM commodity_eod e
             INNER JOIN (SELECT symbol, MAX(date) AS max_date FROM commodity_eod
                          WHERE source = 'yfinance' GROUP BY symbol) latest
               ON e.symbol = latest.symbol AND e.date = latest.max_date
             LEFT JOIN commodity_symbols cs ON e.symbol = cs.symbol
            WHERE e.source = 'yfinance'
            ORDER BY cs.category, cs.name"""
    )
    return [dict(r) for r in cur.fetchall()]


@commodities_router.get("/sync-runs", response_model=list[CommoditySyncRunRow])
def list_commodity_sync_runs(
    limit: Annotated[int, Query(ge=1, le=200)] = 10,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Recent rows from ``commodity_sync_runs`` ledger."""
    cur = con.execute(
        "SELECT * FROM commodity_sync_runs ORDER BY started_at DESC LIMIT ?",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


@commodities_router.get(
    "/export/{dataset}", response_model=list[dict]
)
def export_commodity_dataset(
    dataset: Annotated[
        str, Path(description="One of: eod | monthly | pkr | fx | khistocks | pmex_market_watch")
    ],
    limit: Annotated[
        int,
        Query(
            ge=1, le=200000,
            description="Row cap — defaults to 5000 to avoid 100MB+ payloads",
        ),
    ] = 5000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Bulk export for the Export tab.

    The page originally did ``SELECT *`` with NO row cap, which would
    OOM on ``commodity_fx_rates`` (1.8M rows). We allowlist the dataset
    name (cannot be parameterized as identifier) and cap the row count.
    The default 5000 matches typical "preview + CSV download" use; the
    max 200K covers wider exports without going pathological.
    """
    table = _EXPORT_TABLES.get(dataset)
    if table is None:
        raise HTTPException(
            status_code=404,
            detail=f"unknown dataset {dataset!r}; valid: {sorted(_EXPORT_TABLES)}",
        )
    order_by = {
        "commodity_eod": "symbol, date DESC",
        "commodity_monthly": "symbol, date DESC",
        "commodity_pkr": "symbol, date DESC",
        "commodity_fx_rates": "pair, date DESC",
        "khistocks_prices": "feed, symbol, date DESC",
        "pmex_market_watch": "category, contract, snapshot_date DESC",
    }[table]
    cur = con.execute(f"SELECT * FROM {table} ORDER BY {order_by} LIMIT ?", (limit,))
    return [dict(r) for r in cur.fetchall()]


# ── /v1/commodities/{symbol} (per-symbol detail) ───────────────────────


@commodities_router.get("/{symbol}/eod", response_model=list[CommodityEodRow])
def get_commodity_eod(
    symbol: Annotated[str, Path(description="Symbol or FX pair e.g. GOLD, USD_PKR")],
    limit: Annotated[int, Query(ge=1, le=10000)] = 365,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """OHLCV history for one symbol with FX fallback.

    Tries ``commodity_eod`` (yfinance) first. If empty, falls back to
    ``commodity_fx_rates`` (FX-style symbols like USD_PKR). Date order
    is most-recent first; the caller sorts ascending for charting.
    """
    rows = con.execute(
        """SELECT symbol, date, open, high, low, close, volume
             FROM commodity_eod
            WHERE symbol = ? AND source = 'yfinance'
            ORDER BY date DESC LIMIT ?""",
        (symbol, limit),
    ).fetchall()
    if not rows:
        rows = con.execute(
            """SELECT pair AS symbol, date, open, high, low, close, volume
                 FROM commodity_fx_rates
                WHERE pair = ?
                ORDER BY date DESC LIMIT ?""",
            (symbol, limit),
        ).fetchall()
    return [dict(r) for r in rows]


@commodities_router.get("/{symbol}/pkr", response_model=list[CommodityPkrRow])
def get_commodity_pkr_history(
    symbol: Annotated[str, Path()],
    limit: Annotated[int, Query(ge=1, le=5000)] = 90,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """PKR price history for one symbol."""
    cur = con.execute(
        """SELECT symbol, date, pkr_price, pk_unit, usd_price, usd_pkr, source
             FROM commodity_pkr
            WHERE symbol = ?
            ORDER BY date DESC LIMIT ?""",
        (symbol, limit),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/khistocks ──────────────────────────────────────────────────────


@khistocks_router.get("/feeds", response_model=list[str])
def list_khistocks_feeds(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """DISTINCT feed from ``khistocks_prices``."""
    cur = con.execute(
        "SELECT DISTINCT feed FROM khistocks_prices ORDER BY feed"
    )
    return [r["feed"] for r in cur.fetchall()]


@khistocks_router.get("/latest", response_model=list[KhistocksRow])
def get_khistocks_latest(
    feed: Annotated[
        Optional[str],
        Query(description="Optional feed filter e.g. khistocks_pmex"),
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest row per (symbol, feed). Optionally filtered to one feed."""
    if feed:
        cur = con.execute(
            """SELECT kp.* FROM khistocks_prices kp
                INNER JOIN (SELECT symbol, feed, MAX(date) AS max_date
                              FROM khistocks_prices WHERE feed = ?
                              GROUP BY symbol, feed) latest
                  ON kp.symbol = latest.symbol
                 AND kp.feed = latest.feed
                 AND kp.date = latest.max_date
               WHERE kp.feed = ?
               ORDER BY kp.symbol""",
            (feed, feed),
        )
    else:
        cur = con.execute(
            """SELECT kp.* FROM khistocks_prices kp
                INNER JOIN (SELECT symbol, feed, MAX(date) AS max_date
                              FROM khistocks_prices GROUP BY symbol, feed) latest
                  ON kp.symbol = latest.symbol
                 AND kp.feed = latest.feed
                 AND kp.date = latest.max_date
               ORDER BY kp.feed, kp.symbol"""
        )
    return [dict(r) for r in cur.fetchall()]


@khistocks_router.get(
    "/{symbol}/history", response_model=list[KhistocksRow]
)
def get_khistocks_history(
    symbol: Annotated[str, Path()],
    limit: Annotated[int, Query(ge=1, le=5000)] = 90,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Price history for one khistocks symbol (all feeds the symbol appears in)."""
    cur = con.execute(
        "SELECT * FROM khistocks_prices WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        (symbol, limit),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/pmex-portal ────────────────────────────────────────────────────


@pmex_portal_router.get("/categories", response_model=list[str])
def list_pmex_portal_categories(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """DISTINCT category from ``pmex_market_watch``."""
    cur = con.execute(
        "SELECT DISTINCT category FROM pmex_market_watch ORDER BY category"
    )
    return [r["category"] for r in cur.fetchall()]


@pmex_portal_router.get("/latest", response_model=list[PmexMarketWatchRow])
def get_pmex_portal_latest(
    category: Annotated[
        Optional[str], Query(description="Optional category filter")
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest snapshot per contract. Optionally filtered to one category."""
    if category:
        cur = con.execute(
            """SELECT p.* FROM pmex_market_watch p
                INNER JOIN (SELECT contract, MAX(snapshot_date) AS max_date
                              FROM pmex_market_watch WHERE category = ?
                              GROUP BY contract) latest
                  ON p.contract = latest.contract
                 AND p.snapshot_date = latest.max_date
               WHERE p.category = ?
               ORDER BY p.contract""",
            (category, category),
        )
    else:
        cur = con.execute(
            """SELECT p.* FROM pmex_market_watch p
                INNER JOIN (SELECT contract, MAX(snapshot_date) AS max_date
                              FROM pmex_market_watch GROUP BY contract) latest
                  ON p.contract = latest.contract
                 AND p.snapshot_date = latest.max_date
               ORDER BY p.category, p.contract"""
        )
    return [dict(r) for r in cur.fetchall()]


@pmex_portal_router.get(
    "/{contract}/history", response_model=list[PmexMarketWatchRow]
)
def get_pmex_portal_history(
    contract: Annotated[str, Path()],
    limit: Annotated[int, Query(ge=1, le=5000)] = 90,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Snapshot history for one PMEX-portal contract."""
    cur = con.execute(
        """SELECT * FROM pmex_market_watch
            WHERE contract = ?
            ORDER BY snapshot_date DESC LIMIT ?""",
        (contract, limit),
    )
    return [dict(r) for r in cur.fetchall()]
