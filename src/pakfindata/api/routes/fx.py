"""FX endpoints under /v1/fx/* and small additions to /v1/rates/*.

Route ownership:

    GET /v1/fx/latest                     — all currencies @ latest date per source
    GET /v1/fx/latest/{currency}          — single currency latest row
    GET /v1/fx/history                    — historical rows for one currency+source
    GET /v1/fx/ohlcv/{pair}               — fx_ohlcv history for one pair
    GET /v1/fx/global-pairs               — distinct pairs in commodity_fx_rates
    GET /v1/fx/global-history/{pair}      — global-pair daily close history
    GET /v1/fx/spread-heatmap             — interbank-vs-kerb spread bundle
    GET /v1/fx/sync-runs                  — recent fx_sync_runs

    GET /v1/rates/konia                   — latest KONIA + optional history
    GET /v1/rates/npc                     — all rows from npc_rates

All endpoints are structurally read-only via ``get_read_db`` (mode=ro
URI in :mod:`pakfindata.api.deps`).
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.fx import (
    FXAnalyticsResponse,
    FXNormalizedRow,
    FXOhlcvRow,
    FXPairRow,
    FXRateRow,
    FXSpreadRow,
    FXSyncRunRow,
    GlobalReferenceRateRow,
    KiborRow,
    KoniaRow,
    NPCRatesRow,
)

fx_router = APIRouter(prefix="/v1/fx", tags=["fx"])
rates_extra_router = APIRouter(prefix="/v1/rates", tags=["rates"])

_FX_SOURCES = {
    "interbank": "sbp_fx_interbank",
    "kerb": "forex_kerb",
    "open_market": "sbp_fx_open_market",
}


def _resolve_source_table(source: str) -> str:
    if source not in _FX_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown source '{source}'; expected one of {sorted(_FX_SOURCES)}",
        )
    return _FX_SOURCES[source]


# ---------------------------------------------------------------- /v1/fx


@fx_router.get("/latest", response_model=list[FXRateRow])
def get_fx_latest(
    source: Annotated[str, Query(description="One of: interbank, kerb, open_market")] = "interbank",
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """All currencies at their most recent date in the given source.

    Mirrors the legacy ``_load_all_currency_rates`` Streamlit cache —
    one row per currency, derived ``spread = selling - buying``.
    """
    table = _resolve_source_table(source)
    cur = con.execute(
        f"""SELECT t.currency, t.date, t.buying, t.selling,
                   ROUND(t.selling - t.buying, 4) AS spread
            FROM {table} t
            INNER JOIN (
                SELECT currency, MAX(date) AS max_date FROM {table} GROUP BY currency
            ) mx ON t.currency = mx.currency AND t.date = mx.max_date
            ORDER BY t.currency"""
    )
    return [dict(r) for r in cur.fetchall()]


@fx_router.get("/latest/{currency}", response_model=FXRateRow)
def get_fx_latest_one(
    currency: str,
    source: Annotated[str, Query(description="One of: interbank, kerb, open_market")] = "interbank",
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Latest row for one currency from one source."""
    table = _resolve_source_table(source)
    row = con.execute(
        f"""SELECT currency, date, buying, selling,
                   ROUND(selling - buying, 4) AS spread
            FROM {table} WHERE UPPER(currency) = ?
            ORDER BY date DESC LIMIT 1""",
        (currency.upper(),),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no rows for currency={currency} in source={source}",
        )
    return dict(row)


@fx_router.get("/history", response_model=list[FXRateRow])
def get_fx_history(
    currency: Annotated[str, Query(description="3-letter ISO code, e.g. USD")],
    source: Annotated[str, Query(description="One of: interbank, kerb, open_market")] = "interbank",
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Date-descending history for one currency in one source."""
    table = _resolve_source_table(source)
    cur = con.execute(
        f"""SELECT currency, date, buying, selling,
                   ROUND(selling - buying, 4) AS spread
            FROM {table} WHERE UPPER(currency) = ?
            ORDER BY date DESC LIMIT ?""",
        (currency.upper(), limit),
    )
    return [dict(r) for r in cur.fetchall()]


@fx_router.get("/ohlcv/{pair}", response_model=list[FXOhlcvRow])
def get_fx_ohlcv(
    pair: str,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """``fx_ohlcv`` history for one pair (e.g. ``USD/PKR``)."""
    cur = con.execute(
        """SELECT date, pair, open, high, low, close FROM fx_ohlcv
           WHERE pair = ? ORDER BY date DESC LIMIT ?""",
        (pair, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        # Empty result is valid; fx_ohlcv is sparsely populated.
        return []
    return rows


@fx_router.get("/global-pairs", response_model=list[str])
def get_fx_global_pairs(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Distinct pairs in ``commodity_fx_rates`` (e.g. EUR/USD)."""
    try:
        cur = con.execute(
            "SELECT DISTINCT pair FROM commodity_fx_rates ORDER BY pair"
        )
        return [r["pair"] for r in cur.fetchall()]
    except sqlite3.OperationalError:
        # Table may not exist on fresh installs.
        return []


@fx_router.get("/global-history/{pair}", response_model=list[FXOhlcvRow])
def get_fx_global_history(
    pair: str,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """``commodity_fx_rates`` history for one global pair."""
    try:
        cur = con.execute(
            """SELECT date, pair, NULL AS open, NULL AS high, NULL AS low, close
               FROM commodity_fx_rates WHERE pair = ? ORDER BY date DESC LIMIT ?""",
            (pair, limit),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@fx_router.get("/spread-heatmap", response_model=list[FXSpreadRow])
def get_fx_spread_heatmap(
    limit: Annotated[int, Query(ge=1, le=2000)] = 150,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Interbank-vs-kerb spread for the top 5 currencies.

    Same query the legacy ``_load_spread_heatmap`` Streamlit cache used.
    Returns up to ``limit`` recent rows (date-descending).
    """
    cur = con.execute(
        """SELECT i.currency, i.date,
                  ROUND(k.selling - i.selling, 2) AS spread
           FROM sbp_fx_interbank i
           INNER JOIN forex_kerb k
             ON i.currency = k.currency AND i.date = k.date
           WHERE i.currency IN ('USD','EUR','GBP','SAR','AED')
           ORDER BY i.date DESC LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


@fx_router.get("/sync-runs", response_model=list[FXSyncRunRow])
def get_fx_sync_runs(
    limit: Annotated[int, Query(ge=1, le=200)] = 10,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Recent rows from ``fx_sync_runs`` (operational view)."""
    try:
        cur = con.execute(
            "SELECT * FROM fx_sync_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        # Project only the FXSyncRunRow fields; ignore others.
        keep = {"run_id", "started_at", "ended_at", "mode", "rows_upserted", "status", "error"}
        return [{k: v for k, v in r.items() if k in keep} for r in rows]
    except sqlite3.OperationalError:
        return []


@fx_router.get("/pairs", response_model=list[FXPairRow])
def list_fx_pairs(
    active_only: Annotated[bool, Query()] = True,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """All rows from ``fx_pairs`` master.

    Backs the pair dropdown on fx.py + fx_history.py — replaces the
    legacy ``get_fx_pairs(con, active_only=True)`` repo call.
    """
    try:
        if active_only:
            cur = con.execute(
                "SELECT * FROM fx_pairs WHERE is_active = 1 ORDER BY pair"
            )
        else:
            cur = con.execute("SELECT * FROM fx_pairs ORDER BY pair")
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


def _coerce_native(obj):
    """Recursively coerce numpy scalars/arrays to native Python types.

    Pydantic v2 + FastAPI's JSON serializer rejects ``numpy.bool_``,
    ``numpy.float64``, etc. The engine returns these inside the
    ``trend`` sub-dict and other analytics fields — coerce on the
    way out.
    """
    import numpy as np

    if isinstance(obj, dict):
        return {k: _coerce_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_coerce_native(v) for v in obj]
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    return obj


@fx_router.get("/analytics/{pair:path}", response_model=FXAnalyticsResponse)
def get_fx_analytics(
    pair: str,
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Returns + volatility + simple trend for one FX pair.

    Wraps :func:`pakfindata.analytics_fx.get_fx_analytics` which reads
    ≤300 OHLCV rows and does light pandas math (returns over 1W/1M/3M/
    6M/1Y, rolling volatility, sign-based trend). Light compute —
    safe for blocking /v1.

    The ``{pair:path}`` converter keeps the slash inside pair names
    (e.g. ``USD/PKR``) intact.
    """
    from pakfindata.analytics_fx import get_fx_analytics as _engine_fx_analytics

    return _coerce_native(_engine_fx_analytics(con, pair))


@fx_router.get("/normalized-performance", response_model=list[FXNormalizedRow])
def get_fx_normalized_performance(
    pairs: Annotated[
        str,
        Query(description="Comma-separated FX pairs e.g. USD/PKR,EUR/PKR"),
    ],
    start_date: Annotated[Optional[str], Query()] = None,
    end_date: Annotated[Optional[str], Query()] = None,
    base: Annotated[float, Query(gt=0)] = 100.0,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Wide-format normalized performance across multiple FX pairs.

    Each pair is normalized to ``base`` at the first available date in
    the requested window, then carried forward. Wraps
    :func:`pakfindata.analytics_fx.get_normalized_fx_performance`.

    Response shape — one row per date with all pairs as additional
    keys::

        [{"date": "2026-01-02", "USD/PKR": 100.0, "EUR/PKR": 100.0},
         {"date": "2026-01-03", "USD/PKR": 100.4, "EUR/PKR":  99.8},
         ...]
    """
    from pakfindata.analytics_fx import get_normalized_fx_performance

    pair_list = [p.strip() for p in pairs.split(",") if p.strip()]
    if not pair_list:
        return []
    df = get_normalized_fx_performance(
        con, pair_list, start_date=start_date, end_date=end_date, base=base
    )
    if df is None or df.empty:
        return []
    # df has date as the index; expose it as a column so each row carries
    # the date plus every requested pair as additional keys.
    out = df.reset_index()
    # The reset_index() column name varies (often "date"); rename for clarity.
    out = out.rename(columns={out.columns[0]: "date"})
    out["date"] = out["date"].astype(str)
    return df_to_records(out)


@fx_router.get("/adjusted-metrics", response_model=list[dict])
def list_fx_adjusted_metrics(
    fx_pair: Annotated[Optional[str], Query(description="e.g. USD/PKR")] = None,
    period: Annotated[Optional[str], Query(description="1W | 1M | 3M | 6M | 1Y")] = None,
    symbol: Annotated[Optional[str], Query(description="Equity symbol filter")] = None,
    as_of: Annotated[Optional[str], Query(description="YYYY-MM-DD")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Pre-computed FX-adjusted equity returns from ``fx_adjusted_metrics``.

    The table is populated by ``analytics_fx.compute_and_store_fx_adjusted_metrics``
    (a write path — not exposed here). When empty, the page surfaces a
    "Compute" button which still runs the engine directly via the
    Streamlit process. Read endpoint is a thin pass-through to the repo.
    """
    from pakfindata.db.repositories.fixed_income import get_fx_adjusted_metrics
    return get_fx_adjusted_metrics(
        con,
        as_of_date=as_of,
        symbol=symbol,
        fx_pair=fx_pair,
        period=period,
        limit=limit,
    )


# ---------------------------------------------------------------- /v1/rates extras


@rates_extra_router.get("/konia", response_model=list[KoniaRow])
def get_konia(
    limit: Annotated[int, Query(ge=1, le=2000)] = 1,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest KONIA rows (date-descending)."""
    cur = con.execute(
        "SELECT date, rate_pct FROM konia_daily ORDER BY date DESC LIMIT ?",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


@rates_extra_router.get("/kibor/latest-per-tenor", response_model=list[KiborRow])
def get_kibor_latest_per_tenor(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Latest KIBOR row per tenor (one tenor → one row), sorted by
    short-end tenor first. Single round-trip vs querying history."""
    cur = con.execute(
        """WITH ranked AS (
              SELECT *, ROW_NUMBER() OVER (
                          PARTITION BY tenor
                          ORDER BY date DESC) AS rn
              FROM kibor_daily WHERE offer IS NOT NULL
           )
           SELECT date, tenor, bid, offer FROM ranked WHERE rn = 1
           ORDER BY CASE tenor
                      WHEN '1W' THEN 1 WHEN '2W' THEN 2
                      WHEN '1M' THEN 3 WHEN '3M' THEN 4
                      WHEN '6M' THEN 5 WHEN '9M' THEN 6
                      WHEN '1Y' THEN 7 ELSE 8 END"""
    )
    return [dict(r) for r in cur.fetchall()]


@rates_extra_router.get("/kibor", response_model=list[KiborRow])
def get_kibor_history(
    tenors: Annotated[
        Optional[str],
        Query(description="Comma-separated tenor codes, e.g. 1M,3M,6M,1Y"),
    ] = None,
    days: Annotated[int, Query(ge=1, le=10000)] = 3000,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """KIBOR history from ``kibor_daily``.

    Returns rows date-ascending so chart code can plot directly. With
    no ``tenors`` arg, returns every tenor; with one, restricts to
    that comma-separated set. Drops rows where ``offer`` is NULL since
    chart consumers always plot offer.
    """
    if tenors:
        keys = [t.strip() for t in tenors.split(",") if t.strip()]
        placeholders = ",".join("?" * len(keys))
        cur = con.execute(
            f"""SELECT date, tenor, bid, offer FROM kibor_daily
                WHERE tenor IN ({placeholders}) AND offer IS NOT NULL
                ORDER BY date DESC LIMIT ?""",
            tuple(keys) + (days * len(keys),),
        )
    else:
        cur = con.execute(
            """SELECT date, tenor, bid, offer FROM kibor_daily
               WHERE offer IS NOT NULL
               ORDER BY date DESC LIMIT ?""",
            (days,),
        )
    # Reverse so callers get ascending order suitable for charting.
    rows = [dict(r) for r in cur.fetchall()]
    return list(reversed(rows))


@rates_extra_router.get("/global", response_model=list[GlobalReferenceRateRow])
def get_global_reference_rates(
    rate_names: Annotated[
        Optional[str],
        Query(description="Comma-separated, e.g. SOFR,SONIA,EUSTR,TONA"),
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Most-recent row per rate_name from ``global_reference_rates``.

    If ``rate_names`` is omitted, returns the latest row for every
    rate_name in the table (window-function-friendly). With
    ``rate_names``, restricts to the comma-separated set.
    """
    try:
        if rate_names:
            keys = [n.strip().upper() for n in rate_names.split(",") if n.strip()]
            placeholders = ",".join("?" * len(keys))
            cur = con.execute(
                f"""SELECT date, rate_name, currency, tenor, rate, volume,
                           percentile_25, percentile_75, source
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY rate_name ORDER BY date DESC
                        ) AS _rn
                        FROM global_reference_rates
                        WHERE UPPER(rate_name) IN ({placeholders})
                    ) WHERE _rn = 1
                    ORDER BY rate_name""",
                tuple(keys),
            )
        else:
            cur = con.execute(
                """SELECT date, rate_name, currency, tenor, rate, volume,
                          percentile_25, percentile_75, source
                   FROM (
                       SELECT *, ROW_NUMBER() OVER (
                           PARTITION BY rate_name ORDER BY date DESC
                       ) AS _rn
                       FROM global_reference_rates
                   ) WHERE _rn = 1
                   ORDER BY rate_name"""
            )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


@rates_extra_router.get("/npc", response_model=list[NPCRatesRow])
def get_npc_rates(
    limit: Annotated[int, Query(ge=1, le=2000)] = 200,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Rows from ``npc_rates`` (carry rates). The companion
    ``npc_carry_*`` views are missing per Phase 0.5 coverage gaps;
    the base table itself has data.
    """
    try:
        cur = con.execute(
            """SELECT date, effective_date, currency, tenor, rate,
                      certificate_type, source
               FROM npc_rates ORDER BY date DESC, tenor LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
