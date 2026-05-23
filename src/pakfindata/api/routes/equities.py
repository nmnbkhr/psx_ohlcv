"""Equities endpoints under /v1/symbols, /v1/sectors, /v1/companies,
and /v1/factors.

Backs the Group D pages (Stock Screener, Sector Analysis, Symbol
Financials, Factor Analysis, Company Deep). All read-only, all
auth-gated by the global Bearer middleware.

Route order (FastAPI sensitivity — specific paths first):
    GET /v1/symbols/screener
    GET /v1/symbols/sectors
    GET /v1/sectors/performance
    GET /v1/sectors/symbol-map
    GET /v1/companies/financial-symbols
    GET /v1/companies/{symbol}/financials
    GET /v1/companies/{symbol}/sector-valuation
    GET /v1/companies/{symbol}/profile-extras
    GET /v1/companies/{symbol}/announcements
    GET /v1/companies/{symbol}/dividend-payouts
    GET /v1/factors/raw-data
    GET /v1/factors/risk-stats
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.equities import (
    CompanyAnnouncement,
    CompanyDividendPayout,
    CompanyFinancialsResponse,
    CompanyFinancialsRow,
    CompanyKeyPerson,
    CompanyProfile,
    CompanyProfileExtras,
    FactorRawData,
    FactorRiskRow,
    FactorRow,
    ScreenerRow,
    SectorPerformanceRow,
    SectorSymbolMapRow,
    SectorValuation,
    SymbolRow,
    SymbolVolumeRow,
)

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"

symbols_router = APIRouter(prefix="/v1/symbols", tags=["symbols"])
sectors_router = APIRouter(prefix="/v1/sectors", tags=["sectors"])
companies_router = APIRouter(prefix="/v1/companies", tags=["companies"])
factors_router = APIRouter(prefix="/v1/factors", tags=["factors"])


# ── /v1/symbols ─────────────────────────────────────────────────────


@symbols_router.get("/screener", response_model=list[ScreenerRow])
def get_screener(
    sector: Annotated[
        Optional[str],
        Query(description="Sector name; omit to return all sectors"),
    ] = None,
    min_pe: Annotated[float, Query(ge=0)] = 0.0,
    max_pe: Annotated[float, Query(ge=0)] = 1000.0,
    min_mcap_m: Annotated[
        float,
        Query(ge=0, description="Minimum market cap in millions of PKR"),
    ] = 0.0,
    min_volume: Annotated[float, Query(ge=0)] = 0.0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 200,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Filter symbols by sector, P/E, market cap, and volume.

    Returns up to ``limit`` rows ordered by market_cap desc, with
    nulls last. P/E and market_cap come from ``company_fundamentals``
    if populated; the table is currently empty for those fields on
    most rows, so the page handles all-null gracefully.
    """
    where = ["s.is_active = 1"]
    params: list = []

    if sector and sector != "All":
        where.append("COALESCE(cp.sector_name, s.sector_name) = ?")
        params.append(sector)
    if min_pe > 0:
        where.append("cf.pe_ratio >= ?")
        params.append(min_pe)
    if max_pe < 1000:
        where.append("cf.pe_ratio <= ?")
        params.append(max_pe)
    if min_mcap_m > 0:
        where.append("cf.market_cap >= ?")
        params.append(min_mcap_m * 1e6)
    if min_volume > 0:
        where.append("COALESCE(rm.volume, e.volume) >= ?")
        params.append(min_volume)

    where_sql = " AND ".join(where)
    sql = f"""
        SELECT
            s.symbol,
            COALESCE(cp.company_name, cf.company_name, s.name) AS name,
            COALESCE(cp.sector_name, s.sector_name) AS sector,
            COALESCE(rm.current, cf.price) AS price,
            cf.pe_ratio,
            cf.market_cap,
            cf.free_float_pct,
            COALESCE(rm.volume, e.volume) AS last_volume,
            ROUND(COALESCE(rm.current, cf.price) * COALESCE(rm.volume, e.volume), 0) AS turnover,
            COALESCE(rm.change_pct,
                ROUND((e.close - e.prev_close) / NULLIF(e.prev_close, 0) * 100, 2)
            ) AS change_pct
        FROM symbols s
        LEFT JOIN regular_market_current rm ON s.symbol = rm.symbol
        LEFT JOIN company_fundamentals cf ON s.symbol = cf.symbol
        LEFT JOIN company_profile cp ON s.symbol = cp.symbol
        LEFT JOIN eod_ohlcv e ON s.symbol = e.symbol
            AND e.date = (SELECT MAX(date) FROM eod_ohlcv)
        WHERE {where_sql}
        ORDER BY cf.market_cap DESC NULLS LAST, turnover DESC NULLS LAST
        LIMIT ?
    """
    params.append(limit)
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


@symbols_router.get("", response_model=list[SymbolRow])
def get_symbols(
    active_only: Annotated[
        bool, Query(description="Restrict to symbols with is_active=1")
    ] = True,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Active symbols list from the ``symbols`` master table.

    Backs the symbol pickers on Group F research pages (microstructure,
    signal_dashboard, etc.). Tiny query, cached on the client.
    """
    where = "WHERE is_active = 1" if active_only else ""
    cur = con.execute(
        f"SELECT symbol, name, sector, sector_name, is_active "
        f"FROM symbols {where} ORDER BY symbol"
    )
    return [dict(r) for r in cur.fetchall()]


@symbols_router.get("/top-by-volume", response_model=list[SymbolVolumeRow])
def get_top_by_volume(
    n: Annotated[int, Query(ge=1, le=500, description="Max rows")] = 30,
    days: Annotated[int, Query(ge=1, le=365, description="Lookback window in days")] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Top N symbols by aggregate volume over the last ``days`` trading days.

    Feeds ml_predictions' candidate pool and signal_intelligence's
    correlation heatmap.
    """
    row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
    if not row or not row[0]:
        return []
    cur = con.execute(
        """SELECT symbol, SUM(volume) AS total_volume
             FROM eod_ohlcv
            WHERE date >= date(?, ?)
              AND volume > 0
            GROUP BY symbol
            ORDER BY total_volume DESC
            LIMIT ?""",
        (row[0], f"-{days} days", n),
    )
    return [dict(r) for r in cur.fetchall()]


@symbols_router.get("/futures", response_model=list[str])
def get_futures_symbols(
    min_data_days: Annotated[
        int, Query(ge=1, le=365, description="Min distinct dates per base symbol")
    ] = 30,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Futures base symbols with sufficient history.

    Sourced from ``futures_eod`` market_type='FUT'. Used by strategy_oi
    for the symbol picker.
    """
    cur = con.execute(
        """SELECT base_symbol
             FROM futures_eod
            WHERE market_type = 'FUT' AND volume > 0
            GROUP BY base_symbol
           HAVING COUNT(DISTINCT date) >= ?
            ORDER BY SUM(volume) DESC""",
        (min_data_days,),
    )
    return [r[0] for r in cur.fetchall()]


@symbols_router.get("/sectors", response_model=list[str])
def get_sectors(con: sqlite3.Connection = Depends(get_read_db)) -> list[str]:
    """Distinct sector names. Prefers the ``sectors`` table; falls back
    to ``company_profile.sector_name`` if the master is empty.
    """
    try:
        cur = con.execute(
            "SELECT DISTINCT sector_name FROM sectors WHERE sector_name IS NOT NULL ORDER BY sector_name"
        )
        names = [r[0] for r in cur.fetchall() if r[0]]
        if names:
            return names
    except sqlite3.OperationalError:
        pass
    cur = con.execute(
        "SELECT DISTINCT sector_name FROM company_profile WHERE sector_name IS NOT NULL ORDER BY sector_name"
    )
    return [r[0] for r in cur.fetchall() if r[0]]


# ── /v1/sectors ─────────────────────────────────────────────────────


@sectors_router.get("/performance", response_model=list[SectorPerformanceRow])
def get_sector_performance(
    date: Annotated[
        Optional[str],
        Query(description="Trading date YYYY-MM-DD; defaults to latest", pattern=DATE_RE),
    ] = None,
    min_stocks: Annotated[
        int,
        Query(ge=1, description="Minimum stocks per sector to include"),
    ] = 2,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Per-sector aggregates (avg_change, gainers/losers, total_volume)
    for one trading day.

    Sectors with fewer than ``min_stocks`` constituents are dropped;
    defaults to ``min_stocks=2`` matching the legacy sector_analysis
    convention, and the page raises to 3 for the heatmap call.
    """
    if date is None:
        row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
        if not row or not row[0]:
            return []
        date = row[0]

    cur = con.execute(
        """SELECT
             COALESCE(s.sector_name, e.sector_code) AS sector,
             COUNT(*) AS stocks,
             ROUND(AVG(CASE WHEN e.prev_close > 0
               THEN (e.close - e.prev_close) / e.prev_close * 100 END), 2) AS avg_change,
             SUM(e.volume) AS total_volume,
             SUM(CASE WHEN e.close > e.prev_close THEN 1 ELSE 0 END) AS gainers,
             SUM(CASE WHEN e.close < e.prev_close THEN 1 ELSE 0 END) AS losers
           FROM eod_ohlcv e
           LEFT JOIN sectors s ON s.sector_code = CASE WHEN LENGTH(e.sector_code) < 4
               THEN '0' || e.sector_code ELSE e.sector_code END
           WHERE e.date = ? AND e.prev_close > 0
           GROUP BY COALESCE(s.sector_name, e.sector_code)
           HAVING stocks >= ?
           ORDER BY avg_change DESC""",
        (date, min_stocks),
    )
    return [dict(r) for r in cur.fetchall()]


@sectors_router.get("/symbol-map", response_model=list[SectorSymbolMapRow])
def get_sector_symbol_map(
    date: Annotated[
        Optional[str],
        Query(description="Trading date YYYY-MM-DD; defaults to latest", pattern=DATE_RE),
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """symbol → sector_name lookup for a single trading day.

    Backs the KSE-100 treemap that joins the constituent XLS with
    sector labels.
    """
    if date is None:
        row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
        if not row or not row[0]:
            return []
        date = row[0]

    cur = con.execute(
        """SELECT e.symbol, COALESCE(s.sector_name, e.sector_code) AS sector
           FROM eod_ohlcv e
           LEFT JOIN sectors s ON s.sector_code = CASE WHEN LENGTH(e.sector_code) < 4
               THEN '0' || e.sector_code ELSE e.sector_code END
           WHERE e.date = ?
           GROUP BY e.symbol""",
        (date,),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/companies ───────────────────────────────────────────────────


@companies_router.get("/financial-symbols", response_model=list[str])
def get_financial_symbols(
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[str]:
    """Symbols with at least one row in ``company_financials``."""
    cur = con.execute(
        "SELECT DISTINCT symbol FROM company_financials ORDER BY symbol"
    )
    return [r[0] for r in cur.fetchall()]


@companies_router.get(
    "/{symbol}/financials",
    response_model=CompanyFinancialsResponse,
)
def get_company_financials(
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    period_type: Annotated[
        Optional[str],
        Query(
            description="Filter: 'annual', 'quarterly', or omit for all",
            pattern="^(annual|quarterly)$",
        ),
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """All financial rows for ``symbol``, newest period first.

    Includes a derived ``is_bank`` flag (True if any row has a
    non-NULL ``markup_earned`` — the same probe the legacy page used).
    """
    sym = symbol.upper()
    where = ["symbol = ?"]
    params: list = [sym]
    if period_type:
        where.append("period_type = ?")
        params.append(period_type)

    cur = con.execute(
        f"SELECT * FROM company_financials WHERE {' AND '.join(where)} "
        "ORDER BY period_end DESC",
        params,
    )
    rows = [dict(r) for r in cur.fetchall()]

    is_bank = any(r.get("markup_earned") is not None for r in rows)

    return {
        "symbol": sym,
        "is_bank": is_bank,
        "rows": rows,
    }


@companies_router.get(
    "/{symbol}/sector-valuation",
    response_model=SectorValuation,
)
def get_sector_valuation(
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Symbol P/E vs sector peers from latest ``company_snapshots``.

    Reads ``trading_data.REG.pe_ratio_ttm`` (the actual JSON path —
    the legacy page used ``snapshot_json.fundamentals.pe_ratio`` which
    is a column that never existed, so the feature was silently dead).

    Returns an empty-but-valid response if the symbol has no snapshot
    or sector has fewer than 3 peers — page renders no delta.
    """
    sym = symbol.upper()

    # Latest snapshot for the symbol. Peer matching uses sector_name
    # because company_snapshots.sector_code is empty across the dataset
    # (snapshot scraper never populated the code column); sector_name
    # is populated for every row.
    sym_row = con.execute(
        """SELECT
             NULLIF(sector_code, '') AS sector_code,
             NULLIF(sector_name, '') AS sector_name,
             CAST(json_extract(trading_data, '$.REG.pe_ratio_ttm') AS REAL) AS pe
           FROM company_snapshots
           WHERE symbol = ?
           ORDER BY snapshot_date DESC LIMIT 1""",
        (sym,),
    ).fetchone()

    if not sym_row:
        return {
            "symbol": sym,
            "symbol_pe": None,
            "sector_code": None,
            "sector_name": None,
            "sector_count": 0,
            "sector_avg_pe": None,
            "sector_min_pe": None,
            "sector_max_pe": None,
            "pe_percentile": None,
        }

    sym_pe = sym_row["pe"]
    sector_code = sym_row["sector_code"]
    sector_name = sym_row["sector_name"]

    if not sector_name:
        return {
            "symbol": sym,
            "symbol_pe": sym_pe,
            "sector_code": sector_code,
            "sector_name": None,
            "sector_count": 0,
            "sector_avg_pe": None,
            "sector_min_pe": None,
            "sector_max_pe": None,
            "pe_percentile": None,
        }

    # Sector aggregates (latest snapshot per peer in same sector)
    agg = con.execute(
        """WITH latest AS (
              SELECT symbol,
                     CAST(json_extract(trading_data, '$.REG.pe_ratio_ttm') AS REAL) AS pe,
                     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_date DESC) AS rn
              FROM company_snapshots
              WHERE sector_name = ?
           )
           SELECT
             COUNT(*) AS n,
             AVG(pe) AS avg_pe,
             MIN(pe) AS min_pe,
             MAX(pe) AS max_pe,
             SUM(CASE WHEN pe > ? THEN 1 ELSE 0 END) AS cheaper_count
           FROM latest
           WHERE rn = 1 AND pe > 0 AND pe < 500""",
        (sector_name, sym_pe or 0.0),
    ).fetchone()

    if not agg or (agg["n"] or 0) < 3:
        return {
            "symbol": sym,
            "symbol_pe": sym_pe,
            "sector_code": sector_code,
            "sector_name": sector_name,
            "sector_count": int(agg["n"] or 0) if agg else 0,
            "sector_avg_pe": None,
            "sector_min_pe": None,
            "sector_max_pe": None,
            "pe_percentile": None,
        }

    pe_percentile = (
        (float(agg["cheaper_count"]) / float(agg["n"]) * 100.0)
        if sym_pe and sym_pe > 0 else None
    )

    return {
        "symbol": sym,
        "symbol_pe": sym_pe,
        "sector_code": sector_code,
        "sector_name": sector_name,
        "sector_count": int(agg["n"]),
        "sector_avg_pe": agg["avg_pe"],
        "sector_min_pe": agg["min_pe"],
        "sector_max_pe": agg["max_pe"],
        "pe_percentile": pe_percentile,
    }


@companies_router.get(
    "/{symbol}/profile-extras",
    response_model=CompanyProfileExtras,
)
def get_company_profile_extras(
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """``company_profile`` + ``company_key_people`` bundle.

    Both halves are independent — page handles either being absent.
    """
    sym = symbol.upper()

    profile_row = con.execute(
        "SELECT * FROM company_profile WHERE symbol = ?",
        (sym,),
    ).fetchone()
    profile = dict(profile_row) if profile_row else None

    kp_rows = con.execute(
        "SELECT name, role FROM company_key_people WHERE symbol = ? ORDER BY rowid",
        (sym,),
    ).fetchall()
    key_people = [dict(r) for r in kp_rows]

    return {"profile": profile, "key_people": key_people}


@companies_router.get(
    "/{symbol}/announcements",
    response_model=list[CompanyAnnouncement],
)
def get_company_announcements(
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    limit: Annotated[int, Query(ge=1, le=200)] = 30,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Recent ``corporate_announcements`` for ``symbol``, newest first."""
    sym = symbol.upper()
    cur = con.execute(
        """SELECT announcement_date, title, announcement_type, category,
                  document_url, document_type, summary
           FROM corporate_announcements
           WHERE symbol = ?
           ORDER BY announcement_date DESC
           LIMIT ?""",
        (sym, limit),
    )
    return [dict(r) for r in cur.fetchall()]


@companies_router.get(
    "/{symbol}/dividend-payouts",
    response_model=list[CompanyDividendPayout],
)
def get_company_dividend_payouts(
    symbol: Annotated[str, Path(description="Stock symbol (case-insensitive)")],
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """Fallback dividend history from the global-scraper feed.

    Used by Company Deep's Payouts tab when the smart-client
    per-symbol payouts query returns no rows.
    """
    sym = symbol.upper()
    cur = con.execute(
        """SELECT announcement_date, dividend_percent, dividend_type,
                  dividend_number, fiscal_period, book_closure_from,
                  book_closure_to
           FROM dividend_payouts
           WHERE symbol = ?
           ORDER BY announcement_date DESC
           LIMIT ?""",
        (sym, limit),
    )
    return [dict(r) for r in cur.fetchall()]


# ── /v1/factors ─────────────────────────────────────────────────────


@factors_router.get("/raw-data", response_model=FactorRawData)
def get_factor_raw_data(
    con: sqlite3.Connection = Depends(get_read_db),
) -> dict:
    """Composite factor source: latest ``company_snapshots`` JSON
    columns + price returns from ``eod_ohlcv``.

    Field aliases match the legacy ``_load_factor_data`` SELECT so the
    page-side scoring math is unchanged. ``snapshot_count`` is the
    distinct-symbol coverage in ``company_snapshots`` — used by the
    page to gate the warning banner.
    """
    sql = """
        WITH latest_snapshots AS (
            SELECT
                cs.symbol,
                cs.snapshot_date,
                cs.company_name,
                -- snapshot.sector_code is empty across the dataset;
                -- expose sector_name under the sector_code alias so
                -- the page can group by a populated field.
                cs.sector_name AS sector_code,
                CAST(json_extract(cs.quote_data,  '$.close')                AS REAL) AS price,
                CAST(json_extract(cs.trading_data, '$.REG.ldcp')            AS REAL) AS ldcp,
                CAST(json_extract(cs.trading_data, '$.REG.volume')          AS REAL) AS volume,
                CAST(json_extract(cs.trading_data, '$.REG.high')            AS REAL) AS high,
                CAST(json_extract(cs.trading_data, '$.REG.low')             AS REAL) AS low,
                CAST(json_extract(cs.trading_data, '$.REG.week_52_low')     AS REAL) AS wk52_low,
                CAST(json_extract(cs.trading_data, '$.REG.week_52_high')    AS REAL) AS wk52_high,
                CAST(json_extract(cs.trading_data, '$.REG.pe_ratio_ttm')    AS REAL) AS pe_ratio,
                CAST(json_extract(cs.trading_data, '$.REG.ytd_change')      AS REAL) AS ytd_change,
                CAST(json_extract(cs.trading_data, '$.REG.year_1_change')   AS REAL) AS year_1_change,
                CAST(json_extract(cs.equity_data,  '$.market_cap')          AS REAL) AS market_cap,
                CAST(json_extract(cs.equity_data,  '$.outstanding_shares')  AS REAL) AS outstanding_shares,
                CAST(json_extract(cs.equity_data,  '$.free_float_percent')  AS REAL) AS free_float_pct,
                CAST(json_extract(cs.financials_data, '$.annual[0].eps')    AS REAL) AS eps,
                CAST(json_extract(cs.ratios_data, '$.annual[0].net_profit_margin') AS REAL) AS net_margin,
                CAST(json_extract(cs.ratios_data, '$.annual[0].eps_growth')        AS REAL) AS eps_growth,
                ROW_NUMBER() OVER (PARTITION BY cs.symbol ORDER BY cs.snapshot_date DESC) AS rn
            FROM company_snapshots cs
        ),
        price_history AS (
            SELECT
                symbol,
                (SELECT close FROM eod_ohlcv e2
                   WHERE e2.symbol = eod_ohlcv.symbol
                   ORDER BY date DESC LIMIT 1) AS latest_close,
                (SELECT close FROM eod_ohlcv e3
                   WHERE e3.symbol = eod_ohlcv.symbol
                   ORDER BY date DESC LIMIT 1 OFFSET 20) AS close_20d_ago,
                (SELECT close FROM eod_ohlcv e4
                   WHERE e4.symbol = eod_ohlcv.symbol
                   ORDER BY date DESC LIMIT 1 OFFSET 60) AS close_60d_ago,
                (SELECT AVG(close) FROM
                   (SELECT close FROM eod_ohlcv e5 WHERE e5.symbol = eod_ohlcv.symbol
                    ORDER BY date DESC LIMIT 20)) AS sma_20,
                (SELECT AVG(close) FROM
                   (SELECT close FROM eod_ohlcv e6 WHERE e6.symbol = eod_ohlcv.symbol
                    ORDER BY date DESC LIMIT 50)) AS sma_50
            FROM eod_ohlcv
            GROUP BY symbol
        )
        SELECT
            ls.symbol, ls.snapshot_date, ls.company_name, ls.sector_code,
            ls.price, ls.ldcp, ls.volume, ls.high, ls.low,
            ls.wk52_low, ls.wk52_high, ls.pe_ratio,
            ls.ytd_change, ls.year_1_change,
            ls.market_cap, ls.outstanding_shares, ls.free_float_pct,
            ls.eps, ls.net_margin, ls.eps_growth,
            ph.latest_close, ph.close_20d_ago, ph.close_60d_ago,
            ph.sma_20, ph.sma_50,
            CASE WHEN ph.close_20d_ago > 0
                THEN (ph.latest_close - ph.close_20d_ago) / ph.close_20d_ago * 100
                ELSE NULL END AS return_20d,
            CASE WHEN ph.close_60d_ago > 0
                THEN (ph.latest_close - ph.close_60d_ago) / ph.close_60d_ago * 100
                ELSE NULL END AS return_60d
        FROM latest_snapshots ls
        LEFT JOIN price_history ph ON ls.symbol = ph.symbol
        WHERE ls.rn = 1 AND ls.price > 0
    """
    cur = con.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]

    cnt = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM company_snapshots"
    ).fetchone()[0]

    return {"rows": rows, "snapshot_count": int(cnt or 0)}


@factors_router.get("/risk-stats", response_model=list[FactorRiskRow])
def get_factor_risk_stats(
    symbols: Annotated[
        str,
        Query(description="Comma-separated symbols, e.g. ENGRO,HBL,OGDC"),
    ],
    days: Annotated[int, Query(ge=1, le=3650)] = 90,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """90-day (or custom-window) price-range stats per symbol.

    ``range_pct`` = (max_close - min_close) / avg_close * 100.
    Empty list if all input symbols have no recent data.
    """
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not syms:
        raise HTTPException(status_code=400, detail="symbols= required")

    placeholders = ",".join("?" * len(syms))
    cur = con.execute(
        f"""SELECT
              symbol,
              COUNT(*) AS trading_days,
              AVG(close) AS avg_price,
              MIN(close) AS min_price,
              MAX(close) AS max_price,
              CASE WHEN AVG(close) > 0
                THEN (MAX(close) - MIN(close)) / AVG(close) * 100
                ELSE NULL END AS range_pct
            FROM eod_ohlcv
            WHERE symbol IN ({placeholders})
              AND date >= date('now', ?)
            GROUP BY symbol""",
        tuple(syms) + (f"-{days} days",),
    )
    return [dict(r) for r in cur.fetchall()]
