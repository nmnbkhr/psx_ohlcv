"""Composite repository: research views.

One composite endpoint today: ``movers-enriched``. Combines the
``eod_ohlcv`` movers view with ``sectors`` (sector-name decode) and
``trading_sessions`` (P/E + YTD + 1y-change enrichment).

The trading_sessions enrichment is currently 55 days stale (latest
session_date 2026-03-29 vs today 2026-05-23). The composite still
serves it — staleness is surfaced via the ``data_quality`` field per
``composite_aggregator_pattern.md`` §7. UI renders a banner.

Reads from:
    eod_ohlcv      — close, prev_close, change_pct, volume, turnover
    sectors        — sector_name lookup (sector_code → sector_name)
    trading_sessions — pe_ratio_ttm, ytd_change, year_1_change

All identifier-like inputs are constrained by the route layer (enum
``direction`` validated by Pydantic Literal type; ``sector`` is a free
text but used only as a parameterized WHERE value, never interpolated).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any, Literal

import pandas as pd

Direction = Literal["gainers", "losers", "volume", "value"]


def _latest_eod_date(con: sqlite3.Connection) -> str | None:
    row = con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
    return row[0] if row and row[0] else None


def _latest_trading_session_date(con: sqlite3.Connection) -> str | None:
    row = con.execute(
        "SELECT MAX(session_date) FROM trading_sessions WHERE market_type = 'REG'"
    ).fetchone()
    return row[0] if row and row[0] else None


def _days_stale(asof: str | None, reference: str | None) -> int | None:
    if not asof or not reference:
        return None
    try:
        asof_d = datetime.strptime(asof, "%Y-%m-%d").date()
        ref_d = datetime.strptime(reference, "%Y-%m-%d").date()
        return (asof_d - ref_d).days
    except (TypeError, ValueError):
        return None


def get_movers_enriched(
    con: sqlite3.Connection,
    *,
    direction: Direction = "gainers",
    top_n: int = 15,
    sector: str | None = None,
    pe_max: float | None = None,
    min_volume: int = 50_000,
) -> dict[str, Any]:
    """Return a movers-enriched composite for the latest EOD date.

    Behaviour per direction:
        gainers : ORDER BY change_pct DESC
        losers  : ORDER BY change_pct ASC
        volume  : ORDER BY volume DESC
        value   : WHERE pe BETWEEN (0, pe_max or 15] AND volume > 10_000,
                  ORDER BY pe ASC

    Returns a dict shaped per ``composite_aggregator_pattern.md`` §5:
        {as_of, direction, rows[], data_quality{...}}
    """
    as_of = _latest_eod_date(con)
    ts_latest = _latest_trading_session_date(con)

    rows: list[dict[str, Any]] = []
    if as_of is None:
        # No EOD data at all — degenerate but valid response shape.
        return {
            "as_of": None,
            "direction": direction,
            "rows": rows,
            "data_quality": _data_quality(con, as_of, ts_latest),
        }

    where_clauses = [
        "e.date = ?",
        "e.close > 0",
        "e.volume > ?",
    ]
    params: list[Any] = [as_of, min_volume]

    if direction == "value":
        # value-picks semantics: P/E filter is the defining feature.
        where_clauses.append("ts.pe_ratio_ttm > 0")
        if pe_max is not None:
            where_clauses.append("ts.pe_ratio_ttm <= ?")
            params.append(pe_max)
        else:
            where_clauses.append("ts.pe_ratio_ttm < 15")
        order_by = "ts.pe_ratio_ttm ASC"
    elif direction == "gainers":
        order_by = "change_pct DESC"
    elif direction == "losers":
        order_by = "change_pct ASC"
    elif direction == "volume":
        order_by = "e.volume DESC"
    else:  # pragma: no cover — Literal guards this at the route layer
        raise ValueError(f"unknown direction: {direction!r}")

    if sector is not None:
        where_clauses.append("s.sector_name = ?")
        params.append(sector)

    params.append(top_n)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT e.symbol,
               e.close,
               ROUND(CASE WHEN e.prev_close > 0
                          THEN (e.close - e.prev_close) / e.prev_close * 100
                          WHEN e.open > 0
                          THEN (e.close - e.open) / e.open * 100
                          ELSE NULL END, 2) AS change_pct,
               e.volume,
               e.turnover,
               s.sector_name,
               ts.pe_ratio_ttm,
               ts.ytd_change,
               ts.year_1_change
          FROM eod_ohlcv e
          LEFT JOIN sectors s
            ON '0' || e.sector_code = s.sector_code
          LEFT JOIN trading_sessions ts
            ON e.symbol = ts.symbol
           AND ts.market_type = 'REG'
           AND ts.session_date = (
               SELECT MAX(session_date) FROM trading_sessions
                WHERE market_type = 'REG'
           )
         WHERE {where_sql}
         ORDER BY {order_by}
         LIMIT ?
    """

    df = pd.read_sql_query(sql, con, params=params)
    rows = df.to_dict(orient="records")

    return {
        "as_of": as_of,
        "direction": direction,
        "rows": rows,
        "data_quality": _data_quality(con, as_of, ts_latest),
    }


def _data_quality(
    con: sqlite3.Connection,
    eod_date: str | None,
    ts_latest: str | None,
) -> dict[str, dict[str, Any]]:
    """Surface per-source freshness for the response.

    Per ``composite_aggregator_pattern.md`` §7. eod_ohlcv has a catalog
    row (domain=equity_eod) we read from. sectors + trading_sessions
    have no catalog rows yet, so freshness is computed directly.
    """
    out: dict[str, dict[str, Any]] = {}

    catalog = con.execute(
        "SELECT status, last_row_date FROM data_freshness "
        "WHERE domain = 'equity_eod'"
    ).fetchone()
    if catalog:
        status, last_row_date = catalog
        out["eod_ohlcv"] = {
            "status": status,
            "last_row_date": last_row_date,
        }
    else:
        out["eod_ohlcv"] = {
            "status": "unknown",
            "last_row_date": eod_date,
        }

    sectors_count = con.execute(
        "SELECT COUNT(*) FROM sectors"
    ).fetchone()[0]
    out["sectors"] = {
        "status": "ok" if sectors_count > 0 else "unknown",
        "row_count": sectors_count,
    }

    today_str = date.today().isoformat()
    days_stale = _days_stale(today_str, ts_latest)
    if ts_latest is None:
        ts_status = "unknown"
    elif days_stale is not None and days_stale > 7:
        ts_status = "stale"
    else:
        ts_status = "ok"
    ts_entry: dict[str, Any] = {
        "status": ts_status,
        "last_row_date": ts_latest,
    }
    if days_stale is not None and days_stale > 0:
        ts_entry["days_stale"] = days_stale
    out["trading_sessions"] = ts_entry

    return out
