"""Intraday + Turnover endpoints — /v1/intraday, /v1/turnover.

Backs the Group E pages (intraday.py Dashboard/Charts/Market Pulse,
post_close.py). All read-only, all auth-gated by the global Bearer
middleware.

Scope notes:
- Reads only. JSONL tick-log files, DuckDB index_ohlcv_5s, intraday
  writes (upsert_intraday, promote_intraday_to_eod) are DEFER (Scope
  v2) and not exposed here.
- ``live_ticker.py`` reads ``live_snapshot.json`` directly (file
  artifact, not DB) — also DEFER.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

import pandas as pd
from fastapi import APIRouter, Depends, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.common import df_to_records
from pakfindata.api.schemas.intraday import (
    IntradayBarRow,
    IntradayHourlyBreadthRow,
    IntradayIndexMinuteRow,
    IntradayMinuteBreadthRow,
    IntradaySummaryRow,
    PostCloseRow,
    PostCloseStats,
)

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"

intraday_router = APIRouter(prefix="/v1/intraday", tags=["intraday"])
turnover_router = APIRouter(prefix="/v1/turnover", tags=["turnover"])


# ── /v1/intraday ────────────────────────────────────────────────────


@intraday_router.get("/summary", response_model=list[IntradaySummaryRow])
def get_intraday_summary(
    date: Annotated[str, Query(description="Date (YYYY-MM-DD)", pattern=DATE_RE)],
    market: Annotated[str, Query(description="Market segment (REG/FUT/ODL)")] = "REG",
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Per-symbol day OHLCV summary from ``intraday_daily_summary``."""
    df = pd.read_sql_query(
        """SELECT date, symbol, market, day_open, day_high, day_low,
                  day_close, prev_close, day_volume, day_trades,
                  turnover, vwap, tick_count, first_ts, last_ts,
                  change, change_pct
             FROM intraday_daily_summary
            WHERE date = ? AND market = ?
            ORDER BY day_volume DESC""",
        con, params=(date, market),
    )
    return df_to_records(df)


@intraday_router.get("/bars", response_model=list[IntradayBarRow])
def get_intraday_bars(
    symbol: Annotated[str, Query(description="Symbol (case-insensitive)")],
    date: Annotated[str, Query(description="Date (YYYY-MM-DD)", pattern=DATE_RE)],
    interval: Annotated[str, Query(description="Bar interval (1s/1m/5m/...)")] = "1s",
    limit: Annotated[int, Query(ge=1, le=50000, description="Max rows")] = 20000,
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Single-symbol tick bars from ``intraday_bars``."""
    df = pd.read_sql_query(
        """SELECT symbol, market, date, ts, ts_epoch, interval,
                  open, high, low, close, volume, value,
                  trade_count, vwap
             FROM intraday_bars
            WHERE symbol = ? AND date = ? AND interval = ?
            ORDER BY ts_epoch ASC
            LIMIT ?""",
        con, params=(symbol.upper(), date, interval, limit),
    )
    return df_to_records(df)


@intraday_router.get("/breadth/minute", response_model=list[IntradayMinuteBreadthRow])
def get_intraday_minute_breadth(
    date: Annotated[str, Query(description="Date (YYYY-MM-DD)", pattern=DATE_RE)],
    market: Annotated[str, Query(description="Market segment")] = "REG",
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Minute-level breadth from ``intraday_minute_breadth``."""
    df = pd.read_sql_query(
        """SELECT date, minute, market, advancing, declining,
                  unchanged, total_symbols, net_ticks
             FROM intraday_minute_breadth
            WHERE date = ? AND market = ?
            ORDER BY minute""",
        con, params=(date, market),
    )
    return df_to_records(df)


@intraday_router.get("/breadth/hourly", response_model=list[IntradayHourlyBreadthRow])
def get_intraday_hourly_breadth(
    date: Annotated[str, Query(description="Date (YYYY-MM-DD)", pattern=DATE_RE)],
    market: Annotated[str, Query(description="Market segment")] = "REG",
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Hourly tick / symbol counts from ``intraday_hourly_summary``."""
    df = pd.read_sql_query(
        """SELECT date, hour, market, tick_count, symbol_count
             FROM intraday_hourly_summary
            WHERE date = ? AND market = ?
            ORDER BY hour""",
        con, params=(date, market),
    )
    return df_to_records(df)


@intraday_router.get("/index-minute", response_model=list[IntradayIndexMinuteRow])
def get_intraday_index_minute(
    date: Annotated[str, Query(description="Date (YYYY-MM-DD)", pattern=DATE_RE)],
    symbols: Annotated[
        str,
        Query(description="Comma-separated index symbols (e.g. KSE-100,KSE-30)"),
    ],
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Per-minute index values from ``intraday_index_minute``.

    Renames ``last_value`` to ``value`` to match the field on
    :class:`IntradayIndexMinuteRow`.
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return []
    placeholders = ",".join("?" * len(sym_list))
    df = pd.read_sql_query(
        f"""SELECT date, minute, symbol, last_value AS value
              FROM intraday_index_minute
             WHERE date = ? AND symbol IN ({placeholders})
             ORDER BY minute""",
        con, params=[date] + sym_list,
    )
    return df_to_records(df)


# ── /v1/turnover ────────────────────────────────────────────────────


@turnover_router.get("/stats", response_model=PostCloseStats)
def get_turnover_stats(con: sqlite3.Connection = Depends(get_read_db)):
    """Aggregate stats over ``post_close_turnover``."""
    row = con.execute(
        """SELECT COUNT(*) AS total_rows,
                  COUNT(DISTINCT date) AS total_dates,
                  COUNT(DISTINCT symbol) AS unique_symbols,
                  MIN(date) AS min_date,
                  MAX(date) AS max_date
             FROM post_close_turnover"""
    ).fetchone()
    return PostCloseStats(
        total_rows=row["total_rows"] or 0,
        total_dates=row["total_dates"] or 0,
        unique_symbols=row["unique_symbols"] or 0,
        min_date=row["min_date"],
        max_date=row["max_date"],
    )


@turnover_router.get("/dates", response_model=list[str])
def get_turnover_dates(con: sqlite3.Connection = Depends(get_read_db)):
    """Distinct dates with post_close data, newest first."""
    rows = con.execute(
        "SELECT DISTINCT date FROM post_close_turnover ORDER BY date DESC"
    ).fetchall()
    return [r["date"] for r in rows]


@turnover_router.get("/missing", response_model=list[str])
def get_turnover_missing(
    since: Annotated[
        str,
        Query(description="Range start (YYYY-MM-DD)", pattern=DATE_RE),
    ] = "2024-01-01",
    until: Annotated[
        Optional[str],
        Query(description="Range end (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Dates with EOD but no post_close_turnover rows."""
    query = """SELECT DISTINCT e.date
                 FROM eod_ohlcv e
                 LEFT JOIN (SELECT DISTINCT date FROM post_close_turnover) pc
                   ON e.date = pc.date
                WHERE pc.date IS NULL AND e.date >= ?"""
    params: list = [since]
    if until:
        query += " AND e.date <= ?"
        params.append(until)
    query += " ORDER BY e.date DESC"
    rows = con.execute(query, params).fetchall()
    return [r["date"] for r in rows]


@turnover_router.get("", response_model=list[PostCloseRow])
def get_turnover(
    date: Annotated[
        Optional[str],
        Query(description="Filter by date (YYYY-MM-DD)", pattern=DATE_RE),
    ] = None,
    symbol: Annotated[
        Optional[str],
        Query(description="Filter by symbol (case-insensitive)"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=10000, description="Max rows")] = 2000,
    con: sqlite3.Connection = Depends(get_read_db),
):
    """Rows from ``post_close_turnover`` with optional filters."""
    query = "SELECT * FROM post_close_turnover WHERE 1=1"
    params: list = []
    if date:
        query += " AND date = ?"
        params.append(date)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    query += " ORDER BY date DESC, turnover DESC LIMIT ?"
    params.append(limit)
    df = pd.read_sql_query(query, con, params=params)
    return df_to_records(df)
