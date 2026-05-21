"""ETL: EOD summary tables (market / sector / symbol).

Three entry points used by:

- CLI handlers in ``cli.py`` for ``pfsync summary rebuild-today`` /
  ``rebuild-missing`` (cron + manual)
- Worker handlers in ``worker/handlers/rebuild_eod_summary_*.py``
- Dashboard inline fallbacks for the three "Rebuild …" buttons

The summary tables (``eod_market_summary``, ``eod_sector_summary``,
``eod_symbol_summary``) are deterministic rollups of ``eod_ohlcv``.
Three callers cover the common cases:

- :func:`rebuild_today` — refresh the latest trading day only (cheap,
  ~1s).
- :func:`rebuild_missing` — fill any dates that have ``eod_ohlcv``
  rows but no summary row (medium cost, depends on backlog).
- :func:`rebuild_all` — full rebuild of every date (expensive,
  30-60 min; only invoked manually).

The bulk variants delegate to
:func:`pakfindata.db.repositories.market_summary.refresh_eod_summary_bulk`
which opens its own ``safe_writer`` per batch — we do *not* wrap the
bulk call in another ``safe_writer`` (nested locks).

Catalog datasets touched on success::

    eod_market_summary   source='computed'
    eod_sector_summary   source='computed'
    eod_symbol_summary   source='computed'
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.connections import sqlite_con
from pakfindata.db.repositories.market_summary import (
    init_eod_summary_schema,
    refresh_eod_summary,
    refresh_eod_summary_bulk,
)
from pakfindata.db.safe_writer import safe_writer

DATASETS: tuple[tuple[str, str], ...] = (
    ("eod_market_summary", "computed"),
    ("eod_sector_summary", "computed"),
    ("eod_symbol_summary", "computed"),
)


def _record_all_failed(exc: BaseException) -> None:
    for dataset, source in DATASETS:
        record_catalog_failure(dataset, source=source, error=exc)


def _latest_full_trading_day() -> str | None:
    """Most-recent date in eod_ohlcv with prev_close > 0."""
    con = sqlite_con()
    try:
        row = con.execute(
            "SELECT MAX(date) FROM eod_ohlcv WHERE prev_close > 0"
        ).fetchone()
        return row[0] if row and row[0] else None
    finally:
        con.close()


def _update_catalog(con) -> None:
    for dataset, source in DATASETS:
        update_catalog_from_table(con, dataset, source=source)


def rebuild_today(date: str | None = None) -> dict:
    """Rebuild EOD summary tables for the latest trading day.

    Args:
        date: Optional explicit YYYY-MM-DD. If None, auto-detected as
            the most recent ``eod_ohlcv`` date.

    Returns:
        ``{date, rows_written, duration_ms, as_of}`` — ``rows_written``
        is the count returned by ``refresh_eod_summary`` (symbol-level
        rows for the target date).
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    target = date or _latest_full_trading_day()
    if not target:
        return {
            "date": None,
            "rows_written": 0,
            "skipped_reason": "no eod_ohlcv rows",
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "as_of": as_of,
        }

    try:
        with safe_writer() as con:
            init_eod_summary_schema(con)
            rows_written = refresh_eod_summary(con, target)
            _update_catalog(con)
    except Exception as exc:
        _record_all_failed(exc)
        raise

    return {
        "date": target,
        "rows_written": int(rows_written),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }


def rebuild_missing(batch_size: int = 50) -> dict:
    """Fill in EOD summary rows for any dates not yet summarized.

    Does NOT wrap the bulk refresh in safe_writer — ``refresh_eod_summary_bulk``
    opens its own safe_writer per batch. The catalog write runs in a
    small separate transaction after all batches commit.

    Args:
        batch_size: Forwarded to ``refresh_eod_summary_bulk``.

    Returns:
        ``{dates_considered, dates_processed, batches, rows_written,
           duration_ms, as_of}``
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        r = refresh_eod_summary_bulk(only_missing=True, batch_size=batch_size)
        with safe_writer() as con:
            _update_catalog(con)
    except Exception as exc:
        _record_all_failed(exc)
        raise

    return {
        "dates_considered": int(r.get("dates_considered", 0)),
        "dates_processed": int(r.get("dates_processed", 0)),
        "batches": int(r.get("batches", 0)),
        "rows_written": int(r.get("rows_written", 0)),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }


def rebuild_all(batch_size: int = 50) -> dict:
    """Full rebuild of every date in ``eod_ohlcv``.

    Long-running (30-60 min). Same shape as :func:`rebuild_missing`
    but with ``only_missing=False`` — every date is re-computed.
    Intended for fire-and-forget UI flows; the worker handler runs in
    a separate process so the UI is not blocked.

    Args:
        batch_size: Forwarded to ``refresh_eod_summary_bulk``.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        r = refresh_eod_summary_bulk(only_missing=False, batch_size=batch_size)
        with safe_writer() as con:
            _update_catalog(con)
    except Exception as exc:
        _record_all_failed(exc)
        raise

    return {
        "dates_considered": int(r.get("dates_considered", 0)),
        "dates_processed": int(r.get("dates_processed", 0)),
        "batches": int(r.get("batches", 0)),
        "rows_written": int(r.get("rows_written", 0)),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
