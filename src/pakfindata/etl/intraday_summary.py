"""ETL: intraday summary tables (daily / minute breadth / hourly / index minute).

Two entry points used by:

- Worker handlers in ``worker/handlers/build_intraday_summary*.py``
- Intraday Sync tab inline fallbacks ("Build Summaries for {date}"
  and "Build Missing (all JSONL dates)")

Both wrap :func:`pakfindata.db.repositories.intraday_summary.compute_all`
which aggregates tick logs in ``tick_logs_cloud/ticks_{date}.jsonl``
into four summary tables via in-memory DuckDB. Output is written to
SQLite.

Catalog datasets touched on success (per date)::

    intraday_daily_summary    source='computed'
    intraday_minute_breadth   source='computed'
    intraday_hourly_summary   source='computed'

(``intraday_index_minute`` is also written by ``compute_all`` but the
catalog row was not previously tracked — leaving that out to match
the existing button behavior.)
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.connections import sqlite_con
from pakfindata.db.repositories import intraday_summary as _isum
from pakfindata.db.safe_writer import safe_writer

DATASETS: tuple[tuple[str, str], ...] = (
    ("intraday_daily_summary", "computed"),
    ("intraday_minute_breadth", "computed"),
    ("intraday_hourly_summary", "computed"),
)


def _record_all_failed(exc: BaseException) -> None:
    for dataset, source in DATASETS:
        record_catalog_failure(dataset, source=source, error=exc)


def build_for_date(date: str) -> dict:
    """Aggregate tick JSONL for a single date into the summary tables.

    Args:
        date: YYYY-MM-DD; must correspond to an existing
            ``tick_logs_cloud/ticks_{date}.jsonl`` file.

    Returns:
        Dict shaped::

            {
                "date":            str,
                "source":          str,                       # 'jsonl' or None
                "daily":           int,
                "minute_breadth":  int,
                "hourly":          int,
                "index_minute":    int,
                "timings":         dict,
                "skipped_reason":  str | None,
                "duration_ms":     int,
                "as_of":           str,
            }
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        with safe_writer() as con:
            result = _isum.compute_all(con, date)
            if "error" in result:
                # Source not available — no DB writes happened; don't
                # mark catalog failed.
                return {
                    "date": date,
                    "source": result.get("source"),
                    "skipped_reason": result["error"],
                    "duration_ms": int((time.monotonic() - t0) * 1000),
                    "as_of": as_of,
                }
            for dataset, source in DATASETS:
                update_catalog_from_table(con, dataset, source=source)
    except Exception as exc:
        _record_all_failed(exc)
        raise

    return {
        "date": date,
        "source": result.get("source"),
        "daily": int(result.get("daily", 0)),
        "minute_breadth": int(result.get("minute_breadth", 0)),
        "hourly": int(result.get("hourly", 0)),
        "index_minute": int(result.get("index_minute", 0)),
        "timings": result.get("timings", {}),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }


def build_missing() -> dict:
    """Build summaries for every JSONL date not yet in the summary tables.

    Discovery: lists ``/mnt/e/psxdata/tick_logs_cloud/ticks_*.jsonl``
    files, finds dates not already in ``intraday_daily_summary``, then
    runs :func:`build_for_date` for each in sequence (each date is its
    own ``safe_writer`` block so a single bad date doesn't block the rest).

    Returns:
        ``{dates_considered, dates_built, dates_failed,
           dates_skipped, results, duration_ms, as_of}``
    """
    from pathlib import Path

    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    jl_root = Path("/mnt/e/psxdata/tick_logs_cloud")
    jl_dates: list[str] = []
    if jl_root.is_dir():
        jl_dates = sorted(
            (p.stem.replace("ticks_", "") for p in jl_root.glob("ticks_*.jsonl")),
            reverse=True,
        )

    con = sqlite_con()
    try:
        existing = set(_isum.get_summary_dates(con))
    finally:
        con.close()

    missing = [d for d in jl_dates if d not in existing]

    built = 0
    failed = 0
    skipped = 0
    results: list[dict] = []
    for d in missing:
        try:
            r = build_for_date(d)
            results.append({"date": d, "outcome": "ok", **{k: r.get(k) for k in (
                "daily", "minute_breadth", "hourly", "index_minute",
                "skipped_reason"
            )}})
            if r.get("skipped_reason"):
                skipped += 1
            else:
                built += 1
        except Exception as exc:
            failed += 1
            results.append({"date": d, "outcome": "failed", "error": str(exc)[:200]})

    return {
        "dates_considered": len(missing),
        "dates_built": built,
        "dates_failed": failed,
        "dates_skipped": skipped,
        "results": results,
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
