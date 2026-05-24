"""Stuck-job detection across worker queues and *_sync_runs tables.

Read-only. Returns structured data describing rows that have been in a
"running" state for longer than a configurable threshold. Consumers
(daily digest, Streamlit viewer, ad-hoc CLI) decide how to display or
act on the results.

Relationship to `pakfindata.worker.sweep`:

  worker.sweep      runs at worker startup; PID-based liveness check;
                    WRITES to mark jobs as `failed` when the worker is
                    gone. Action is corrective.

  this module       runs from the daily digest (and on-demand CLI);
                    time-threshold check; READ-ONLY surfacing of state.
                    Action is operator visibility.

Both abstractions are correct. Same-named-different-mechanism is the
"surface-similarity != root-cause similarity" pattern (Phase 2.A.5.6c)
applied at the abstraction layer instead of the data layer.

Coverage scope (10 tables, two predicate conventions):

  status='running' (7 tables):
    jobs, scrape_jobs, bond_sync_runs, fi_sync_runs, fx_sync_runs,
    mutual_fund_sync_runs, sukuk_sync_runs

  ended_at IS NULL (3 tables):
    commodity_sync_runs, instruments_sync_runs, sync_runs

Adding a new variant table: append a `_TableSpec` to `SYNC_TABLES`
below. Schema-introspection was rejected at design time because
implicit detection breaks on the edge cases (e.g. a future
sync_runs-variant that has both `status` and `ended_at` with different
semantics). The explicit list forces awareness when scope grows.

Usage:

  from pakfindata.observability.stuck_jobs import find_stuck_jobs
  stuck = find_stuck_jobs(threshold_hours=24)
  for row in stuck:
      print(row.table, row.id, row.started_at, row.age_hours)

  # CLI:
  pfsync stale-sync-sweep --threshold 24
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from typing import NamedTuple


class _TableSpec(NamedTuple):
    table: str
    id_column: str
    timestamp_column: str
    stuck_predicate: str


# The full predicate dict. Order is stable so digest output is
# deterministic across runs.
SYNC_TABLES: tuple[_TableSpec, ...] = (
    _TableSpec("jobs",                   "id",      "enqueued_at", "status = 'running'"),
    _TableSpec("scrape_jobs",            "job_id",  "started_at",  "status = 'running'"),
    _TableSpec("bond_sync_runs",         "run_id",  "started_at",  "status = 'running'"),
    _TableSpec("fi_sync_runs",           "run_id",  "started_at",  "status = 'running'"),
    _TableSpec("fx_sync_runs",           "run_id",  "started_at",  "status = 'running'"),
    _TableSpec("mutual_fund_sync_runs",  "run_id",  "started_at",  "status = 'running'"),
    _TableSpec("sukuk_sync_runs",        "run_id",  "started_at",  "status = 'running'"),
    _TableSpec("commodity_sync_runs",    "run_id",  "started_at",  "ended_at IS NULL"),
    _TableSpec("instruments_sync_runs",  "run_id",  "started_at",  "ended_at IS NULL"),
    _TableSpec("sync_runs",              "run_id",  "started_at",  "ended_at IS NULL"),
)


@dataclass(frozen=True)
class StuckJob:
    """Single stuck row identified by table + id + timestamp."""
    table: str
    id: str          # always rendered as str — schemas mix INTEGER (jobs) and TEXT (sync_runs)
    started_at: str
    age_hours: float

    def __str__(self) -> str:
        return f"{self.table}.{self.id} running since {self.started_at} ({self.age_hours:.1f}h)"


def _readonly_con(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def find_stuck_jobs(
    threshold_hours: float = 24.0,
    db_path: str | None = None,
    con: sqlite3.Connection | None = None,
) -> list[StuckJob]:
    """Return rows running longer than threshold_hours across all 10 tables.

    Either `con` (an existing read-only connection) or `db_path` must
    be provided. If neither is given, `pakfindata.config.get_db_path()`
    is used.

    The returned list is sorted by age_hours descending — oldest stuck
    rows first — so digest output lands the most-urgent items at the top.
    """
    if con is None:
        if db_path is None:
            from pakfindata.config import get_db_path
            db_path = get_db_path()
        con = _readonly_con(db_path)
        owns_con = True
    else:
        owns_con = False

    try:
        results: list[StuckJob] = []
        for spec in SYNC_TABLES:
            # age in hours = (julianday('now') - julianday(<ts>)) * 24
            sql = (
                f"SELECT {spec.id_column} AS id, {spec.timestamp_column} AS started_at, "
                f"(julianday('now') - julianday({spec.timestamp_column})) * 24.0 AS age_hours "
                f"FROM {spec.table} "
                f"WHERE {spec.stuck_predicate} "
                f"AND (julianday('now') - julianday({spec.timestamp_column})) * 24.0 >= ?"
            )
            for row in con.execute(sql, (threshold_hours,)).fetchall():
                results.append(
                    StuckJob(
                        table=spec.table,
                        id=str(row["id"]),
                        started_at=row["started_at"],
                        age_hours=float(row["age_hours"]),
                    )
                )
        results.sort(key=lambda r: r.age_hours, reverse=True)
        return results
    finally:
        if owns_con:
            con.close()


def main(argv: list[str] | None = None) -> int:
    """`pfsync stale-sync-sweep` CLI entry point.

    Also runnable as `python -m pakfindata.observability.stuck_jobs`.
    """
    p = argparse.ArgumentParser(
        description="Detect stuck jobs across the worker queues and *_sync_runs tables",
    )
    p.add_argument(
        "--threshold", type=float, default=24.0,
        help="age threshold in hours (default: 24)",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="suppress header; print stuck rows only (script-friendly)",
    )
    args = p.parse_args(argv)

    stuck = find_stuck_jobs(threshold_hours=args.threshold)
    if not args.quiet:
        print(f"stuck-job sweep — threshold={args.threshold}h — {len(stuck)} rows across {len({s.table for s in stuck})} tables")
        if stuck:
            print()
    for row in stuck:
        print(f"  {row}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
