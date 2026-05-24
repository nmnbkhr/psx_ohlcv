"""Cleanup script for konia_daily pollution (FOLLOWUP-2b, sub-wave 2.A.5.5a).

Removes 633 misrouted daily-OHLCV-summary rows from konia_daily. Originally
filed as part of FOLLOWUP-2 (Phase 2.A.2 remediation notes); the konia_daily
slice is its own sub-wave per the per-table investigation discipline
established in 2.A.5.4.

The shape:

    konia_daily column     polluted value              actual meaning
    --------------------   -------------------------   --------------------
    date (TEXT PK)         symbol code (incl futures)  symbol
    rate_pct (REAL NN)     timestamp_ms at 16:00 PKT   close-marker epoch
    volume_billions        open price                  open
    high                   close price                 close (misleadingly named)
    low                    share volume                volume (misleadingly named)
    scraped_at='dps'       literal magic marker        sentinel for the misroute

Misroute provenance: defunct ingestion path wrote a 3-field daily summary
(open, close, volume) for each (symbol, date) tuple into konia_daily.
The canonical destinations were eod_ohlcv (regular equity) and
futures_eod (derivatives) which carry full OHLCV plus high, low, and
turnover.

The recovery audit (2.A.5.5 Step 0) cross-joined the 633 polluted rows
against eod_ohlcv + futures_eod and verified:

    Exact (open, close, volume) match in eod_ohlcv         502
    Exact match in futures_eod                              91
    Match with |close diff| <= 0.01 in eod_ohlcv           +31    (penny-precision)
    Match with |close diff| <= 0.01 in futures_eod         +3
    ----------------------------------------------------------
    Total duplicates (99.05% of polluted set):             627
    Post-delisting phantom rows:                             6
    ----------------------------------------------------------
    Grand total                                            633

The 6 orphan rows (BILF, DMTX, PMI, PMPK on dates >=4mo after their
2025-01-20 delistings; CJPL on 2026-03-16, 10 days after 2026-03-06
suspension; LIVENR never listed) are post-delisting phantoms produced by
the scraper continuing to poll already-delisted symbols. The polluted
3-field format itself is corrupt (no high/low/turnover); no canonical
price exists for these (symbol, date) tuples; deletion loses no useful
data.

Predicate (verified triple-convergent — all three forms match exactly 633):

    scraped_at = 'dps'
    AND date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'

The 2-criterion AND form is the cleanup script's choice: first criterion is
the source marker (literal sentinel 'dps' written by the misroute path),
second is the shape marker (date column not an ISO-8601 date string).

Gates (any divergence by even one row halts the run before commit):

  Pre-flight predictions (verified against canonical DB at script time):
    pre_total      = 3,334
    pred_match     =   633
    pred_post_total= 2,701
    pred_post_dates= 2,701
    eod_ohlcv      = 615,392 (must be unchanged post-run)
    futures_eod    = 370,839 (must be unchanged post-run)

  Mid-run safety: 5% breaker on deleted-count
    breaker_max = pred_match * 1.05 = 664

  Post-flight: all four post-conditions must match predictions exactly.
  If anything diverges, the safe_writer transaction rolls back.

Single-batch deletion (633 rows fits in one batch even at 10K batch size).
Catalog recompute for 'konia' domain runs inside the same safe_writer
transaction so freshness data stays in sync.

Manual backup required before live run:

    cp ~/psxdata_rescue/psx.sqlite \\
       /tmp/psx_pre_2a5_5a_$(date +%Y%m%d_%H%M).sqlite

Usage:
    python scripts/cleanup_konia_daily_pollution.py --dry-run
    python scripts/cleanup_konia_daily_pollution.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


PREDICATE_WHERE = (
    "scraped_at = 'dps' "
    "AND date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"
)

# Predictions verified live against /home/smnb/psxdata_rescue/psx.sqlite
# at script-authoring time (2026-05-24). If any of these diverge by even
# one row at runtime, the script halts.
PRED_PRE_TOTAL: int = 3_334
PRED_MATCH: int = 633
PRED_POST_TOTAL: int = 2_701
PRED_POST_DATES: int = 2_701
PRED_EOD_OHLCV: int = 615_392
PRED_FUTURES_EOD: int = 370_839

BATCH_SIZE: int = 10_000
BREAKER_MAX: int = int(PRED_MATCH * 1.05)  # 664


def _readonly_con(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _snapshot(con: sqlite3.Connection) -> dict[str, int]:
    """Take the six gating counts in one read pass."""
    return {
        "pre_total":   con.execute("SELECT COUNT(*) FROM konia_daily").fetchone()[0],
        "match":       con.execute(
            f"SELECT COUNT(*) FROM konia_daily WHERE {PREDICATE_WHERE}"
        ).fetchone()[0],
        "post_total":  con.execute(
            f"SELECT COUNT(*) FROM konia_daily WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "post_dates":  con.execute(
            f"SELECT COUNT(DISTINCT date) FROM konia_daily WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "eod_ohlcv":   con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0],
        "futures_eod": con.execute("SELECT COUNT(*) FROM futures_eod").fetchone()[0],
    }


def _check_preflight(snap: dict[str, int]) -> None:
    expected = {
        "pre_total":   PRED_PRE_TOTAL,
        "match":       PRED_MATCH,
        "post_total":  PRED_POST_TOTAL,
        "post_dates":  PRED_POST_DATES,
        "eod_ohlcv":   PRED_EOD_OHLCV,
        "futures_eod": PRED_FUTURES_EOD,
    }
    failed: list[str] = []
    for key, pred in expected.items():
        actual = snap[key]
        marker = "ok" if actual == pred else "MISMATCH"
        print(f"  {key:<12} predicted={pred:>12,}  actual={actual:>12,}  [{marker}]")
        if actual != pred:
            failed.append(f"{key}: predicted {pred:,}, got {actual:,}")
    if failed:
        raise RuntimeError(
            "Pre-flight gate FAILED. Disposition needs revisit before commit.\n"
            + "\n".join(f"  - {f}" for f in failed)
        )


def _batched_delete(con: sqlite3.Connection) -> int:
    deleted_total = 0
    delete_sql = (
        f"DELETE FROM konia_daily WHERE rowid IN ("
        f"  SELECT rowid FROM konia_daily WHERE {PREDICATE_WHERE} LIMIT ?"
        f")"
    )
    while True:
        cur = con.execute(delete_sql, (BATCH_SIZE,))
        n = cur.rowcount
        if n == 0:
            break
        deleted_total += n
        pct = 100.0 * deleted_total / PRED_MATCH
        print(f"  [APPLIED] deleted {deleted_total:>5,} / {PRED_MATCH:,}  ({pct:5.1f}%)")
        if deleted_total > BREAKER_MAX:
            raise RuntimeError(
                f"5% safety breaker tripped: deleted={deleted_total:,} > "
                f"breaker_max={BREAKER_MAX:,}. Rolling back."
            )
    return deleted_total


def _check_postflight(con: sqlite3.Connection, deleted: int) -> None:
    if deleted != PRED_MATCH:
        raise RuntimeError(
            f"Delete-count mismatch: deleted={deleted:,} expected={PRED_MATCH:,}"
        )
    snap = _snapshot(con)
    # After DELETE, _snapshot's "pre_total" really reads post-cleanup state.
    checks = [
        ("post_total",            snap["pre_total"],   PRED_POST_TOTAL),
        ("post_dates",            snap["post_dates"],  PRED_POST_DATES),
        ("eod_ohlcv_unchanged",   snap["eod_ohlcv"],   PRED_EOD_OHLCV),
        ("futures_eod_unchanged", snap["futures_eod"], PRED_FUTURES_EOD),
    ]
    failed: list[str] = []
    for key, actual, pred in checks:
        marker = "ok" if actual == pred else "MISMATCH"
        print(f"  {key:<22} predicted={pred:>12,}  actual={actual:>12,}  [{marker}]")
        if actual != pred:
            failed.append(f"{key}: predicted {pred:,}, got {actual:,}")
    if failed:
        raise RuntimeError(
            "Post-flight gate FAILED. Rolling back.\n"
            + "\n".join(f"  - {f}" for f in failed)
        )


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"konia_daily cleanup (2.A.5.5a) — DB: {db_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"Predicate: {PREDICATE_WHERE}")
    print()

    print("Step 1 — Pre-flight gate (predictions vs current state):")
    ro = _readonly_con(db_path)
    try:
        snap = _snapshot(ro)
    finally:
        ro.close()
    _check_preflight(snap)

    if dry_run:
        print()
        print(f"  DRY: would delete {PRED_MATCH:,} rows in batches of {BATCH_SIZE:,}")
        print(f"  DRY: 5% safety breaker max = {BREAKER_MAX:,}")
        print(f"  DRY: would recompute data_freshness for domain 'konia'")
        print()
        print("DRY RUN complete. To apply: re-run without --dry-run "
              "(remember to back up the DB first).")
        return 0

    print()
    print(f"Step 2 — Batched DELETE (batch={BATCH_SIZE:,}, breaker={BREAKER_MAX:,}):")
    with safe_writer() as con:
        deleted = _batched_delete(con)
        print()
        print("Step 3 — Post-flight gate:")
        _check_postflight(con, deleted)
        print()
        print("Step 4 — Catalog recompute for domain 'konia':")
        update_catalog_from_table(con, "konia", source="cleanup_2a5_5a")
        row = con.execute(
            "SELECT last_row_date, row_count FROM data_freshness WHERE domain = 'konia'"
        ).fetchone()
        if row is not None:
            print(f"  [APPLIED] konia: last_row_date={row[0]!r} row_count={row[1]}")
    print()
    print("Cleanup complete. FOLLOWUP-2b closed.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview gate state without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
