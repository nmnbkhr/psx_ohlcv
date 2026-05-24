"""Cleanup script for regular_market_current pollution (FOLLOWUP-2c part 2, sub-wave 2.A.5.6b).

Removes 46 column-swap-polluted rows from regular_market_current. Originally
filed as part of FOLLOWUP-2 (Phase 2.A.2 remediation notes); the
regular_market_current slice is its own sub-sub-wave following the 2.A.5.6
SPLIT decision (forex_kerb had a distinct root cause — synthetic test seed
— and was handled as 2.A.5.6a).

Root cause: column-swap bug in a defunct ingestion path. The polluted rows
exhibit swapped (symbol, ts) positions:

    regular_market_current.symbol (PK)  holds ISO timestamp '2026-MM-DDTHH:MM:SS...+05:00'
    regular_market_current.ts           holds a real symbol code (BOP, KEL, ...)

9 distinct ts-as-symbol values across 46 polluted rows — all 9 are in the
`symbols` master table. The canonical regular_market_current row for each
of those 9 symbols (PK=symbol, ts=ISO-timestamp) ALREADY exists with
either current state (2026-05-23) for actively-traded names or a frozen
last-snapshot for suspended ones (WASLR). Deleting the polluted rows
(PK=ISO-timestamp) does NOT touch the canonical rows because PKs differ.

Per-row classification of the 46 polluted rows (cross-checked against
eod_ohlcv with date extracted from the polluted symbol-column):

    Class                                            Count
    ------------------------------------------------ -----
    Exact (open, close, volume) match in eod_ohlcv    24    redundant duplicate
    Value-diff intraday snapshot                      15    not authoritative; canonical is eod_ohlcv close
    Weekend snapshot (PSX closed Sundays)              4    KEL x3, BOP x1 — off-hours scrape
    NC-suffix special trading class (no eod by design) 2    DSLNC, HASCOLNC
    Recently-stale phantom (1 day past last eod)       1    WASLR 2026-03-20 (eod stops 2026-03-19)
    -----------------------------------------------------
    Total                                             46

All 46 are safe to delete. Uniqueness in the polluted-table tail is
explained by scrape-timing (off-hours / pre-open / past-suspension), not
by recoverable canonical state. Per the 2.A.5.5b phantom-row pattern,
only the WASLR row is strictly phantom; the others are weekend or
special-class artifacts that have no canonical EOD by design.

Predicate (verified double-convergent — both forms match exactly 46):

    ts     NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
    AND symbol GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*'

Gates (any divergence by even one row halts the run before commit):

  Pre-flight predictions:
    pre_total                  =     695
    pred_match                 =      46
    pred_post_total            =     649
    pred_post_distinct_symbol  =     649 (PK is symbol)
    pred_canonical_9_survive   =       9 (canonical PK=symbol rows for the 9 ts-symbols)
    eod_ohlcv (invariance)     = 615,392

  Mid-run safety: 5% breaker on deleted-count
    breaker_max = pred_match * 1.05 = 48

  Post-flight: all four post-conditions must match predictions exactly,
  AND the 9 canonical (PK=symbol) rows for BOP/CNERGY/DSLNC/FNEL/
  HASCOLNC/HUMNL/KEL/UNITY/WASLR must all survive.

Manual backup required before live run:

    cp ~/psxdata_rescue/psx.sqlite \\
       /tmp/psx_pre_2a5_6b_$(date +%Y%m%d_%H%M).sqlite

Usage:
    python scripts/cleanup_regular_market_current_pollution.py --dry-run
    python scripts/cleanup_regular_market_current_pollution.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


PREDICATE_WHERE = (
    "ts NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*' "
    "AND symbol GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*'"
)

CANONICAL_9 = (
    "BOP", "CNERGY", "DSLNC", "FNEL", "HASCOLNC",
    "HUMNL", "KEL", "UNITY", "WASLR",
)

PRED_PRE_TOTAL: int = 695
PRED_MATCH: int = 46
PRED_POST_TOTAL: int = 649
PRED_POST_DISTINCT_SYMBOL: int = 649
PRED_CANONICAL_9: int = 9
PRED_EOD_OHLCV: int = 615_392

BATCH_SIZE: int = 10_000
BREAKER_MAX: int = int(PRED_MATCH * 1.05)  # 48


def _readonly_con(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _snapshot(con: sqlite3.Connection) -> dict[str, int]:
    placeholders = ",".join("?" for _ in CANONICAL_9)
    return {
        "pre_total":     con.execute("SELECT COUNT(*) FROM regular_market_current").fetchone()[0],
        "match":         con.execute(
            f"SELECT COUNT(*) FROM regular_market_current WHERE {PREDICATE_WHERE}"
        ).fetchone()[0],
        "post_total":    con.execute(
            f"SELECT COUNT(*) FROM regular_market_current WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "post_distinct": con.execute(
            f"SELECT COUNT(DISTINCT symbol) FROM regular_market_current WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "canonical_9":   con.execute(
            f"SELECT COUNT(*) FROM regular_market_current WHERE symbol IN ({placeholders})",
            CANONICAL_9,
        ).fetchone()[0],
        "eod_ohlcv":     con.execute("SELECT COUNT(*) FROM eod_ohlcv").fetchone()[0],
    }


def _check_preflight(snap: dict[str, int]) -> None:
    expected = {
        "pre_total":     PRED_PRE_TOTAL,
        "match":         PRED_MATCH,
        "post_total":    PRED_POST_TOTAL,
        "post_distinct": PRED_POST_DISTINCT_SYMBOL,
        "canonical_9":   PRED_CANONICAL_9,
        "eod_ohlcv":     PRED_EOD_OHLCV,
    }
    failed: list[str] = []
    for key, pred in expected.items():
        actual = snap[key]
        marker = "ok" if actual == pred else "MISMATCH"
        print(f"  {key:<14} predicted={pred:>12,}  actual={actual:>12,}  [{marker}]")
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
        f"DELETE FROM regular_market_current WHERE rowid IN ("
        f"  SELECT rowid FROM regular_market_current WHERE {PREDICATE_WHERE} LIMIT ?"
        f")"
    )
    while True:
        cur = con.execute(delete_sql, (BATCH_SIZE,))
        n = cur.rowcount
        if n == 0:
            break
        deleted_total += n
        pct = 100.0 * deleted_total / PRED_MATCH
        print(f"  [APPLIED] deleted {deleted_total:>3,} / {PRED_MATCH:,}  ({pct:5.1f}%)")
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
    checks = [
        ("post_total",            snap["pre_total"],     PRED_POST_TOTAL),
        ("post_distinct_symbol",  snap["post_distinct"], PRED_POST_DISTINCT_SYMBOL),
        ("canonical_9_survive",   snap["canonical_9"],   PRED_CANONICAL_9),
        ("eod_ohlcv_unchanged",   snap["eod_ohlcv"],     PRED_EOD_OHLCV),
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
    print(f"regular_market_current cleanup (2.A.5.6b) — DB: {db_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"Predicate: {PREDICATE_WHERE}")
    print()

    print("Step 1 — Pre-flight gate:")
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
        print(f"  DRY: would recompute data_freshness for domain 'regular_market_current'")
        print()
        print("DRY RUN complete.")
        return 0

    print()
    print(f"Step 2 — Batched DELETE (batch={BATCH_SIZE:,}, breaker={BREAKER_MAX:,}):")
    with safe_writer() as con:
        deleted = _batched_delete(con)
        print()
        print("Step 3 — Post-flight gate:")
        _check_postflight(con, deleted)
        print()
        print("Step 4 — Catalog recompute for domain 'regular_market_current':")
        update_catalog_from_table(con, "regular_market_current", source="cleanup_2a5_6b")
        row = con.execute(
            "SELECT last_row_date, row_count FROM data_freshness WHERE domain = 'regular_market_current'"
        ).fetchone()
        if row is not None:
            print(f"  [APPLIED] regular_market_current: last_row_date={row[0]!r} row_count={row[1]}")
    print()
    print("Cleanup complete. regular_market_current portion of FOLLOWUP-2 closed.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
