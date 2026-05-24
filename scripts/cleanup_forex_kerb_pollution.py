"""Cleanup script for forex_kerb pollution (FOLLOWUP-2c part 1, sub-wave 2.A.5.6a).

Removes 60 synthetic test-seed rows from forex_kerb. Originally filed as part
of FOLLOWUP-2 (Phase 2.A.2 remediation notes); the forex_kerb slice is its own
sub-sub-wave following the 2.A.5.6 SPLIT decision.

Root cause: deliberate test/sample data, NOT a scraper misroute. All 60 rows
share an exact triple-convergent signature:

    source = 'SAMPLE'
    date IN ('PIB', 'GOP_SUKUK')      # fixed-income instrument names, not FX
    buying = 30.0                     # uniform sentinel value

Distinct from pib_auctions (240K-row tick misroute) and konia_daily (633-row
whole-row column shift) — those were scraper bugs; this is leftover synthetic
data written in a single 5-second burst on 2026-01-29 12:39. The forex_kerb
"currencies" PIB / GOP_SUKUK are fixed-income instruments, never real FX
codes. Selling values (~13.4-13.8) look like mock sukuk/PIB yields. No
cross-table dedup needed — the data is not canonical anywhere.

Predicate (verified triple-convergent — all three forms match exactly 60):

    source = 'SAMPLE'
    AND date IN ('PIB', 'GOP_SUKUK')
    AND buying = 30.0

The full 3-criterion AND is the cleanup choice. Each criterion alone matches
60; pair-wise ANDs match 60; the triple AND matches 60. ZERO legitimate rows
exhibit any of the markers. Strongest convergence in the FOLLOWUP-2 set so
far.

Gates (any divergence by even one row halts the run before commit):

  Pre-flight predictions (verified against canonical DB at script time):
    pre_total          =   773
    pred_match         =    60
    pred_post_total    =   713
    pred_post_currencies =  23  (post: legitimate currencies only)
    pred_post_dates    =    31  (post: legitimate trading-day dates)

  Mid-run safety: 5% breaker on deleted-count
    breaker_max = pred_match * 1.05 = 63

  Post-flight: all four post-conditions must match predictions exactly.
  If anything diverges, the safe_writer transaction rolls back.

Single-batch deletion (60 rows fits in one 10K batch). Catalog recompute for
'fx_kerb' domain runs inside the same safe_writer transaction so freshness
data stays in sync.

Manual backup required before live run:

    cp ~/psxdata_rescue/psx.sqlite \\
       /tmp/psx_pre_2a5_6a_$(date +%Y%m%d_%H%M).sqlite

Usage:
    python scripts/cleanup_forex_kerb_pollution.py --dry-run
    python scripts/cleanup_forex_kerb_pollution.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


PREDICATE_WHERE = (
    "source = 'SAMPLE' "
    "AND date IN ('PIB', 'GOP_SUKUK') "
    "AND buying = 30.0"
)

PRED_PRE_TOTAL: int = 773
PRED_MATCH: int = 60
PRED_POST_TOTAL: int = 713
PRED_POST_CURRENCIES: int = 23
PRED_POST_DATES: int = 31

BATCH_SIZE: int = 10_000
BREAKER_MAX: int = int(PRED_MATCH * 1.05)  # 63


def _readonly_con(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _snapshot(con: sqlite3.Connection) -> dict[str, int]:
    return {
        "pre_total":      con.execute("SELECT COUNT(*) FROM forex_kerb").fetchone()[0],
        "match":          con.execute(
            f"SELECT COUNT(*) FROM forex_kerb WHERE {PREDICATE_WHERE}"
        ).fetchone()[0],
        "post_total":     con.execute(
            f"SELECT COUNT(*) FROM forex_kerb WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "post_currencies": con.execute(
            f"SELECT COUNT(DISTINCT currency) FROM forex_kerb WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "post_dates":     con.execute(
            f"SELECT COUNT(DISTINCT date) FROM forex_kerb WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
    }


def _check_preflight(snap: dict[str, int]) -> None:
    expected = {
        "pre_total":       PRED_PRE_TOTAL,
        "match":           PRED_MATCH,
        "post_total":      PRED_POST_TOTAL,
        "post_currencies": PRED_POST_CURRENCIES,
        "post_dates":      PRED_POST_DATES,
    }
    failed: list[str] = []
    for key, pred in expected.items():
        actual = snap[key]
        marker = "ok" if actual == pred else "MISMATCH"
        print(f"  {key:<16} predicted={pred:>8,}  actual={actual:>8,}  [{marker}]")
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
        f"DELETE FROM forex_kerb WHERE rowid IN ("
        f"  SELECT rowid FROM forex_kerb WHERE {PREDICATE_WHERE} LIMIT ?"
        f")"
    )
    while True:
        cur = con.execute(delete_sql, (BATCH_SIZE,))
        n = cur.rowcount
        if n == 0:
            break
        deleted_total += n
        pct = 100.0 * deleted_total / PRED_MATCH
        print(f"  [APPLIED] deleted {deleted_total:>4,} / {PRED_MATCH:,}  ({pct:5.1f}%)")
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
        ("post_total",      snap["pre_total"],       PRED_POST_TOTAL),
        ("post_currencies", snap["post_currencies"], PRED_POST_CURRENCIES),
        ("post_dates",      snap["post_dates"],      PRED_POST_DATES),
    ]
    failed: list[str] = []
    for key, actual, pred in checks:
        marker = "ok" if actual == pred else "MISMATCH"
        print(f"  {key:<16} predicted={pred:>8,}  actual={actual:>8,}  [{marker}]")
        if actual != pred:
            failed.append(f"{key}: predicted {pred:,}, got {actual:,}")
    if failed:
        raise RuntimeError(
            "Post-flight gate FAILED. Rolling back.\n"
            + "\n".join(f"  - {f}" for f in failed)
        )


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"forex_kerb cleanup (2.A.5.6a) — DB: {db_path}")
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
        print(f"  DRY: would recompute data_freshness for domain 'fx_kerb'")
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
        print("Step 4 — Catalog recompute for domain 'fx_kerb':")
        update_catalog_from_table(con, "fx_kerb", source="cleanup_2a5_6a")
        row = con.execute(
            "SELECT last_row_date, row_count FROM data_freshness WHERE domain = 'fx_kerb'"
        ).fetchone()
        if row is not None:
            print(f"  [APPLIED] fx_kerb: last_row_date={row[0]!r} row_count={row[1]}")
    print()
    print("Cleanup complete. forex_kerb portion of FOLLOWUP-2 closed.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
