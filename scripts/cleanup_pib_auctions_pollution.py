"""Cleanup script for pib_auctions pollution (FOLLOWUP-2a, sub-wave 2.A.5.4a).

Removes 240,872 misrouted intraday-tick rows from pib_auctions. Originally
filed as "wider scraper pollution" in FOLLOWUP-2 (Phase 2.A.2 remediation
notes). The recovery audit (read-only, sub-wave 2.A.5.4) proved the
disposition is safe CLEANUP — not destructive — because every polluted row
already exists as a canonical entry in tick_data.

The proof:
  - 239,904 real-symbol rows + 968 numeric-leading rows ('786' is a real
    PSX ticker) = 240,872 total.
  - After PKT->UTC adjustment (the polluted pib_type values are PKT
    display strings; tick_data stores UTC epoch), 240,872 of 240,872
    rows match tick_data exactly on (symbol, timestamp, price).
  - Recovery would be a no-op; cleanup loses no information.

Predicate (verified exact against current DB; matches 240,872 rows):

    maturity_date = 'insert'
    AND auction_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'

The 2-criterion predicate is the verified-tight form of the 3-criterion
defense-in-depth from Step 0 — the tenor LIKE '%-%-%' arm becomes
redundant once the auction_date NOT GLOB date arm is in place.

Gates (any divergence by even one row halts the run before commit):

  Pre-flight predictions (verified against canonical DB at script time):
    pre_total                    = 241,835
    pred_match (will be deleted) = 240,872
    pred_post_total              =     963
    pred_post_dates              =     270
    tick_data_count              = 10,048,488 (must be unchanged post-run)

  Mid-run safety: 5% breaker on deleted-count
    breaker_max = pred_match * 1.05 = 252,915

  Post-flight: all four post-conditions must match predictions exactly.
  If anything diverges, the safe_writer transaction rolls back.

Batched deletion with progress reporting at 10K rows. Single safe_writer
transaction. Catalog recompute for 'pib' domain runs inside the same
transaction so freshness data stays in sync.

Manual backup required before live run:

    cp ~/psxdata_rescue/psx.sqlite \\
       /tmp/psx_pre_2a5_4a_$(date +%Y%m%d_%H%M).sqlite

Usage:
    python scripts/cleanup_pib_auctions_pollution.py --dry-run
    python scripts/cleanup_pib_auctions_pollution.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


PREDICATE_WHERE = (
    "maturity_date = 'insert' "
    "AND auction_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"
)

# Predictions verified live against /home/smnb/psxdata_rescue/psx.sqlite
# at script-authoring time (2026-05-24). If any of these diverge by even
# one row at runtime, the script halts.
PRED_PRE_TOTAL: int = 241_835
PRED_MATCH: int = 240_872
PRED_POST_TOTAL: int = 963
PRED_POST_DATES: int = 270
PRED_TICK_DATA: int = 10_048_488

BATCH_SIZE: int = 10_000
BREAKER_MAX: int = int(PRED_MATCH * 1.05)  # 252,915


def _readonly_con(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _snapshot(con: sqlite3.Connection) -> dict[str, int]:
    """Take the five gating counts in one read pass."""
    return {
        "pre_total": con.execute("SELECT COUNT(*) FROM pib_auctions").fetchone()[0],
        "match": con.execute(
            f"SELECT COUNT(*) FROM pib_auctions WHERE {PREDICATE_WHERE}"
        ).fetchone()[0],
        "post_total": con.execute(
            f"SELECT COUNT(*) FROM pib_auctions WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "post_dates": con.execute(
            f"SELECT COUNT(DISTINCT auction_date) FROM pib_auctions "
            f"WHERE NOT ({PREDICATE_WHERE})"
        ).fetchone()[0],
        "tick_data": con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0],
    }


def _check_preflight(snap: dict[str, int]) -> None:
    expected = {
        "pre_total": PRED_PRE_TOTAL,
        "match": PRED_MATCH,
        "post_total": PRED_POST_TOTAL,
        "post_dates": PRED_POST_DATES,
        "tick_data": PRED_TICK_DATA,
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
    """Delete the polluted rows in BATCH_SIZE chunks with progress
    reporting. Returns the total deleted. Raises if breaker trips."""
    deleted_total = 0
    delete_sql = (
        f"DELETE FROM pib_auctions WHERE rowid IN ("
        f"  SELECT rowid FROM pib_auctions WHERE {PREDICATE_WHERE} LIMIT ?"
        f")"
    )
    while True:
        cur = con.execute(delete_sql, (BATCH_SIZE,))
        n = cur.rowcount
        if n == 0:
            break
        deleted_total += n
        pct = 100.0 * deleted_total / PRED_MATCH
        print(f"  [APPLIED] deleted {deleted_total:>7,} / {PRED_MATCH:,}  ({pct:5.1f}%)")
        if deleted_total > BREAKER_MAX:
            raise RuntimeError(
                f"5% safety breaker tripped: deleted={deleted_total:,} > "
                f"breaker_max={BREAKER_MAX:,}. Rolling back."
            )
    return deleted_total


def _check_postflight(con: sqlite3.Connection, deleted: int) -> None:
    """Re-snapshot the table and verify all four post-conditions plus
    tick_data invariance. Raises on any mismatch — safe_writer rolls back."""
    if deleted != PRED_MATCH:
        raise RuntimeError(
            f"Delete-count mismatch: deleted={deleted:,} expected={PRED_MATCH:,}"
        )
    snap = _snapshot(con)
    checks = [
        ("post_total",     snap["pre_total"],   PRED_POST_TOTAL),
        ("post_dates",     snap["post_dates"],  PRED_POST_DATES),
        ("tick_data_unchanged", snap["tick_data"], PRED_TICK_DATA),
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
    print(f"pib_auctions cleanup (2.A.5.4a) — DB: {db_path}")
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
        print(f"  DRY: would recompute data_freshness for domain 'pib'")
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
        print("Step 4 — Catalog recompute for domain 'pib':")
        update_catalog_from_table(con, "pib", source="cleanup_2a5_4a")
        row = con.execute(
            "SELECT last_row_date, row_count FROM data_freshness WHERE domain = 'pib'"
        ).fetchone()
        if row is not None:
            print(f"  [APPLIED] pib: last_row_date={row[0]!r} row_count={row[1]}")
    print()
    print("Cleanup complete. FOLLOWUP-2a closed.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview gate state without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
