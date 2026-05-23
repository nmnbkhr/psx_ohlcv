"""One-shot cleanup for pre-Phase-2.A.2 catalog pollution.

Closes the residue from the two bugs Phase 2.A.1 distinguished:

  Bug A (catalog.py SET-clause omission, fixed in commit 0553ca9):
    data_freshness rows where date_column got stuck at the DEFAULT 'date'
    despite the source table using a different column. Affects exactly
    two domains today — announcements and tick_data. Explicit UPDATE
    here re-points their date_column at the correct source column so
    the NEXT sync re-populates the row correctly via the now-working
    ON CONFLICT path.

  Bug B (literal junk strings in source date columns):
    Five tables each holding rows with a sentinel string ('ZUMA',
    'TBILL', 'MUFAP', 'WTL') where the date column should hold a date.
    Phase 2.A.1's validators structurally prevent future occurrences —
    a write that lands 'ZUMA' inside safe_writer rolls back via the
    date_format check. The 1,720 EXISTING rows still need a one-shot
    cleanup. Each DELETE has an exact value match — no LIKE wildcards.
    Each DELETE is protected by a circuit breaker that raises if the
    rowcount exceeds an audited threshold.

All three classes of work (Bug A UPDATEs + Bug B DELETEs + catalog
recompute) run inside ONE safe_writer block. If anything in steps
1-3 fails, the whole transaction rolls back — no half-cleaned state
where data is removed but the catalog still points at it (or vice
versa).

Idempotent. Re-running once clean:
  - Bug A UPDATEs: no-op (rows already have correct date_column)
  - Bug B DELETEs: every count is 0
  - Recompute: catalog rows already correct, ON CONFLICT path runs but
    every column ends up at its current value

Manual backup required before live run (deliberately NOT automated —
scripts that take backups encourage running without thinking):

    cp ~/psxdata_rescue/psx.sqlite \\
       /tmp/psx_pre_2a2_cleanup_$(date +%Y%m%d_%H%M).sqlite

Usage:
    python scripts/cleanup_catalog_pollution.py --dry-run
    python scripts/cleanup_catalog_pollution.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


# --- Bug A: date_column stuck at the default. --------------------------------
# (description, domain, correct_date_column)
BUG_A_FIXES: list[tuple[str, str, str]] = [
    (
        "announcements: date_column 'date' → 'announcement_date'",
        "announcements",
        "announcement_date",
    ),
    (
        "tick_data: date_column 'date' → 'timestamp'",
        "tick_data",
        "timestamp",
    ),
]


# --- Bug B: literal pollution in source-table date columns. ------------------
# (description, table, column, bad_value, expected_max)
BUG_B_DELETES: list[tuple[str, str, str, str, int]] = [
    ("pib_auctions: auction_date='ZUMA'",            "pib_auctions",          "auction_date",   "ZUMA",  200),
    ("forex_kerb: date='TBILL'",                     "forex_kerb",            "date",           "TBILL",  50),
    ("konia_daily: date='ZUMA'",                     "konia_daily",           "date",           "ZUMA",    5),
    ("instrument_membership: effective_date='MUFAP'", "instrument_membership", "effective_date", "MUFAP", 1800),
    ("regular_market_current: ts='WTL'",             "regular_market_current", "ts",             "WTL",     5),
]


# --- Catalog recompute after Bug B cleanup. ----------------------------------
# data_freshness.last_row_date for these domains was poisoned by the
# alphabetic-MAX of strings ('ZUMA' > '2026-…'). Recompute clears it.
RECOMPUTE_DOMAINS: list[str] = [
    "pib",
    "fx_kerb",
    "konia",
    "instrument_membership",
    "regular_market_current",
]


def _preview_bug_a(con: sqlite3.Connection) -> None:
    """SELECT current state of the two Bug A rows."""
    for desc, domain, target_col in BUG_A_FIXES:
        row = con.execute(
            "SELECT date_column FROM data_freshness WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row is None:
            print(f"  [DRY] {desc} — domain not in data_freshness; SKIP")
        else:
            current = row[0]
            verb = "would set" if current != target_col else "already correct, no-op"
            print(
                f"  [DRY] {desc} — {verb} "
                f"(current: {current!r}, target: {target_col!r})"
            )


def _apply_bug_a(con: sqlite3.Connection) -> None:
    """UPDATE date_column on the two affected rows."""
    for desc, domain, target_col in BUG_A_FIXES:
        cur = con.execute(
            """
            UPDATE data_freshness
               SET date_column = ?,
                   status = 'unknown',
                   last_sync_error = NULL,
                   updated_at = datetime('now')
             WHERE domain = ?
            """,
            (target_col, domain),
        )
        print(f"  [APPLIED] {desc} — affected {cur.rowcount} row(s)")


def _preview_bug_b(con: sqlite3.Connection) -> None:
    """SELECT COUNT(*) for each pending DELETE, with per-line per-table output."""
    for desc, table, column, value, max_n in BUG_B_DELETES:
        n = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} = ?", (value,)
        ).fetchone()[0]
        marker = "WARN" if n > max_n else "ok"
        print(
            f"  [DRY] {desc} — would affect {n} row(s) "
            f"(circuit breaker max: {max_n}, status: {marker})"
        )


def _apply_bug_b(con: sqlite3.Connection) -> None:
    """DELETE pollution from each source table. Circuit-breaker raises
    if any single DELETE exceeds its audited maximum."""
    for desc, table, column, value, max_n in BUG_B_DELETES:
        cur = con.execute(
            f"DELETE FROM {table} WHERE {column} = ?", (value,)
        )
        n = cur.rowcount
        print(f"  [APPLIED] {desc} — affected {n} row(s)")
        if n > max_n:
            raise RuntimeError(
                f"Cleanup affected {n} row(s) for '{table}.{column}={value}', "
                f"expected at most {max_n}. Pausing. Audit and re-set the "
                f"circuit breaker if the upstream conditions changed."
            )


def _preview_recompute(con: sqlite3.Connection) -> None:
    for domain in RECOMPUTE_DOMAINS:
        row = con.execute(
            "SELECT last_row_date FROM data_freshness WHERE domain = ?",
            (domain,),
        ).fetchone()
        current = row[0] if row else "(domain absent)"
        print(
            f"  [DRY] recompute data_freshness for {domain!r} "
            f"(current last_row_date: {current!r})"
        )


def _apply_recompute(con: sqlite3.Connection) -> None:
    """Re-derive last_row_date + row_count for the 5 Bug B-affected rows.

    Uses update_catalog_from_table which re-reads source_table +
    date_column from the catalog row and recomputes MAX/COUNT. Runs
    only AFTER 2.A.2.1's ON CONFLICT fix is in tree (which it is by
    construction — that fix landed in the previous commit).
    """
    for domain in RECOMPUTE_DOMAINS:
        update_catalog_from_table(con, domain, source="cleanup_2a2")
        row = con.execute(
            "SELECT last_row_date, row_count FROM data_freshness WHERE domain = ?",
            (domain,),
        ).fetchone()
        new_date, new_count = row if row else (None, None)
        print(
            f"  [APPLIED] recomputed {domain!r}: "
            f"last_row_date={new_date!r} row_count={new_count}"
        )


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Catalog cleanup — DB: {db_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print()

    if dry_run:
        # Read-only preview path — never touches safe_writer.
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            print("Step 1 — Bug A date_column UPDATEs:")
            _preview_bug_a(con)
            print()
            print("Step 2 — Bug B source-table DELETEs:")
            _preview_bug_b(con)
            print()
            print("Step 3 — data_freshness recompute (5 domains):")
            _preview_recompute(con)
        finally:
            con.close()
        print()
        print("DRY RUN complete. To apply: re-run without --dry-run "
              "(remember to back up the DB first).")
        return 0

    # Live path — all three steps inside ONE safe_writer block.
    # If any step raises, the whole transaction rolls back; we never
    # end up with data deleted but catalog still pointing at it.
    with safe_writer() as con:
        print("Step 1 — Bug A date_column UPDATEs:")
        _apply_bug_a(con)
        print()
        print("Step 2 — Bug B source-table DELETEs:")
        _apply_bug_b(con)
        print()
        print("Step 3 — data_freshness recompute (5 domains):")
        _apply_recompute(con)
    print()
    print("Cleanup complete. Pollution closed.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview rowcounts without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
