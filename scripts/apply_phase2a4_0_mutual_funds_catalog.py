"""Catalog correction for `mutual_funds` (2.A.4.0).

Fixes the 2.A.3.6 catalog honesty pass which mis-flipped this row.

The 2.A.3.6 script (`apply_phase2a3_6_catalog_honesty.py`) flipped
domains with `notes='empty table'` to `status='unknown'`. That was
correct for `sukuk` (sukuk_quotes has 0 rows). It was WRONG for
`mutual_funds` — the `notes='empty table'` value was a stale
annotation, not factual: `mutual_fund_nav` (the catalog row's
source_table) has 568,039 rows through 2026-05-23.

The 2.A.4 Step 0 audit caught this when investigating
market_research.py's `_load_funds_snapshot()` query, which returns
4 rows today against a healthy source table.

Fix: call `update_catalog_from_table` to recompute `last_row_date`
and `row_count` from `mutual_fund_nav`. status flips to 'ok'
because the recompute path's default is 'ok'. Uses the helper
(post-2.A.2.1b, the helper correctly preserves source_table /
date_column / display_name).

Idempotent. Re-running: rowcount=1 each time, values converge.

Usage:
    python scripts/apply_phase2a4_0_mutual_funds_catalog.py --dry-run
    python scripts/apply_phase2a4_0_mutual_funds_catalog.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


def _preview(con: sqlite3.Connection) -> None:
    row = con.execute(
        "SELECT source_table, date_column, status, last_row_date, row_count "
        "FROM data_freshness WHERE domain = 'mutual_funds'"
    ).fetchone()
    if row is None:
        print("  [DRY] mutual_funds — not in data_freshness; SKIP")
        return
    source_table, date_column, status, last_row_date, row_count = row

    fresh = con.execute(
        f"SELECT MAX({date_column}), COUNT(*) FROM {source_table}"
    ).fetchone()
    new_date, new_count = fresh[0], fresh[1]

    print(
        f"  [DRY] mutual_funds — would recompute via update_catalog_from_table\n"
        f"        current: status={status!r} "
        f"last_row_date={last_row_date!r} row_count={row_count}\n"
        f"        target:  status='ok' "
        f"last_row_date={new_date!r} row_count={new_count}"
    )


def _apply(con: sqlite3.Connection) -> None:
    update_catalog_from_table(
        con,
        "mutual_funds",
        source="catalog_correction_2a4_0",
    )
    row = con.execute(
        "SELECT status, last_row_date, row_count FROM data_freshness "
        "WHERE domain = 'mutual_funds'"
    ).fetchone()
    print(
        f"  [APPLIED] mutual_funds — "
        f"status={row[0]!r} last_row_date={row[1]!r} row_count={row[2]}"
    )


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Phase 2.A.4.0 — mutual_funds catalog correction — DB: {db_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print()

    if dry_run:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            _preview(con)
        finally:
            con.close()
        print()
        print("DRY RUN complete. To apply: re-run without --dry-run.")
        return 0

    with safe_writer() as con:
        _apply(con)
    print()
    print("Correction complete.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview state without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
