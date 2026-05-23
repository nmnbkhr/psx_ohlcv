"""Catalog honesty pass — 2.A.3.6.

Three catalog rows currently lie about freshness:

    mutual_funds  → status='ok', notes='empty table'
    pkisrv        → already flipped to 'failed' in 2.A.3.2 — exempt
    sukuk         → status='ok', notes='empty table'

`status='ok'` against an empty table is a false-positive that hides
real freshness problems from UI badges. The honest signal is
`status='unknown'` (which the UI's quality-status derivation maps to
a warning, not green). The notes='empty table' string adds no
information beyond what the status already communicates — clear it.

WHERE clause includes `notes = 'empty table'` so this script is a
no-op for any row that was already updated (e.g. pkisrv after
2.A.3.2 — its notes now point at DEBT-PHASE2-FOLLOWUP-3 and are
intentionally specific).

Idempotent. Re-running once applied: rowcount=0 (the WHERE filters
out already-cleaned rows).

Usage:
    python scripts/apply_phase2a3_6_catalog_honesty.py --dry-run
    python scripts/apply_phase2a3_6_catalog_honesty.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.safe_writer import safe_writer

CANDIDATE_DOMAINS: tuple[str, ...] = ("mutual_funds", "pkisrv", "sukuk")


def _preview(con: sqlite3.Connection) -> None:
    rows = con.execute(
        f"""
        SELECT domain, status, last_row_date, notes
          FROM data_freshness
         WHERE domain IN ({','.join('?' * len(CANDIDATE_DOMAINS))})
           AND last_row_date IS NULL
           AND notes = 'empty table'
         ORDER BY domain
        """,
        CANDIDATE_DOMAINS,
    ).fetchall()
    if not rows:
        print("  [DRY] no rows match — all candidates already honest")
        return
    for d, s, lrd, n in rows:
        print(
            f"  [DRY] {d} — would set status='unknown', clear notes "
            f"(current: status={s!r} notes={n!r})"
        )


def _apply(con: sqlite3.Connection) -> None:
    cur = con.execute(
        f"""
        UPDATE data_freshness
           SET status     = 'unknown',
               notes      = NULL,
               updated_at = datetime('now')
         WHERE domain IN ({','.join('?' * len(CANDIDATE_DOMAINS))})
           AND last_row_date IS NULL
           AND notes = 'empty table'
        """,
        CANDIDATE_DOMAINS,
    )
    print(f"  [APPLIED] catalog honesty pass — affected {cur.rowcount} row(s)")


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Phase 2.A.3.6 — catalog honesty — DB: {db_path}")
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
    print("Done.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview state without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
