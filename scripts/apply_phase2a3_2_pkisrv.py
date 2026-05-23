"""One-shot remediation for the pkisrv catalog row (2.A.3.2).

`pkisrv_daily` is empty in current DB AND all four backups
(`/mnt/e/psxdata/backups/psx_2026051{1,4,5}.sqlite` +
`/tmp/psx_pre_2a2_cleanup_20260523_1725.sqlite`). 1,049 source files
sit unloaded on disk at `/mnt/e/psxdata/rates/pkisrv/` back to 2020.

The sync path is broken (or never wired for the PKISRV branch of
`sources/mufap_rates.py`). Investigation deferred to Phase 2.A.5
alongside the other scraper work — see DEBT-PHASE2-FOLLOWUP-3.

Until then, the catalog row currently shows `status='ok'` with
`last_row_date=NULL` and `notes='empty table'` — that's a lie. This
script flips it to `status='failed'` with notes that point operators
at the deferred work item AND the on-disk source data.

Idempotent. Re-running once applied: the SET clause writes the same
values, rowcount=0 (technically rowcount=1 because UPDATE matches,
but values don't change).

Usage:
    python scripts/apply_phase2a3_2_pkisrv.py --dry-run
    python scripts/apply_phase2a3_2_pkisrv.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.safe_writer import safe_writer

PKISRV_NOTES = (
    "1,049 source files at /mnt/e/psxdata/rates/pkisrv/ unloaded. "
    "Sync path broken. See DEBT-PHASE2-FOLLOWUP-3."
)


def _preview(con: sqlite3.Connection) -> None:
    row = con.execute(
        "SELECT status, last_row_date, notes FROM data_freshness "
        "WHERE domain = 'pkisrv'"
    ).fetchone()
    if row is None:
        print("  [DRY] pkisrv — not in data_freshness; SKIP")
        return
    already = row[0] == "failed" and row[2] == PKISRV_NOTES
    verb = "already at target" if already else "would set"
    print(
        f"  [DRY] pkisrv — {verb} "
        f"(current: status={row[0]!r} notes={(row[2] or '')[:50]!r}…)"
    )


def _apply(con: sqlite3.Connection) -> None:
    cur = con.execute(
        """
        UPDATE data_freshness
           SET status     = 'failed',
               notes      = ?,
               updated_at = datetime('now')
         WHERE domain = 'pkisrv'
        """,
        (PKISRV_NOTES,),
    )
    print(f"  [APPLIED] pkisrv — affected {cur.rowcount} row(s)")


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Phase 2.A.3.2 — pkisrv catalog flip — DB: {db_path}")
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
    print("Remediation complete.")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview state without applying changes.",
    )
    args = p.parse_args()
    raise SystemExit(main(args.dry_run))
