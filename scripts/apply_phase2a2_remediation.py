"""One-shot remediation for the 4 domains with wider scraper pollution.

After scripts/cleanup_catalog_pollution.py removed the 5 sentinel-string
patterns (ZUMA/TBILL/MUFAP/WTL/etc.), 4 source tables still contain
~241K rows where symbol codes (BOP/UBL/MLCF/ZTL/PIB/WASLR/...) sit
in date columns. These rows are NOT in 2.A.2's scope — they were
surfaced by the cleanup recompute step. Phase 2.A.3 will investigate
the upstream scraper bug class.

Until then, the affected catalog rows show garbage `last_row_date`
(alphabetic MAX of strings), which falsely signals 'ok'. This script
flips them to a state that's honest about the unknown freshness:

    last_row_date = NULL
    status        = 'failed'
    notes         = pointer to DEBT-PHASE2-FOLLOWUP-2

That way UI freshness queries see status='failed' and render a
warning rather than the misleading garbage date.

Idempotent. Re-running once applied: every domain already at the
target state, the SET clause writes the same values.

Usage:
    python scripts/apply_phase2a2_remediation.py --dry-run
    python scripts/apply_phase2a2_remediation.py
"""

from __future__ import annotations

import argparse
import sqlite3

from pakfindata.config import get_db_path
from pakfindata.db.safe_writer import safe_writer


# Domains with wider source-table pollution (symbol codes in date cols)
# that remains AFTER the 2.A.2.2 cleanup. instrument_membership is
# deliberately EXCLUDED — its only pollution was the MUFAP sentinel
# (cleaned by 2.A.2.2), so its catalog row is honest after recompute.
REMEDIATION_DOMAINS: list[str] = [
    "pib",
    "fx_kerb",
    "konia",
    "regular_market_current",
]

NOTES_VALUE = (
    "freshness gated by DEBT-PHASE2-FOLLOWUP-2 — source table contains "
    "symbol codes in date column; cleanup deferred to Phase 2.A.3 "
    "investigation"
)


def _preview(con: sqlite3.Connection) -> None:
    for domain in REMEDIATION_DOMAINS:
        row = con.execute(
            "SELECT last_row_date, status, notes FROM data_freshness "
            "WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row is None:
            print(f"  [DRY] {domain} — not in data_freshness; SKIP")
            continue
        cur_date, cur_status, cur_notes = row
        target_match = (
            cur_date is None
            and cur_status == "failed"
            and cur_notes == NOTES_VALUE
        )
        verb = "already at target" if target_match else "would set"
        print(
            f"  [DRY] {domain} — {verb} "
            f"(current: last_row_date={cur_date!r} status={cur_status!r})"
        )


def _apply(con: sqlite3.Connection) -> None:
    for domain in REMEDIATION_DOMAINS:
        cur = con.execute(
            """
            UPDATE data_freshness
               SET last_row_date = NULL,
                   status        = 'failed',
                   notes         = ?,
                   updated_at    = datetime('now')
             WHERE domain = ?
            """,
            (NOTES_VALUE, domain),
        )
        print(f"  [APPLIED] {domain} — affected {cur.rowcount} row(s)")


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Phase 2.A.2 remediation — DB: {db_path}")
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
