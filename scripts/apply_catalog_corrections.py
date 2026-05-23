"""Apply catalog corrections to `data_freshness`.

Generic, data-driven. Each correction in `CORRECTIONS` describes one
catalog row that was found to be incorrect after-the-fact, along with
the target values and the audit-trail reason.

Re-running is safe: each entry is compared against current state and
skipped when already correct. Idempotent by design.

Entries here are HISTORY. Do not delete an entry after it applies —
future investigators reading this file should see the full chain of
corrections that have been made to the catalog, with their reasons.

Two correction shapes are supported:

  - kind="set"          one or more direct column writes (notes, status,
                        etc.). Use when the correction is a literal
                        column value, e.g. annotating a row.
  - kind="recompute"    call `update_catalog_from_table` to recompute
                        last_row_date / row_count from the source
                        table. Use when the catalog row drifted from
                        the source-table truth (the 2.A.4.0 shape).

Usage:
    python scripts/apply_catalog_corrections.py --dry-run   # preview
    python scripts/apply_catalog_corrections.py             # apply
"""

from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog_from_table
from pakfindata.db.safe_writer import safe_writer


CORRECTIONS: list[dict[str, Any]] = [
    {
        "id": "2.A.5.1a",
        "domain": "fx_interbank",
        "kind": "set",
        "set": {
            "notes": "USD-only by upstream design; see DEBT-PHASE2-FOLLOWUP-4",
        },
        "reason": (
            "2.A.3.6 catalog honesty pass set notes='empty table' based "
            "on a stale annotation. The table correctly has 1 valid USD "
            "row (SBP publishes interbank for USD only as structural "
            "design). Status remains 'ok'."
        ),
    },
]


def _current_row(con: sqlite3.Connection, domain: str) -> dict[str, Any] | None:
    cur = con.execute(
        "SELECT status, notes, source_table, date_column, "
        "last_row_date, row_count "
        "FROM data_freshness WHERE domain = ?",
        (domain,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "notes": row[1],
        "source_table": row[2],
        "date_column": row[3],
        "last_row_date": row[4],
        "row_count": row[5],
    }


def _needs_set(current: dict[str, Any], target: dict[str, Any]) -> bool:
    return any(current.get(k) != v for k, v in target.items())


def _preview_set(c: dict[str, Any], current: dict[str, Any] | None) -> str:
    if current is None:
        return f"  [{c['id']}] {c['domain']} — NOT IN data_freshness; SKIP"
    if not _needs_set(current, c["set"]):
        return f"  [{c['id']}] {c['domain']} — already correct; SKIP"
    diff_lines = [
        f"        {k}: {current.get(k)!r} → {v!r}"
        for k, v in c["set"].items()
        if current.get(k) != v
    ]
    return (
        f"  [{c['id']}] {c['domain']} — would update:\n"
        + "\n".join(diff_lines)
    )


def _preview_recompute(c: dict[str, Any], current: dict[str, Any] | None,
                       con: sqlite3.Connection) -> str:
    if current is None:
        return f"  [{c['id']}] {c['domain']} — NOT IN data_freshness; SKIP"
    src = current["source_table"]
    dcol = current["date_column"]
    if not src or not dcol:
        return (
            f"  [{c['id']}] {c['domain']} — "
            f"source_table/date_column missing on catalog row; CANNOT recompute"
        )
    fresh = con.execute(
        f"SELECT MAX({dcol}), COUNT(*) FROM {src}"
    ).fetchone()
    new_date, new_count = fresh[0], fresh[1]
    return (
        f"  [{c['id']}] {c['domain']} — would recompute from {src}.{dcol}\n"
        f"        current: last_row_date={current['last_row_date']!r} "
        f"row_count={current['row_count']}\n"
        f"        target:  last_row_date={new_date!r} "
        f"row_count={new_count}"
    )


def _apply_set(con: sqlite3.Connection, c: dict[str, Any]) -> str:
    current = _current_row(con, c["domain"])
    if current is None:
        return f"  [{c['id']}] {c['domain']} — NOT IN data_freshness; SKIPPED"
    if not _needs_set(current, c["set"]):
        return f"  [{c['id']}] {c['domain']} — already correct; SKIPPED"
    cols = ", ".join(f"{k} = ?" for k in c["set"].keys())
    params = list(c["set"].values()) + [c["domain"]]
    con.execute(
        f"UPDATE data_freshness SET {cols} WHERE domain = ?",
        params,
    )
    return f"  [{c['id']}] {c['domain']} — APPLIED"


def _apply_recompute(con: sqlite3.Connection, c: dict[str, Any]) -> str:
    update_catalog_from_table(
        con,
        c["domain"],
        source=f"catalog_correction_{c['id'].replace('.', '_')}",
    )
    row = con.execute(
        "SELECT status, last_row_date, row_count FROM data_freshness "
        "WHERE domain = ?",
        (c["domain"],),
    ).fetchone()
    return (
        f"  [{c['id']}] {c['domain']} — APPLIED: "
        f"status={row[0]!r} last_row_date={row[1]!r} row_count={row[2]}"
    )


def main(dry_run: bool) -> int:
    db_path = get_db_path()
    print(f"Catalog corrections — DB: {db_path}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"Entries: {len(CORRECTIONS)}")
    print()

    if dry_run:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            for c in CORRECTIONS:
                current = _current_row(con, c["domain"])
                if c["kind"] == "set":
                    print(_preview_set(c, current))
                elif c["kind"] == "recompute":
                    print(_preview_recompute(c, current, con))
                else:
                    print(f"  [{c['id']}] unknown kind={c['kind']!r}; SKIP")
        finally:
            con.close()
        print()
        print("DRY RUN complete. To apply: re-run without --dry-run.")
        return 0

    with safe_writer() as con:
        for c in CORRECTIONS:
            if c["kind"] == "set":
                print(_apply_set(con, c))
            elif c["kind"] == "recompute":
                print(_apply_recompute(con, c))
            else:
                print(f"  [{c['id']}] unknown kind={c['kind']!r}; SKIPPED")
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
