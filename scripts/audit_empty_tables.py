"""Standing inventory of empty/sparse tables — on-demand or cron.

Categorizes every user-table in the DB by row count + catalog
status + a static classification table (KNOWN_INTENTIONAL_EMPTY,
KNOWN_SCRAPER_BROKEN, etc.). Output: a markdown report at
``/mnt/e/psxdata/audit/empty_tables_<YYYYMMDD>.md`` plus a short
summary to stdout.

Read-only — no writes, no safe_writer. Safe to run while the API +
worker are live.

Categories
----------
1. **Expected empty (Scope v2 / future analytics)**: tables that
   are intentionally unpopulated until a future milestone.
2. **Backup artifact**: tables whose name literally contains a
   backup timestamp.
3. **Operational sync-log (empty = healthy)**: log tables that
   stay empty until something fails / runs.
4. **Scraper-broken or never-wired (deferred to 2.A.5)**: tables
   whose upstream loader is known to be broken.
5. **Sparse but populated (1-9 rows)**: low row counts; may be
   intentional or warrant investigation.
6. **Catalog says failed/unknown**: cross-reference with
   data_freshness — if the catalog flags the row, surface it.
7. **Unclassified low row count (< 100)**: not in any of the
   above buckets — likely needs human triage.
8. **Healthy (≥ 100 rows, catalog ok)**: implicit; not listed
   except as count summary.

Usage
-----
    python scripts/audit_empty_tables.py
    python scripts/audit_empty_tables.py --out /tmp/audit.md
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterable

from pakfindata.config import get_db_path


# Static classification. Update when scope shifts or scrapers fix.
KNOWN_INTENTIONAL_EMPTY: frozenset[str] = frozenset({
    # Scope v2 (intraday klines + raw ticks live in disk-fed flows)
    "psxt_klines_1m", "psxt_klines_5m", "psxt_klines_15m",
    "psxt_klines_1h", "psxt_klines_1d", "psxt_klines_1w",
    "psx_ticks", "tick_logs", "tick_ohlcv",
    # Future-feature analytics snapshots
    "bond_analytics_snapshots", "company_signal_snapshots",
    "sukuk_analytics_snapshots",
    "hmm_nccpl_flows", "hmm_sbp_reserves",
    # Legacy / superseded
    "psx_eod",  # superseded by eod_ohlcv
    # Downstream of empty upstreams
    "sukuk_yield_curve",  # waits on sukuk_master fully populated
    "sukuk_quotes",       # waits on sukuk_master fully populated
    "intraday_breadth",   # waits on intraday_bars fully populated
})

KNOWN_BACKUP_ARTIFACTS_PREFIXES: tuple[str, ...] = (
    "intraday_bars_backup_",  # one-off backup artifacts; safe to drop
)

KNOWN_OPLOG_EMPTY_OK: frozenset[str] = frozenset({
    "fi_sync_runs",
    "job_notifications",
    "sukuk_sync_runs",
    "sync_failures",
})

KNOWN_SCRAPER_BROKEN: frozenset[str] = frozenset({
    "financial_announcements",       # sources/announcements broken
    "ipo_listings",                  # scraper broken per CLAUDE.md
    "pkisrv_daily",                  # FOLLOWUP-3
    "sbp_pma_docs",                  # deep_scraper output
    "sbp_primary_market_docs",       # deep_scraper output
    "equity_structure",              # no known producer
    "compliance_screening",          # never wired
    "psx_market_stats",              # CLAUDE.md remove candidate
    "term_reference_rates",          # CLAUDE.md "never populated"
})

SPARSE_THRESHOLD = 10        # < this row count → "sparse"
LOW_THRESHOLD = 100          # < this row count → "low" (but not sparse)


def _list_tables(con: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
    ]


def _row_count(con: sqlite3.Connection, table: str) -> int:
    try:
        return con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except sqlite3.OperationalError:
        return -1


def _catalog_status(con: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """Map source_table → (domain, status). Catalog stores
    source_table (the SQL table) and domain (the catalog key)."""
    out: dict[str, tuple[str, str]] = {}
    try:
        for domain, source_table, status in con.execute(
            "SELECT domain, source_table, status FROM data_freshness"
        ):
            out[source_table] = (domain, status)
    except sqlite3.OperationalError:
        pass
    return out


def _classify(
    table: str,
    rows: int,
    catalog: dict[str, tuple[str, str]],
) -> tuple[str, str]:
    """Return (category, note). Category is the bucket name."""
    cat_entry = catalog.get(table)

    if any(table.startswith(p) for p in KNOWN_BACKUP_ARTIFACTS_PREFIXES):
        return "backup_artifact", "drop candidate"

    if table in KNOWN_INTENTIONAL_EMPTY:
        return "intentional_empty", "scope-v2 / future"

    if table in KNOWN_OPLOG_EMPTY_OK and rows == 0:
        return "oplog_empty_ok", "no events to log yet"

    if table in KNOWN_SCRAPER_BROKEN:
        return "scraper_broken", "deferred to 2.A.5"

    if cat_entry is not None:
        domain, status = cat_entry
        if status in {"failed", "unknown"}:
            return "catalog_flagged", f"catalog domain={domain!r} status={status!r}"

    if rows == 0:
        return "unclassified_empty", "needs triage"
    if rows < SPARSE_THRESHOLD:
        return "sparse", f"{rows} rows"
    if rows < LOW_THRESHOLD:
        return "low", f"{rows} rows"

    return "healthy", f"{rows} rows"


CATEGORY_ORDER = [
    "unclassified_empty",
    "catalog_flagged",
    "scraper_broken",
    "sparse",
    "low",
    "oplog_empty_ok",
    "intentional_empty",
    "backup_artifact",
    "healthy",
]

CATEGORY_HEADINGS = {
    "unclassified_empty": (
        "Unclassified empty tables — **NEEDS TRIAGE**"
    ),
    "catalog_flagged": (
        "Catalog flagged (`failed` / `unknown`) — review pending"
    ),
    "scraper_broken": (
        "Scraper-broken or never-wired — deferred to Phase 2.A.5"
    ),
    "sparse": (
        f"Sparse but populated (1–{SPARSE_THRESHOLD - 1} rows)"
    ),
    "low": (
        f"Low row count ({SPARSE_THRESHOLD}–{LOW_THRESHOLD - 1} rows)"
    ),
    "oplog_empty_ok": (
        "Operational log tables (empty = healthy)"
    ),
    "intentional_empty": (
        "Intentional empty (Scope v2 / future analytics)"
    ),
    "backup_artifact": (
        "Backup artifacts — DROP candidates"
    ),
    "healthy": (
        f"Healthy (≥ {LOW_THRESHOLD} rows, catalog ok)"
    ),
}


def render_markdown(
    db_path: str,
    findings: list[tuple[str, int, str, str]],
) -> str:
    """Return the full markdown report."""
    today = date.today().isoformat()
    lines: list[str] = []
    lines.append(f"# Empty/Sparse Table Audit — {today}")
    lines.append("")
    lines.append(f"**DB:** `{db_path}`")
    lines.append(
        "**Source:** `scripts/audit_empty_tables.py` "
        "(read-only; safe to run live)"
    )
    lines.append("")

    by_cat: dict[str, list[tuple[str, int, str]]] = {}
    for tbl, rows, cat, note in findings:
        by_cat.setdefault(cat, []).append((tbl, rows, note))

    # Summary line
    summary_bits = []
    for cat in CATEGORY_ORDER:
        n = len(by_cat.get(cat, []))
        if n:
            summary_bits.append(f"{cat}={n}")
    lines.append("## Summary")
    lines.append("")
    lines.append("```")
    lines.append(", ".join(summary_bits))
    lines.append("```")
    lines.append("")

    for cat in CATEGORY_ORDER:
        rows = by_cat.get(cat) or []
        if cat == "healthy" and rows:
            # Healthy is implicit — render count only
            lines.append(f"## {CATEGORY_HEADINGS[cat]}")
            lines.append("")
            lines.append(f"{len(rows)} tables (not listed individually).")
            lines.append("")
            continue
        if not rows:
            continue
        lines.append(f"## {CATEGORY_HEADINGS[cat]}")
        lines.append("")
        lines.append("| Table | Rows | Note |")
        lines.append("|---|---:|---|")
        for tbl, n, note in sorted(rows):
            lines.append(f"| `{tbl}` | {n} | {note} |")
        lines.append("")

    return "\n".join(lines)


def run(db_path: str) -> list[tuple[str, int, str, str]]:
    """Return findings list: (table, rows, category, note)."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        catalog = _catalog_status(con)
        tables = _list_tables(con)
        out: list[tuple[str, int, str, str]] = []
        for t in tables:
            rows = _row_count(con, t)
            cat, note = _classify(t, rows, catalog)
            out.append((t, rows, cat, note))
        return out
    finally:
        con.close()


def main(out_path: str | None) -> int:
    db = get_db_path()
    findings = run(db)
    md = render_markdown(db, findings)

    target = Path(
        out_path
        or f"/mnt/e/psxdata/audit/empty_tables_{date.today().strftime('%Y%m%d')}.md"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(md)

    counts: dict[str, int] = {}
    for _, _, cat, _ in findings:
        counts[cat] = counts.get(cat, 0) + 1

    print(f"Audit complete — DB: {db}")
    print(f"Report: {target}")
    print()
    for cat in CATEGORY_ORDER:
        n = counts.get(cat, 0)
        if n:
            print(f"  {cat:24s} {n}")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--out",
        help="Override output path. Default: "
        "/mnt/e/psxdata/audit/empty_tables_<YYYYMMDD>.md",
    )
    args = p.parse_args()
    raise SystemExit(main(args.out))
