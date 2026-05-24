"""scripts/daily_digest.py — Phase 2.B.1

Daily observability digest. Reads canonical DB and writes a markdown summary
of what needs human attention to a date-stamped file under
`/mnt/e/psxdata/digest/digest_YYYYMMDD.md`.

Read-only. No DB writes. Idempotent — re-runnable on demand without
side effects beyond overwriting the day's digest file.

Sections (priority-ordered):

  CRITICAL  validator errors / failed catalog rows / stuck jobs > 24h
  WARNING   stuck jobs 2-24h / sync staleness / validator warnings
  INFO      cron-log health / unknown catalog rows

Empty sections still appear with "no findings" — empty != broken
detector; the digest's reader needs to know the check ran and found
nothing.

Scope (Phase 2.B.1):

  - Stuck-job detection covers `jobs` + the 5 *_sync_runs tables that
    have an explicit `status` column. The 3 tables that use
    `ended_at IS NULL` semantics (commodity_sync_runs,
    instruments_sync_runs, sync_runs) are deferred to 2.B.3 where a
    single sweep abstraction will unify both conventions.

  - Validator results scope: `data_quality_results.run_at` within the
    last 24h. Currently the validator engine only has rules seeded
    for `indices`; 2.A.5.2/.3 deferred validator seeding for
    pkisrv / sovereign_curve pending custom_sql primitive (Phase 2.B
    or later work). The "validator failures" section will read mostly
    empty until more rules are seeded — that's honest, not broken.

  - Cron-log health: filesystem-level check that
    `~/.local/share/pakfindata/logs/daily_sync_YYYYMMDD.log` exists
    for each PSX trading day in the last 7 days. Missing logs flag
    the "cron ran but nothing happened" failure class
    (FOLLOWUP-6).

Usage:

  python scripts/daily_digest.py              # write today's digest
  python scripts/daily_digest.py --quiet      # no stdout (for cron)

Cron-friendly: exits 0 on success even when the digest contains
findings. The presence/absence of findings is the meaningful signal,
not the exit code; the 2.B.2 sub-wave wires MAILTO delivery so a
non-empty critical section triggers email.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pakfindata.config import get_db_path


DIGEST_DIR = Path("/mnt/e/psxdata/digest")
LOG_DIR = Path.home() / ".local/share/pakfindata/logs"
PKT_OFFSET = timedelta(hours=5)

# *_sync_runs tables that have an explicit `status` column (covered in 2.B.1).
# The other three tables — commodity_sync_runs, instruments_sync_runs,
# sync_runs — use `ended_at IS NULL` semantics; deferred to 2.B.3 sweep
# abstraction.
SYNC_RUNS_WITH_STATUS: tuple[str, ...] = (
    "bond_sync_runs",
    "fi_sync_runs",
    "fx_sync_runs",
    "mutual_fund_sync_runs",
    "sukuk_sync_runs",
)

STUCK_WARN_HOURS = 2
STUCK_CRITICAL_HOURS = 24
STALE_SYNC_HOURS = 36


def _readonly_con(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


# --- Section data collectors. Each returns a list of strings. ----------------


def _validator_errors(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """
        SELECT rule_id, domain, measured, run_at FROM data_quality_results
        WHERE passed = 0 AND severity = 'error'
          AND run_at > datetime('now', '-24 hours')
        ORDER BY run_at DESC
        """
    ).fetchall()
    return [
        f"`{r['rule_id']}` (domain={r['domain']}): measured={r['measured']!r} at {r['run_at']}"
        for r in rows
    ]


def _failed_catalog_rows(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """
        SELECT domain, status, last_row_date, last_sync_error, notes
        FROM data_freshness WHERE status = 'failed'
        ORDER BY domain
        """
    ).fetchall()
    out = []
    for r in rows:
        note_fragment = (r["last_sync_error"] or r["notes"] or "(none)")[:80]
        out.append(
            f"`{r['domain']}`: last_row_date={r['last_row_date']!r} — {note_fragment}"
        )
    return out


def _stuck_jobs(con: sqlite3.Connection, min_hours: int, max_hours: int | None) -> list[str]:
    """Stuck-row finder over `jobs` + the 5 status='running' sync_runs tables.

    min_hours: lower bound (rows must be running >= this long).
    max_hours: upper bound (rows must be running < this long). None = no upper bound.
    """
    out: list[str] = []

    where_upper = (
        f"AND enqueued_at > datetime('now', '-{max_hours} hours')"
        if max_hours is not None
        else ""
    )
    rows = con.execute(
        f"""
        SELECT id, job_type, enqueued_at FROM jobs
        WHERE status = 'running'
          AND enqueued_at < datetime('now', '-{min_hours} hours')
          {where_upper}
        ORDER BY enqueued_at
        """
    ).fetchall()
    out.extend(
        f"jobs.id={r['id']} ({r['job_type']}) running since {r['enqueued_at']}"
        for r in rows
    )

    for tbl in SYNC_RUNS_WITH_STATUS:
        where_upper_t = (
            f"AND started_at > datetime('now', '-{max_hours} hours')"
            if max_hours is not None
            else ""
        )
        rows = con.execute(
            f"""
            SELECT run_id, started_at FROM {tbl}
            WHERE status = 'running'
              AND started_at < datetime('now', '-{min_hours} hours')
              {where_upper_t}
            ORDER BY started_at
            """
        ).fetchall()
        out.extend(
            f"{tbl}.run_id={r['run_id']} running since {r['started_at']}"
            for r in rows
        )
    return out


def _sync_staleness(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        f"""
        SELECT domain, last_sync_at, last_row_date FROM data_freshness
        WHERE status = 'ok'
          AND last_sync_at IS NOT NULL
          AND last_sync_at < datetime('now', '-{STALE_SYNC_HOURS} hours')
        ORDER BY last_sync_at
        """
    ).fetchall()
    return [
        f"`{r['domain']}`: last_sync_at={r['last_sync_at']}, last_row_date={r['last_row_date']!r}"
        for r in rows
    ]


def _validator_warnings(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """
        SELECT rule_id, domain, measured, run_at FROM data_quality_results
        WHERE passed = 0 AND severity = 'warn'
          AND run_at > datetime('now', '-24 hours')
        ORDER BY run_at DESC
        """
    ).fetchall()
    return [
        f"`{r['rule_id']}` (domain={r['domain']}): measured={r['measured']!r} at {r['run_at']}"
        for r in rows
    ]


def _cron_log_health(today_pkt: datetime) -> list[str]:
    out: list[str] = []
    if not LOG_DIR.exists():
        return [f"WARN: log directory {LOG_DIR} not found"]

    log_dates = sorted(
        p.stem.replace("daily_sync_", "")
        for p in LOG_DIR.glob("daily_sync_*.log")
    )
    if log_dates:
        out.append(f"daily_sync logs present: {len(log_dates)} files (most recent: {', '.join(log_dates[-3:])})")
    else:
        out.append("WARN: no daily_sync logs found")

    # Trading-day gap check (last 7 days).
    missing: list[str] = []
    for i in range(1, 8):
        d = (today_pkt - timedelta(days=i)).date()
        if d.weekday() >= 5:  # Saturday/Sunday — PSX closed
            continue
        d_compact = d.isoformat().replace("-", "")
        if d_compact not in log_dates:
            missing.append(d.isoformat())
    if missing:
        out.append(f"MISSING trading-day logs (last 7d): {', '.join(missing)}")

    return out


def _unknown_catalog_rows(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """
        SELECT domain, status, row_count, notes FROM data_freshness
        WHERE status NOT IN ('ok', 'failed')
        ORDER BY domain
        """
    ).fetchall()
    return [
        f"`{r['domain']}`: status={r['status']}, rows={r['row_count']}, notes={(r['notes'] or '(none)')[:60]}"
        for r in rows
    ]


# --- Markdown rendering ------------------------------------------------------


def _render_section(title: str, findings: list[str]) -> str:
    if not findings:
        return f"### {title}\n\nno findings\n\n"
    body = "\n".join(f"- {f}" for f in findings)
    return f"### {title}\n\n{body}\n\n"


def _render_digest(today_pkt: datetime, db_path: str, sections: dict) -> str:
    md: list[str] = []
    md.append(f"# pakfindata daily digest — {today_pkt.date().isoformat()}\n\n")
    md.append(f"**DB:** `{db_path}`\n")
    md.append(f"**Generated:** {datetime.now().isoformat(timespec='seconds')}\n")
    md.append("**Read-only.** No DB writes; this digest is a snapshot of state\n\n")

    md.append("## Critical\n\n")
    for title, findings in sections["critical"]:
        md.append(_render_section(title, findings))

    md.append("## Warning\n\n")
    for title, findings in sections["warning"]:
        md.append(_render_section(title, findings))

    md.append("## Info\n\n")
    for title, findings in sections["info"]:
        md.append(_render_section(title, findings))

    md.append("---\n\n")
    md.append("**Scope notes**\n\n")
    md.append("- Stuck-job detection covers `jobs` and 5 `*_sync_runs` tables with explicit `status` column.\n")
    md.append("- 3 `*_sync_runs` tables using `ended_at IS NULL` semantics (commodity_sync_runs, instruments_sync_runs, sync_runs) deferred to Phase 2.B.3.\n")
    md.append("- Validator results scope: `data_quality_results.run_at` within last 24h. Most domains have zero seeded rules (Phase 2.B+).\n")
    md.append("- Cron-log health: trading-day gaps in last 7d (excludes weekends).\n")
    return "".join(md)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--quiet", action="store_true", help="suppress stdout (for cron)")
    p.add_argument(
        "--output-dir", type=Path, default=DIGEST_DIR,
        help=f"override output directory (default: {DIGEST_DIR})",
    )
    args = p.parse_args(argv)

    db_path = get_db_path()
    con = _readonly_con(db_path)
    today_pkt = datetime.now(timezone.utc) + PKT_OFFSET

    sections = {
        "critical": [
            ("Validator errors (last 24h)",         _validator_errors(con)),
            ("Catalog rows status='failed'",        _failed_catalog_rows(con)),
            (f"Stuck jobs (>{STUCK_CRITICAL_HOURS}h)", _stuck_jobs(con, STUCK_CRITICAL_HOURS, None)),
        ],
        "warning": [
            (f"Stuck jobs ({STUCK_WARN_HOURS}-{STUCK_CRITICAL_HOURS}h)",
             _stuck_jobs(con, STUCK_WARN_HOURS, STUCK_CRITICAL_HOURS)),
            (f"Sync staleness (status=ok, last_sync_at older than {STALE_SYNC_HOURS}h)",
             _sync_staleness(con)),
            ("Validator warnings (last 24h)",       _validator_warnings(con)),
        ],
        "info": [
            ("Cron-log health (last 7 trading days)", _cron_log_health(today_pkt)),
            ("Catalog rows status not in {ok, failed}", _unknown_catalog_rows(con)),
        ],
    }
    con.close()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / f"digest_{today_pkt.date().isoformat().replace('-', '')}.md"
    out_path.write_text(_render_digest(today_pkt, db_path, sections))

    # Findings summary on stdout (helpful in cron MAILTO; suppressed with --quiet)
    if not args.quiet:
        total_crit  = sum(len(f) for _, f in sections["critical"])
        total_warn  = sum(len(f) for _, f in sections["warning"])
        total_info  = sum(len(f) for _, f in sections["info"])
        print(f"digest written: {out_path}")
        print(f"  critical: {total_crit}  warning: {total_warn}  info: {total_info}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
