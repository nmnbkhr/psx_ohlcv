"""scripts/daily_digest.py — Phase 2.B.1 + 2.B.2

Daily observability digest. Reads canonical DB and writes a markdown summary
of what needs human attention to a date-stamped file under
`/mnt/e/psxdata/digest/digest_YYYYMMDD.md`.

Phase 2.B.2 adds differential-delivery alerting (no SMTP — local alert
files only). On each run, per-section counts are compared against the
prior run's snapshot in `state.json`. If any section count differs (or
no prior state exists), a focused alert file is written to
`/mnt/e/psxdata/digest/alerts/alert_YYYYMMDD_HHMM.md` containing the
critical+warning sections and a state-delta summary.

Read-only on canonical DB. Writes to: digest file, alert file
(conditionally), state.json. Idempotent — re-runnable on demand;
re-running with the same state will not produce a new alert.

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

  python scripts/daily_digest.py              # write today's digest + alert if state changed
  python scripts/daily_digest.py --quiet      # no stdout (for cron)
  python scripts/daily_digest.py --no-alert   # write digest only; skip alert/state logic

Cron-friendly: exits 0 on success even when the digest contains
findings. The presence/absence of findings is the meaningful signal,
not the exit code.

Why a local alert file instead of cron MAILTO: the host has no SMTP
infrastructure (no sendmail/msmtp/postfix/mailx). Adding one creates
a system-level dependency that Phase 2.B is explicitly designed to
avoid. A local file in a known directory satisfies the "operators
are guaranteed to see findings" intent without SMTP setup. Layering
real email delivery on top is a 2-line msmtp config (out of scope).

Why differential delivery instead of any-non-empty: same findings
every day until FOLLOWUPs resolve = alert fatigue. State-change-only
delivery means each alert means "today differs from yesterday" which
is structurally more useful than "things are still bad." Also catches
RESOLUTIONS (when counts go down), which a strict threshold misses.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pakfindata.config import get_db_path
from pakfindata.observability.stuck_jobs import find_stuck_jobs


DIGEST_DIR = Path("/mnt/e/psxdata/digest")
ALERTS_DIR = DIGEST_DIR / "alerts"
STATE_PATH = DIGEST_DIR / "state.json"
LOG_DIR = Path.home() / ".local/share/pakfindata/logs"
PKT_OFFSET = timedelta(hours=5)

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
    """Stuck-row finder. Delegates to `observability.stuck_jobs.find_stuck_jobs`
    (Phase 2.B.3a). Covers 10 tables across two predicate conventions —
    a strict superset of the original 6-table embedded query that lived
    here pre-2.B.3b.

    Returns formatted strings consistent with the StuckJob.__str__
    convention: `<table>.<id> running since <started_at> (<age>h)`.
    """
    rows = find_stuck_jobs(threshold_hours=min_hours, con=con)
    if max_hours is not None:
        rows = [r for r in rows if r.age_hours < max_hours]
    return [str(r) for r in rows]


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


# --- Alert delivery (differential, no SMTP) ---------------------------------


def _section_counts(sections: dict) -> dict[str, int]:
    """Flatten the three-tier section dict to a flat name->count map.

    Used for state comparison. Names are stable across runs (they're
    hardcoded in main()) so JSON dict-equality is the diff test.
    """
    counts: dict[str, int] = {}
    for tier in ("critical", "warning", "info"):
        for title, findings in sections[tier]:
            counts[title] = len(findings)
    return counts


def _load_prior_state(state_path: Path) -> dict | None:
    if not state_path.exists():
        return None
    try:
        return json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError):
        # Corrupt state file — treat as no prior state. Don't crash the
        # digest; the alert will fire as if first run.
        return None


def _state_changed(prior: dict | None, current_counts: dict[str, int]) -> tuple[bool, list[str]]:
    """Compare prior state vs current counts. Returns (changed, delta_lines).

    delta_lines describes the change in human-readable form, one line
    per section that moved.
    """
    if prior is None:
        return True, ["first run — no prior state on file"]

    prior_counts: dict[str, int] = prior.get("section_counts", {})
    delta_lines: list[str] = []
    # All section names from current run
    for name, cur_n in current_counts.items():
        prev_n = prior_counts.get(name, 0)
        if cur_n != prev_n:
            arrow = "↑" if cur_n > prev_n else "↓"
            delta_lines.append(f"{name}: {prev_n} {arrow} {cur_n}")
    # Sections present in prior but not current (removed sections)
    for name, prev_n in prior_counts.items():
        if name not in current_counts and prev_n != 0:
            delta_lines.append(f"{name}: {prev_n} → 0 (section removed)")

    return (len(delta_lines) > 0), delta_lines


def _render_alert(
    today_pkt: datetime,
    db_path: str,
    sections: dict,
    delta_lines: list[str],
    digest_path: Path,
) -> str:
    """Focused alert markdown: state delta + critical+warning sections only.

    Skips info tier (alerts are about things that need attention, not
    background-context info).
    """
    md: list[str] = []
    md.append(f"# pakfindata alert — {today_pkt.isoformat(timespec='minutes')}\n\n")
    md.append(f"**Triggered:** state changed vs prior run\n")
    md.append(f"**Full digest:** `{digest_path}`\n")
    md.append(f"**DB:** `{db_path}`\n\n")

    md.append("## State delta\n\n")
    for line in delta_lines:
        md.append(f"- {line}\n")
    md.append("\n")

    md.append("## Critical\n\n")
    for title, findings in sections["critical"]:
        md.append(_render_section(title, findings))

    md.append("## Warning\n\n")
    for title, findings in sections["warning"]:
        md.append(_render_section(title, findings))

    md.append("---\n\n")
    md.append("Info-tier findings are in the full digest, not this alert.\n")
    md.append("Alert mechanism: local file, no SMTP. Differential delivery: ")
    md.append("a fresh alert is written only when section counts change vs the prior run.\n")
    return "".join(md)


def _save_state(state_path: Path, run_at_iso: str, counts: dict[str, int]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_run": run_at_iso, "section_counts": counts}
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


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
    p.add_argument(
        "--no-alert", action="store_true",
        help="write digest only; skip alert + state comparison",
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

    # Differential-delivery alert + state update
    alert_path: Path | None = None
    delta_lines: list[str] = []
    if not args.no_alert:
        alerts_dir = args.output_dir / "alerts"
        state_path = args.output_dir / "state.json"
        current_counts = _section_counts(sections)
        prior_state = _load_prior_state(state_path)
        changed, delta_lines = _state_changed(prior_state, current_counts)
        run_at_iso = datetime.now().isoformat(timespec="seconds")
        if changed:
            alerts_dir.mkdir(parents=True, exist_ok=True)
            alert_path = alerts_dir / f"alert_{today_pkt.strftime('%Y%m%d_%H%M')}.md"
            alert_path.write_text(
                _render_alert(today_pkt, db_path, sections, delta_lines, out_path)
            )
        _save_state(state_path, run_at_iso, current_counts)

    if not args.quiet:
        total_crit  = sum(len(f) for _, f in sections["critical"])
        total_warn  = sum(len(f) for _, f in sections["warning"])
        total_info  = sum(len(f) for _, f in sections["info"])
        print(f"digest written: {out_path}")
        print(f"  critical: {total_crit}  warning: {total_warn}  info: {total_info}")
        if not args.no_alert:
            if alert_path is not None:
                print(f"alert written: {alert_path}")
                for line in delta_lines:
                    print(f"  delta: {line}")
            else:
                print("no alert: state unchanged vs prior run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
