"""Daily Digest viewer — Phase 2.B.5.

Read-only Streamlit page rendering the daily observability digest with
live drill-down tables. Two panels with explicit temporal semantics:

  Panel A — Digest snapshot
    Historical state captured at the digest's run time. Rendered as
    markdown straight from /mnt/e/psxdata/digest/digest_YYYYMMDD.md.
    Time-stamped: "Digest snapshot — YYYY-MM-DD (Xh Ym ago)".

  Panel B — Live state
    Current data as of now, queried fresh from canonical sources
    (find_stuck_jobs + /v1/freshness + /v1/quality). Time-stamped:
    "Live state — refreshed YYYY-MM-DD HH:MM PKT".

The two-panel layout exists because state moves between digest run
and viewer load. Markdown is the time-stamped record of "what was
true at digest time"; tables show "what's true now." When they
disagree, the disagreement is informative — typically it means a
stuck job was resolved (or a new one appeared) between cron run
and current load.

Drill-down strategy:
  - stuck-jobs: in-process call to pakfindata.observability.stuck_jobs.find_stuck_jobs
    (no API endpoint by design — the observability namespace exists for
    internal tooling consumption; an /v1/observability route would be
    ceremony with no consumer)
  - sync staleness: /v1/freshness (existing route, 2.A.1 era)
  - validator issues: /v1/quality (existing route, 2.A.1 era)

Delta indicator: stuck-jobs section compares the digest snapshot's
parsed row IDs against the live find_stuck_jobs() set. Rows present
in live but not in snapshot are flagged "NEW since snapshot"; rows
present in snapshot but absent from live are listed under an
expander as "resolved since snapshot." Helps operators answer "is
this alert still actionable?" at a glance.

Scope guardrails (per Phase 2.B planning doc):
  - VIEWER ONLY. No buttons for acknowledge / clear-stuck / re-run-cron.
  - Read-only. No DB writes from this page.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from pakfindata.observability.stuck_jobs import find_stuck_jobs
from pakfindata.ui.api import client as api_client


DIGEST_DIR = Path("/mnt/e/psxdata/digest")
PKT_OFFSET = timedelta(hours=5)


# --- digest-file parsing -----------------------------------------------------


@st.cache_data(ttl=300, show_spinner=False)
def _list_recent_digests(days: int = 7) -> list[dict]:
    """Last N days of digest metadata: date, path (if exists), counts.

    Counts parsed from each file's "## Critical" / "## Warning" /
    "## Info" sections. Cached 5min — dropdown changes infrequently
    and the parse is filesystem-bound for 7 small markdown files.
    """
    today_pkt = (datetime.now(timezone.utc) + PKT_OFFSET).date()
    out = []
    for i in range(days):
        d = today_pkt - timedelta(days=i)
        path = DIGEST_DIR / f"digest_{d.strftime('%Y%m%d')}.md"
        if path.exists():
            counts = _parse_section_counts(path)
            out.append({"date": d.isoformat(), "path": str(path), "exists": True, "counts": counts})
        else:
            out.append({"date": d.isoformat(), "path": None, "exists": False, "counts": None})
    return out


def _parse_section_counts(path: Path) -> dict[str, int]:
    """Count "- " bullets per top-level (## Critical / Warning / Info) block.

    "no findings" lines are excluded. The count is the human-meaningful
    "findings in this severity tier" total, not the raw line count.

    Truncates at the first "---" horizontal rule so the digest's
    scope-notes footer (which also uses "- " bullets) isn't counted
    as part of the last severity section.
    """
    txt = path.read_text()
    footer_idx = txt.find("\n---\n")
    if footer_idx > 0:
        txt = txt[:footer_idx]
    counts = {"critical": 0, "warning": 0, "info": 0}
    sections = re.split(r"^## ", txt, flags=re.MULTILINE)
    for section in sections:
        if not section.strip():
            continue
        first_line, _, body = section.partition("\n")
        title = first_line.strip().lower()
        if title not in counts:
            continue
        bullets = re.findall(r"^- (?!no findings)", body, flags=re.MULTILINE)
        counts[title] = len(bullets)
    return counts


def _parse_stuck_jobs_from_markdown(path: Path) -> set[str]:
    """Extract "{table}.{id}" keys from the snapshot's stuck-jobs section.

    Matches the StuckJob.__str__ format from Phase 2.B.3a:
    `<table>.<id> running since <ts> (<age>h)`.
    """
    txt = path.read_text()
    m = re.search(
        r"### Stuck jobs \(>24h\)\n\n(.+?)(?=\n### |\n## |\n---|\Z)",
        txt, re.DOTALL,
    )
    if not m:
        return set()
    body = m.group(1)
    keys: set[str] = set()
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("- ") or line.startswith("- no findings"):
            continue
        # "- table.id running since ..."
        rest = line[2:]
        key = rest.split(" running")[0].strip()
        if key:
            keys.add(key)
    return keys


def _file_age_human(path: Path) -> str:
    """Format file mtime as "Xd Yh ago" / "Xh Ym ago" / "Xm ago"."""
    delta = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    total_min = int(delta.total_seconds() // 60)
    if total_min < 60:
        return f"{total_min}m ago"
    h, m = divmod(total_min, 60)
    if h < 24:
        return f"{h}h {m}m ago"
    d, h = divmod(h, 24)
    return f"{d}d {h}h ago"


# --- live-state queries (each cached 60s) ------------------------------------


@st.cache_data(ttl=60, show_spinner=False)
def _live_stuck_jobs_df() -> pd.DataFrame:
    rows = find_stuck_jobs(threshold_hours=24)
    if not rows:
        return pd.DataFrame(columns=["table", "id", "started_at", "age_hours"])
    return pd.DataFrame(
        [
            {
                "table": r.table,
                "id": r.id,
                "started_at": r.started_at,
                "age_hours": round(r.age_hours, 1),
            }
            for r in rows
        ]
    )


@st.cache_data(ttl=60, show_spinner=False)
def _live_sync_staleness_df() -> pd.DataFrame:
    fresh = api_client.get_freshness()
    if not fresh:
        return pd.DataFrame()
    df = pd.DataFrame(fresh)
    if df.empty:
        return df
    cutoff = (datetime.now() - timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S")
    if "status" in df.columns and "last_sync_at" in df.columns:
        df = df[(df["status"] == "ok") & (df["last_sync_at"].fillna("") < cutoff)]
    return df


@st.cache_data(ttl=60, show_spinner=False)
def _live_quality_issues_df() -> pd.DataFrame:
    summary = api_client.get_quality_summary()
    if not summary:
        return pd.DataFrame()
    df = pd.DataFrame(summary)
    if df.empty:
        return df
    # Filter to domains with any non-zero error/warn counts when available
    for col in ("errors", "warnings", "failed"):
        if col in df.columns:
            df = df[df[col].fillna(0) > 0] if df[df[col].fillna(0) > 0].shape[0] else df
    return df


# --- rendering --------------------------------------------------------------


def _render_snapshot_panel(selected: dict) -> Path | None:
    """Render Panel A. Returns the path of the rendered file (for Panel B's
    delta computation) or None if no digest exists for this date."""
    if not selected["exists"]:
        st.info(
            f"No digest for {selected['date']}. Cron at 04:00 PKT writes the daily digest; "
            "run `python scripts/daily_digest.py` to generate one now."
        )
        return None

    path = Path(selected["path"])
    age = _file_age_human(path)
    st.subheader(f"Digest snapshot — {selected['date']} ({age})")
    st.caption(f"Source: `{path}` — historical state captured at digest run time")
    st.markdown(path.read_text())
    return path


def _render_live_panel(snapshot_path: Path | None) -> None:
    """Render Panel B. If snapshot_path is given, compute delta indicators
    for the stuck-jobs table."""
    now_pkt = (datetime.now(timezone.utc) + PKT_OFFSET).strftime("%Y-%m-%d %H:%M PKT")
    st.subheader(f"Live state — refreshed {now_pkt}")
    st.caption(
        "Drill-downs against current DB / API. Cache TTL=60s. "
        "If live differs from snapshot, state has moved since the digest run."
    )

    # --- stuck jobs (with delta indicator) ---
    snapshot_keys: set[str] = (
        _parse_stuck_jobs_from_markdown(snapshot_path) if snapshot_path else set()
    )
    stuck_df = _live_stuck_jobs_df()
    live_keys = (
        {f"{r['table']}.{r['id']}" for _, r in stuck_df.iterrows()}
        if not stuck_df.empty else set()
    )

    new_since = live_keys - snapshot_keys
    resolved_since = snapshot_keys - live_keys

    st.markdown(f"**Stuck jobs (>24h)** — {len(stuck_df)} rows")
    if not stuck_df.empty:
        display_df = stuck_df.copy()
        display_df["delta"] = display_df.apply(
            lambda r: "NEW" if f"{r['table']}.{r['id']}" in new_since else "",
            axis=1,
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No stuck jobs >24h currently.")

    if resolved_since:
        with st.expander(f"↻ Resolved since snapshot ({len(resolved_since)})"):
            for key in sorted(resolved_since):
                st.write(f"- {key}")

    st.divider()

    # --- sync staleness ---
    stale_df = _live_sync_staleness_df()
    st.markdown(
        f"**Sync staleness (status=ok, last_sync_at > 36h)** — "
        f"{len(stale_df) if not stale_df.empty else 0} rows"
    )
    if not stale_df.empty:
        cols = [
            c for c in ["domain", "last_sync_at", "last_row_date", "row_count", "status"]
            if c in stale_df.columns
        ]
        st.dataframe(stale_df[cols] if cols else stale_df, use_container_width=True, hide_index=True)
    else:
        st.info("No stale syncs currently.")

    st.divider()

    # --- validator issues ---
    quality_df = _live_quality_issues_df()
    st.markdown(
        f"**Validator issues** — "
        f"{len(quality_df) if not quality_df.empty else 0} domains with errors/warnings"
    )
    if not quality_df.empty:
        st.dataframe(quality_df, use_container_width=True, hide_index=True)
    else:
        st.info("No validator failures currently.")


def render_digest_viewer() -> None:
    st.title("Daily Digest")
    st.caption(
        "Two-panel viewer for the daily observability digest. "
        "Snapshot panel shows historical state at digest run time. "
        "Live state panel shows current state with drill-down tables. "
        "Read-only — no operator actions taken from this page."
    )

    digests = _list_recent_digests(days=7)
    options: list[tuple[dict, str]] = []
    for d in digests:
        if d["exists"]:
            c = d["counts"]
            label = f"{d['date']}  (critical: {c['critical']}, warning: {c['warning']}, info: {c['info']})"
        else:
            label = f"{d['date']}  (no digest — manual fix needed)"
        options.append((d, label))

    # Default to today if it has a digest, else most recent that does
    default_idx = 0
    if not options[0][0]["exists"]:
        for i, o in enumerate(options):
            if o[0]["exists"]:
                default_idx = i
                break

    cols = st.columns([4, 1])
    with cols[0]:
        selected_label = st.selectbox(
            "Snapshot date",
            [o[1] for o in options],
            index=default_idx,
            help="Last 7 days of digest output. Section counts are parsed from each file.",
        )
    with cols[1]:
        st.write("")  # vertical alignment with selectbox
        if st.button("Refresh", help="Clear cache and re-query live data"):
            st.cache_data.clear()
            st.rerun()

    selected = next(o[0] for o in options if o[1] == selected_label)

    snapshot_path = _render_snapshot_panel(selected)

    st.divider()

    _render_live_panel(snapshot_path)

    st.divider()
    st.caption(
        "Operator actions (clearing stuck jobs, re-running cron, investigating root causes) "
        "happen via CLI / systemd / direct DB ops — not from this page."
    )
