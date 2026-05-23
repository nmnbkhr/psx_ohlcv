# Phase 2.B — Observability

**Sub-phase parent:** [`phase2_plan.md`](phase2_plan.md)
**Goal:** Make the system visible. "Did yesterday's sync run clean?"
becomes a single command, not a journalctl grep. Worker lifecycle fix
lands. This sub-phase is the safety net for 2.C.
**Duration estimate:** ~1.5 weeks (see stack choice below).
**Starting point:** Phase 2.A complete (clean catalog, validators
operational).

**Phase 2.B prerequisites met:**
- ☑ `data_freshness` rows trustworthy (Phase 2.A.2 catalog fix)
- ☑ `data_quality_results` populated (Phase 2.A.1 validators)
- ☑ Jobs queue history meaningful (Phase 1.4-1.6)
- ☑ `daily_sync.sh` emits structured `STEP_*` log lines

---

## Stack choice — settled explicitly, not by vibe

Two competing shapes for "observability":

### Lightweight: journalctl + daily digest script

- One bash/python script run after `daily_sync.sh`
- Emits a digest (plain text, ~50 lines) covering yesterday
- Output to file + optionally email
- View as plain text in `less` or VS Code

### Full stack: Prometheus + Grafana

- Prometheus exporter in API + worker
- 4 Grafana dashboards
- Alertmanager for paging
- Loki or similar for log aggregation
- Permanent maintenance overhead

### When each is the right choice

**Lightweight is right when ALL of these hold:**

| Criterion | This project |
|---|---|
| Solo project, no SRE team | ☑ |
| Operations are once-daily + ad-hoc | ☑ (cron 03:45 + UI clicks) |
| Total compute footprint is small | ☑ (2 systemd services, 1 cron) |
| Failure modes are countable | ☑ (API down, worker down, step failed, validation failed) |
| Today's pain point is "did it run clean?" | ☑ (current answer: `journalctl -u` + manual grep) |

**Full stack becomes right when ANY of these holds:**

| Criterion | This project |
|---|---|
| Multi-service contention (3+ services interacting) | ☒ (2 services, both single-tenant) |
| Sub-second latency SLOs | ☒ (daily cron; reads p95 < 200ms; jobs are seconds-to-hours) |
| Alerting beyond email-on-cron-failure | ☒ (no pager team) |
| Cross-week time-series queries that don't fit SQL | ☒ (SQL against `jobs` + `data_freshness` + `data_quality_results` covers it) |

**Decision: lightweight.** Every "lightweight" row is checked; every
"full stack" row is unchecked. There is no Prometheus signal here that
SQL can't answer — and there is no team to maintain Grafana.

**This is not a compromise — it's a fit.** A solo operator running
two services on one box doesn't need the same telemetry surface as a
50-engineer multi-cluster deploy. Picking lightweight reduces Phase 2.B
from ~4 weeks (Prometheus + Grafana + Alertmanager + Loki) to ~1.5
weeks (one script + a unit-file fix + alert wiring).

### Upgrade trigger — when to revisit

Revisit the choice if EITHER:

1. **You find yourself opening the daily digest more than 3 times per
   day to investigate something.** That means the digest isn't enough —
   you want a live view, which is what dashboards exist for.
2. **A second operator joins** (user, on-call partner, contractor).
   Coordination across two people benefits from a shared, real-time
   surface; plain-text digests don't.

Until either trigger fires, the lightweight path stays.

The digest script gets a banner at the top:

```
# Daily Digest — pakfindata observability
#
# If you've opened this file 3+ times today, or you've started sharing
# operational responsibility with a second person, file an upgrade
# ticket against Phase 2.B and move to Prometheus + Grafana.
```

so the trigger is impossible to forget about.

---

## Milestones

| # | Title | Effort | Dependencies |
|---|---|---:|---|
| 2.B.1 | Daily digest script + cron wiring | 3d | 2.A complete |
| 2.B.2 | Worker `PartOf=` unit-file fix | 1d | independent of 2.B.1 |
| 2.B.3 | Alert wiring (failure-trigger emails) | 2d | 2.B.1 |
| 2.B.4 | Documentation + upgrade-trigger guardrails | 1d | 2.B.1-3 |

Note: 2.B.2 is a unit-file fix, not an observability question. It's
parked here because it surfaced during the Milestone 1.8.2 fallback
test and is too small to deserve its own phase, but it has no logical
ordering dependency on 2.B.1.

---

## Milestone 2.B.1 — Daily digest script

**File:** NEW `scripts/daily_digest.py` (or `.sh` — pick during Step 0)

### What the digest must answer

Each section is a single ~5-line block in the output. The whole digest
fits on one screen.

```
═══ Yesterday's daily_sync.sh ═══
  STEP_OK  count=11   STEP_FAIL count=0   total_duration_s=1142
  Failed steps: (none)

═══ Jobs (last 24h) ═══
  Total: 18   ok: 17   failed: 1   timeout: 0
  By type:
    sync_indices                   3x  median=8s
    sync_rates_bundle              3x  median=6s
    sync_treasury_auctions         3x  median=3s
    build_intraday_summary         3x  median=12s
    rebuild_eod_summary_today      3x  median=2s
    sync_regular_market            3x  median=4s
  Failures:
    job_id=147 type=build_intraday_summary error="FileNotFoundError ticks_2026-05-22.jsonl"

═══ data_freshness (stale rows) ═══
  domain                     last_sync_at         row_count   status
  ftp_rates                  (never)              0           unknown
  mutual_funds               (never)              0           unknown
  (only domains > expected staleness threshold listed)

═══ data_quality_results (last 24h) ═══
  domain          rule                          severity  status   count
  konia_daily     range(rate_pct,0..50)         error     pass     1
  psx_indices     date_format(index_date)       error     pass     1
  sovereign_curve source_coverage(PKRV...)      warn      fail     1
  Failed rules above WARN threshold: 1

═══ Service uptime (last 24h) ═══
  pakfindata-api.service     uptime=23h47m  restarts=0
  pakfindata-worker.service  uptime=23h47m  restarts=0

═══ Disk + DB ═══
  psx.sqlite: 8.2 GB    /mnt/e/psxdata: 1.4 TB free / 3.6 TB
  Last backup: psx_20260530.sqlite (today, 02:00)
```

### Step 0

```bash
echo "═══ Inventory data sources the digest needs ═══"
echo ""
echo "1. STEP_OK / STEP_FAIL log lines from daily_sync.sh"
grep -E "STEP_(OK|FAIL)" \
    ~/.local/share/pakfindata/logs/daily_sync_*.log | wc -l
echo ""
echo "2. jobs table"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT COUNT(*) FROM jobs WHERE enqueued_at > datetime('now', '-1 day');"
echo ""
echo "3. data_freshness staleness threshold per domain (need to define)"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT COUNT(DISTINCT domain) FROM data_freshness;"
echo ""
echo "4. data_quality_results table from 2.A.1"
sqlite3 ~/psxdata_rescue/psx.sqlite "
SELECT COUNT(*) FROM data_quality_results
WHERE checked_at > datetime('now', '-1 day');"
echo ""
echo "5. systemd uptime via systemctl show"
systemctl --user show pakfindata-api.service \
    -p ActiveState -p ActiveEnterTimestamp
```

### Sub-waves

- **2.B.1.1 — Skeleton + STEP log parser.** Tail the most recent
  `daily_sync_<date>.log`, count `STEP_OK` / `STEP_FAIL`, sum
  `duration_s=` values, surface failed steps with their exit codes.
- **2.B.1.2 — Jobs section.** SQL against `jobs` table — last 24h
  aggregated by `job_type`, list failures with `error` column.
- **2.B.1.3 — `data_freshness` + `data_quality_results` sections.**
  Staleness threshold lives in a small config dict
  (`domain → max_age_hours`). Quality failures of any severity surfaced.
- **2.B.1.4 — Uptime + disk section.** `systemctl show -p` for the
  two services; `du` / `df` for DB + data root.
- **2.B.1.5 — Cron wiring.** Add to crontab: `50 3 * * *
  ~/projects/pakfindata/scripts/daily_digest.py
  >> ~/.local/share/pakfindata/logs/digest_$(date +\%Y\%m\%d).log
  2>&1`. Runs 5 minutes after `daily_sync.sh` (which finishes by ~04:15
  most days based on Phase 0.3 measurements).

### Exit criteria

- `scripts/daily_digest.py` runs cleanly on demand
- Output answers all 5 sections above on real data
- Cron entry exists; logs accumulate under
  `~/.local/share/pakfindata/logs/digest_*.log`
- Retention matches `daily_sync` (30 days; old logs auto-cleaned)

### What NOT to do in 2.B.1

- Don't add an HTTP endpoint. The digest is a file, not a service.
- Don't introduce Jinja templates — f-strings are fine for a 50-line
  output.
- Don't write a parser for arbitrary `journalctl` output. The
  `daily_sync` log format is structured (`STEP_OK name=X duration_s=N`);
  parse only that.
- Don't compute anything the underlying tables don't expose. If
  `data_quality_results` from 2.A.1 doesn't have what you need, fix
  it in 2.A.1, not here.

---

## Milestone 2.B.2 — Worker `PartOf=` unit-file fix

**Files:**
- `~/.config/systemd/user/pakfindata-worker.service`
- (and snapshot copy in `deploy/systemd/pakfindata-worker.service` for repo)

**Bug** (from Milestone 1.8.2 fallback test, recorded in known_debt):

```ini
[Unit]
PartOf=pakfindata-api.service
```

This means: stopping the API stops the worker. But starting the API
does NOT start the worker. After every API redeploy (including
`systemctl restart pakfindata-api`) the worker is silently dead and
the cron pipeline blindly times out 30 minutes later on every
enqueued job.

### Three options + pick

| Option | Lifecycle |
|---|---|
| Keep `PartOf=`, add `Wants=pakfindata-api.service` to API unit | Half-fix. Doesn't actually restart the worker on API start. |
| Replace `PartOf=` with `BindsTo=` + `After=` | Worker dies AND restarts when API does. Clean. |
| Decouple entirely — remove `PartOf=` | Worker survives API outages. Worker can still process jobs (which it does — they're DB-backed). But API restarts no longer signal anything to the worker. |

**Pick: option 3 (decouple).** Reasoning:

- The worker doesn't depend on the API for execution. Jobs queue in
  SQLite; worker polls directly. They share data, not lifecycle.
- API restarts happen often (every endpoint change). Each one
  rippling into a worker restart causes unnecessary job churn.
- A worker that survives an API outage actually does work — drains
  the queue even when no new enqueues land. That's a feature.
- Daily digest (2.B.1) and 2.B.3 alerts cover the visibility need;
  we don't need lifecycle coupling for failure detection.

### Sub-waves

- **2.B.2.1 — Audit current unit files.** Diff
  `~/.config/systemd/user/pakfindata-{api,worker}.service` against
  `deploy/systemd/pakfindata-{api,worker}.service` in the repo.
- **2.B.2.2 — Patch.** Remove `PartOf=` from the worker unit. Update
  the repo snapshot to match.
- **2.B.2.3 — Reload + verify.** `systemctl --user daemon-reload`;
  restart API; confirm worker still active.
- **2.B.2.4 — Regression test.** Add a manual test note to
  `docs/operations/worker_service.md`: after every API redeploy,
  `systemctl --user is-active pakfindata-worker` should still be
  `active`.

### Exit criteria

- API restart no longer stops the worker
- Worker survives 24h API outage; resumes draining the queue when API
  returns (already true; just verified)
- `deploy/systemd/pakfindata-worker.service` in the repo matches the
  installed unit
- Manual regression test documented

### What NOT to do in 2.B.2

- Don't migrate to system-level (not user-level) systemd units. That's
  a separate hardening question, Phase 3 territory.
- Don't add restart-on-failure escalation (`Restart=on-failure`) here.
  That belongs in a dedicated reliability milestone if ever needed.

---

## Milestone 2.B.3 — Alert wiring

**Goal:** when something fails, an email lands. Not "Slack notification
via Alertmanager + custom webhook" — actual `mail` or `mailx` invocation
from cron.

### What triggers an alert

| Condition | Detected by | Action |
|---|---|---|
| Any `STEP_FAIL` line in last 24h | `daily_digest.py` exit code non-zero | cron `MAILTO=` sends digest |
| `data_quality` error-severity failure | digest section count > 0 | same |
| Worker not active when digest runs | `systemctl is-active` non-zero | same |
| `data_freshness` row older than its staleness threshold | digest section count > 0 | same |
| `daily_sync.sh` cron exit non-zero | cron's built-in `MAILTO=` | direct |

**Mechanism:** the cron entry sets `MAILTO=noman.bukhari@gmail.com` at
the top of the file. Cron emails the user only if the script writes to
stderr OR exits non-zero. The digest script exits non-zero iff any
alert condition fires; otherwise it stays silent.

This is the standard Unix pattern. No new infrastructure.

### Sub-waves

- **2.B.3.1 — Mail delivery check.** Confirm the host has a working
  MTA. `echo test | mail -s 'test' $EMAIL` arrives.
- **2.B.3.2 — Digest exit codes.** Make `daily_digest.py` return:
  - `0` if everything's fine (no email sent by cron)
  - `1` if any STEP_FAIL or job failure or quality error
  - `2` if worker is down
- **2.B.3.3 — Cron config.** `MAILTO=$EMAIL` at the top of crontab.
  Wire `daily_sync.sh` and `daily_digest.py` cron lines to inherit it.

### Exit criteria

- Real STEP_FAIL on a deliberately broken sync triggers a mail
- Worker stopped → digest exits 2 → mail
- Clean run → no mail (cron stays silent on exit 0)

### What NOT to do in 2.B.3

- Don't add Slack / Discord / Telegram / Pushover. Phase 3 if ever.
- Don't add severity tiers in the alert path itself (info vs warn vs
  error). The digest body conveys severity in plain text.

---

## Milestone 2.B.4 — Documentation + upgrade-trigger guardrails

**Files:**
- NEW `docs/operations/observability.md`
- UPDATE `docs/operations/dr_drill_log.md` (cross-reference)

### What the doc covers

- The stack-choice reasoning (excerpted from this file)
- How to read the daily digest (5 sections explained)
- How to investigate when the digest says something failed (links to
  `journalctl` recipes, `jobs` table queries)
- **The two upgrade triggers** — reproduced verbatim from this file's
  "Upgrade trigger" section. Place them prominently:
  - "Have I read this digest more than 3 times today?"
  - "Has anyone else joined the operations?"

### Sub-waves

- **2.B.4.1 — Write `observability.md`.**
- **2.B.4.2 — Add the upgrade-trigger banner** to the top of
  `scripts/daily_digest.py` so the human reads it before the digest
  content.
- **2.B.4.3 — Update CLAUDE.md** with a one-line pointer to
  `observability.md` under a new Operations section.

### Exit criteria

- `docs/operations/observability.md` exists
- Daily digest output begins with the upgrade-trigger banner
- CLAUDE.md mentions the digest as the canonical "is the system OK?"
  surface

### What NOT to do in 2.B.4

- Don't write a Grafana migration guide. If/when the upgrade trigger
  fires, write it then with real motivation. Pre-writing it adds
  pressure to migrate when the trigger isn't really there yet.

---

## Phase 2.B exit criteria (rollup)

- ☐ `daily_digest.py` runs nightly via cron; output covers 5 sections
- ☐ Worker survives API restart (`PartOf=` removed)
- ☐ Email alerts fire on real failure conditions
- ☐ Documentation lists the two upgrade triggers prominently
- ☐ "Did yesterday's sync run clean?" answered by `cat
  ~/.local/share/pakfindata/logs/digest_<date>.log` — not a journalctl
  grep

When all 5 boxes check, send the Phase 2.C kickoff prompt.

---

## Notes for the eventual Prometheus upgrade

If/when the trigger fires, the migration is:

- API: add `prometheus_fastapi_instrumentator` (one decorator)
- Worker: emit metrics via `prometheus_client` + a `/metrics` endpoint
- Scrape config + Grafana dashboards (4 panels — System Health, Jobs
  Queue, Data Quality, Sync History)
- Alertmanager replaces the cron `MAILTO=` path

**Estimated migration effort when triggered:** ~2 weeks. Less than
the original 4-week estimate because the digest script already
identified the metrics surface — Prometheus would expose the same
numbers, just continuously instead of daily.

Do NOT do this migration prematurely. The whole point of writing this
section is so you don't have to design the upgrade path under
pressure later.
