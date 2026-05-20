# pakfindata — Architecture Roadmap

**Generated:** 2026-05-20

This roadmap tracks the service-decomposition arc that started after the
May 9 2026 corruption incident. The older instrument-universe roadmap
at [`docs/ROADMAP.md`](../ROADMAP.md) is a separate planning document
for data coverage (equities → ETFs → bonds etc.) and is unaffected by
this work.

## Phase numbering

| Phase | Theme | Status |
|---|---|---|
| **0** | Stabilization — write-path safety + observability primitives | ✅ **DONE 2026-05-20** |
| **1** | Service decomposition — Streamlit + FastAPI + worker split | scheduled, plan written |
| **2** | Observability — logs, metrics, traces, alerting | planning starts after Phase 1 |
| **3** | Cloud-ready — Postgres + containerization + multi-user | planning starts after Phase 2 |

---

## Phase 0 — Stabilization (DONE)

**Goal:** make the May 9 corruption class structurally impossible and
build the primitives Phase 1+ needs (catalog, cron, recovery runbook,
inventory).

**Branch:** `phase3-ui-arch` (the branch was named before Phase 0 was
carved out; keep the name).
**Tag:** `v0.0-phase0` (cut at the end of Milestone 0.5).

### Milestones

| # | Title | Closed | Commits | Net |
|---|---|---|---|---|
| 0.1 | SafeWriter migration | 2026-05-18 | 16 (waves W1–W5c) | +147/-59 |
| 0.2 | data_freshness catalog (single source of truth) | 2026-05-18 | 11 (ef18577→6fe441e) | +784/-75 |
| 0.3 | daily_sync.sh + cron pipeline | 2026-05-18 | 6 (ec63da5→35d6927) | +1117/-42 |
| 0.4 | DR drill + corruption runbook | 2026-05-20 | 4 (2d5b880→ea53c9c) | +778/-10 |
| 0.5 | Page inventory + Phase 1 prep | 2026-05-20 | 5 (this milestone) | docs only |

### Achieved

- **safe_writer migration is complete.** Every UI sync button uses the
  context manager; per-row commits on cached singleton connections (the
  May-9 root cause) are now structurally impossible.
- **`data_freshness` is the single source of truth for catalog state.**
  Every safe_writer-managed sync writes a row atomically with its data
  writes. Dashboard / Intraday dropdown / Index Monitor freshness
  badges no longer disagree.
- **`scripts/daily_sync.sh` runs the full pipeline at 03:45 PKT via
  cron** (when cron is enabled — see known_debt). The pipeline produces
  20+ catalog rows in ~19 minutes; trading-day-aware; per-step failures
  don't abort the rest.
- **DR drill verified the recovery path** on a copy of the live 14 GB
  DB. High-value tables (eod_ohlcv, tick_data, pib_auctions, etc.)
  recover at 98-100% even from a partial scan. Runbook captures real
  numbers, not theory.
- **Page inventory + Phase 1 wave plan + debt registry** are written,
  so Phase 1 can start without re-discovering the codebase.

### Deviations from the original plan

- **0.1 grew from 5 → 11 sub-waves** because the safe_writer audit
  uncovered Option-A `con.commit()` scaffolds in 6 places that weren't
  in the original plan. All caught + cleaned in the milestone.
- **0.2 took a pragmatic Option-A path** — extended the pre-existing
  `data_freshness` table rather than creating a new `data_catalog`.
  Migration was idempotent; avoided orphaning the existing table.
- **0.3 found `daily_sync.sh` already existed** as a stale March-2026
  stub (wrong env, wrong DB path, ran nothing useful). Full rewrite
  instead of new file.
- **0.4 ran a PARTIAL drill** (18.7% of pages scanned, 1h09m wall
  time) rather than the full 5-hour scan. The partial scan was
  sufficient to validate the recovery path; the runbook documents the
  5-hour projection. Quarterly drill plan compensates.
- **0.5 surfaced 28 hidden URL-only pages** that the original Phase 0
  plan didn't account for. Classification handled cleanly in the
  inventory.

### Phase 0 deliverables — pointer index

- `src/pakfindata/db/safe_writer.py` (Phase 0.1)
- `src/pakfindata/db/catalog.py` (Phase 0.2)
- `src/pakfindata/utils/trading_calendar.py` (Phase 0.3)
- `scripts/daily_sync.sh` (Phase 0.3)
- `scripts/dr_drill.sh` (Phase 0.4)
- `scripts/sqlite_page_recover.py` (pre-Phase 0; validated in 0.4)
- `scripts/backup_psx_sqlite.sh` (pre-Phase 0; validated in 0.4)
- `docs/operations/cron_setup.md` (Phase 0.3)
- `docs/operations/dr_drill_log.md` (Phase 0.4)
- `docs/runbooks/db_corruption.md` (Phase 0.4)
- `docs/architecture/page_inventory.md` (Phase 0.5)
- `docs/architecture/phase1_migration_groups.md` (Phase 0.5)
- `docs/architecture/phase1_plan.md` (Phase 0.5)
- `docs/architecture/known_debt.md` (Phase 0.5)
- `docs/architecture/roadmap.md` (this file, Phase 0.5)
- CLI subcommand additions: `pfsync indices sync`, `pfsync summary
  rebuild-today/rebuild-missing`, `pfsync intraday ticks-fetch /
  ticks-load / summaries-build`, `pfsync parquet export-today /
  export-all / sync-missing / status`. (Phase 0.3)
- CLI catalog parity in 6 existing handlers: rates, treasury,
  fx-rates, market-summary, regular-market, announcements. (Phase 0.3)

---

## Phase 1 — Service Decomposition (NEXT)

Full plan: [`phase1_plan.md`](phase1_plan.md). Migration groups:
[`phase1_migration_groups.md`](phase1_migration_groups.md). Estimated
~7 weeks across 16 milestones. End-state:

- Streamlit never writes to the DB.
- FastAPI is the only thing that talks to SQLite for writes.
- A worker process is the only thing that does ETL.
- `daily_sync.sh` becomes a single `curl` POST to the worker queue.

Phase 1 starts from tag `v0.0-phase0`.

---

## Phase 2 — Observability (PLANNING)

Not planned in detail yet. Expected scope (subject to revision when
Phase 1 closes):

- Structured logs from FastAPI + worker → centralized log store.
- Prometheus metrics on API latency, worker job duration, catalog
  freshness, queue depth.
- Grafana dashboards (Bloomberg-aesthetic — dark + gold accents to
  match the existing UI).
- Alerts on: catalog row `status='failed'` for > N hours; worker job
  queue depth > threshold; daily_sync no-op (the cloud-JSONL gap from
  Phase 0.3 surfaces here).
- Fixes for the `announcements sync` 18-minute loop and other
  performance debt that Phase 1's worker model brings into view.
- Replacement for the broken pages (FTP Monitor, Global Rates, NPC
  Rates, Website Scan) — once monitoring data is flowing, the missing
  tables become natural to backfill.

---

## Phase 3 — Cloud-Ready (PLANNING)

Not planned yet. Expected scope:

- Postgres migration (SQLite → Postgres). The pydantic + SQLAlchemy
  shims in Phase 1 should make this mechanical.
- Containerization (Docker + compose; later Kubernetes).
- Multi-user auth (replacing the single static API token from Phase 1).
- CI/CD (currently no CI; commits go straight to the branch).
- Cloud-native storage for tick data (the JSONL files on NTFS go away).
- Public API documentation (OpenAPI publishing).

---

## How to read this roadmap

- **DONE** phases have a tag in git history and an exit-criteria
  checklist that passed. If you want to know what changed in a given
  phase, `git log v0.X-phaseY^..v0.(X+1)-phase(Y+1)` plus the
  milestone final-audit files at `/mnt/e/psxdata/phase0_*_final_audit_*.md`.
- **NEXT** phases have a written plan + waves + milestone breakdown
  ready to execute.
- **PLANNING** phases have a one-paragraph scope sketch and are not
  ready to execute. They'll get their own page-inventory-style audit
  when the prior phase closes.
- The boundary between phases is a git tag, not a date.
