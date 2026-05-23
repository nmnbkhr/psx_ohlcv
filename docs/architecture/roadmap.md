# pakfindata — Architecture Roadmap

**Generated:** 2026-05-20
**Updated:** 2026-05-23 (Phase 1 complete; Phase 2 planning closed)

This roadmap tracks the service-decomposition arc that started after the
May 9 2026 corruption incident. The older instrument-universe roadmap
at [`docs/ROADMAP.md`](../ROADMAP.md) is a separate planning document
for data coverage (equities → ETFs → bonds etc.) and is unaffected by
this work.

## Phase numbering

| Phase | Theme | Status |
|---|---|---|
| **0** | Stabilization — write-path safety + observability primitives | ✅ **DONE 2026-05-20** (tag `v0.0-phase0`) |
| **1** | Service decomposition — Streamlit + FastAPI + worker split | ✅ **DONE 2026-05-23** (tag `v0.1-phase1`) |
| **2** | Data Coverage + Observability + Scope v2 | 📐 **PLAN WRITTEN 2026-05-23** — execution begins next |
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

## Phase 1 — Service Decomposition (DONE)

**Goal:** Streamlit becomes a thin read client over `/v1`. Worker
process is the only thing that runs ETL. Cron, UI, and CLI all
converge on the same code paths.

**Branch:** `phase1-decomposition` (merged to `master` 2026-05-23).
**Tag:** `v0.1-phase1` (commit `c842cf9` on `master`).

### Milestones

| # | Title | Closed | Notes |
|---|---|---|---|
| 1.1 | FastAPI scaffold + auth + systemd | 2026-05-20 | 3 commits; `pakfindata-api.service` on 127.0.0.1:8001 |
| 1.2 | Read endpoints — Wave A | 2026-05-20 | 16 `/v1` routes; OpenAPI v0.1 snapshot |
| 1.3 | Streamlit Wave A migration | 2026-05-20 | Dashboard + Market Pulse via `pakfindata.ui.api.client` |
| 1.4 | Worker scaffold + jobs queue | 2026-05-20 | 4 `/v1/jobs/*` routes; 1 `ping` handler |
| 1.5 | First sync migration — indices | 2026-05-20 | `etl.indices.sync()` shared by CLI + worker + UI |
| 1.6 | Bulk migration — 14 UI buttons + 5 CLI handlers | 2026-05-21 | 9 etl modules; worker registry now 15 handlers |
| 1.7 | UI migration — 41 pages across Groups A-G | 2026-05-22 | 152 `/v1` routes total; OpenAPI v0.2 snapshot; 9 dead pages deleted |
| 1.8 | Cron → API job enqueue + Phase 1 tag | 2026-05-23 | `daily_sync.sh` 6 STEPs migrated; CLI fallback intact |

### Achieved

- **152 `/v1` endpoints** — Streamlit reads via FastAPI; OpenAPI v0.2
  snapshot at `docs/api/openapi_v0.2.json` (v0.1 retained for history).
- **15 worker handlers** under one jobs queue — UI buttons enqueue,
  cron enqueues, CLI ad-hoc enqueues, all converge on
  `etl.<domain>.<fn>()` → `safe_writer` → `data_freshness`.
- **Page inventory finalized.** 41 fully migrated + 1 partial
  (research_terminal SQL editor carve-out) + 14 engine-call SKIPs +
  5 scraper-maintenance SKIPs + 2 composite-aggregator SKIPs + 4
  file-fed DEFERs + 5 broken-dep DEFERs + 1 untouched + 9 DELETED.
- **`daily_sync.sh` on worker pipeline** with CLI fallback. 6 STEPs
  enqueue via `/v1/jobs/<type>?source=cron`; 6 stay on direct CLI
  per Phase 1.6 skip list. API-down falls back to CLI; worker-down
  jobs persist as `pending` un-cancelled (per Milestone 1.8.3 design).
- **API + worker as systemd user services.** Bearer auth via global
  middleware; `~/.config/pakfindata/api.env` chmod 600.

### Phase 0 invariants preserved through all 8 milestones

- `src/pakfindata/db/safe_writer.py` at `447b889` unchanged
- `src/pakfindata/api_client.py` at `8c804a4` unchanged
- Cron schedule still `45 3 * * *` (execution path migrated, not timing)
- Pre-existing dirty `services/tick_service.py` deliberately not
  committed — slated for Phase 2.C.1

### Deviations from the original plan

- **1.7 grew from 1 milestone to 7 sub-groups (A-G)** — original plan
  underestimated the breadth; Group F's "engine-call-only" taxonomy +
  Group G's "composite-aggregator" taxonomy were discovered, not
  predicted.
- **Cloud-JSONL rsync** still not in cron — known_debt DEBT-PHASE2;
  daily_sync.sh `intraday_summaries_build` silently no-ops on missing
  files. Phase 2.B alerting surfaces this naturally.
- **Worker `PartOf=` lifecycle bug** surfaced during 1.8.2 fallback
  testing — recorded in known_debt; fix scheduled for Phase 2.B.2.

### Phase 1 deliverables — pointer index

- `src/pakfindata/api/` (the FastAPI service; 152 routes across
  `routes/`, `schemas/`, `deps/`)
- `src/pakfindata/worker/` (the job runner; 15 handlers in `handlers/`)
- `src/pakfindata/etl/` (shared functions; 9 domains)
- `src/pakfindata/ui/api/client.py` (~25-function wrapper module
  used by every migrated page)
- `src/pakfindata/db/jobs.py` (jobs table + enqueue/cancel/get helpers)
- `deploy/systemd/pakfindata-{api,worker}.service` (systemd unit files)
- `docs/api/openapi_v0.1.json` + `docs/api/openapi_v0.2.json` (API
  snapshots; v0.2 is current)
- `docs/architecture/page_inventory.md` (final post-1.7 disposition
  table)

---

## Phase 2 — Data Coverage + Observability + Scope v2 (NEXT)

Full plan: [`phase2_plan.md`](phase2_plan.md). Three sub-phases:

- **2.A — Data Coverage** ([`phase2_a_data_coverage.md`](phase2_a_data_coverage.md))
  4 milestones, ~3 weeks. Declarative validators (Phase 2.A.1), catalog
  ON CONFLICT root-cause fix (2.A.2 — REORDERED above backfills because
  the bug is live, not stale), regressed-table restoration (2.A.3 —
  includes `tbill_auctions` 175→12 row recovery + `pkisrv_daily` empty
  investigation), composite-aggregator endpoints (2.A.4).
- **2.B — Observability** ([`phase2_b_observability.md`](phase2_b_observability.md))
  4 milestones, ~1.5 weeks. **Stack choice settled: lightweight
  (`daily_digest.py` + cron `MAILTO=`), NOT Prometheus + Grafana.** Two
  explicit upgrade triggers baked into the digest banner: "read 3+
  times in a day" or "second operator joins." Worker `PartOf=` fix
  lands in 2.B.2 as decoupled lifecycle.
- **2.C — Scope v2** ([`phase2_c_scope_v2.md`](phase2_c_scope_v2.md))
  5 milestones, ~5 weeks. `tick_service.py` lands (2.C.1 — 3-commit
  structure: README first, harness second, dirty 8-line diff last).
  `load_ticks_from_disk` + `upsert_intraday` + `promote_intraday_to_eod`
  to worker. Live Ticker + intraday_quant_lab page migrations.
  intraday.py Index/Dedup/Sync tabs (held from 1.7.E).

**Sub-phase ordering defended:** 2.A → 2.B → 2.C. Reasoning in
[`phase2_plan.md`](phase2_plan.md#why-this-order). Short version: 2.A.2
catalog fix unblocks 2.B (otherwise dashboards show ZUMA/WTL rows);
2.B observability is the safety net for 2.C tick_service work.

**Risk register:** [`phase2_risks.md`](phase2_risks.md). Top risks:
2.A.4 composite endpoint shape (High impact, Medium probability —
two-prototype mitigation), 2.A.3.1 tbill backup recoverability
(Medium/Low), 2.C.4 Live Ticker WebSocket need (Medium/Medium).
**R-2.C.1 (tick_service.py landing) was downgraded from Medium to
Low impact** during planning close — the diff audit measured 8 lines
of insertion after 16 days dirty, meaning the file is "uncommitted
because the rule said so" not "uncommitted because actively volatile."
A small additive guard does not have May-9-class corruption surface.

**Scope-v2 boundary's original reason (corruption blast-radius
containment) is obsolete** per Phase 2.C plan. safe_writer +
data_freshness + tape-recorder pattern + worker decomposition
discharge the structural purpose. Phase 2.C is unblocked because
nothing structural blocks it, not because we're newly taking risk.

Estimated duration: ~8-12 weeks across 13 milestones. Phase 2 starts
from tag `v0.1-phase1`.

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
