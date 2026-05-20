# Phase 1 — Service Decomposition

**Goal:** Streamlit never writes to the DB. All data access via FastAPI.
All ETL in a worker process.

**Duration estimate:** 6-7 weeks of focused work; longer if scope expands
during execution (likely — the file splits in Waves B/D/E/F often surface
hidden dependencies).

**Starting point:** Tag `v0.0-phase0` (after Milestone 0.5 closes).

**Phase 0 prerequisites met:**
- ☑ safe_writer migration complete (every UI sync button uses it)
- ☑ `data_freshness` is the single source of truth for catalog state
- ☑ `daily_sync.sh` runs the full pipeline via CLI
- ☑ DR drill verified the recovery path
- ☑ Page inventory + Phase 1 wave plan written

## Milestones

| # | Title | Files touched | Effort | Dependencies |
|---|---|---|---:|---|
| 1.1 | FastAPI scaffold + auth + Postgres-readiness shims | NEW: src/pakfindata/api/ + tests | 1d | v0.0-phase0 |
| 1.2 | Read endpoints — Wave A (market overview) | NEW: api/routes/market.py | 2d | 1.1 |
| 1.3 | Streamlit reads via API — Wave A pages | 5 page files | 2d | 1.2 |
| 1.4 | Jobs table + worker scaffold (RQ or arq) | NEW: worker/ + jobs schema | 2d | 1.1 |
| 1.5 | First migrated write — Sync Indices via worker | 3 files (UI + api + worker) | 1d | 1.4 |
| 1.6 | Remaining Wave A write paths via worker | ~5 files | 2d | 1.5 |
| 1.7 | Wave B — Fixed income split + migration | fixed_income.py → 8 files | 5d | 1.6 |
| 1.8 | Wave C — FX consolidation | 5 → 2 files | 2d | 1.7 |
| 1.9 | Wave D — Funds split + migration | fund_explorer.py + funds.py | 3d | 1.7 |
| 1.10 | Wave E — Equities split + migration | company_deep.py → 4 files | 4d | 1.9 |
| 1.11 | Wave F — Intraday split (HIGH RISK) | intraday.py → 7 files | 5d | 1.10 |
| 1.12 | Wave G — Research / strategies / advanced | 24 files (read-only API swap) | 3d | 1.11 |
| 1.13 | Wave H — ALM + ADMIN consolidation (Market Sync v1) | 9 files + new admin page | 3d | 1.12 |
| 1.14 | Cron migrates to API job enqueue | scripts/daily_sync.sh | 0.5d | 1.13 |
| 1.15 | Deprecate legacy ETL paths | cleanup commit | 1d | 1.14 |
| 1.16 | Phase 1 final audit + tag v0.1-phase1 | docs only | 0.5d | 1.15 |

Total: ~36 working days = **~7 weeks** assuming 5 working days per week and no slips.

## Hard rules for Phase 1

1. **Same per-milestone discipline as Phase 0:** Step 0 audit, one
   sub-wave = one commit, DO NOT TOUCH list per milestone.
2. **FastAPI must come up cleanly as a systemd service before any UI
   migration.** Milestone 1.1 includes the systemd unit.
3. **The worker must run as a systemd service before any ETL migration
   in 1.4+.**
4. **Each milestone has a rollback path:** if 1.4 breaks, 1.3's UI
   keeps reading from the API. If the API breaks, the UI falls back to
   direct DB reads behind a feature flag. The flag is removed in 1.16.
5. **DO NOT migrate to Postgres yet.** Phase 1 is decomposition only.
   Postgres is Phase 3. But: use SQLAlchemy / pydantic models for the
   API surface so the eventual Postgres migration is mechanical.
6. **DO NOT add observability (Prometheus / OpenTelemetry) yet.**
   That's Phase 2. Logs to stdout, errors to a `job_errors` table, and
   the existing `~/.cron_daily_sync.log` are enough.
7. **DO NOT add multi-user auth.** Phase 1 ships with a single static
   API token (env var). Multi-user is Phase 3.
8. **DO NOT add CI/CD or containerization.** Both are Phase 3.

## Exit criteria for Phase 1

- `grep -rn "sqlite3.connect" src/pakfindata/ui/` returns **zero** hits.
  UI never opens DB directly.
- `grep -rn "client.connection" src/pakfindata/ui/` returns zero hits.
  UI uses the API client, not the DB client.
- FastAPI is the only thing that talks to SQLite for writes. Workers
  read the job queue, do the work, write back via repos, mark the job
  done. Streamlit never writes.
- FastAPI uptime > 99% during a week of normal operation (manual
  observation, not formal SLO yet).
- All sync paths go through the worker job queue. `daily_sync.sh`
  becomes a one-liner: `curl -X POST .../v1/jobs/queue/daily-cron`.
- One developer (not the current owner) could read the architecture
  docs and contribute a new endpoint in their first day.
- All Wave A–H pages render correctly via API, with no perceptible
  performance regression.
- `docs/architecture/api_surface.md` documents every public endpoint
  with request/response samples. (Written in 1.16.)

## What stays out of Phase 1

- Postgres migration (Phase 3)
- Observability stack — Prometheus, Grafana, OpenTelemetry (Phase 2)
- Multi-user auth (Phase 3)
- Containerization (Phase 3)
- CI/CD (Phase 3)
- Public API documentation / OpenAPI publishing (Phase 3)
- Replacement for broken pages (FTP Monitor, Global Rates, NPC Rates,
  Website Scan, Stock Graph GNN) — Phase 2 will pick these up

## Architecture sketch

```
                                                ┌─────────────────────┐
                                                │  Cron (03:45 PKT)   │
                                                │  daily_sync.sh →    │
                                                │  POST /v1/jobs/queue│
                                                └─────────┬───────────┘
                                                          │
┌──────────────┐   HTTPS    ┌──────────────────────┐      │  enqueue
│  Streamlit   │ ─────────▶ │   FastAPI            │ ─────┘
│  (UI only)   │ ◀───────── │   - /v1/* reads      │
│  read-only   │   JSON     │   - /v1/jobs/queue   │
└──────────────┘            │   - /v1/jobs/status  │
                            └──────────┬───────────┘
                                       │ enqueue
                                       ▼
                            ┌──────────────────────┐
                            │   Worker process     │
                            │   (RQ / arq)         │
                            │   - polls job queue  │
                            │   - calls            │
                            │     pakfindata.cli   │
                            │     handlers         │
                            │   - writes via       │
                            │     safe_writer +    │
                            │     catalog          │
                            └──────────┬───────────┘
                                       │ writes
                                       ▼
                            ┌──────────────────────┐
                            │   SQLite             │
                            │   (psx.sqlite)       │
                            └──────────────────────┘
```

Key invariants:
- **Streamlit never writes.** It only reads (via API) or POSTs job
  requests (which the worker processes asynchronously).
- **The worker is the only writer.** safe_writer + catalog wrap every
  write (Phase 0.1 + 0.2 guarantees).
- **The API is stateless.** No long-running reads; long-running compute
  goes to the worker and the API returns a job_id the UI polls.
- **The job queue is the choke-point.** Cron, UI buttons, and other API
  callers all enqueue through the same path.

## Per-milestone notes

### 1.1 — FastAPI scaffold
- NEW: `src/pakfindata/api/main.py`, `api/routes/`, `api/auth.py`,
  `api/models/` (pydantic), `api/db_session.py`
- Single static `PAKFINDATA_API_TOKEN` env var (auth)
- systemd unit: `~/.config/systemd/user/pakfindata-api.service`
- Smoke test: `curl localhost:8000/v1/health` returns `{"status":"ok"}`

### 1.2 — Wave A read endpoints
- `/v1/freshness`, `/v1/eod/latest`, `/v1/indices`, `/v1/breadth`,
  `/v1/symbols/latest`, `/v1/regular-market/snapshot`
- Pydantic response models for every endpoint
- 5xx → JSON error envelope `{error: {code, message, request_id}}`

### 1.3 — Wave A UI migration
- Each page replaces its `con = sqlite_con()` + SQL with
  `await api_client.get_eod_latest()` etc.
- New module: `src/pakfindata/ui/api_client.py`
- Behind a feature flag `PAKFINDATA_USE_API=true` so we can A/B during
  the migration. Flag removed in 1.16.

### 1.4 — Worker scaffold
- NEW: `src/pakfindata/worker/main.py`, `worker/jobs/`,
  `db/migrations/v4_jobs_table.sql`
- jobs table: `id, type, payload, status, created_at, started_at,
  finished_at, error, result_json`
- systemd unit: `~/.config/systemd/user/pakfindata-worker.service`
- One worker per process, single-threaded — SQLite write lock is the
  bottleneck so parallel workers buy nothing.

### 1.5 — First worker job: Sync Indices
- POST `/v1/jobs/queue` → `{type: "sync.indices"}`
- Worker dequeues, calls `pakfindata.cli.handle_indices_sync(args)`
- Dashboard "Sync Indices" button → POSTs the job, polls
  `/v1/jobs/{id}` until done, then re-renders.
- This is the smallest possible end-to-end worker test.

### 1.6–1.13 — Wave-by-wave migration
- Each wave follows the same pattern: API endpoints → UI migration →
  worker jobs for any sync buttons. See `phase1_migration_groups.md`
  for per-wave details.

### 1.14 — daily_sync.sh becomes one-liner
- Replaces all the `pfsync ...` invocations with a single
  `curl -X POST $API/v1/jobs/queue/daily-cron`.
- Worker decomposes the daily-cron job into its sub-jobs and runs them
  in order.
- Cron job at 03:45 PKT now just enqueues; the worker does the work.

### 1.15 — Deprecate legacy ETL paths
- Remove the inline write code from Streamlit pages (UI button bodies
  that still do their own safe_writer block).
- Remove the deprecated promotion paths in `tick.py`, `intraday.py`,
  `futures.py` (the `promote_*` functions marked DEPRECATED in
  CLAUDE.md).
- Single cleanup commit, large but mechanical.

### 1.16 — Final audit + tag
- `grep` checks for the exit criteria above
- `docs/architecture/api_surface.md` documents every endpoint
- Tag `v0.1-phase1`
- Roadmap update

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Wave F (intraday.py split) breaks the dirtiest file | Medium | High | Split into 7 small commits, one tab at a time. Each commit smoke-tested with Streamlit. |
| Worker job queue contention with single SQLite writer | Medium | Medium | Single-threaded worker by design; document the rationale. Phase 3 Postgres removes this constraint. |
| FastAPI deployment under systemd hits permission/path issues like cron did | Medium | Low | The cron-env dry-run pattern from Phase 0.3 applies here too — test under `env -i` before declaring it production-ready. |
| Streamlit's blocking nature makes API polling awkward | Low | Medium | Use `streamlit-autorefresh` + cached fetches; the existing UI already follows this pattern. |
| Postgres-readiness shims slow down Phase 1 | Low | Low | Keep them minimal — pydantic models + SQLAlchemy core, no ORM. Avoid premature abstraction. |
| Wave G has 24 read-only pages — temptation to ship them all in one PR | Medium | Low | Hard rule: max 5 pages per commit, each separately smoke-tested. |

## Open questions for Phase 1 start (resolve in Milestone 1.1)

1. **Worker framework: RQ or arq?** RQ is sync, arq is async. arq pairs
   better with FastAPI's async story; RQ has less moving parts. Pick
   one in 1.1 based on what `pakfindata.cli` handlers look like (most
   are sync today).
2. **API port + bind address?** `127.0.0.1:8000` for v1, no public
   exposure. Phase 3 adds reverse proxy + TLS.
3. **API token rotation?** v1 ships with single static token; rotation
   is manual. Phase 3 handles per-user tokens.
4. **What's in `result_json`?** Worker returns the structured one-line
   STEP=… STATUS=… output from `pakfindata.cli` handlers; Phase 0.3
   already designed the format with worker logging in mind.
