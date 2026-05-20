# Known Debt — Consolidated Registry

**Generated:** 2026-05-20
**Source:** Phase 0 audits 0.1–0.4 + page_inventory.md (0.5)

Every deferred item from the Phase 0 stabilization work, organized by
the phase that should pick it up. **This is not a wishlist** — every
item here was identified during real work and has a concrete trigger.

Format: `[bucket]` Item — Source milestone — Why deferred.

## DEBT-PHASE1 — must address during Phase 1

Items that block or interact with Phase 1's service decomposition. Fix
during or as part of Phase 1; not before.

### Code

- **`services/fi_sync_service.py` migration** (Milestone 0.1) — fi
  service still uses non-safe-writer commits; was DO NOT TOUCH during
  Phase 0. Phase 1 worker migration is the natural time to either
  re-wire it through safe_writer or delete it if the worker subsumes
  its job.
- **`ui/page_views/indices.py` safe_writer + catalog wiring**
  (Milestone 0.1 sub-wave W5a was skipped; Milestone 0.2 sub-wave 2.4c
  was skipped) — pre-existing dirty file. The Sync Index Membership
  button still functions but doesn't update the catalog row. Fix as
  part of Wave A migration.
- **`services/tick_service.py` cleanup** — pre-existing dirty across
  Phases 0.1–0.4. Reconcile with the worker model in Phase 1.
- **`fixed_income.py` (2503 lines, 8 nav entries)** — must be split
  during Wave B (Milestone 1.7). See page_inventory.md.
- **`fund_explorer.py` (2007 lines, 4 nav entries)** — split during
  Wave D (Milestone 1.9).
- **`intraday.py` (2499 lines, 7 tabs, dirty)** — split during Wave F
  (Milestone 1.11). Highest-risk single change in Phase 1.
- **`company_deep.py` (1240 lines)** — split during Wave E
  (Milestone 1.10).
- **6 pages still bypass safe_writer or have unclear write paths**:
  `indices.py`, `commodities.py`, `futures.py` (legacy promotion),
  `eod_loader.py` (legacy), `live_ohlcv.py` (legacy),
  `data_acquisition.py` — Phase 1 migration cleans up the first 3;
  the last 3 are DELETE candidates.

### Data quality (surfaced by Milestone 0.2 catalog)

- **`konia_daily` has `date='ZUMA'` rows** (Milestone 0.2 backfill) —
  source-side pollution. Fix the scraper that wrote ZUMA, then
  re-sync. Phase 1 catalog migration is the natural touchpoint.
- **`fx_kerb` has `date='TBILL'` rows** (Milestone 0.2) — same shape;
  scraper pollution.
- **`instrument_membership` has `effective_date='MUFAP'`** (Milestone 0.2).
- **`pib_auctions` has `auction_date='ZUMA'`** (Milestone 0.2).
- **`regular_market_current` has `ts='WTL'`** (Milestone 0.2) — same
  pollution class.

### Catalog (Milestone 0.2)

- **`announcements` + `tick_data` get spurious `status='failed'`**
  (Milestone 0.3 daily-cron drill found, Milestone 0.4 audit
  re-confirmed). Root cause: `db/catalog.py::update_catalog`'s
  ON CONFLICT clause doesn't update `date_column` or `source_table`,
  so the Milestone 0.2 backfill's correct date_column values for
  `corporate_announcements` (`announcement_date`) and `tick_data`
  (`timestamp` unix epoch) were never persisted. Catalog row still
  records `date_column='date'`. Data IS syncing; only badge wrong.
  Fix is one-line in catalog.py — but catalog.py was DO NOT TOUCH
  through Phase 0; Phase 1 migration is the right time.

### Migration prep (Milestone 0.5)

- **Decide worker framework: RQ vs arq** (Phase 1 Milestone 1.1) —
  arq pairs better with FastAPI async; RQ has fewer moving parts.
  Pick during 1.1 once the CLI handlers' async story is clearer.
- **Single static API token vs per-user from day 1** — Phase 1 plan
  picks single token; revisit if Phase 2 brings multi-user requirements
  forward.

## DEBT-PHASE2 — pick up during Phase 2 observability work

These will surface naturally when Phase 2 brings logs/metrics into
view.

- **Cloud JSONL rsync dependency** (Milestone 0.3 follow-up;
  [[cloud-jsonl-rsync-dependency]] memory). `daily_sync.sh`
  `intraday_summaries_build` silently no-ops when
  `/mnt/e/psxdata/tick_logs_cloud/ticks_<date>.jsonl` is missing.
  `~/sync_psx_cloud.sh` is not in cron. Schedule a separate cron at
  03:30 PKT OR call rsync inline from `daily_sync.sh`. Phase 2's
  monitoring will make the "silently no-ops" failure visible.
- **`announcements sync` 18-minute per-symbol loop dominates daily
  pipeline cost** (Milestone 0.3 measured) — batch the
  `fetch_company_payouts` calls via a single bulk endpoint if PSX has
  one, or async-pool the 600 symbols. Phase 2 latency dashboards
  surface this naturally.
- **Observability stack itself** — Prometheus + Grafana + structured
  logging are Phase 2's main deliverable; not built in Phase 0.
- **dashboard.py:433/445 refresh_all handler** — un-marked `con.commit()`
  calls deferred from Milestone 0.1; Market Sync v1 in Phase 1 Wave H
  may eliminate them entirely. Keep on the radar.
- **`compute_sector_rollups` duplicate emission** (Milestone 0.1
  Wave 2b) — the function emits the same set of rows twice during
  rebuild. Cosmetic.

## DEBT-PHASE3 — Postgres migration handles naturally

Items that disappear when SQLite → Postgres.

- **`init_*_schema` functions using `executescript`** (Milestone 0.1
  found 13 of these) — latent footgun because `executescript` commits
  any pending transaction. We worked around it case-by-case (e.g.
  `market_summary.py:100` splits on `;` instead). Phase 3 Postgres has
  no equivalent footgun.
- **Single-threaded worker because of SQLite write lock** — Postgres
  removes the constraint; can parallelize jobs.
- **NTFS-3g latency on the DR drill** (Milestone 0.4) — Phase 3 may
  move data root to NVMe entirely or to a cloud-native store; NTFS
  goes away.
- **02:00 backup cron misses days when laptop sleeps** (Milestone 0.3
  & 0.4 audits) — Phase 3 backup runs on a server, not a laptop. If
  Phase 1/2 want to fix it sooner: move to `anacron` (catch-up
  semantics) or invoke from `daily_sync.sh` Step 0.
- **`/home/smnb/psxdata_rescue/` only has 54 GB free** (Milestone 0.4
  drill audit) — workspace can't live on NVMe at current free-space.
  Phase 3 server has different storage; this constraint vanishes.
- **`pakfindata.duckdb` empty placeholder** (project memory) — Phase 3
  decides DuckDB role; for now it's an in-memory analytics-con only.

## DEBT-OPS — ongoing housekeeping

Items that don't have a phase home; they're recurring.

- **Quarterly DR drill schedule** — Q3 2026, Q4 2026, Q1 2027
  scheduled in `docs/operations/dr_drill_log.md`. Each drill appends
  a new entry; never replace.
- **Hardcoded PSX holiday list needs annual refresh**
  (Milestone 0.3) — `src/pakfindata/utils/trading_calendar.py`
  `PSX_HOLIDAYS_2027` is currently empty placeholder. Update in
  December 2026 when PSX publishes the 2027 calendar.
- **`sources/sectors.py` scraper broken** — known per CLAUDE.md /
  page_inventory.md; Website Scan + Data Quality pages depend on it.
  Fix or replace whenever a new sector taxonomy load is needed.
- **Cron is currently DISABLED** (found in Milestone 0.4 audit
  2026-05-20). Every entry in `crontab -l` is prefixed `#DISABLED#`.
  Re-enable when Phase 0.5 closes — `crontab -l | sed 's/^#DISABLED# //'
  | crontab -`. Verify with `crontab -l | grep daily_sync`. Or
  intentionally leave disabled; document in
  `docs/operations/cron_setup.md`.
- **5 March-2026 sync wrapper scripts**
  (`scripts/sync_{all,etf,fx,rates,treasury}.sh`) — hardcoded wrong
  env (`handwriting`), wrong conda (`/opt/miniconda`), wrong DB path.
  Not on hot path. Delete during the post-Phase-1 cleanup PR.
- **30 GB DR drill workspace at `/mnt/e/psxdata/dr_drill_20260520/`** —
  keep through Q3 2026 drill (cross-reference), then delete.
- **Forensic copies from May 9 2026** — 2 × 29 GB at
  `~/psxdata_rescue/psx.sqlite.{CORRUPT,BROKEN_REPLACED}_*`. Keep
  indefinitely; they're the post-mortem evidence for the incident
  that drove Phase 0.

## DEBT-FOLLOWUP — minor improvements identified during drills

- **`scripts/sqlite_page_recover.py --tables-from-priority-list`
  flag** (Milestone 0.4) — so emergency recoveries don't need to
  remember the canonical 18-table list. Quality-of-life; runbook
  captures the list explicitly.
- **`dr_drill.sh` workspace-on-NVMe support** (Milestone 0.4) —
  blocked by 54 GB free constraint; revisit when forensic copies
  are deletable.
- **`sqlite_page_recover.py` `--dry-run` behavior** — flag exists in
  argparse but during the 2026-05-20 drill, the tool wrote output
  anyway. Investigate whether `--dry-run` truly suppresses writes
  before relying on it.

## Cross-references

- Phase 0 master plan: `PHASE0_MASTER_PLAN.md` (project root)
- Phase 0 milestone reports: `/mnt/e/psxdata/phase0_0{1,2,3,4}_final_audit_*.md`
- Memory files: `/home/smnb/.claude/projects/-home-smnb-projects-pakfindata/memory/MEMORY.md`
- Phase 1 plan: [`phase1_plan.md`](phase1_plan.md)
- Page inventory: [`page_inventory.md`](page_inventory.md)
