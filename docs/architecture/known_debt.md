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
- **Composite-aggregator page pattern — needs domain-scoped composite
  endpoints** (Milestone 1.7 Group G.4) — `market_research.py` (1066
  LOC, 13 `_load_*`) and `futures.py` (1381 LOC, 22+ reads) combine
  cross-domain reads into custom analytical queries (`trading_sessions`
  JOINs, ODL/OI composites) that don't compose from the existing per-
  domain /v1 endpoints. Adding 5+ one-page-specific endpoints would
  create dashboard-shaped sprawl; better to design a Phase 2 layer of
  composite endpoints (`/v1/dashboard/research`,
  `/v1/dashboard/derivatives`) that pre-aggregate at the API boundary.
  Until then both pages stay on direct DB reads (skipped in G.4 with
  documented rationale). Distinct from:
    * **engine-call-only** (F.6 — page reads ARE engine inputs,
      e.g. `sector_breadth.py`, `advanced_hawkes.py`)
    * **scraper-maintenance** (G.3 PMEX / G.1 SBP EasyData /
      G.4.9 market_summary — single-page domain owner of a tracking
      table or separate DB).
- **`pakfindata-worker.service` has `PartOf=pakfindata-api.service`**:
  stopping API stops worker, but restarting API does NOT restart worker.
  Manual `systemctl --user restart pakfindata-worker` needed after API
  redeploys. Surfaced during Milestone 1.8.2 fallback testing.
  Fix: change `PartOf=` to `BindsTo=` + `Requires=` pattern, OR remove
  the coupling and let them be independent. Phase 2 ops cleanup.
- **DEBT-PHASE2-FOLLOWUP-2: wider scraper pollution beyond the
  ZUMA/MUFAP sentinels** (Milestone 2.A.2 cleanup recompute) —
  `scripts/cleanup_catalog_pollution.py` removed 1,720 sentinel-string
  rows (ZUMA/TBILL/MUFAP/WTL) but the recompute step revealed an
  additional ~241K rows where SYMBOL CODES (BOP/UBL/MLCF/ZTL/PIB/
  WASLR/...) sit in date columns. Affected:
  `pib_auctions.auction_date` (~240K), `konia_daily.date` (~600),
  `forex_kerb.date` (~60), `regular_market_current.ts` (~50). These
  are NOT sentinel-string mistakes — they're a scraper-class bug
  writing symbol-keyed records into the wrong column. Out of 2.A.2
  scope. `scripts/apply_phase2a2_remediation.py` flipped the 4
  affected catalog rows to `status='failed'`,
  `last_row_date=NULL`, `notes=…` so freshness queries show a
  warning instead of a false-positive garbage date. Phase 2.A.3
  investigates the upstream scrapers, decides recoverable vs delete,
  and runs the deeper cleanup.
- **Helper-function test coverage gap caught by 2.A.2.1b** (Milestone
  2.A.2 follow-up) — Reproducer in `test_catalog_conflict_repro.py`
  was extended in 2.A.2.1b after discovering the original tests
  covered `update_catalog` but not the `update_catalog_from_table`
  helper. Lesson: helper functions need their own coverage; testing
  the underlying primitive is insufficient. Apply this discipline at
  every future helper introduction.
- **DEBT-PHASE2-FOLLOWUP-5: sovereign_curve multi-source
  consolidation broken since pre-backup era** (Milestone 2.A.3.3b
  — **CLOSED via 2.A.5.3, 2026-05-24**) — `pakfindata/CLAUDE.md`
  and project memory document the `sovereign_curve` table as a
  67K-row consolidation of PKRV + PKISRV + MTB + PIB + KIBOR (the
  original design intent). 2.A.3 Step 0 bisect shows the actual
  state across all four extant backups (May 11/14/15 + 2.A.2
  pre-cleanup snapshot) is **single-source KIBOR only, 41,747
  rows**. The other source feeds
  (`sources/sbp_rates_processor.py` per the canonical-EOD-path
  memory) either never ran post-2026-05-09 NTFS corruption or
  never ran at all — there's no extant backup state where they
  did. Downstream effect: `curve_analytics.py`,
  `debt_terminal.py`, and other `sovereign_curve` readers see only
  the KIBOR slice of what they were designed against. Phase 2.A.5
  investigates the processor + runs the consolidation alongside
  the FOLLOWUP-2/3 scraper work. 2.A.3 explicitly does not touch
  the processor (Hard Rule 6). The original design intent is
  preserved here for future investigators.

  **CLOSURE (2.A.5.3, 2026-05-24):**

  Outcome: FIX-partial-with-documented-gaps. Consolidation logic
  existed at `sources/sbp_rates_processor.py::process_all()` and
  worked structurally; never invoked outside its in-module CLI
  since `9dddd14` (Jan 2026). Same case-B-variant as FOLLOWUP-3
  (pkisrv loader).

  Ran `process_all()` end-to-end. Produced **67,498 rows across
  6 sources** (KIBOR / PKRV / PKISRV / MTB / PIB / POLICY) spanning
  2005-06-09 → 2026-05-24 (5,482 dates). 67K-row CLAUDE.md
  recollection confirmed within rounding.

  Documented gaps in the consolidated curve:
    - **PKISRV pre-2025: empty.** FOLLOWUP-3 closure documents the
      upstream format break — pre-2025 MUFAP files contain bond
      prices not yield-curve tenors; not recoverable as PKISRV
      from this source.
    - **PIB sparse: 84 rows only.** Pakinvestbonds.xlsx archive
      "New Format" sheet contains limited PIB cutoff coverage
      (2024-07-09 → 2026-03-26). Archive may need refresh in a
      later wave; separate concern, not blocking.
    - **POLICY single-row.** `page_snapshot.json` is stale
      (2026-04-16) and effectively empty (only `policy_rate=10.5`,
      no KIBOR/MTB/PIB tenor data). Snapshot refresh is a
      separate scraper concern.

  **Case-C surfaced during smoke test:** `kibor_daily` contains
  rows that violate the consolidator's input contract.
  `sources/fx_sync.py:87` inserts FX rates with currency codes
  (USD/EUR/GBP/JPY/...) as `tenor`; `sources/sbp_easydata.py:864`
  inserts balance-sheet aggregates (`bank_nonbank`, `grand_total`,
  `interbank`) as `tenor`. Resolved via consolidator-side
  whitelist (Option A) rather than source-side cleanup (Option B):
    - `process_kibor_from_db` now rejects any `tenor` not in
      `TENOR_DAYS` (622 rows rejected against current kibor_daily
      content).
    - `1Y → 365` added to `TENOR_DAYS` so 2,730 historic 1Y KIBOR
      rows get correct days value.
    - The filter is documented in the function docstring with a
      pointer to FOLLOWUP-9, NOT silent.
  Architectural fix (stopping the upstream writes) is Phase 2.B
  scope and is filed as FOLLOWUP-9 below.

  Catalog row recomputed: `status='ok'`,
  `last_row_date='2026-05-24'`, `row_count=67498`, notes describe
  the multi-source nature + forward refs to FOLLOWUP-3 and -9.

  No validators seeded (recency check requires `custom_sql`;
  deferred per 2.A.3.3 PKISRV decision).

  Cross-references:
    - FOLLOWUP-3 closure (pre-2025 PKISRV gap) is the binding
      constraint on this sub-wave's PKISRV slice.
    - FOLLOWUP-9 (kibor_daily content contract violation) is the
      new architectural debt surfaced; the whitelist guard is
      a defensive primitive that remains correct even after -9
      lands.

- **DEBT-PHASE2-FOLLOWUP-9: kibor_daily content contract violated
  by FX + balance-sheet writes** (Milestone 2.A.5.3 side-finding,
  2026-05-24) — `kibor_daily` is documented as the KIBOR rates
  table with `(date, tenor) PK` where `tenor` is a KIBOR maturity
  string (`1W`, `3M`, `6M`, ...). Two upstream paths now write
  rows that violate that contract:

  - `sources/fx_sync.py:87` — the FX microservice integration
    (see [[project-canonical-db-state-2026-04-28]] +
    FOLLOWUP-7) writes interbank FX rates into `kibor_daily`
    with currency codes (`USD`, `EUR`, `GBP`, `JPY`, `AED`,
    `KWD`, ~24 currencies total) as `tenor` and the FX bid/ask
    as `bid`/`offer`. ~600 rows currently in this shape, 2026-
    02-25 → 2026-05-07.
  - `sources/sbp_easydata.py:864` — SBP EasyData balance-sheet
    sync writes aggregate values into `kibor_daily` with labels
    (`bank_nonbank`, `grand_total`, `interbank`) as `tenor` and
    rupee amounts as `bid`/`offer`. ~80 rows currently in this
    shape, 2026-02-25 → 2026-05-05.

  Downstream impact: any `kibor_daily` reader that does not
  whitelist tenor values would propagate FX rates and balance-
  sheet figures into rate-curve analytics. The 2.A.5.3a
  whitelist guard in `process_kibor_from_db` prevents propagation
  into `sovereign_curve`, but other readers may exist (audit
  during 2.B observability work).

  Fix candidates (architectural choice, Phase 2.B work):
    (a) introduce a dedicated `fx_rates_daily` table for the
        microservice path; route FX rates there instead of
        `kibor_daily`. Cleanup existing rows.
    (b) route SBP EasyData balance-sheet aggregates to a
        separate `sbp_balance_sheet_daily` table; remove from
        `kibor_daily`.
    (c) leave the consolidator-side whitelist as the canonical
        defence; accept that `kibor_daily` is now de-facto
        polymorphic.

  (a) and (b) are the architecturally correct fixes. (c) is the
  current state after 2.A.5.3a. Forward reference: Phase 2.B
  daily digest should add an alert for "table reader expects
  tenor type X but encounters values outside X" as a generic
  contract-violation detector. Same class as FOLLOWUP-6 / -8
  observability gaps.
- **DEBT-PHASE2-FOLLOWUP-4: sbp_fx_interbank is sparse by upstream
  design** (Milestone 2.A.3.3 — **DOCUMENT-PERMANENT confirmed
  2026-05-23 via 2.A.5.1**) — SBP publishes the daily interbank
  series for USD only. Other currencies (EUR, GBP, JPY, etc.) are
  kerb-market only and live in `forex_kerb` with different
  semantics. The 127-row 2-month window prior CLAUDE.md notes
  recorded was pre-2026-05-09 NTFS state and isn't in any extant
  backup (May 11/14/15 all show 0 rows; May 23 + current show 1
  USD row). Do NOT backfill from `forex_kerb` — different markets,
  different price discovery. The `sbp_fx_interbank.usd_present`
  validator seeded in 2.A.3.3 catches the case where USD silently
  disappears from the publisher; recency-of-latest is not currently
  enforced by the check framework (no built-in recency check —
  would need `custom_sql`, deferred). Re-fetching multi-currency
  history (if SBP exposes it via EasyData) is a 2.A.5 question.

  **CLOSURE (2.A.5.1, 2026-05-23):** Live read-only scrape of
  `https://www.sbp.org.pk/dfmd/pma.asp` returned 1 USD row
  (buying=278.2463, selling=278.6714 for 2026-05-22) and zero
  rows for any other currency. `sources/sbp_fx.py` line ~78
  hardcodes `"currency": "USD"`; module docstring states the WAR
  page only shows USD/PKR rates. The upstream publishes USD only
  as structural design — no scraper fix can produce additional
  currencies from this source. Catalog `notes` updated from
  `'empty table'` to `'USD-only by upstream design; see
  DEBT-PHASE2-FOLLOWUP-4'` (committed as 2.A.5.1a via
  `scripts/apply_catalog_corrections.py`). Status remains `'ok'`.
  No fix possible at this layer; closes as
  **DOCUMENT-PERMANENT**.

- **DEBT-PHASE2-FOLLOWUP-6: cron-driven sbp_fx writes silently
  diverge from successful HTTP fetches** (Milestone 2.A.5.1
  side-finding, 2026-05-23) — The SBP PMA scraper succeeds when
  invoked manually (live test 2026-05-23 returned the 2026-05-22
  USD rate), but `data_freshness.fx_interbank.last_sync_at` shows
  `2026-05-18 14:30:20`, five days stale despite the daily cron at
  03:45 PKT. Something between the successful HTTP fetch and the
  DB write is failing silently — neither the cron run nor the
  worker job emits a visible error, and the catalog freshness
  badge stays green because `status='ok'` reflects "the latest row
  exists" rather than "today's expected fetch wrote a row".
  Forward reference: Phase 2.B daily digest should detect this
  class automatically — when `data_freshness.last_sync_at` lags
  N days beyond a successful upstream availability check, fire an
  alert. The 2.A.5.1 investigation is the first concrete test
  case for the freshness-vs-staleness detector.

- **DEBT-PHASE2-FOLLOWUP-7: FX microservice sync_runs stuck in
  'running' since 2026-04-19** (Milestone 2.A.5.1 side-finding,
  2026-05-23) — `sources/fx_sync.py` integrates with a separate
  FX microservice on `localhost:8100` that maintains its own
  `sync_runs` table (distinct from the Phase 1.4 jobs table the
  worker queue uses). The most recent successful run completed
  `2026-04-15`; subsequent runs are in `running` state with no
  completion or failure timestamp, going back 34 days. The Phase
  1.4.4 stale-job sweep covers the worker queue but does not
  inspect the microservice's own sync_runs table. Investigation
  options: (a) extend the worker sweep to also reconcile the
  microservice's sync_runs table, treating runs older than N
  hours as stale, OR (b) add stale-detection to the microservice
  itself so it reaps abandoned runs on its own schedule. Phase
  2.B observability work.
- **DEBT-PHASE2-FOLLOWUP-3: pkisrv_daily sync path broken — 1,049
  files unloaded** (Milestone 2.A.3.2 — **CLOSED via 2.A.5.2,
  2026-05-23**) — `pkisrv_daily` is empty in current DB and all four
  backups (May 11/14/15 + the 2.A.2 pre-cleanup snapshot). 1,049
  source files (CSVs + XLSXs) sit at `/mnt/e/psxdata/rates/pkisrv/`
  going back to 2020-02-01 — they ARE the upstream source per the
  canonical MUFAP path. The loader
  (`sources/mufap_rates.py::backfill_to_db_fast()` per the project
  memory) either never had its PKISRV branch wired or broke during
  recovery and was never re-run. `sovereign_curve` is also empty
  for `source='PKISRV'`, so the downstream consolidation has nothing
  to consolidate from. Phase 2.A.5 investigates the loader, decides
  parser strategy for the heterogeneous MUFAP files, and runs the
  initial bulk load. 2.A.3.2 only flipped the catalog row to
  `status='failed'` with notes pointing here so freshness queries
  stop reporting it as 'ok'.

  **CLOSURE (2.A.5.2, 2026-05-23):**

  Outcome: FIX (zero-line rewire). Parser was correct; CLI invocation
  existed; ran end-to-end via existing `mufap backfill-db` command.
  Populated `pkisrv_daily` with 1,530 records spanning 2025-02-03 →
  2026-05-06 (306 dates, ~16 months, five tenors: 1M / 3M / 6M / 9M
  / 1Y). Catalog row updated to `status='ok'`, `row_count=1530`,
  `last_row_date='2026-05-06'`. Smoke test against 8 sample files
  across the date range confirmed the parser before any DB write.

  Constraint surfaced during investigation: pre-2025 PKISRV files
  contain bond prices (per-issue valuations, multi-dealer quotes in
  2023, single-column FMA prices in 2024), NOT yield-curve tenors.
  Upstream MUFAP changed publication format in 2025. The 802
  pre-2025 files in `/mnt/e/psxdata/rates/pkisrv/` are NOT
  recoverable as yield-curve data from this source. Multi-year
  backfill of PKISRV history (FOLLOWUP-3 original framing implied
  this) is structurally unavailable.

  Forward reference: 2.A.5.3 (sovereign_curve consolidation) will
  have PKISRV data for 2025+ only. Pre-2025 PKISRV slice in
  `sovereign_curve` will remain empty due to the same format-break
  constraint. FOLLOWUP-5 closure must acknowledge this.

  Related new debt:
    - FOLLOWUP-8: MUFAP backfill operational gap (pkfrv/pkisrv
      populated only by manual Treasury Dashboard button / CLI; not
      in cron).

- **DEBT-PHASE2-FOLLOWUP-8: MUFAP backfill not in cron — pkfrv /
  pkisrv stale without manual click** (Milestone 2.A.5.2
  side-finding, 2026-05-23) — `sources/mufap_rates.py` exposes
  `download_and_sync` and `backfill_to_db_fast` (callable as
  `mufap backfill-db` or via the Treasury Dashboard "Sync" button),
  but neither is invoked from `scripts/daily_sync.sh` or any
  registered worker job. The pkrv path stays fresh because
  `etl/rates.py` independently scrapes the SBP PMA page for the
  PKRV curve; pkfrv and pkisrv have no SBP fallback. Effect: at
  Step 0 of 2.A.5.2, pkfrv was 26 days stale (last MUFAP write
  2026-04-27) and pkisrv was 0 rows (never populated). After
  2.A.5.2a backfill, pkisrv is current to 2026-05-06 but will
  again drift unless MUFAP backfill is wired into the daily
  schedule. Forward reference: Phase 2.B daily digest should
  detect the freshness-vs-staleness class automatically (same
  pattern as FOLLOWUP-6). Fix candidates: (a) add a `mufap_sync`
  worker job + cron step, (b) extend the SBP PMA scraper to also
  drop PKISRV / PKFRV (if upstream exposes them via SBP), (c)
  schedule the existing `mufap backfill-db` CLI directly from
  `daily_sync.sh`. Phase 2.B work; do not address inside
  2.A.5.
- **`tbill_auctions` 175-row memory invalidated** (Milestone 2.A.3
  Step 0 audit) — Prior CLAUDE.md / Phase 0 audit notes recorded
  `tbill_auctions` as a 175-row table (2024-06 → 2026-04). Step 0
  bisect across `/mnt/e/psxdata/backups/psx_2026051{1,4,5}.sqlite`
  + `/tmp/psx_pre_2a2_cleanup_20260523_1725.sqlite` shows 4 / 4 / 4
  / 12 rows respectively; current DB has 12. The 175-row state was
  pre-2026-05-09 NTFS corruption and isn't in any extant backup. No
  restoration path. Re-fetching from SBP EasyData (2024-06 → present)
  is genuinely new fetch work and crosses 2.A.3 Hard Rule 6 ("no
  scraper fixes in this milestone") — push to 2.A.5 alongside the
  FOLLOWUP-2/3/5 scraper investigations.

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
