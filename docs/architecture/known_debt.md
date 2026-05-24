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
- **`pakfindata-worker.service` has `PartOf=pakfindata-api.service`**
  (Milestone 1.8.2 surface — **CLOSED via 2.B.4, 2026-05-24**):
  stopping API stopped worker, but restarting API did NOT restart
  worker, requiring manual `systemctl --user restart pakfindata-worker`
  after API redeploys. Fix chosen (of the two candidates): remove
  `PartOf=` entirely so API restarts don't cascade to the worker.
  `After=` retained for startup ordering only (worker waits for API
  when both start together, doesn't follow API lifecycle thereafter).
  Decoupling test confirmed: stopping API leaves worker active;
  restarting API leaves worker untouched. In-flight jobs now survive
  API restarts. Edit landed in both
  `/home/smnb/.config/systemd/user/pakfindata-worker.service`
  (runtime) and `deploy/systemd/pakfindata-worker.service`
  (repo copy).
- **DEBT-PHASE2-FOLLOWUP-2: wider scraper pollution beyond the
  ZUMA/MUFAP sentinels** (Milestone 2.A.2 cleanup recompute —
  **FULLY CLOSED via 2.A.5.4-2.A.5.6, 2026-05-24**) —
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

  **PARTIAL CLOSURE (2.A.5.4a, 2026-05-24) — pib_auctions portion only:**

  `scripts/cleanup_pib_auctions_pollution.py` removed 240,872
  misrouted rows from `pib_auctions`. The disposition is
  CLEANUP-as-DEDUPLICATION (not destructive): the read-only
  recovery audit (2.A.5.4) joined the polluted rows against
  `tick_data` with PKT→UTC offset correction and found a 100%
  match — every polluted row exists as a canonical tick in
  `tick_data` already. The misroute appears to have recorded
  the same intraday feed into `pib_auctions` with
  PKT-display-string timestamps instead of UTC epoch.

  Five row-count gates (pre_total / match / post_total /
  post_dates / tick_data_invariance) all matched predictions
  exactly. Predicate (verified tight, replaces the original
  3-criterion defense-in-depth):

      maturity_date='insert'
      AND auction_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'

  Post-cleanup: `pib_auctions` = 963 rows / 270 distinct dates;
  catalog `pib` row recomputed to `last_row_date='2026-05-20'`,
  `row_count=963`. tick_data unchanged (10,048,488 rows).

  **Sub-wave note**: The recovery audit was protective overhead
  for this sub-wave; the pattern stays for future ones. Treat
  "polluted rows" as possibly-canonical until cross-table
  verification rules that out. Right answer (cleanup is safe)
  came from the audit, not from the original framing being
  right by accident.

  **Partially closed (2.A.5.5a, 2026-05-24) — konia_daily portion:**
  633 rows removed via `scripts/cleanup_konia_daily_pollution.py`.
  Triple-convergent predicate (source marker `scraped_at='dps'`,
  shape marker `date NOT GLOB ISO-date`, AND-intersection — all
  three match exactly 633). Cross-table verification: 627 / 633
  (99.05%) are duplicates of canonical eod_ohlcv / futures_eod
  OHLCV with the column shift (date←symbol, rate_pct←ts_ms,
  volume_billions←open, high←close, low←volume). 6 remaining
  orphans are post-delisting phantom rows (delisted-symbol-polling
  pattern — see new audit pattern below). Catalog 'konia' row
  recovered to `last_row_date='2026-05-23'`, `row_count=2,701`.
  Six gates (pre_total / match / post_total / post_dates /
  eod_invariance / futures_invariance) all matched predictions
  exactly.

  **Partially closed (2.A.5.6a, 2026-05-24) — forex_kerb portion:**
  60 rows removed via `scripts/cleanup_forex_kerb_pollution.py`.
  Triple-convergent predicate (`source='SAMPLE'`, `date IN
  ('PIB','GOP_SUKUK')`, `buying=30.0` — each criterion alone and
  the AND-triple all match exactly 60). Root cause distinct from
  pib_auctions / konia_daily: synthetic test seed written in a
  single 5-second burst on 2026-01-29 12:39, NOT a scraper
  misroute. No cross-table dedup needed — the data was never
  canonical anywhere. Catalog `fx_kerb` row recovered to
  `last_row_date='2026-05-18'`, `row_count=713`. Five gates
  (pre_total / match / post_total / post_currencies / post_dates)
  all matched predictions exactly.

  **FINAL CLOSURE (2.A.5.6b, 2026-05-24) — regular_market_current portion:**
  46 rows removed via
  `scripts/cleanup_regular_market_current_pollution.py`.
  Double-convergent predicate (`ts NOT GLOB ISO-date`,
  `symbol GLOB ISO-date` — both match exactly 46). Root cause is
  a column-swap bug in a defunct ingestion path: `symbol` (PK)
  held an ISO timestamp, `ts` held a real symbol code. The 9
  distinct ts-symbols all have separate canonical (PK=symbol)
  rows that survived the cleanup intact. Per-row classification
  cross-checked against eod_ohlcv: 24 exact duplicates, 15
  intraday-snapshot value-diffs, 4 Sunday off-hours snapshots,
  2 NC-suffix special-class rows, 1 phantom (WASLR 2026-03-20,
  one day past last eod). Catalog `regular_market_current` row
  recovered to `last_row_date='2026-05-23T15:53:47.401556+05:00'`,
  `row_count=649`. Six gates (pre_total / match / post_total /
  post_distinct / canonical_9_survive / eod_ohlcv_invariance) all
  matched predictions exactly.

  **Four root causes, four tables — architectural finding:**

  The original FOLLOWUP-2 entry hypothesized "PSX symbol codes in
  date columns across 4 tables" as a single bug class. Investigation
  proved that surface pattern hid FOUR genuinely different bugs:

  | Sub-wave  | Table                     | Rows    | Root cause |
  |-----------|---------------------------|---------|-----------|
  | 2.A.5.4a  | `pib_auctions`            | 240,872 | **Misrouted ticks** — bulk-load script with positional column alignment hit the wrong table; PKT-display-string timestamps written into `pib_type` while UTC-epoch canonical lives in `tick_data` |
  | 2.A.5.5a  | `konia_daily`             |     633 | **Column-shifted equity OHLCV** — defunct ingestion path with whole-row column shift: (date←symbol, rate_pct←ts_ms, volume_billions←open, high←close, low←volume); canonical OHLCV lives in `eod_ohlcv`/`futures_eod` |
  | 2.A.5.6a  | `forex_kerb`              |      60 | **Synthetic test seed** — developer/demo data left in production DB; `source='SAMPLE'` literal, single 5-second burst, mock yield-like values for two fixed-income "currencies" (PIB / GOP_SUKUK) that aren't FX codes |
  | 2.A.5.6b  | `regular_market_current`  |      46 | **Column-swap bug** — runtime bug swapping the PK column (`symbol`) with the timestamp column (`ts`); canonical PK=symbol rows still exist with the correct shape |

  Total pollution removed: **241,611 rows** (240,872 + 633 + 60 + 46).
  Total useful data lost: **0** — every cleanup was verified against
  canonical state (tick_data / eod_ohlcv / futures_eod) or against
  test-data provenance markers before deletion. The four root causes
  did not share a single upstream fix — each was its own debt.

  See the new audit pattern below ("Surface-similarity ≠ root-cause
  similarity") for the lesson this investigation produced.
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
- **DEBT-PHASE2-FOLLOWUP-11: stale DB copies on /mnt/e/psxdata/
  pending reclaim** (Milestone 2.A.5.4a side-finding, 2026-05-24) —
  Canonical DB is `~/psxdata_rescue/psx.sqlite` on NVMe (post
  2026-04-21 NTFS corruption + chkdsk recovery). The /mnt/e copies
  remain as insurance during the post-incident soak window. Soak
  window long expired (>1 month past chkdsk on 2026-04-23), but
  the cleanup has not been scheduled. Total reclaimable: ~52 GB.

  Files on `/mnt/e/psxdata/` as of 2026-05-24:

  | File | Size | Date | Status |
  |---|---|---|---|
  | `psx.sqlite.bak.20260421` | 22 GB | 2026-04-21 | **KEEP** (explicit cold backup, the one resurrected by chkdsk) |
  | `psx.sqlite` (+ `-shm`, `-wal`) | 23 GB | 2026-05-14 | Verify no live writers, then DELETE — pre-corruption snapshot |
  | `tick_bars_cloud_2026-05-08.db` | 6.9 GB | 2026-05-08 | Old snapshot — DELETE if no rsync consumer points to it |
  | `tick_bars.db` | 6.2 GB | 2026-04-23 | Old snapshot — DELETE |
  | `psx.sqlite.migration_backup` | 5.6 GB | 2026-04-19 | One-off migration backup — DELETE |
  | `psx_corrupt.sqlite` | 5.1 GB | 2026-04-11 | Old corruption corpse — DELETE |
  | `pakfindata.duckdb.DEAD` | 3.2 GB | 2026-04-06 | DEAD-suffix archive — DELETE |
  | `pakfindata.duckdb.archive.DEAD` | 1.8 GB | 2026-03-31 | Same — DELETE |
  | `psx.sqlite.bad`, `.corrupted` | ~200 MB | 2026-01-30 | Old corruption corpses — DELETE |
  | `pakfindata.duckdb.empty_schema.DEAD`, `pakfindata.db`, `psx_ohlcv.db`, `psx.db` | <1 MB total | various | Tiny historical artifacts — DELETE |

  **Pre-deletion verification required**:
  - `psx.sqlite` on /mnt/e has `-shm` and `-wal` companion files
    (mtime 2026-05-24 01:39) — confirm no live process holds an
    fd against it before delete (`lsof` or `fuser`).
  - `tick_bars_cloud_2026-05-08.db` — confirm
    `~/sync_psx_cloud.sh` and downstream readers (tick_replay,
    cloud-tick analytics) point at the canonical path
    `/mnt/e/psxdata/tick_bars_cloud.db` (no dated suffix) or
    NVMe equivalent.

  Memory cross-reference: `project_dup_dbs_cleanup.md`
  (originally filed during NTFS-recovery soak window).
- **DEBT-PHASE2-FOLLOWUP-12: `update_catalog_from_table` doesn't
  handle INTEGER epoch timestamp columns** (Milestone 2.A.5.7
  verification side-finding, 2026-05-24) —
  `db/catalog.py::update_catalog_from_table` runs
  `SELECT MAX(<date_column>) FROM <source_table>` and stores the
  raw scalar as `last_row_date`. For tables where the configured
  `date_column` is an INTEGER unix epoch (e.g. `tick_data.timestamp`),
  this stores `1776250199` instead of `'2026-04-15'` — the same
  DATE-on-INTEGER-epoch class surfaced as an audit pattern in
  2.A.5.4b. The catalog row's `status` becomes `'ok'` but the
  human-readable `last_row_date` displays the raw integer in any
  UI / digest / freshness check that reads the row.

  **Reproducer**: 2.A.5.7 ran a verification recompute against the
  three remaining `catalog_flagged` domains (announcements,
  tick_data, indices). announcements and indices recovered to
  clean ISO dates; tick_data recovered to `'1776250199'` raw
  epoch (= 2026-04-15 10:49:59 UTC). The 2.A.5.7 close manually
  patched the row with
  `UPDATE data_freshness SET last_row_date = DATE(MAX(timestamp),
  'unixepoch') WHERE domain='tick_data'`. The manual patch is a
  point fix; the helper still has the bug.

  **Fix**: extend `catalog.py` to read a per-domain `value_type`
  hint (one of `'iso_date'`, `'iso_timestamp'`, `'epoch_seconds'`,
  `'epoch_millis'`) and apply the appropriate
  `DATE(<col>, 'unixepoch')` / `DATE(<col>/1000, 'unixepoch')`
  / pass-through conversion during MAX(). Alternative: add a
  type-detection step that runs `SELECT typeof(<col>)` once and
  branches on result. Phase 2.B observability work.

  **Cross-references**:
    - Audit pattern that predicted this: "DATE() on INTEGER epoch
      timestamps" (filed 2.A.5.4b).
    - The pattern is now both descriptive (it explains the bug) and
      prescriptive (the fix shape is documented).

  **CLOSED 2026-05-24 by 2.B.6**. Added explicit `value_type` kwarg
  to `update_catalog_from_table` with four valid values
  (`iso_date` default / `iso_timestamp` / `epoch_seconds` /
  `epoch_millis`), each branch validating shape vs declared type
  and raising on mismatch (defense-in-depth). Three live callers
  migrated: `cli.py` tick_data → `epoch_seconds`;
  `etl/regular_market.py` DATASETS tuple expanded to 3-tuple with
  `iso_timestamp` for both regular_market_current and
  regular_market_snapshots; `scripts/cleanup_regular_market_current_pollution.py`
  updated for re-run hygiene. 5 reproducer tests added. Helper's
  docstring was the bug (it pushed type-handling onto callers and
  3 of 3 live callers ignored that advice) — now the helper does
  the work. See also: "parameterized caller invisibility" audit
  primitive below — `regular_market_snapshots` surfaces only
  through `DATASETS` iteration and was not visible to grep for
  literal-argument calls; reading the ETL module caught it.

- **Audit pattern: DATE() on INTEGER epoch timestamps**
  (Milestone 2.A.5.4 recovery audit, 2026-05-24) — When auditing
  tables whose `timestamp` column is INTEGER unix epoch (e.g.
  `tick_data.timestamp`), the bare SQL function `DATE(timestamp)`
  returns empty string because `DATE()` expects an ISO-8601 string,
  not a raw integer. The query silently returns 0 rows where it
  should return all rows for the given date — a false negative that
  looks like data loss. The 2.A.5.4 recovery audit nearly committed
  a recovery script on this footing before catching the bug.

  Correct forms (always specify the storage unit):

  ```sql
  DATE(timestamp,       'unixepoch')   -- second epochs (tick_data)
  DATE(timestamp/1000,  'unixepoch')   -- millisecond epochs
  datetime(timestamp,   'unixepoch')   -- for timestamp comparison
  -- or compare epoch ranges directly:
  WHERE timestamp >= strftime('%s', '2026-04-15 00:00:00')
    AND timestamp <  strftime('%s', '2026-04-16 00:00:00')
  ```

  Apply this discipline at every audit of a timestamp-typed table:
  before writing the date filter, verify the column's storage unit
  (`SELECT typeof(timestamp), MIN(timestamp), MAX(timestamp) FROM
  <table>;`). If `typeof` is `integer` and `MIN` looks like an
  epoch (>1e9, <2e10), use the `'unixepoch'` modifier or
  range-on-epoch form. Wrong query produces silent false negatives.
- **Audit pattern: scrapers polling delisted symbols ("phantom row"
  class)** (Milestone 2.A.5.5 cross-table audit, 2026-05-24) —
  Two FOLLOWUP-2 sub-waves have now surfaced the same scraper-class
  pattern: write paths that do NOT validate `symbol-was-listed-on-date`
  before inserting. The pattern shows up as a small tail of rows
  in the polluted set that have no canonical counterpart and date
  positions that are POST-delisting or POST-suspension:

  - **konia_daily, 2.A.5.5a**: 6 of 633 polluted rows are post-delisting
    phantoms — BILF / DMTX / PMI / PMPK on dates 4-9 months after their
    2025-01-20 delistings (eod_ohlcv coverage cleanly ends 2025-01-20);
    CJPL on 2026-03-16, ten days after its 2026-03-06 suspension
    (eod_ohlcv last date for CJPL); LIVENR has no eod_ohlcv row at
    any date and is absent from the `symbols` table.
  - **pib_auctions, 2.A.5.4a** (different mechanism but related class):
    240,872 rows were intraday-tick duplicates of tick_data with PKT/UTC
    offset — not phantoms, but they exhibited the same "scraper wrote
    something the canonical path also wrote, with no listed-on-date
    validation" anti-pattern.

  **Class definition**: a row R(symbol, date, ...) is a "phantom"
  when (a) the symbol has no listing in `symbols`, OR (b) the
  symbol exists in `symbols` but `eod_ohlcv` / `futures_eod`
  coverage for that symbol ends strictly before `R.date`. Phantom
  rows are unrecoverable as canonical data because no legitimate
  price feed would publish for them on those dates — the symbol
  stopped trading first.

  **Cleanup discipline for orphan rows**: when the cross-table
  duplicate check leaves a tail of "unique to polluted table"
  rows, classify each one. If they fit the phantom class (per
  the above), document them in the closure as "uniqueness is
  itself a sign of pollution, not a sign of recoverable data."
  Only escalate to RECOVERY-then-cleanup if at least one orphan
  row sits inside the symbol's canonical listing window AND no
  canonical record exists for that (symbol, date).

  **Forward reference to Phase 2.B (observability)**: a
  listed-on-date validation rule belongs in the source-side
  pre-write check (or as a `safe_writer` validator) for any
  scraper that takes a symbol-keyed row. Candidate primitive:
  `is_listed_on(symbol, date, con)` returning True iff
  `eod_ohlcv` or `futures_eod` has a row for that (symbol,
  date) OR the date is within ±5 trading days of an existing
  row (slack for initial-listing race conditions). Block writes
  outside that window with a clear error. This is FORWARD
  REFERENCE only — do not expand FOLLOWUP-2 / 2.A.5 scope to
  fix the scrapers; document the pattern, file the candidate
  primitive, and let Phase 2.B own the architectural work.
- **Audit pattern: surface-similarity ≠ root-cause similarity**
  (Milestone 2.A.5.6 full FOLLOWUP-2 closure, 2026-05-24) —
  When multiple tables exhibit the same pollution shape (e.g.
  "symbol codes appearing in date columns across N tables"),
  treat the shape as a side effect of "data ended up where it
  shouldn't be," NOT as evidence that one upstream bug
  produced all of them. Each affected table requires
  independent diagnosis. FOLLOWUP-2 is the existence proof:
  four tables all exhibited "symbol code in date column"
  pollution; investigation revealed four genuinely different
  root causes (misrouted ticks via bulk-load positional
  alignment; whole-row column shift in a defunct ingestion;
  synthetic test seed left in prod; column-swap PK/timestamp
  bug). The four needed four different cleanup scripts, four
  different predicates, four different cross-table checks, and
  four different narratives.

  **How to apply**:

  1. When multiple tables show the same pollution shape, start
     each table's Step 0 audit independently. Don't write a
     combined script before diagnoses converge.
  2. The shape-similarity is the surface; the root cause is
     what writes there. Map each polluted table to its
     suspected writer separately. If the writers turn out to be
     the same path, combining is allowed; if they're different
     paths, the cleanup MUST split.
  3. Predicate tightness can independently converge per table
     and still cover different root causes. Triple-convergent
     predicates per table (one shape marker + one source marker
     + one value marker) are a strong signal that the
     within-table diagnosis is firm — but they say nothing
     about whether the *across-tables* root cause is shared.
  4. The disposition (CLEANUP vs RECOVERY-then-CLEANUP) can
     converge across tables even when root causes don't.
     Same outcome ≠ same cause; don't let outcome-similarity
     mask root-cause differences in the commit narrative or
     the architectural debt entry.

  **Why this matters for Phase 2.B observability**: anomaly
  detectors that group tables by pollution shape will collapse
  distinct root causes into a single alert and obscure the
  underlying scraper-class bugs. Daily-digest design should
  classify by **(table, writer-path)** not by surface-shape
  pattern alone. The phantom-row check (FOLLOWUP-2.A.5.5b)
  and the DATE-on-epoch check (FOLLOWUP-2.A.5.4b) are
  table-shape primitives; the writer-path primitive is the
  missing peer.
- **Audit pattern: parameterized caller invisibility**
  (Milestone 2.B.6 Step 0, 2026-05-24) — Grep for
  `function_name(literal_argument, ...)` misses callers that
  iterate parameter tuples, read from config dicts, or otherwise
  pass the argument indirectly. The audit shows the function being
  used N times in M places by literal-name search, but the live
  call count is N+K — the K hidden callers route the same
  argument through one extra layer of indirection.

  **Existence proofs from real audits**:

  - 2.B.0 stuck-job sweep: searched `*_runs` suffix and missed
    `scrape_jobs` (no `_runs` suffix, different naming convention
    within the same table family).
  - 2.B.6 helper migration: grep for
    `update_catalog_from_table.*regular_market_snapshots` returned
    zero matches; the dataset is touched only by
    `etl/regular_market.py`'s `for dataset, source in DATASETS`
    iteration over a (`regular_market_current`,
    `regular_market_snapshots`) tuple. Grep saw the call against
    `DATASETS[i][0]` but not against the literal dataset id.

  **How to apply**:

  1. Step 0 audits MUST sweep both `grep "function_name(\"<arg>"`
    AND read the dispatch/ETL modules that the function lives
    near. Parameterized iteration over `(name, ...)` tuples is
    common in this codebase's ETL layer (`DATASETS = (...)` is
    the idiomatic pattern).
  2. When migrating a function signature, run a final pass:
    `grep "function_name(" --include="*.py" | head -50` and
    eyeball each call site. Calls where the first positional
    argument is a *variable* (not a string literal) are exactly
    the invisible callers.
  3. Surface the miss in the disposition summary even when it
    didn't slip through — surfacing strengthens the primitive
    for the next audit.

  **Cross-references**: same class as the phantom-row primitive
  (FOLLOWUP-2.A.5.5b) and the DATE-on-epoch primitive
  (FOLLOWUP-2.A.5.4b) — each surfaced from a real audit miss
  that nearly produced a wrong-direction commit. Each names the
  blind spot, gives the corrective check, and points to the
  audit where it appeared.
- **Methodology note: prediction gate semantics**
  (Milestone 2.B.6 net-diff overshoot, 2026-05-24) — Prediction
  gates halt on numerical divergence to surface findings, not to
  enforce a magnitude. Two interpretations of "diverged":

  - **Magnitude divergence + scope match**: content matches what
    was approved; line count was estimated low.
    Disposition: surface the analysis; commit if all overshoot
    maps to explicit scope. Adjust future predictions to account
    for the content cost of similar features.
  - **Magnitude match + scope drift**: numerical prediction met
    but content diverged from approval.
    Disposition: halt; revert; replan.

  **Existence proof**: the 2.B.6 +307/-13 net diff vs predicted
  70-135 was the first explicit test of this distinction. Every
  line over budget mapped to user-approved scope expansions
  (docstring rewrite-not-delete, the 4th audit primitive,
  DATASETS 3-tuple expansion, validation-with-raise). All four
  were explicit asks in the approval round; their line cost was
  approved alongside the content. The disposition surfaced the
  overshoot, mapped each over-budget line to its approval, and
  committed — magnitude divergence with scope match.

  **How to apply**: a prediction gate's primary function is
  *"did I implement what was approved?"* not *"did I implement
  it in the predicted lines?"* The gate is a forcing function
  for the surface-before-commit step, not a hard line-count
  budget. When predicted N±X overshoots into actual N+kX:

  1. Re-derive the prediction from the approved-content checklist
     (docstring scope, test boilerplate cost, audit-artifact form,
     etc.). If the re-derivation matches the actual, the original
     prediction was the gap, not the implementation.
  2. Surface the re-derivation alongside the disposition. Commit
     when over-budget lines map to explicit scope; halt only when
     a line cannot be traced to an approval.
  3. The cost of being wrong about magnitude is a stricter future
     prediction; the cost of being wrong about scope is a wasted
     commit. The gate optimizes against the second, not the first.

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
