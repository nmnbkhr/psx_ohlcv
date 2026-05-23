# Phase 1 Migration Groups

**Generated:** 2026-05-20
**Source:** [`page_inventory.md`](page_inventory.md) (Phase 0.5 sub-wave 5.1)

Groups the 31 **KEEP** and 14 **REFACTOR** pages into shippable Phase 1
migration waves. Each wave covers one functional domain, fits roughly
in a week, and has a clear "done" criterion. **DELETE** and **DEFER**
pages are not in scope for Phase 1 (separate cleanup).

The grouping is shaped by:
- **Data co-location** — pages that read the same tables migrate together.
- **Write-path bundling** — pages whose sync buttons enqueue the same
  background jobs go in the same wave so the worker scope is contained.
- **Risk concentration** — heavier pages (intraday, fund_explorer split,
  fixed_income split) cluster late so the easier wins de-risk the
  FastAPI + worker scaffolding first.

---

## Group A — Core market overview (Week 1)

**Pages:** Dashboard, Market Pulse, Index Monitor, Market Summary, Quote Monitor (regular_market)

**API endpoints needed:**
- `/v1/freshness` — read `data_freshness` (Phase 0.2 catalog)
- `/v1/eod/latest` — latest `eod_market_summary` + `eod_sector_summary`
- `/v1/indices` — read `psx_indices`
- `/v1/breadth` — daily breadth roll-up
- `/v1/symbols/latest` — latest `eod_symbol_summary` rows
- `/v1/regular-market/snapshot` — latest `regular_market_current` row

**Worker jobs needed:**
- `sync.indices` — wraps `pfsync indices sync`
- `sync.regular_market` — wraps `pfsync regular-market snapshot`
- `summary.rebuild_today` — wraps `pfsync summary rebuild-today`

**Risk:** LOW. All targets are well-scoped read pages or have been
through the safe_writer + catalog migration. Index Monitor still
needs catalog wiring (sub-wave 2.4c was skipped) — bundle that fix
with the migration.

**Done criterion:** `grep -rn "sqlite3.connect" src/pakfindata/ui/page_views/{dashboard,market_pulse,indices,market_summary,regular_market}.py` returns zero hits.

---

## Group B — Fixed income (Week 2)

**Pages:** Rates Overview, Yield Curves, Curve Analytics, Treasury Auctions, Bond Market, Benchmark Monitor, Debt Terminal, Treasury

**The big work:** `fixed_income.py` (2503 lines, 8 nav entries) splits into
separate page files during this wave:
- `yield_curves.py`
- `treasury_auctions.py`
- `bonds_screener.py`
- `sukuk_screener.py`
- `fi_overview.py`
- `sbp_pma_archive.py`
- `fi_yield_curve.py`
- `govt_fixed_income.py`

**API endpoints needed:**
- `/v1/rates` — kibor + konia + policy
- `/v1/curves/{source}` — sovereign_curve filtered by source
- `/v1/treasury/auctions` — tbill + pib + gis
- `/v1/bonds/{otc,trading-daily,trading-summary}`
- `/v1/benchmark/snapshot`
- `/v1/yield-curves/{pkrv,pkisrv,pkfrv}`

**Worker jobs needed:**
- `sync.rates` — KIBOR + KONIA + PKRV
- `sync.treasury` — T-Bill + PIB
- `sync.treasury_gis` — GIS sukuk
- `sync.bond_market` — SBP SMTV
- `sync.benchmark` — SBP benchmark snapshot
- `compute.curve_fit` — Linear/Spline/NSS fits (heavy compute, was
  in `engine/curve_analytics.py`)

**Risk:** MEDIUM. The fixed_income.py split is the biggest single
refactor in Phase 1. Curve Analytics writes synthetic rates back to
sovereign_curve (engine layer, not UI) — keep that exception but
document it in the new module.

**Done criterion:** `fixed_income.py` deleted; 8 new files exist, each
< 500 lines; all 8 nav entries still resolve to working pages.

---

## Group C — FX (Week 3 first half)

**Pages:** FX Dashboard (KEEP), Interbank vs Open (KEEP)

**MERGE targets (consolidated into FX Dashboard tabs in this wave):**
- Currency Dashboard (was `fx.py::render_fx_overview`)
- Rate History (was `fx_history.py`)
- FX Monitor + FX Analytics (hidden, were `fx.py`)

After this wave, the FX surface is 2 pages instead of 5.

**API endpoints needed:**
- `/v1/fx/interbank` — sbp_fx_interbank
- `/v1/fx/kerb` — forex_kerb
- `/v1/fx/avg/{daily,monthly}` — sbp_fx_*_avg
- `/v1/fx/open-market` — sbp_fx_open_market

**Worker jobs needed:**
- `sync.fx_interbank` (covers sbp + kerb via `pfsync fx-rates sync-all`)

**Risk:** LOW — all already on safe_writer + catalog from Phase 0.1
W3 / W4.

**Done criterion:** 5 FX pages → 2 FX pages; URL backwards-compat
preserved by redirects.

---

## Group D — Funds (Week 3 second half)

**Pages:** Fund Explorer (KEEP, target of the split), Fund Analytics (REFACTOR)

**MERGE targets (consolidated into Fund Explorer tabs):**
- VPS Pension
- Top Performers
- ETFs
- Fund Directory (hidden)

The big work: `fund_explorer.py` (2007 lines, 4 nav entries) splits
into:
- `fund_explorer.py` (Fund Explorer landing)
- `vps_pension.py`
- `top_performers.py`
- `etfs.py`

Plus rewriting `funds.py` (621 lines, Fund Analytics) so it doesn't
duplicate Fund Explorer.

**API endpoints needed:**
- `/v1/funds/{categories,nav,performance,risk}`
- `/v1/etfs` — etf_nav

**Worker jobs needed:**
- `sync.mufap` — wraps `pfsync mufap sync`
- `compute.fund_risk` — `mufap_compute_risk` style metrics

**Risk:** MEDIUM. Funds engine has historically had bugs around
risk-metric backfill (W2c-style fixes); the split needs careful
testing.

**Done criterion:** `fund_explorer.py` < 500 lines; 4 standalone fund
pages; `funds.py` removed or reduced to a thin re-export.

---

## Group E — Equities (Week 4)

**Pages:** Stock Screener (KEEP), Company Profile (REFACTOR), Sector Analysis (KEEP), Factors (KEEP)

**MERGE targets:**
- Symbol Financials → Company Profile / Financials tab
- Price Chart (hidden) → Stock Screener / Company Profile
- Rankings (hidden) → Stock Screener
- Symbols (hidden) → DELETE in cleanup wave

**The work:** Split `company_deep.py` (1240 lines) into:
- `company_profile.py` (profile + key people)
- `company_financials.py` (statements + ratios + history)
- `company_events.py` (announcements + dividends)
- `company_signals.py` (signal snapshots, currently 0 rows but
  scaffolded)

**API endpoints needed:**
- `/v1/symbols` — symbols + sectors
- `/v1/companies/{symbol}/{profile,financials,events,quotes}`
- `/v1/sectors/{name}/summary`
- `/v1/factors` — factor model output (compute moves to worker)

**Worker jobs needed:**
- `sync.symbols` — refresh symbols universe
- `sync.companies` — deep_scraper
- `compute.factors` — factor model (was in factor_analysis.py)

**Risk:** MEDIUM. The factor model has heavy compute; needs careful
worker offload.

**Done criterion:** `company_deep.py` deleted; 4 new files exist.

---

## Group F — Intraday + futures + post-close (Week 5)

**Pages:** Intraday (REFACTOR), Live Ticker (KEEP), Futures & Odd Lot (REFACTOR), Post Close (KEEP)

**The big work:** Split `intraday.py` (2499 lines, 7 tabs, pre-existing
dirty) into:
- `intraday_dashboard.py`
- `intraday_charts.py`
- `intraday_market_pulse.py`
- `intraday_volume.py`
- `intraday_movers.py`
- `intraday_index.py`
- `intraday_sync.py` (move to ADMIN per Market Sync v1)

Also: `futures.py` (1381 lines) cleanup — remove legacy promotion paths
that bypass safe_writer.

**API endpoints needed:**
- `/v1/intraday/{bars,ticks,daily,minute-breadth,hourly}`
- `/v1/futures` — futures_eod
- `/v1/post-close/turnover`

**Worker jobs needed:**
- `sync.intraday_ticks_fetch` (wraps `pfsync intraday ticks-fetch`)
- `sync.intraday_ticks_load` (wraps `pfsync intraday ticks-load`)
- `sync.intraday_summaries_build` (wraps `pfsync intraday summaries-build`)
- `sync.market_summary_eod` (wraps `pfsync market-summary day --import-eod`)

**Risk:** HIGH. Intraday is the largest data domain (intraday_bars
~11.4M rows, tick_data ~10M rows). Pre-existing dirty file; touching
it is the riskiest single change in Phase 1.

**Done criterion:** `intraday.py` deleted; 7 new files exist; futures
promotion paths use safe_writer.

---

## Group G — Research, strategies, advanced (Week 6)

**Pages (KEEP, all read-only):**
- Research, Signal Analysis, Microstructure, Tick Analytics (REFACTOR),
  Tick Replay, Quant Lab, Macro Cycles, Sector Breadth, Market Research,
  ML Predictions
- 10 strategy pages: VPIN, OFI, CVD, Basis, VWAP, Macro Regime HMM,
  Flow Intelligence, Sector Rotation, OI, Pairs Trading
- Advanced: Signal Intelligence, Order Book Sim, Hawkes Process,
  RL Execution

**API endpoints needed:**
- Mostly read from existing eod / intraday / tick_data endpoints from
  earlier groups — minimal new surface
- `/v1/ml/predictions` — ml_predictions table reads
- `/v1/parquet/views/*` — parquet-backed analytics

**Worker jobs needed:**
- `compute.signal_score` — composite 1-100 score (Quant Lab + Signal
  Analysis)
- `compute.factor_model`, `compute.hmm_regime`, etc — moved from inline
  page compute to worker for slow pages

**Risk:** LOW per-page (all read-only), but VOLUME is high — 24 pages.
Wave can run in parallel with Group H if a second contributor is on it.

**Done criterion:** No page in this group does inline heavy compute;
all expensive operations are worker jobs the page polls or that
populate a result table the page reads.

---

## Group H — ALM, admin, ops (Week 7)

**Pages (KEEP):**
- ALM Dashboard
- Schema Explorer, App Lineage
- SBP EasyData, PSX Scraper
- Quote Monitor (regular_market.py snapshot view)
- Data Quality
- AI Chat, AI Insights

**MERGE targets:**
- Data Status (synthetic in app.py) + Sync Center (synthetic) + Sync
  Monitor + Data Sync (hidden) → single ADMIN Market Sync v1 page

**API endpoints needed:**
- `/v1/alm/{positions,products,liquidity,sensitivity}`
- `/v1/admin/{jobs,catalog,schema,lineage}`
- `/v1/admin/sync/{queue,history}` — worker job queue surface

**Worker jobs needed:**
- `sync.announcements` (wraps `pfsync announcements sync` — slow loop;
  Phase 2 batch optimization)
- `sync.sbp_easydata` — already rate-limited
- `sync.psx_scraper`

**Risk:** LOW. ALM is specialty; admin pages are inspection-only.

**Done criterion:** Market Sync v1 admin page exists; all 4 admin/data
duplicates redirect to it; ad-hoc sync buttons elsewhere either go
through the worker via API or display "deprecated — use ADMIN" banner.

---

## Out of scope for Phase 1

### DELETE (cleanup PR after Phase 1 closes)
Live Market, Live OHLCV, Live Indices, Symbols, Price Chart, Rankings,
EOD Loader, History, Settings, Sukuk (duplicate), Yield Curve (duplicate).

### DEFER (keep direct-DB pattern; revisit Phase 2)
- FTP Monitor — broken (`ftp_rates` table missing)
- Global Rates — broken (`global_rates` table missing)
- NPC Rates — broken (`npc_carry` views missing)
- WS Relay — operational diagnostic, rarely opened
- Website Scan — broken (`sources/sectors.py` scraper dependency)
- Stock Graph (GNN) — needs `torch_geometric` (not installed)
- LLM Sentiment — LLM calls; quality concerns
- Instruments — rarely used
- Sukuk, Yield Curve, SBP Auctions, FX Monitor, FX Analytics, Fund Directory — hidden URL-only duplicates of main-nav entries

## Wave summary

| Wave | Domain | Pages | Risk | Effort |
|---|---|---:|---|---|
| A | Market overview | 5 | LOW | 1 week |
| B | Fixed income (+ fixed_income.py split) | 8 | MEDIUM | 1 week |
| C | FX (+ consolidation) | 2 (down from 5) | LOW | 0.5 week |
| D | Funds (+ fund_explorer.py split) | 5 | MEDIUM | 0.5 week |
| E | Equities (+ company_deep.py split) | 4 | MEDIUM | 1 week |
| F | Intraday (+ intraday.py split) | 4 | HIGH | 1 week |
| G | Research/strategies/advanced | 24 | LOW | 1 week |
| H | ALM/admin/ops (+ Market Sync v1) | 9 | LOW | 0.5 week |
| **Total** | | **61 nav entries** | | **~6.5 weeks** |

The waves are deliberately ordered A→H so the FastAPI scaffold + worker
+ auth model land first (Wave A); each subsequent wave benefits from
the infrastructure built before it.
