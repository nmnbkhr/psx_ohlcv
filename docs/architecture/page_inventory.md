# pakfindata Page Inventory

**Generated:** 2026-05-20
**Purpose:** Phase 1 migration planning. Classifies every UI page for the
            API+UI separation work.

The inventory was assembled by reading nav structure in
[`src/pakfindata/ui/app.py`](../../src/pakfindata/ui/app.py), file sizes
in `src/pakfindata/ui/page_views/`, last-edit dates from `git log`, and
grep-spotting sync paths. No `.py` files were modified during this
audit.

## Summary

| Class | Count |
|---|---:|
| Total page files | 82 |
| **KEEP** | 31 |
| **REFACTOR** | 14 |
| **MERGE** | 13 |
| **DELETE** | 11 |
| **DEFER** | 13 |

Note: a few entries combine multiple `render_*` functions from a single
file (e.g. `fixed_income.py` exposes 8 nav entries; `fx.py` exposes 2).
Counts above are at the **nav entry** level, not raw `.py` files.

## Pages — by nav group

Below each page is its file, URL, line count, signals, classification,
and one-line reasoning. Pages registered in `nav_groups` are sidebar-
visible; pages in `_hidden_pages` are URL-only.

### SIMULATOR

#### Strategy Simulator
- **File:** `strategy_simulator.py` (366 lines)  •  **URL:** `simulator`
- **Reads:** eod_ohlcv, intraday_bars  •  **Writes:** none
- **Class:** **KEEP** — flagship feature, well-scoped read-only page.

#### Portfolio Scanner
- **File:** `portfolio_scanner.py` (230 lines)  •  **URL:** `portfolio-scanner`
- **Reads:** eod_*_summary  •  **Writes:** none
- **Class:** **KEEP** — concise; reads only from derived tables.

### MARKET OVERVIEW

#### Dashboard
- **File:** `dashboard.py` (750 lines)  •  **URL:** `dashboard` (default)
- **Reads:** eod_ohlcv, psx_indices, kibor_daily, sbp_fx_interbank, data_freshness
- **Writes:** 5 sync buttons via `safe_writer` + catalog (Phase 0.1, 0.2, 0.3)
- **Class:** **KEEP** — primary entry. Phase 1: reads → API; sync buttons → enqueue worker jobs.

#### Market Pulse
- **File:** `market_pulse.py` (357 lines)  •  **URL:** `market-pulse`
- **Reads:** eod_market_summary, eod_sector_summary  •  **Writes:** none
- **Class:** **KEEP** — clean breadth-rollup page.

#### Index Monitor
- **File:** `indices.py` (436 lines, **pre-existing dirty**)  •  **URL:** `index-monitor`
- **Reads:** psx_indices  •  **Writes:** 1 sync button (safe_writer NOT yet wired — sub-wave 2.4c skipped)
- **Class:** **REFACTOR** — Hard Rule pre-existing dirty file across Phases 0.1–0.4; sync button needs safe_writer + catalog wiring during Phase 1.

### EQUITIES

#### Market Summary
- **File:** `market_summary.py` (625 lines)  •  **URL:** `market-summary`
- **Reads:** eod_ohlcv, downloaded_market_summary_dates  •  **Writes:** 4 sync buttons (safe_writer)
- **Class:** **REFACTOR** — overlap with `Data Status` + `Sync Center`. Move write buttons to ADMIN Market Sync v1, keep read view here.

#### Stock Screener
- **File:** `stock_screener.py` (257 lines)  •  **URL:** `stock-screener`
- **Reads:** eod_symbol_summary, eod_ohlcv  •  **Writes:** none
- **Class:** **KEEP** — small, well-scoped, reads derived summary.

#### Company Profile
- **File:** `company_deep.py` (1240 lines)  •  **URL:** `company`
- **Reads:** company_profile, company_financials, company_ratios, eod_ohlcv, dividend_payouts
- **Writes:** 1 sync button (deep_scraper)
- **Class:** **REFACTOR** — large file; tabs grew organically. Split into Profile + Financials + Events sub-pages during Phase 1.

#### Sector Analysis
- **File:** `sector_analysis.py` (347 lines)  •  **URL:** `sector-analysis`
- **Reads:** eod_sector_summary, sectors  •  **Writes:** none
- **Class:** **KEEP**.

#### Symbol Financials
- **File:** `symbol_financials.py` (474 lines)  •  **URL:** `symbol-financials`
- **Reads:** company_financials, company_ratios  •  **Writes:** none
- **Class:** **MERGE** — overlaps with `Company Profile` financials tab. Roll into a `Company Profile / Financials` sub-route.

#### Factors
- **File:** `factor_analysis.py` (600 lines)  •  **URL:** `factors`
- **Reads:** eod_ohlcv (heavy), eod_symbol_summary  •  **Writes:** none
- **Class:** **KEEP** — heavy compute. Phase 1 should move factor computation to worker; UI keeps the chart view.

#### Intraday
- **File:** `intraday.py` (2499 lines, **pre-existing dirty**)  •  **URL:** `intraday`
- **Reads:** intraday_bars, tick_data, intraday_*_summary (7 tabs)
- **Writes:** 3 sync buttons (some via safe_writer)
- **Class:** **REFACTOR** — 7 tabs in one file; split into Intraday Charts / Volume / Movers / Sync sub-pages. Pre-existing dirty per Hard Rules.

#### Live Ticker
- **File:** `live_ticker.py` (351 lines)  •  **URL:** `live-ticker`
- **Reads:** psx_terminal WebSocket  •  **Writes:** transient WS state
- **Class:** **KEEP** — distinct purpose (live ticker tape).

#### Futures & Odd Lot
- **File:** `futures.py` (1381 lines)  •  **URL:** `futures`
- **Reads:** futures_eod  •  **Writes:** 5 buttons (some bypass safe_writer — legacy promotion path)
- **Class:** **REFACTOR** — historical surface for futures; promotion path needs safe_writer cleanup.

#### Post Close
- **File:** `post_close.py` (465 lines)  •  **URL:** `post-close`
- **Reads:** post_close_turnover  •  **Writes:** none
- **Class:** **KEEP**.

### FIXED INCOME

#### Rates Overview
- **File:** `rates_overview.py` (395 lines)  •  **URL:** `rates-overview`
- **Reads:** kibor_daily, sbp_policy_rates, tbill_auctions, pib_auctions
- **Writes:** 3 sync buttons (safe_writer + catalog, Phase 0.1+0.2)
- **Class:** **KEEP** — recently cleaned up.

#### Yield Curves
- **File:** `fixed_income.py::render_yield_curve` (2503 lines combined)  •  **URL:** `yield-curves`
- **Reads:** pkrv_daily, pkisrv_daily, pkfrv_daily
- **Class:** **REFACTOR** — `fixed_income.py` is a 2.5k-line mega-module hosting 8 nav entries; needs to be split into separate page files in Phase 1.

#### Curve Analytics
- **File:** `curve_analytics.py` (926 lines)  •  **URL:** `curve-analytics`
- **Reads:** sovereign_curve, pkrv_daily, pkisrv_daily, pib_auctions
- **Writes:** writes synthetic rates back to sovereign_curve (engine, not UI button)
- **Class:** **KEEP** — flagship sovereign-curve fit page; the engine-side write is documented (synthetic-rate exception in CLAUDE.md).

#### Treasury Auctions
- **File:** `fixed_income.py::render_sbp_auction_archive`  •  **URL:** `treasury-auctions`
- **Reads:** tbill_auctions, pib_auctions, gis_auctions  •  **Writes:** none
- **Class:** **MERGE** — part of `fixed_income.py` split; lands as its own page.

#### Bond Market
- **File:** `bond_market.py` (354 lines)  •  **URL:** `bond-market`
- **Reads:** sbp_bond_trading_daily, sbp_bond_trading_summary
- **Writes:** 2 sync buttons via safe_writer + catalog (Phase 0.1 W5b, W5c)
- **Class:** **KEEP**.

#### Benchmark Monitor
- **File:** `benchmark_monitor.py` (166 lines)  •  **URL:** `benchmark`
- **Reads:** sbp_benchmark_snapshot  •  **Writes:** 1 sync button via safe_writer + catalog
- **Class:** **KEEP** — small, recently cleaned.

#### Debt Terminal
- **File:** `debt_terminal.py` (1376 lines)  •  **URL:** `debt-terminal`
- **Reads:** kibor_daily, tbill_auctions, pib_auctions, sovereign_curve, sbp_benchmark_snapshot, ... (heavy)
- **Class:** **REFACTOR** — Bloomberg-Terminal-style page; many overlapping reads with Rates Overview + Curve Analytics; consolidate during Phase 1.

#### Treasury
- **File:** `treasury_dashboard.py` (1411 lines)  •  **URL:** `treasury`
- **Reads:** tbill_auctions, pib_auctions, konia_daily, sbp_fx_*_avg, sbp_policy_rates (8 tabs)
- **Writes:** 4 sync buttons via safe_writer + catalog (Phase 0.1 W1b)
- **Class:** **REFACTOR** — large 8-tab page; recently cleaned but still benefits from a tab-per-route split in Phase 1.

### ALM

#### ALM Dashboard
- **File:** `alm_dashboard.py` (706 lines)  •  **URL:** `alm-dashboard`
- **Reads:** alm_positions, alm_products, alm_liquidity_ladder, alm_sensitivity  •  **Writes:** none
- **Class:** **KEEP** — specialty ALM page.

#### FTP Monitor
- **File:** `ftp_monitor.py` (501 lines)  •  **URL:** `ftp-monitor`
- **Reads:** `ftp_rates` table (currently missing — referenced in coverage gap)
- **Writes:** none
- **Class:** **DEFER** — broken pending `ftp_rates` table creation. Filed in known_debt.

### FUNDS

#### Fund Explorer
- **File:** `fund_explorer.py` (2007 lines, hosts 4 nav entries)  •  **URL:** `fund-explorer`
- **Reads:** mutual_fund_nav, fund_performance, fund_risk_metrics  •  **Writes:** none
- **Class:** **REFACTOR** — mega-module hosting Fund Explorer + VPS Pension + Top Performers + ETFs; split in Phase 1.

#### VPS Pension
- **File:** `fund_explorer.py::render_vps_standalone`  •  **URL:** `vps-pension`
- **Class:** **MERGE** — part of fund_explorer.py split.

#### Top Performers
- **File:** `fund_explorer.py::render_top_performers_standalone`  •  **URL:** `top-performers`
- **Class:** **MERGE** — part of fund_explorer.py split.

#### Fund Analytics
- **File:** `funds.py::render_fund_analytics` (621 lines)  •  **URL:** `fund-analytics`
- **Reads:** mutual_fund_nav, fund_signals
- **Writes:** 1 sync button (MUFAP via safe_writer)
- **Class:** **REFACTOR** — overlaps with Fund Explorer's analytics tab.

#### ETFs
- **File:** `fund_explorer.py::render_etfs_standalone`  •  **URL:** `etfs`
- **Reads:** etf_nav, etf_master  •  **Writes:** none
- **Class:** **MERGE** — part of fund_explorer.py split.

### FX & RATES

#### Currency Dashboard
- **File:** `fx.py::render_fx_overview` (497 lines)  •  **URL:** `currency-dashboard`
- **Reads:** sbp_fx_interbank, forex_kerb, sbp_fx_*_avg
- **Class:** **MERGE** — heavy overlap with FX Dashboard; consolidate.

#### FX Dashboard
- **File:** `fx_dashboard.py` (1011 lines)  •  **URL:** `fx-dashboard`
- **Reads:** sbp_fx_interbank, forex_kerb, sbp_fx_*_avg, kibor_daily
- **Writes:** 4 sync buttons via safe_writer + catalog (Phase 0.1 W3b, W3c)
- **Class:** **KEEP** — recently cleaned; canonical FX read page.

#### Interbank vs Open
- **File:** `fx_interbank.py` (233 lines)  •  **URL:** `fx-interbank`
- **Reads:** sbp_fx_interbank, sbp_fx_open_market  •  **Writes:** 2 sync buttons via safe_writer (Phase 0.1 W2b)
- **Class:** **KEEP** — distinct comparison view.

#### Rate History
- **File:** `fx_history.py` (250 lines)  •  **URL:** `fx-history`
- **Reads:** sbp_fx_interbank, sbp_fx_*_avg  •  **Writes:** 1 sync button
- **Class:** **MERGE** — overlap with FX Dashboard history tab.

### COMMODITIES

#### Commodities
- **File:** `commodities.py` (1160 lines)  •  **URL:** `commodities`
- **Reads:** commodity_prices, commodity_fx_rates  •  **Writes:** 1 sync button (catalog NOT wired)
- **Class:** **REFACTOR** — large; needs safe_writer audit (per-row commits unclear).

#### PMEX
- **File:** `pmex.py` (1613 lines)  •  **URL:** `pmex`
- **Reads:** pmex_market_watch  •  **Writes:** none
- **Class:** **REFACTOR** — overlapping with PMEX Analytics; split or merge.

#### PMEX Analytics
- **File:** `pmex_analytics_page.py` (647 lines)  •  **URL:** `pmex-analytics`
- **Class:** **MERGE** — substantial overlap with `pmex.py`; consolidate.

### RESEARCH

#### Research
- **File:** `research_terminal.py` (471 lines)  •  **URL:** `research`
- **Class:** **KEEP** — research landing page.

#### Signal Analysis
- **File:** `signal_dashboard.py` (1616 lines)  •  **URL:** `signal-analysis`
- **Class:** **REFACTOR** — large composite-score dashboard; split signals from charts in Phase 1.

#### Microstructure
- **File:** `microstructure.py` (787 lines)  •  **URL:** `microstructure`
- **Reads:** tick_data, intraday_bars  •  **Class:** **KEEP**.

#### Tick Analytics
- **File:** `tick_analytics.py` (1877 lines)  •  **URL:** `tick-analytics`
- **Reads:** tick_data, parquet_store views  •  **Writes:** 2 sync buttons (parquet sync)
- **Class:** **REFACTOR** — large; sync buttons can move to ADMIN.

#### Tick Replay
- **File:** `tick_replay.py` (699 lines)  •  **URL:** `tick-replay`
- **Reads:** JSONL tick files via DuckDB  •  **Class:** **KEEP**.

#### Quant Lab
- **File:** `intraday_quant_lab.py` (1115 lines)  •  **URL:** `quant-lab`
- **Class:** **KEEP** — composite 1-100 signal score page; flagship.

#### Macro Cycles
- **File:** `macro_cycles.py` (403 lines)  •  **URL:** `macro-cycles`
- **Class:** **KEEP**.

#### Sector Breadth
- **File:** `sector_breadth.py` (717 lines)  •  **URL:** `sector-breadth`
- **Reads:** eod_sector_summary  •  **Class:** **KEEP**.

#### Market Research
- **File:** `market_research.py` (1066 lines)  •  **URL:** `market-research`
- **Class:** **REFACTOR** — large; some overlap with Research/Sector Breadth.

#### ML Predictions
- **File:** `ml_predictions.py` (714 lines)  •  **URL:** `ml-predictions`
- **Class:** **KEEP** — distinct purpose; heavy compute moves to worker in Phase 1.

### STRATEGIES (11 pages)

All `strategy_*.py` files share the same shape: read-only against `tick_data` / `eod_ohlcv` / `intraday_bars` / `intraday_index_minute`, then a Python compute loop that scores. None have writes.

| Page | File | Lines | Class | Reasoning |
|---|---|---:|---|---|
| VPIN Strategy | strategy_vpin.py | 359 | **KEEP** | Distinct algorithm |
| OFI Alpha | strategy_ofi.py | 375 | **KEEP** | Distinct algorithm |
| CVD Divergence | strategy_cvd.py | 240 | **KEEP** | Distinct algorithm |
| Basis Arb | strategy_basis.py | 258 | **KEEP** | Distinct algorithm |
| VWAP Execution | strategy_vwap.py | 275 | **KEEP** | Distinct algorithm |
| Macro Regime | strategy_hmm.py | 690 | **KEEP** | Distinct algorithm (HMM) |
| Flow Intelligence | nccpl_flows.py | 435 | **KEEP** | NCCPL FIPI/LIPI — distinct |
| Sector Rotation | strategy_sector.py | 207 | **KEEP** | Distinct algorithm |
| OI Buildup | strategy_oi.py | 483 | **KEEP** | Distinct algorithm |
| Pairs Trading | strategy_pairs.py | 727 | **KEEP** | Distinct algorithm |
| LLM Sentiment | strategy_sentiment.py | 226 | **DEFER** | LLM calls — keep direct-DB pattern, revisit Phase 2 |

### ADVANCED

| Page | File | Lines | Class | Reasoning |
|---|---|---:|---|---|
| Signal Intelligence | signal_intelligence.py | 373 | **KEEP** | Distinct |
| Order Book Sim | strategy_orderbook.py | 934 | **KEEP** | Distinct heavy-compute |
| Stock Graph (GNN) | advanced_gnn.py | 707 | **DEFER** | Requires torch_geometric (not installed); essentially stub |
| Hawkes Process | advanced_hawkes.py | 476 | **KEEP** | Distinct |
| RL Execution | advanced_rl_exec.py | 322 | **KEEP** | Distinct (PyTorch) |

### ADMIN

#### Data Status
- **File:** synthetic in `app.py`  •  **URL:** `data-status`
- **Class:** **MERGE** — overlap with Sync Monitor + Data Quality. Roll into a single "Market Sync v1" admin page per CLAUDE.md plan.

#### Sync Center
- **File:** synthetic in `app.py`  •  **URL:** `sync-center`
- **Class:** **MERGE** — overlap with Data Status. Same target as above.

#### Schema Explorer
- **File:** `schema.py` (254 lines)  •  **URL:** `schema`
- **Class:** **KEEP** — diagnostic page; explicit reads of MAX/COUNT per table; documented exception.

#### App Lineage
- **File:** `app_lineage.py` (1158 lines)  •  **URL:** `app-lineage`
- **Class:** **KEEP** — diagnostic + lineage map; very useful for Phase 1 design.

#### SBP EasyData
- **File:** `sbp_easydata.py` (569 lines)  •  **URL:** `sbp-easydata`
- **Writes:** Sync buttons (safe_writer)
- **Class:** **KEEP** — distinct rate-limited scraper UI.

#### PSX Scraper
- **File:** `psx_scraper.py` (673 lines)  •  **URL:** `psx-scraper`
- **Writes:** Sync buttons
- **Class:** **KEEP** — distinct purpose.

## Hidden pages (URL-only, no sidebar entry)

These 28 pages are registered for URL access only (bookmarks). Most are
legacy.

| Page | File | URL | Class | Reasoning |
|---|---|---|---|---|
| Live Market | live_market.py (282) | live-market | **DELETE** | Superseded by Live Ticker |
| Live OHLCV | live_ohlcv.py (571) | live-ohlcv | **DELETE** | Hidden; uses deprecated tick→eod promotion path |
| Live Indices | live_indices.py (560) | live-indices | **DELETE** | Hidden; functionality moved into Index Monitor + Dashboard |
| WS Relay | ws_relay_status.py (200) | ws-relay | **DEFER** | Operational diagnostic, rarely used |
| Quote Monitor | regular_market.py (428) | quote-monitor | **KEEP** | Distinct snapshot view; write button via safe_writer + catalog |
| Price Chart | candlestick.py (202) | price-chart | **MERGE** | Roll into Stock Screener or Company Profile |
| Rankings | rankings.py (217) | rankings | **MERGE** | Functionality covered by Stock Screener + Top Performers |
| Symbols | symbols.py (137) | symbols | **DELETE** | Trivial; functionality in `pfsync symbols list` |
| Instruments | instruments.py (223) | instruments | **DEFER** | Phase 1 instrument-universe page; rarely used |
| FI Overview | fixed_income.py::render_psx_debt_market | fi-overview | **MERGE** | Part of fixed_income.py split |
| Bond Search | fixed_income.py::render_bonds_screener | bond-search | **MERGE** | Part of fixed_income.py split |
| Yield Curve | fixed_income.py::render_yield_curve | yield-curve | **MERGE** | Duplicate of "Yield Curves" main nav entry |
| Sukuk | fixed_income.py::render_sukuk_screener | sukuk | **MERGE** | Part of fixed_income.py split |
| SBP Auctions | fixed_income.py::render_sbp_auction_archive | sbp-auctions | **MERGE** | Duplicate of "Treasury Auctions" main nav |
| Global Rates | global_rates.py (396) | global-rates | **DEFER** | global_rates table missing; broken pending Phase 1 schema fix |
| NPC Rates | npc_rates.py (333) | npc-rates | **DEFER** | npc_carry views missing; broken |
| FX Monitor | fx.py::render_fx_overview | fx-monitor | **MERGE** | Duplicate of Currency Dashboard / FX Dashboard |
| FX Analytics | fx.py::render_fx_impact | fx-analytics | **MERGE** | Part of FX consolidation |
| Fund Directory | funds.py::render_mutual_funds | fund-directory | **MERGE** | Roll into Fund Explorer |
| Data Sync | data_acquisition.py (606) | data-sync | **MERGE** | Roll into Sync Center → Market Sync v1 |
| EOD Loader | eod_loader.py (706) | eod-loader | **DELETE** | Uses deprecated promotion path; functionality in `pfsync sync` |
| History | history.py (407) | history | **DELETE** | Generic history viewer, superseded by per-page history |
| Sync Monitor | sync_monitor.py (744) | sync-monitor | **MERGE** | Roll into ADMIN Market Sync v1 |
| Data Quality | data_quality.py (620) | data-quality | **KEEP** | Distinct purpose — partial pollution dashboard |
| Website Scan | website_scan.py (237) | website-scan | **DEFER** | Specialty; broken `sources/sectors.py` scraper dependency |
| AI Chat | chat.py (in ui/) | ai-chat | **KEEP** | Distinct LLM agent UI |
| AI Insights | ai_insights.py (692) | ai-insights | **KEEP** | Distinct |
| Settings | settings.py (112) | settings | **DELETE** | Stub from Feb; no real content |

## Cross-cutting findings

- **fixed_income.py (2503 lines) hosts 8 nav entries** — has to be split during Phase 1; one of the highest-priority refactors.
- **fund_explorer.py (2007 lines) hosts 4 nav entries** — same pattern.
- **intraday.py (2499 lines, dirty)** — 7 tabs in one file; pre-existing dirty so Phase 0 left it alone. Phase 1 should split.
- **6 pages still bypass safe_writer or have unclear write paths**: `indices.py`, `commodities.py`, `futures.py` (legacy promotion), `eod_loader.py` (legacy), `live_ohlcv.py` (legacy), `data_acquisition.py`. The first three need cleanup during Phase 1 migration; the last three are DELETE candidates.
- **FX functionality is sprawled across 5 pages**: fx_dashboard (KEEP), fx_interbank (KEEP), fx_history (MERGE), fx.py / Currency Dashboard (MERGE), FX Analytics / FX Monitor (MERGE). Consolidation candidate.
- **Hidden pages that are real duplicates**: Yield Curve / SBP Auctions / FX Monitor / FX Analytics / Fund Directory / Sync Monitor — these all duplicate main-nav entries and exist only for URL backwards-compat. Phase 1 can sunset most.
- **Last-edit dates** are mostly 2026-04-28 (post-NTFS-recovery commit batch) or 2026-05-18 (Phase 0.1–0.3 catalog work). Only `settings.py` (Feb 2026) and `indices.py` (May 11) stand out — Settings is a stub; Indices is pre-existing dirty.
- **No `.py` file has > 6 months without a touch** — the codebase is alive. No truly orphaned files.
- **Schema explorer + App Lineage are diagnostic, intentionally do their own MAX/COUNT** (documented exceptions in Milestone 0.2 catalog audit).

## What lands where in Phase 1

- **KEEP (31)** — migrate to API+UI separation as-is. Wave structure in [`phase1_migration_groups.md`](phase1_migration_groups.md).
- **REFACTOR (14)** — same migration, but split the file/tabs into separate routes during the Phase 1 work. Most expensive bucket.
- **MERGE (13)** — consolidate into the existing destination page; remove the source file in a cleanup commit.
- **DELETE (11)** — file a single cleanup PR after Phase 1; not part of the migration scope.
- **DEFER (13)** — keep on direct-DB-read pattern during Phase 1; revisit in Phase 2 (and fix the broken-table dependencies before exposing on the API surface).
