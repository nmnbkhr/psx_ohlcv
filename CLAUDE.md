# pakfindata — Bloomberg Terminal-style Pakistan Financial Data Platform

## Quick Start
```bash
conda activate psx
cd ~/projects/pakfindata
streamlit run src/pakfindata/ui/app.py
```

## Stack
- **Runtime:** Python 3.12, conda env `psx`, interpreter at `~/miniforge3/envs/psx/bin/python3.12`
- **UI:** Streamlit + Plotly (82 page views)
- **Data:** SQLite (`/mnt/e/psxdata/psx.sqlite`, 147 tables), DuckDB (`/mnt/e/psxdata/pakfindata.duckdb`) for analytics
- **Scraping:** requests + lxml (PSX website), DrissionPage for JS-heavy sources
- **Compute:** PyTorch 2.11.0+cu130 (CUDA, RTX 4080), numpy, pandas, scipy, sklearn
- **APIs:** PSX DPS (`dps.psx.com.pk`), PSX Terminal (`psxterminal.com`), SBP EasyData (`easydata.sbp.org.pk`), MUFAP

## Architecture
- Single Streamlit app with multi-page sidebar layout
- Bloomberg Terminal aesthetic: `#0B0E11` background, `#C8A96E` gold accents
- JetBrains Mono (code/data) + Space Grotesk (UI headings)
- All TA computations in raw numpy/pandas/scipy — ZERO external TA libraries
- Dual-write: SQLite (primary) + DuckDB (analytics) for key tables

## Modules Overview

### UI Pages (82 views in `src/pakfindata/ui/page_views/`)
- **Market Overview:** Dashboard, Market Pulse, Index Monitor, Live Ticker
- **Equities:** Market Summary, Stock Screener, Company Profile, Sector Analysis, Symbol Financials, Factors
- **Intraday:** Intraday terminal (7 tabs: Dashboard, Charts, Market Pulse, Volume, Movers, Index, Dedup, Sync)
- **Intraday Quant Lab:** Signal dashboard with composite 1-100 score
- **Futures & Odd Lot:** Futures terminal, Post Close
- **Fixed Income:** Rates Overview, Yield Curves, **Curve Analytics**, Treasury Auctions, Treasury Dashboard (8 tabs)
- **FX:** FX Dashboard, FX Interbank, FX History
- **Funds:** Fund Explorer, MUFAP analytics, ETF NAV
- **Commodities:** PMEX, commodity analytics
- **Strategies:** Simulator, CVD, OFI, OI, VPIN, VWAP, Pairs, Sector Rotation, Sentiment, HMM, Orderbook
- **Research:** AI Insights, ML Predictions, Microstructure, Signal Intelligence
- **Data Management:** Market Summary sync, Intraday sync, SBP EasyData, Settings

### Data Sources (58 modules in `src/pakfindata/sources/`)
- **PSX DPS API:** EOD OHLCV (`/timeseries/eod/{symbol}`), intraday ticks (`/timeseries/int/{symbol}`), market summary (`.Z` bulk files)
- **PSX Terminal API:** Klines (1m/5m/15m/1h/1d/1w), symbols list
- **SBP:** Treasury auctions (PMA page), KIBOR/PKRV/KONIA rates, GIS sukuk, policy rates, EasyData API (18K+ variables)
- **MUFAP:** Yield curves (PKRV/PKISRV/PKFRV), mutual fund NAV (1.9M+ records)
- **Others:** forex.pk (kerb rates), NCCPL flows, IPO scraper, ETF scraper, global rates (SOFR/SONIA/EUSTR)

### Engines (44+ modules in `src/pakfindata/engine/`)
- Signal scoring, commentary generation, ML features/models
- Hawkes process, VPIN toxicity, Hurst exponent regime classification
- Pairs trading, sector rotation, CVD/OFI/OI strategies
- GNN stock graph, RL execution, orderbook simulation
- Fund risk engine, ALM engine, macro regime HMM
- **tick_predictor** — replay overlay predictions with Bayesian credibility
- **curve_analytics** — sovereign yield curve (Linear/Spline/NSS), confidence scoring, PKRV anchoring
- **sukuk_pricer** — Sukuk fair value via PKISRV curve, YTM, modified duration, MUFAP comparison

### DB Repositories (32 modules in `src/pakfindata/db/repositories/`)
- EOD, intraday, tick data, futures, symbols, financials
- Fixed income, treasury, yield curves, global rates
- FX, NCCPL flows, ETF, instruments, bond market

## Data Paths

Two-tier storage layout (post NTFS-corruption recovery, 2026-04-21):

**Databases — NVMe ext4 (`/home/smnb/psxdata_rescue/`)**
- Main SQLite DB: `/home/smnb/psxdata_rescue/psx.sqlite`
- Reason: durable WAL, fast random writes, no FUSE quirks
- Override via env var: `PSX_DB_PATH`

**Bulk data — NTFS via FUSE (`/mnt/e/psxdata/`)**
- Parquet stores: `/mnt/e/psxdata/parquet/<table>/*.parquet`
- JSONL tick logs, intraday JSON, market summary CSVs, SBP EasyData CSVs
- Reason: bigger drive, sequential reads, fsync speed irrelevant
- Override via env var: `PSX_DATA_ROOT`

**DuckDB**
- In-memory only, built fresh per process via `db.connections.analytics_con()`
- The legacy `pakfindata.duckdb` file is abandoned (12 KB residual)

**Canonical entry points (always use these, never hardcode paths):**
- Read DB path: `from pakfindata.settings import get_settings; get_settings().db_path`
- Read data root: `get_settings().data_root`
- Analytics queries: `from pakfindata.db.connections import analytics_con`
- SQLite writes: `from pakfindata.db.connections import sqlite_con`

**Specific path references**
- **MUFAP NAV:** `/mnt/e/psxdata/mufapnav/`
- **Market summary files:** `~/data/market_summary/csv/` and `~/data/market_summary/raw/`
- **EOD JSON (per-symbol API):** `~/data/eod_json/{date}/{SYMBOL}.json`
- **EOD CSV (per-symbol):** `~/data/eod_csv/{date}/{SYMBOL}.csv`
- **Intraday JSON (per-symbol):** `/mnt/e/psxdata/intraday/{date}/{SYMBOL}.json`
- **Intraday CSVs:** `~/psxdata/intraday/{date}/dps_ticks_{date}.csv`, `psxt_backfill_*.csv`
- **Tick logs (cloud):** `/mnt/e/psxdata/tick_logs_cloud/ticks_{date}.jsonl` (8.5 GB, 26 dates, has `market` col)
- **SBP EasyData:** `/mnt/e/psxdata/sbp_easydata/` (raw/, datasets/, series/)
- **MUFAP rates (PKRV/PKISRV/PKFRV):** `/mnt/e/psxdata/rates/{pkrv,pkisrv,pkfrv}/*.csv` (3,178 files back to 2020)
- **SBP rate archives:** `/mnt/e/psxdata/sbp_rates/archives/` (tb.xlsx, Pakinvestbonds.xlsx, etc.)
- **Cloud sync script:** `~/sync_psx_cloud.sh` (rsync from Oracle VM `psx-cloud`)

## Key Data Flows

### Market Summary (bulk daily file)
```
PSX DPS → .Z file → extract → parse → CSV (~/data/market_summary/csv/{date}.csv)
  → "Sync to DB" → eod_ohlcv (REG) + futures_eod (FUT/CONT/ODL)
```

### Per-Symbol EOD (API → DB, parallel)
```
"Fetch All Symbols → DB" button (Market Summary page)
  → 3 parallel shards × 10 workers = 30 concurrent API calls
  → /timeseries/eod/{symbol} for each active symbol
  → Save JSON to ~/data/eod_json/{date}/
  → Save CSV to ~/data/eod_csv/{date}/
  → Upsert into eod_ohlcv (source='per_symbol_api')
  → Skips SUSPENDED/DELISTED symbols (>30 days, based on company_listing_status.first_seen)
```

### Intraday Ticks (2-step: API → Disk → DB)
```
Step 1: "Fetch All Ticks → Disk" button (Intraday Sync tab)
  → 3 parallel shards × 10 workers
  → /timeseries/int/{symbol} for each active symbol
  → Save JSON to /mnt/e/psxdata/intraday/{date}/{SYMBOL}.json

Step 2: "Load Ticks from Disk" button
  → Reads JSON files from disk
  → Batch insert into intraday_bars + tick_data (single commit, fast)
  → Dual-write to DuckDB
```

### Cloud Tick Logs → Local
```
~/sync_psx_cloud.sh (rsync from Oracle VM psx-cloud)
  → tick_logs → /mnt/e/psxdata/tick_logs_cloud/
  → intraday → /mnt/e/psxdata/intraday_cloud/
  → tick_bars.db → /mnt/e/psxdata/tick_bars_cloud.db

JSONL → per-symbol JSON conversion (for missed days):
  Parse ticks_{date}.jsonl → group by symbol → write {SYMBOL}.json
  Also generates: dps_ticks_{date}.csv, dps_eod_daily.csv,
  psxt_{date}_1m.csv, psxt_backfill_{5m,15m,1h,1w}.csv
```

### Treasury / Fixed Income
```
SBP PMA page → T-Bill/PIB auctions, GIS sukuk → tbill_auctions, pib_auctions, gis_auctions
SBP rates scraper → KIBOR, PKRV, KONIA → kibor_daily, pkrv_daily, konia_daily
SBP EasyData API → 25 priority datasets → CSV on disk → sync to DB
  Rate limit: 250 req/hr, 2000 req/day, ~15s between requests
  API key in sbp_easydata.py, SSL verify=False (SBP cert issues)
MUFAP API → POST /WebRegulations/GetSecpFileById with fk_HeaderSubMenuTabId: 46
  3,191 files back to 2020 (1,064 PKRV + 1,034 PKISRV + 1,080 PKFRV)
  Old CSV URLs (mufap.com.pk/pdf/...) are dead — use the new API
  Tools: `python -m pakfindata.sources.mufap_rates {status|backfill-disk|backfill-db|sync}`
  Fast sync: backfill_to_db_fast() — parallel parse + batch commit, 13.5s for 2008 files
```

### Sovereign Yield Curve (consolidated)
```
SBP Excel archives (tb.xlsx, Pakinvestbonds.xlsx) → sbp_rates_processor →
MUFAP pkrv_daily / pkisrv_daily → sbp_rates_processor → sovereign_curve table
SBP KIBOR kibor_daily → sovereign_curve

Tools: `python -m pakfindata.sources.sbp_rates_downloader download`
       `python -m pakfindata.sources.sbp_rates_processor process`

sovereign_curve sources: PKRV (1M→20Y), PKISRV (1M-1Y only), MTB (3M/6M/12M cutoffs),
  PIB (2Y-30Y cutoffs), KIBOR (3M/6M/12M), POLICY (O/N)
Also: PKRV_SYN/PKISRV_SYN (synthetic rates via spline), {SOURCE}_SYN tenor='_RMSE' for NSS fit tracking
```

## Symbol Filtering (for batch downloads)
- Active symbols from `symbols` table (`is_active = 1`)
- Skip SUSPENDED/DELISTED from `company_listing_status` where `first_seen <= date('now', '-30 days')`
- `first_seen` = last trading date in eod_ohlcv (effective suspension date, NOT sync timestamp)
- Suffix symbols (XD/XB/XR/NC/WU) are kept — API returns empty but they're not filtered
- ~561 active, ~62 suspended skipped, ~500 fetch successfully

## Key Tables (SQLite)
| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `eod_ohlcv` | Daily OHLCV | symbol, date, OHLCV, source, processname |
| `futures_eod` | Derivatives daily | symbol, date, market_type, base_symbol |
| `intraday_bars` | Tick-level trades | symbol, ts, ts_epoch, OHLCV, interval |
| `tick_data` | Raw tick data | symbol, timestamp, price, volume |
| `symbols` | Symbol master | symbol, name, sector, is_active |
| `company_listing_status` | Suspension tracking | symbol, status, first_seen, is_current |
| `symbol_status_history` | SCD2 status changes | symbol, status, start_date, end_date |
| `kibor_daily` | KIBOR rates | date, tenor, bid, offer |
| `tbill_auctions` | T-Bill auctions | auction_date, tenor, cutoff_yield |
| `pib_auctions` | PIB auctions | auction_date, tenor, cutoff_yield |
| `gis_auctions` | GIS sukuk | auction_date, gis_type, cutoff_rental_rate |
| `sbp_policy_rates` | SBP policy rate | rate_date, policy_rate |
| `mutual_fund_nav` | Fund NAV (1.9M rows) | fund_id, date, nav |
| `downloaded_market_summary_dates` | Download tracking | date, status, row_count |
| `sovereign_curve` | Unified yield curve (67K rows, 5 sources) | date, source, tenor, days, yield_pct, bid, offer |
| `pkrv_daily` | Conventional sovereign curve (16K rows) | date, tenor_months, yield_pct, change_bps |
| `pkisrv_daily` | Islamic sovereign curve (1.5K rows, 5 tenors only) | date, tenor, yield_pct |
| `pkfrv_daily` | Floating-rate PIB valuations | date, bond_code, fma_price |
| `eod_symbol_summary` | **Summary** — per-symbol derived + ranks (~500/day) | date, symbol, change_pct, turnover, is_gainer, is_loser, rank_change_desc/asc, rank_volume, rank_turnover |
| `eod_market_summary` | **Summary** — breadth rollup (1 row per date) | date, total, gainers, losers, unchanged, avg_change, total_volume, total_value |
| `eod_sector_summary` | **Summary** — per-sector rollup | date, sector_name, stocks, avg_change, total_volume, up, down |

## Rules
- **Additive-only**: Never modify existing files from prior phases. Create new files or extend.
- **No external TA libs**: All indicators (RSI, MACD, Bollinger, etc.) must be raw numpy/pandas/scipy
- **UTC internally**: All timestamps UTC; convert to PKT (`Asia/Karachi`) in frontend display only
- **Bloomberg aesthetic**: Every new page/chart must follow the dark theme with gold accents
- **CLI tool**: `pfsync` is the CLI entry point (renamed from `psxsync` at v3.7.0)
- **Commit messages**: Imperative mood, < 72 chars
- **Parallel downloads**: Use 3 shards × 10 workers for batch API fetches
- **DB writes**: Batch inserts with single commit for speed (not per-symbol commits)
- **Dual-write**: Key tables write to both SQLite and DuckDB
- **Fetch → Ingest → Summarize separation** (mandatory for new sync paths):
  on-demand sync must not fetch-and-write in one call. Three explicit, re-runnable
  steps — `fetch_to_disk(date)` (HTTP → cache, no DB), `ingest_from_disk(con, path)`
  (disk → base table, no HTTP), `refresh_summary(con, date)` (base → summary table).
- **View pages are read-only**: Dashboard, Market Pulse, Index Monitor, Rates
  Overview, etc. MUST NOT trigger syncs. Their "Refresh" button calls
  `st.cache_data.clear()` + `st.rerun()` only. All loading lives under the
  ADMIN → Market Sync page.
- **Proper values, not guesses**: When source data is missing, return `None`
  and render a visible absence marker (`—`, "N/A", or staleness badge). Never
  fabricate values, never silently carry forward a stale row, never compute
  `mid = (buying + selling) / 2` unless both legs are actually present. Prefer
  the source with wider coverage (e.g., `sovereign_curve` over individual
  rate tables) when coverage differs. If a page reads from both a short-history
  and a long-history table, surface which path served the number.

## NOT Installed (needed for future GNN strategy)
- torch_geometric, torch_scatter, torch_sparse, torch_cluster

## Canonical EOD Population Path

As of 2026-04-20, `eod_ohlcv` is populated by exactly **one** path in day-to-day
operations:

```
PSX DPS → market_summary .Z file → extract → CSV on disk
  (~/data/market_summary/csv/{date}.csv)
  → ingest_market_summary_csv(con, csv_path)
  → upsert_eod(con, reg_df, source='market_summary')
  → (FUT/CONT/ODL rows routed to futures_eod, not eod_ohlcv)
```

Then turnover is filled in by the **Post Close** step (separate .zip from PSX):

```
PSX Post Close → post_close_turnover table
  + UPDATE eod_ohlcv SET turnover = ? WHERE symbol = ? AND date = ?
```

**Fallback (for corrupted/empty .Z):**
```
PSX DPS → closing_rates/{date}.pdf → parse → CSV
  → ingest_closing_rates_pdf() → upsert_eod(source='closing_rates_pdf')
```
(Function exists in `sources/closing_rates_pdf.py`; needs to be wired into the
Market Sync pipeline as Step 1's fallback branch — currently has zero callers.)

**Deprecated paths (kept in code for legacy UI buttons, not used in normal ops):**
- `promote_tick_ohlcv_to_eod` (tick.py) — from `tick_ohlcv` table; caller: hidden Live OHLCV page.
- `promote_intraday_to_eod` (intraday.py) — from `intraday_bars`; callers: Intraday page + hidden Live OHLCV.
- `migrate_from_eod_ohlcv` (futures.py) — one-off FUT/CONT/ODL extraction; caller: Futures page.
- `backfill_eod_sources` (eod.py) — one-off source-column backfill; zero callers.

All four carry `DEPRECATED` docstrings. Do not invoke from new code paths.

## Source-to-Table Catalog (canonical writers)

Reference for Market Sync v1 — one canonical writer per base table. Callers
should route through these functions; inline INSERTs at other sites are
convergence targets (marked with `NOTE(market-sync-v1)` comments in-source).

### Equities & derivatives
| Table | Canonical writer | Source module |
|---|---|---|
| `eod_ohlcv` | `upsert_eod` / `ingest_market_summary_csv` | `db/repositories/eod.py` |
| `futures_eod` | `upsert_futures_eod` | `db/repositories/futures.py` |
| `post_close_turnover` | `upsert_post_close` | `db/repositories/post_close.py` |

### Indices
| Table | Canonical writer | Source module |
|---|---|---|
| `psx_indices` | `save_index_data(con, idx_data)` | `sources/indices.py` |
| *(also)* `upsert_index_data` | repo wrapper | `db/repositories/market.py` |

**Secondary writers** (convergence targets):
- `sources/indices.py::backfill_indices()` inline INSERT — should delegate to `save_index_data`.

### Rates family
| Table | Canonical writer | Location |
|---|---|---|
| `sbp_policy_rates` | `upsert_policy_rate(con, data)` | `db/repositories/fixed_income.py:3070` |
| `kibor_daily` | `upsert_kibor_point(con, data)` | `db/repositories/yield_curves.py:201` |
| `konia_daily` | `upsert_konia_rate(con, data)` | `db/repositories/yield_curves.py:170` |
| `tbill_auctions` | `upsert_tbill_auction(con, data)` | `db/repositories/treasury.py:79` |
| `pib_auctions` | `upsert_pib_auction(con, data)` | `db/repositories/treasury.py:119` |
| `gis_auctions` | `upsert_gis_auction(con, data)` | `db/repositories/treasury.py:158` |
| `pkrv_daily` | `upsert_pkrv_point(con, data)` | `db/repositories/yield_curves.py:89` |
| `pkisrv_daily` | `upsert_pkisrv_point(con, data)` | `db/repositories/yield_curves.py:115` |
| `pkfrv_daily` | `upsert_pkfrv_point(con, data)` | `db/repositories/yield_curves.py:139` |

**Alternative source paths** (all legitimate, converge on repo writers):
- SBP EasyData API — `sources/sbp_easydata.py::sync_kibor_to_db / sync_policy_rate_to_db / sync_fx_to_db` (inline INSERTs)
- SBP Treasury scraper — `sources/sbp_treasury.py::SBPTreasuryScraper.sync_treasury` (uses repo upserts)
- MUFAP bulk backfill — `sources/mufap_rates.py::backfill_to_db_fast` (inline INSERTs for speed — `NOTE(market-sync-v1)` at line ~418)
- SBP PIB archive — `sources/sbp_pib_archive.py` (uses repo upsert)
- SBP GSP/SIR archives — `sources/sbp_gsp.py` / `sources/sbp_sir.py` (use repo upserts)

### Sovereign curve (consolidated)
| Table | Canonical writer | Location |
|---|---|---|
| `sovereign_curve` | `UPSERT_SQL` bulk executemany | `sources/sbp_rates_processor.py:45` |

**Layering exception** (documented in-source):
- `engine/curve_analytics.py:609, 618` writes synthetic rates (`{SOURCE}_SYN` tenor + `_RMSE` metadata) directly into `sovereign_curve`. Future: migrate to a dedicated `sovereign_curve_synthetic` table so the base table contains only source-observed data.

### FX
| Table | Canonical writer | Location |
|---|---|---|
| `sbp_fx_interbank` | `upsert_fx_interbank(con, data)` | `db/repositories/fx_extended.py:66` |
| `forex_kerb` | inline `INSERT` in `fx_extended.py:109` — *promote to `upsert_forex_kerb` during Market Sync v1* | same file |

**Alternative source paths:**
- SBP FX scraper — `sources/sbp_fx.py` (uses repo upsert)
- SBP EasyData API — `sources/sbp_easydata.py::sync_fx_to_db` (inline INSERT)
- FX microservice (localhost:8100) — `sources/fx_sync.py::sync_fx_rates / backfill_fx_history` (inline INSERT, `NOTE(market-sync-v1)` in module docstring)

### UI buttons that currently trigger writes (move to Market Sync v1)
| Page | Button | Writes to |
|---|---|---|
| Dashboard | "Refresh" (top-right) | market_summary snapshot, indices, rates family |
| Dashboard | "Sync EOD" | eod_ohlcv (forked daemon) |
| Dashboard | "Sync Indices" | psx_indices |
| Dashboard | "Sync Rates" | KIBOR + treasury + policy |
| Rates Overview | "Sync KIBOR", "Sync Treasury" | kibor_daily, tbill/pib_auctions |
| Treasury Dashboard | "Sync T-Bill / PIB" | tbill_auctions, pib_auctions |
| FX Interbank | "Sync FX" (calls both sync_fx_to_db + sync_kibor_to_db) | sbp_fx_interbank, kibor_daily |
| FX Dashboard | "Sync from FX Microservice" | sbp_fx_interbank, kibor_daily (via fx_sync.py) |
| Market Summary | "Download", "Sync to DB", "Fetch All Symbols → DB", "Sync Range to DB" | eod_ohlcv, futures_eod |
| EOD Loader (hidden) | multiple | eod_ohlcv |
| Futures | "Sync futures" + 4 ingest buttons | futures_eod, occasionally eod_ohlcv |

## Market Sync Architecture (v1)

Single admin page (**"Market Sync"** in the ADMIN pillar) orchestrates all source
loading and summary building for Market Overview + Fixed Income + FX sections.
Supersedes the scattered sync buttons on Dashboard, Market Summary (currently in
EQUITIES — **moves to ADMIN**), Data Status, Sync Center, and hidden EOD Loader /
Data Sync / Data Acquisition pages.

### Pipeline (per date)
1. **Download to disk** — CSV/JSON landed under `~/data/` or `/mnt/e/psxdata/`; no DB writes.
2. **Ingest disk → base tables** — reads local files, upserts into base tables.
3. **Build summaries** — reads base tables, writes `eod_*_summary` (+ `sovereign_curve`).

Each step is a separate button; full chain runs via a single "Run All".

### Run modes
- **Backfill** — auto-detect missing dates, fill all.
- **Single date** — calendar picker.
- **Date range** — start/end pickers, loops.
- **Manual pick** — availability grid with checkboxes; each date colored by
  what's present on disk / in DB / in summaries.

### Masters (slow-changing, separate tab, manual trigger)
- Symbols universe (`fetch_market_watch` → `symbols`, `sectors`)
- Listing status, categories, etc.
Not triggered by the Daily Pipeline.

### Scope v1 — base tables loaded
- **Equity core:** `eod_ohlcv`, `futures_eod`, `psx_indices`
- **Rates:** `sbp_policy_rates`, `kibor_daily`, `konia_daily`, `tbill_auctions`,
  `pib_auctions`, `pkrv_daily`, `pkisrv_daily`, `pkfrv_daily`, `gis_auctions`
- **FX:** `sbp_fx_interbank`, `forex_kerb`

### Scope v1 — summaries built
- `eod_symbol_summary`, `eod_market_summary`, `eod_sector_summary`
- `sovereign_curve` rebuild (consolidates PKRV + PKISRV + MTB + PIB + KIBOR)

### Scope v2 (next)
- Intraday bars + tick data + turnover pipelines (see `intraday_bars`, `tick_data`,
  tick_logs cloud). Includes detailed per-minute turnover rollups.
- MUFAP NAV, announcements, NCCPL flows, ETFs
- SBP EasyData 18K variables (tables currently missing from DB — see Coverage Gaps)
- `global_rates`, deep fundamentals

## Coverage Gaps & Cleanup

From 2026-04-20 audit. Re-run via `summary_coverage(con)` in
`db/repositories/market_summary.py`.

### Missing tables / views (referenced, never created)
- `sbp_easydata_observations`, `sbp_easydata_datasets`, `sbp_easydata_series` —
  disk has data at `/mnt/e/psxdata/sbp_easydata/`, DB tables absent.
- `global_rates` — SOFR/SONIA/EUSTR source module exists, table missing.
- `ftp_rates` — ALM engine writes here, table not created → FTP Monitor page
  renders empty.
- `v_npc_vs_rfr_spread`, `v_npc_carry_trade` — views referenced in
  `npc_rates` repo, not created in DB. Breaks FX Dashboard carry tab.
- `term_reference_rates` — table exists with 0 rows, never populated.

### Short-history / stale (needs backfill)
- `tbill_auctions`: 175 rows, 2024-06 → 2026-04 (SBP EasyData has longer history).
- `gis_auctions`: 66 rows, stale since **2023-12-21**.
- `sbp_fx_interbank`: 127 rows, 2-month window.
- `forex_kerb`: 621 rows, 2-month window.

### Remove candidates
- `psx_market_stats`: 0 rows, schema exists but never populated.

### Not duplicates (keep all)
- `sovereign_curve` (67K rows, 21-year) is the UNIFIED curve consolidating
  PKRV + PKISRV + MTB + PIB + KIBOR. Individual source tables remain for
  single-source specificity. For wide-history reads, prefer `sovereign_curve`.
- `sbp_fx_interbank` vs `forex_kerb` — different markets (interbank vs kerb).
- `sbp_fx_daily_avg` (72K rows) — daily average rates (wider history); used
  by Macro Regime HMM as primary, falls back to `sbp_fx_interbank`.
- `sbp_fx_open_market` (529 rows) — lightly used by FX Interbank page; distinct
  from both `sbp_fx_interbank` and `forex_kerb`.
- `sbp_benchmark_snapshot` (11K rows) — snapshot table used as cascade
  fallback in Rates Overview and Debt Terminal.

## Summary Tables Roadmap (Market Sync v1 Step 3)

Derived tables to build after base-table ingest. Priority reflects expected
read-volume reduction on view pages.

### High priority
| Table | Derives from | Replaces / accelerates |
|---|---|---|
| `rates_daily_snapshot(date, policy, kibor_3m/6m/12m, konia, tbill_3m/6m/12m, pkrv_1y/10y, sofr_on)` | policy + KIBOR + KONIA + TBill + PKRV + global_reference_rates | Every dashboard page's rates strip (6+ single-row reads → 1 lookup) |
| `yield_curve_daily(date, source, tenor, days, yield_pct)` | latest-per-(date,source,tenor) from `sovereign_curve` | Curve Analytics, Debt Terminal, ALM Dashboard |
| `curve_metrics_daily(date, source, level_2y, slope_2y_10y, butterfly, steepness_bps)` | `pkrv_daily`, `pkisrv_daily` | Treasury Dashboard steepness; Macro Regime features |
| `fx_daily_snapshot(date, currency, interbank_mid, kerb_mid, open_market_mid, spread_bps)` | 4 FX tables combined | FX Dashboard 6 ccy × 3 sources → 1 row per ccy |

### Medium priority
| Table | Derives from | Replaces |
|---|---|---|
| `fx_spread_daily(date, currency, interbank_kerb_bps, interbank_open_bps)` | FX tables | Interbank vs Open heatmap |
| `kibor_term_structure_daily(date, 1w, 1m, 3m, 6m, 12m)` | `kibor_daily` pivoted | Treasury KIBOR table, carry trade |
| `turnover_market_type_rollup(date, market_type, symbols, volume, turnover)` | `post_close_turnover` GROUP BY | Post Close stats, market summary |
| `auction_latest_per_tenor(tenor, latest_auction_date, cutoff_yield, waa, amount_accepted)` | `tbill_auctions` + `pib_auctions` | Rates Overview, Treasury auctions tab |

### Low priority
| Table | Derives from | Replaces |
|---|---|---|
| `npc_carry_spread_daily(date, currency, tenor, npc_rate, rfr_rate, kibor_offer, carry_bps)` | `npc_rates` + `global_reference_rates` + `kibor_daily` | Rebuilds the missing `v_npc_*` views |

**Build discipline** — every summary-table writer MUST:
- Return `None`/skip for rows where any input is missing (no silent zero).
- Stamp `date` and `ingested_at` so staleness is visible.
- Be idempotent — re-running for the same date replaces prior rows.
- Be callable from Market Sync v1 Step 3 per-date and over a date range.

## Market Sync Architecture (v1)

## DuckDB Status
- `pakfindata.duckdb` persistent file is **0 MB (empty)**
- "DuckDB" usage in pages is actually **in-memory DuckDB** (via `analytics_con()`) with Parquet views
- tick_analytics.py uses SQLite (`tick_bars.db`) despite `_duck_con()` naming
- JSONL tick files (8.5 GB) read on-demand via `duckdb.read_json_auto()` in tick_replay.py
- See `memory/project_duckdb_fix_plan.md` for 6-phase migration plan

## Testing
```bash
pytest tests/ -v
```
