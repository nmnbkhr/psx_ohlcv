# pakfindata — Data Store Reference

Complete inventory of databases, tables, file storage, sync modules, and external data sources.

---

## 1. Databases

### 1.1 SQLite — `psx.sqlite` (Main)

**Path:** `/mnt/e/psxdata/psx.sqlite` (override: `PSX_DB_PATH` env var)
**Mode:** WAL, 64 MB cache, 256 MB mmap, 30 s busy timeout

| # | Table | Primary Key | Description |
|---|-------|-------------|-------------|
| 1 | `symbols` | symbol | Stock symbols, names, sectors |
| 2 | `eod_ohlcv` | symbol, date | End-of-day OHLCV |
| 3 | `intraday_bars` | symbol, ts | Intraday OHLCV bars |
| 4 | `intraday_sync_state` | symbol | Per-symbol intraday sync tracking |
| 5 | `tick_logs` | symbol, market, timestamp, price | Full tick logs from JSONL |
| 6 | `psx_indices` | index_code, index_date | KSE-100, KSE-30, KMI-30 etc. |
| 7 | `psx_market_stats` | stat_date, board_type | Daily market-wide statistics |
| 8 | `psx_eod` | — | PSX EOD summary |
| 9 | `sectors` | — | Sector master list |
| 10 | `symbol_status_definitions` | — | Status labels (XD, XB, XR) |
| 11 | `symbol_status_history` | symbol, status, start_date | SCD2 status transitions |
| 12 | `company_listing_status` | — | SUSPENDED, WINDING-UP, DEFAULTER |
| 13 | `company_profile` | symbol | Company details |
| 14 | `company_key_people` | symbol, role, name | Directors, managers |
| 15 | `company_snapshots` | symbol, snapshot_date | Full JSON document storage |
| 16 | `company_quote_snapshots` | symbol, ts | Time-series quote snapshots |
| 17 | `company_signal_snapshots` | symbol, ts, signal_key | Derived metrics per snapshot |
| 18 | `company_fundamentals` | symbol | Latest fundamentals |
| 19 | `company_fundamentals_history` | symbol, date | Historical fundamentals |
| 20 | `company_financials` | symbol, period_end, period_type | Annual/quarterly financials |
| 21 | `company_ratios` | symbol, period_end, period_type | Financial ratios |
| 22 | `company_payouts` | symbol, ex_date, payout_type | Dividend/bonus history |
| 23 | `corporate_announcements` | symbol, announcement_date, title_hash | Corporate announcements |
| 24 | `company_announcements` | symbol, announcement_date, announcement_time, title | Raw scraped announcements |
| 25 | `financial_announcements` | symbol, announcement_date, fiscal_period | Results announcements |
| 26 | `corporate_events` | symbol, event_date, event_type | AGM, EOGM, board meetings |
| 27 | `dividend_payouts` | symbol, announcement_date, dividend_number | Dividend records |
| 28 | `announcements_sync_status` | — | Sync progress tracking |
| 29 | `trading_sessions` | symbol, session_date, market_type, contract_month | Intraday/EOD with microstructure |
| 30 | `equity_structure` | symbol, as_of_date | Share capital, float |
| 31 | `instruments` | instrument_id | Master all tradeable instruments (EQUITY/ETF/REIT/INDEX) |
| 32 | `instrument_membership` | parent_id, child_id, effective_date | Index/ETF constituents |
| 33 | `ohlcv_instruments` | instrument_id, date | Non-equity OHLCV |
| 34 | `instrument_rankings` | as_of_date, instrument_id | Performance rankings |
| 35 | `instruments_sync_runs` | — | Instrument sync tracking |
| 36 | `instrument_registry` | registry_id | Unified lookup across asset classes |
| 37 | `fx_pairs` | pair | Currency pairs |
| 38 | `fx_ohlcv` | pair, date | Daily FX rates |
| 39 | `fx_adjusted_metrics` | as_of_date, symbol, fx_pair, period | FX-adjusted returns |
| 40 | `fx_sync_runs` | — | FX sync tracking |
| 41 | `mutual_funds` | fund_id | Fund master |
| 42 | `mutual_fund_nav` | fund_id, date | NAV time-series |
| 43 | `mutual_fund_sync_runs` | — | MUFAP sync tracking |
| 44 | `bonds_master` | bond_id | Bond master |
| 45 | `bond_quotes` | bond_id, date | Bond prices/yields |
| 46 | `yield_curve_points` | curve_date, tenor_months, bond_type | Interpolated yield curves |
| 47 | `bond_analytics_snapshots` | bond_id, as_of_date | Bond analytics |
| 48 | `bond_sync_runs` | — | Bond sync tracking |
| 49 | `sukuk_master` | instrument_id | Sukuk instruments |
| 50 | `sukuk_quotes` | instrument_id, quote_date | Sukuk quotes |
| 51 | `sukuk_yield_curve` | curve_name, curve_date, tenor_days | Sukuk yield curves |
| 52 | `sukuk_analytics_snapshots` | instrument_id, as_of_date | Sukuk analytics |
| 53 | `sbp_primary_market_docs` | doc_id | SBP document archive |
| 54 | `sukuk_sync_runs` | — | Sukuk sync tracking |
| 55 | `fi_instruments` | instrument_id | Fixed income instruments |
| 56 | `fi_quotes` | instrument_id, quote_date | Fixed income quotes |
| 57 | `fi_curves` | curve_name, curve_date, tenor_days | Fixed income curves |
| 58 | `fi_analytics` | instrument_id, as_of_date | Fixed income analytics |
| 59 | `sbp_pma_docs` | doc_id | SBP Primary Market Activities docs |
| 60 | `fi_events` | event_id | Structured facts from auctions |
| 61 | `fi_sync_runs` | — | FI sync tracking |
| 62 | `sbp_policy_rates` | rate_date | SBP policy rate |
| 63 | `kibor_rates` | rate_date, tenor_months | KIBOR rates |
| 64 | `sbp_lending_deposit_rates` | rate_date, bank_type | Bank lending/deposit rates |
| 65 | `fund_risk_metrics` | fund_id | Risk metrics |
| 66 | `fund_signals` | fund_name, signal_date, signal_type | Trading signals |
| 67 | `fund_calendar_returns` | fund_id, year | Calendar year returns |
| 68 | `compliance_screening` | entity_name, entity_type, screened_at | AML screening |
| 69 | `alm_products` | product_code | ALM product catalog |
| 70 | `alm_positions` | as_of_date, product_code, bucket | Balance sheet positions |
| 71 | `alm_ftp_rates` | as_of_date, product_code | FTP rates |
| 72 | `alm_sensitivity` | as_of_date, scenario | NII/EVE scenarios |
| 73 | `alm_liquidity_ladder` | as_of_date, bucket | Maturity ladder |
| 74 | `alm_ftp_pnl` | month, product_code | Monthly FTP P&L |
| 75 | `commodity_symbols` | symbol | Commodity definitions |
| 76 | `commodity_eod` | symbol, date, source | Daily commodity OHLCV |
| 77 | `commodity_monthly` | symbol, date, source | Monthly reference prices |
| 78 | `commodity_pkr` | symbol, date | PKR-denominated prices |
| 79 | `commodity_fx_rates` | pair, date | Daily FX rates |
| 80 | `khistocks_prices` | symbol, date, feed | Local market data |
| 81 | `pmex_market_watch` | contract, snapshot_date | PMEX portal data |
| 82 | `commodity_sync_runs` | — | Commodity sync tracking |
| 83 | `user_interactions` | session_id | Page visits, searches, clicks |
| 84 | `scrape_jobs` | job_id | Background scraping jobs |
| 85 | `job_notifications` | — | Job completion notifications |
| 86 | `sync_runs` | run_id | EOD sync tracking |
| 87 | `sync_failures` | run_id, symbol | Failed syncs |
| 88 | `data_freshness` | domain | Per-domain freshness |
| 89 | `schema_version` | — | Migration tracking |

**Additional tables from repositories:**

| Table | Source Repository | Description |
|-------|-------------------|-------------|
| `etf_master` | etf.py | ETF definitions |
| `etf_nav` | etf.py | ETF NAV time-series |
| `intraday_breadth` | breadth.py | Market breadth metrics |
| `tick_data` | tick.py | Individual tick records |
| `tick_ohlcv` | tick.py | Tick OHLCV aggregates |
| `tick_daily_summary` | tick_summary.py | Daily tick summary |
| `post_close_turnover` | post_close.py | Turnover data |
| `sbp_bond_trading_daily` | bond_market.py | SBP bond trading |
| `sbp_bond_trading_summary` | bond_market.py | Trading summary |
| `sbp_benchmark_snapshot` | bond_market.py | Benchmark snapshots |
| `pkrv_daily` | yield_curves.py | PKR conventional yield curve |
| `pkisrv_daily` | yield_curves.py | PKR Islamic Shariah yield curve |
| `pkfrv_daily` | yield_curves.py | PKR FX yield curve |
| `konia_daily` | yield_curves.py | Karachi Overnight Index Average |
| `kibor_daily` | yield_curves.py | KIBOR rate curve |
| `futures_eod` | futures.py | Futures daily data |
| `ipo_listings` | ipo.py | IPO records |
| `sbp_fx_interbank` | fx_extended.py | SBP interbank FX |
| `sbp_fx_open_market` | fx_extended.py | Open market FX |
| `forex_kerb` | fx_extended.py | Kerb/street market FX |
| `npc_rates` | npc_rates.py | NPC rates |
| `global_reference_rates` | global_rates.py | Global benchmarks (SOFR, SONIA, etc.) |
| `term_reference_rates` | global_rates.py | Term reference rates |
| `nccpl_fipi` | nccpl_flows.py | Foreign Individual Portfolio Investor flows |
| `nccpl_lipi` | nccpl_flows.py | Local Institution Portfolio Investor flows |
| `nccpl_fipi_sector` | nccpl_flows.py | FIPI by sector |
| `nccpl_flows_derived` | nccpl_flows.py | Derived flow metrics |
| `fund_performance` | fixed_income.py | Fund performance history |
| `fund_performance_latest` | fixed_income.py | Latest performance |
| `fund_nav_latest` | fixed_income.py | Latest NAV |
| `pdf_parse_log` | financials.py | PDF parsing audit log |
| `company_website_scan` | website_scan.py | Website scan results |

---

### 1.2 SQLite — `tick_bars.db` (Tick Data)

**Path:** `/mnt/e/psxdata/tick_bars.db`

| Table | Description |
|-------|-------------|
| `ohlcv_5s` | 5-second OHLCV bars (symbol, ts) |
| `raw_ticks` | Raw equity ticks (timestamp-based) |
| `index_ohlcv_5s` | Index 5-second OHLCV |
| `index_raw_ticks` | Index raw ticks |

**Written by:** `tick_service.py` (WebSocket → 5-sec bar aggregation)
**Read by:** tick_analytics UI, tick_summary, DuckDB analytics

---

### 1.3 SQLite — `commod.db` (Commodities)

**Path:** `/mnt/e/psxdata/commod/commod.db`

| Table | Description |
|-------|-------------|
| `pmex_ohlc` | PMEX daily OHLC (trading_date, symbol) |
| `pmex_margins` | PMEX margin requirements (report_date, contract_code) |
| `pmex_intraday_snapshots` | PMEX intraday snapshots (contract, snapshot_ts) |

---

### 1.4 DuckDB — In-Memory Analytics Engine

**No `.duckdb` file** — entirely in-memory, singleton cached connection.
Creates views over Parquet globs + SQLite ATTACH READ_ONLY.
**Fallback chain:** Parquet first → SQLite → tick_bars.db

**Connection:** `src/pakfindata/db/connections.py` → `analytics_con()`, `duck()`, `duck_fetchone()`

**Views created from Parquet + SQLite:**
- `eod_ohlcv`, `intraday_bars`, `ohlcv_5s`, `index_ohlcv_5s`
- `raw_ticks`, `index_raw_ticks`, `tick_logs`, `psx_eod`

**Consumers (28 files):**
- All engine strategies (ml_features, pairs_trading, cvd, vpin, ofi, oi, hawkes, rl_execution, vwap, sector_rotation, macro_regime_hmm, gnn_stock_graph, orderbook_sim, etc.)
- Services (portfolio_simulator, fusion_service, tick_service)
- UI pages (tick_analytics, intraday, ml_predictions, signal_dashboard, strategy pages)
- Repositories (tick_summary, intraday)

---

## 2. File-Based Storage

### 2.1 Parquet — `/mnt/e/psxdata/parquet/`

Managed by `src/pakfindata/db/parquet_store.py`. Daily-partitioned files, read via DuckDB `read_parquet()` with `union_by_name=true`.

| Directory | Source DB | Source Table | Date Column | Partition Type |
|-----------|-----------|-------------|-------------|----------------|
| `eod_ohlcv/` | psx.sqlite | eod_ohlcv | date | YYYY-MM-DD |
| `intraday_bars/` | psx.sqlite | intraday_bars | ts | timestamp prefix |
| `tick_logs/` | psx.sqlite | tick_logs | source_file | date from filename |
| `psx_eod/` | psx.sqlite | psx_eod | — | single all.parquet |
| `ohlcv_5s/` | tick_bars.db | ohlcv_5s | ts | timestamp prefix |
| `raw_ticks/` | tick_bars.db | raw_ticks | ts | unix |
| `index_ohlcv_5s/` | tick_bars.db | index_ohlcv_5s | ts | timestamp prefix |
| `index_raw_ticks/` | tick_bars.db | index_raw_ticks | ts | unix |

### 2.2 Commodity Parquet — `/mnt/e/psxdata/commod/`

| Directory | Description |
|-----------|-------------|
| `pmex_daily/ohlc_parquet/` | PMEX daily OHLC parquets |
| `pmex_daily/margins_parquet/` | PMEX margin parquets |
| `pmex_daily/intraday_rollup/` | PMEX intraday rollup parquets |

### 2.3 JSONL Tick Logs

| Path | Description |
|------|-------------|
| `/mnt/e/psxdata/tick_logs_cloud/` | Cloud/remote ticks (primary) |
| `/mnt/e/psxdata/tick_logs/` | Local ticks (fallback) |

**Format:** `ticks_YYYY-MM-DD.jsonl` or `YYYY-MM-DD.jsonl`

### 2.4 JSON Files

| Path | Description |
|------|-------------|
| `/mnt/e/psxdata/intelligence_alerts.json` | Alert data |
| `/mnt/e/psxdata/intraday_sync_progress.json` | Intraday sync state |
| `/mnt/e/psxdata/intraday/{date}/{SYMBOL}.json` | Per-symbol intraday dumps |

### 2.5 CSV Directories

| Path | Description |
|------|-------------|
| `/mnt/e/psxdata/csv/` | Generic CSV directory |
| `/mnt/e/psxdata/market_summary/csv/` | Market summary CSVs |
| `/mnt/e/psxdata/closing_rates/csv/` | PDF-converted closing rates |

### 2.6 Other

| Path | Description |
|------|-------------|
| `/mnt/e/psxdata/mufapnav/nav_history/` | Mutual fund NAV files |
| `/mnt/e/psxdata/sbp_easydata/series/` | SBP EasyData cached series |
| `/mnt/e/psxdata/sbp_easydata/datasets/` | SBP EasyData cached datasets |
| `/mnt/e/psxdata/logs/` | Sync logs |
| `/mnt/e/psxdata/backups/` | SQLite backups |
| `/mnt/e/psxdata/downloads/daily/{date}/off_market/` | Block trade CSVs |
| `/mnt/e/psxdata/commod/pmex_intraday/` | PMEX intraday files |
| `/mnt/e/psxdata/commod/pmex_margins/` | Downloaded margins XLS |

---

## 3. Sync Modules

### 3.1 CLI Entry Point

`pfsync` → `pakfindata.cli:main` (defined in pyproject.toml)

### 3.2 Main Sync Orchestrators

| Module | External Source | Target Tables |
|--------|----------------|---------------|
| `sync.py` | PSX DPS `/timeseries/eod/{symbol}` | eod_ohlcv, symbols, sync_runs, indices |
| `sync_async.py` | PSX DPS (concurrent HTTP) | Same as sync.py |
| `sync_timeseries.py` | PSX DPS `/timeseries/int/{symbol}` | intraday_bars |
| `sync_instruments.py` | PSX DPS `/timeseries/eod/{symbol}` | ohlcv_instruments, instruments |
| `sync_fx.py` | Dynamic FX pairs | fx_ohlcv, fx_pairs |
| `sync_bonds.py` | CSV files + manual seeding | bonds_master, bond_quotes |
| `sync_sukuk.py` | CSV + SBP primary market docs | sukuk_master, sukuk_quotes, sukuk_yield_curve |
| `sync_fixed_income.py` | CSV + SBP PMA | fi_instruments, fi_quotes, fi_curves |
| `sync_psx_debt.py` | PSX DPS `/debt-market`, `/debt/{symbol}` | fi_instruments, fi_quotes |
| `sync_mufap.py` | MUFAP website (POST JSON) | mutual_funds, mutual_fund_nav |
| `commodities/sync.py` | yfinance + FRED + World Bank | commodity_symbols, commodity_eod, commodity_fx, commodity_monthly, commodity_pkr |

### 3.3 Background Services

| Service | PID/Status Files | Description |
|---------|------------------|-------------|
| `services/eod_sync_service.py` | eod_sync.pid, eod_sync_status.json, eod_sync.log | Runs sync_all() in background |
| `services/fi_sync_service.py` | fi_sync.pid, fi_sync_status.json, fi_sync.log | SBP PMA + MSM sync |
| `services/tick_service.py` | — | WebSocket → tick_bars.db 5-sec bars |
| `worker_async.py` | scrape_jobs table | Background job executor (sync_eod, deep_scrape, sync_intraday) |

### 3.4 Shell Scripts (`scripts/`)

| Script | Schedule | Description |
|--------|----------|-------------|
| `sync_all.sh` | Manual | Master orchestrator — runs all scripts sequentially |
| `daily_sync.sh` | `30 13 * * 1-5` (1:30 PM PKT weekdays) | EOD sync + Friday maintenance |
| `sync_rates.sh` | `30 12 * * 1-5` (12:30 PM) | KIBOR, KONIA, yield curves |
| `sync_fx.sh` | `0 13 * * 1-5` (1:00 PM) | SBP + forex.pk FX rates |
| `sync_treasury.sh` | `0 16 * * 5` (4:00 PM Fridays) | T-Bill, PIB, GIS auctions |
| `sync_etf.sh` | `0 14 * * 1-5` (2:00 PM) | ETF data |
| `fetch_market_data.sh` | As needed | Market summaries and snapshots |
| `health_check.sh` | Periodic | Sync service health |
| `backup.sh` | Periodic | SQLite backup |
| `restore.sh` | Manual | SQLite restore |

---

## 4. External Data Sources

### 4.1 Pakistan — Markets

| Source | Domain | Auth | Data |
|--------|--------|------|------|
| **PSX DPS** | `dps.psx.com.pk` | None | EOD/intraday OHLCV, indices, debt, ETFs, announcements, company data |
| **PSX Terminal** | `psxterminal.com/api` | None | Historical OHLCV, market data |
| **PSX WebSocket** | `wss://psxterminal.com/` | None | Real-time stock ticks |
| **PSX Financials** | `financials.psx.com.pk` | None | Financial report PDFs |
| **PMEX (OHLC)** | `mportal.pmex.com.pk` | None | Commodity OHLC (gold, silver, crude) |
| **PMEX (Watch)** | `dportal.pmex.com.pk` | None | Live commodity prices |
| **PMEX (Margins)** | `pmex.com.pk` | None (Cloudflare) | Daily margin requirement XLSX |
| **MUFAP** | `mufap.com.pk` | None | Mutual fund NAV, performance |
| **NCCPL** | `nccpl.com.pk` | None | FIPI/LIPI flows, sector flows |
| **Forex.PK** | `forex.pk` | None | Open market FX rates |
| **KHI Stocks** | `khistocks.com` | None | FIPI/LIPI sector mirror data |

### 4.2 Pakistan — Central Bank (SBP)

| Source | Domain/Endpoint | Auth | Data |
|--------|-----------------|------|------|
| **SBP EasyData** | `easydata.sbp.org.pk/api/v1` | API Key (hardcoded, expires 90 days) | 18,000+ macro variables — KIBOR, CPI, money supply, remittances |
| **SBP PMA** | `sbp.org.pk/dfmd/pma.asp` | None | Primary market auctions (MTB/PIB/GIS) |
| **SBP MSM** | `sbp.org.pk/dfmd/msm.asp` | None | Policy rates, KIBOR, MTB/PIB yields |
| **SBP KIBOR** | `sbp.org.pk/ecodata/kibor/` | None | Historical KIBOR PDFs |
| **SBP KONIA** | `sbp.org.pk/ecodata/` | None | Overnight repo rate PDFs |
| **SBP FX** | `sbp.org.pk/dfmd/pma.asp` | None | Interbank FX rates (USD/PKR) |
| **SBP PIB Archive** | `sbp.org.pk/ecodata/Pakinvestbonds.pdf` | None | Historical PIB auction data (42-page PDF) |
| **SBP Lending Rates** | `sbp.org.pk/ecodata/Lendingdepositrates_Arch.xls` | None | Bank lending/deposit rates |
| **SBP Bond Market** | `sbp.org.pk/ecodata/` | None | Bond trading data |
| **SBP NPC** | `sbp.org.pk/NPC-/page-npc.html` | None | NPC rates |
| **SBP GSP** | `gsp.sbp.org.pk` | None | Government securities |

### 4.3 International — Rates

| Source | Domain | Auth | Data |
|--------|--------|------|------|
| **NY Fed** | `markets.newyorkfed.org/api/rates` | None | SOFR, SOFR averages (30D/90D/180D) |
| **Bank of England** | `bankofengland.co.uk` | None | SONIA |
| **ECB** | `data-api.ecb.europa.eu` | None | EUSTR |
| **Bank of Japan** | `stat-search.boj.or.jp` | None | TONA |

### 4.4 International — Commodities & Markets

| Source | Domain | Auth | Data |
|--------|--------|------|------|
| **FRED** | `fred.stlouisfed.org` | `FRED_API_KEY` env var (free) | Monthly commodity prices, Pakistan CPI, 20+ series |
| **yfinance** | Yahoo Finance (Python lib) | None | Daily OHLCV for commodity futures, FX pairs, ETFs |
| **World Bank** | `thedocs.worldbank.org` | None | Pink Sheet — 70+ commodities monthly (1960–present) |
| **Investing.com** | `investing.com/commodities/` | None (scraping) | Crude palm oil, coal, rebar, nickel |
| **GoldPriceZ** | `goldpricez.com/api` | `GOLDPRICEZ_API_KEY` env var | Gold prices in PKR (tola) |
| **ExchangeRate-API** | `open.er-api.com` | None (free tier) | FX rates fallback |

### 4.5 LLM Providers

| Source | Auth | Usage |
|--------|------|-------|
| **OpenAI** | `OPENAI_API_KEY` env var | Market commentary, AI insights, chat |
| **Anthropic** | `ANTHROPIC_API_KEY` env var | Alternative LLM for analysis |
| **Ollama** | None (localhost:11434) | Local LLM for announcement classification |

### 4.6 Local Services

| Service | URL | Description |
|---------|-----|-------------|
| **WebSocket Relay** | `ws://localhost:8765/ws/` | Real-time tick broadcast (ticks, indices, firehose) |
| **REST API** | `http://localhost:8000` | Internal REST API for all data types |
| **FX Trading Module** | `localhost:8100` | FX rate syncing microservice |

---

## 5. Configuration

### 5.1 Path Constants

```
DATA_ROOT           = /mnt/e/psxdata                          (PSX_DATA_ROOT env)
DEFAULT_DB_PATH     = /mnt/e/psxdata/psx.sqlite               (PSX_DB_PATH env)
TICK_DB_PATH        = /mnt/e/psxdata/tick_bars.db
PARQUET_ROOT        = /mnt/e/psxdata/parquet/
JSONL_DIR           = /mnt/e/psxdata/tick_logs_cloud
JSONL_DIR_LOCAL     = /mnt/e/psxdata/tick_logs
LOGS_DIR            = /mnt/e/psxdata/logs
BACKUP_DIR          = /mnt/e/psxdata/backups
CSV_DIR             = /mnt/e/psxdata/csv
COMMOD_DATA_ROOT    = /mnt/e/psxdata/commod
COMMOD_DB_PATH      = /mnt/e/psxdata/commod/commod.db
PMEX_DAILY_ROOT     = /mnt/e/psxdata/commod/pmex_daily
MUFAP_NAV_DIR       = /mnt/e/psxdata/mufapnav/nav_history
SBP_EASYDATA_DIR    = /mnt/e/psxdata/sbp_easydata
```

### 5.2 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PSX_DATA_ROOT` | No | Override data root (default: `/mnt/e/psxdata`) |
| `PSX_DB_PATH` | No | Override SQLite path |
| `FRED_API_KEY` | Optional | FRED API access (free) |
| `GOLDPRICEZ_API_KEY` | Optional | Gold prices in PKR |
| `OPENAI_API_KEY` | Optional | AI features |
| `ANTHROPIC_API_KEY` | Optional | Alternative AI |
| `OPENAI_ORG_ID` | Optional | OpenAI organization |

### 5.3 SQLite PRAGMAs (connection.py)

```sql
PRAGMA journal_mode   = WAL
PRAGMA synchronous    = NORMAL
PRAGMA cache_size     = -64000     -- 64 MB
PRAGMA busy_timeout   = 30000     -- 30 s
PRAGMA temp_store     = MEMORY
PRAGMA mmap_size      = 268435456 -- 256 MB
PRAGMA foreign_keys   = ON
```

---

## 6. Data Flow

```
                        ┌─────────────────────────────────┐
                        │       External Sources          │
                        │  PSX DPS · SBP · MUFAP · NCCPL │
                        │  FRED · yfinance · World Bank   │
                        └──────────────┬──────────────────┘
                                       │ HTTP / WebSocket / scrape
                                       ▼
                        ┌──────────────────────────────────┐
                        │     sources/ + sync modules      │
                        │  (fetchers, parsers, upserts)    │
                        └──────────────┬───────────────────┘
                                       │ sqlite3 writes
                         ┌─────────────┼─────────────┐
                         ▼             ▼             ▼
                   ┌──────────┐ ┌───────────┐ ┌──────────┐
                   │psx.sqlite│ │tick_bars.db│ │commod.db │
                   │ ~89+ tbl │ │  4 tables  │ │ 3 tables │
                   └─────┬────┘ └─────┬──────┘ └──────────┘
                         │            │
                         ▼            ▼
                   ┌──────────────────────────────┐
                   │   parquet_store.py (nightly)  │
                   │   SQLite → daily .parquet     │
                   └──────────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │  /mnt/e/psxdata/parquet/     │
                   │  8 table directories          │
                   └──────────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │  DuckDB (in-memory)          │
                   │  read_parquet() + ATTACH     │
                   │  analytics_con() singleton   │
                   └──────────────┬───────────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │  Engine   │ │ Services │ │    UI    │
              │strategies │ │portfolio │ │Streamlit │
              │ 15+ algo  │ │ fusion   │ │ pages    │
              └──────────┘ └──────────┘ └──────────┘
```
