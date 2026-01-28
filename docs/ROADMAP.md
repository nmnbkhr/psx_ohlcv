# PSX OHLCV Explorer - Multi-Phase Roadmap

This document outlines the phased approach to expanding the PSX OHLCV Explorer beyond equities to cover the full spectrum of tradeable instruments on the Pakistan Stock Exchange.

---

## Phase 1: Indexes + ETFs + REITs (Read-Only Analytics Universe)

**Status**: IMPLEMENTED (Validation & Data Coverage Ongoing)

### Goal
Extend the instrument universe beyond equities using the same OHLCV pipeline. Introduce instrument classification, metadata, and analytics for Index, ETF, and REIT instruments while keeping the system strictly additive and non-breaking.

This phase upgrades the application from an equity-only tool to a broader **investment analytics platform** without introducing execution, leverage, or complex risk modeling.

### Scope

- **Data Model**
  - New instrument master table (`instruments`) supporting EQUITY, INDEX, ETF, and REIT types
  - Optional instrument membership mapping (`instrument_membership`) for index constituents
  - New analytics output table for rankings (`instrument_rankings`)
  - Existing equity OHLCV tables remain unchanged

- **Ingestion**
  - Reuse existing DPS EOD OHLCV pipeline (`/timeseries/eod/{symbol}`) where supported
  - Manual configuration for Index/ETF/REIT instrument metadata via controlled config file
  - Instruments without DPS OHLCV are retained as metadata-only entries

- **Analytics**
  - Return calculations (1D, 1W, 1M, 3M, 1Y where data exists)
  - Volatility metrics
  - Relative strength vs benchmark index (KSE-100 when available)
  - Ranking tables by instrument type (ETF / REIT / INDEX)

- **CLI**
  - `psxsync universe` command group for seeding and managing instrument universe
  - `psxsync instruments` command group for EOD syncing and ranking computation

- **UI**
  - Two new additive pages only:
    - 📦 Instruments (browse and filter ETF/REIT/INDEX universe)
    - 🏆 Rankings (performance comparison and relative strength)
  - All existing UI pages remain unchanged and fully functional

### Out of Scope

- Intraday or real-time analytics for non-equity instruments
- Fundamental or financial statement analysis for ETFs/REITs
- Automated ticker discovery from external internet sources
- FX, bonds, Sukuk, derivatives, or options
- Any refactoring, renaming, or modification of existing UI pages, charts, queries, or database tables

### Success Metrics

1. ✅ `psxsync universe seed-phase1` successfully populates ETF/REIT/INDEX instruments
2. ✅ `psxsync instruments sync-eod --types ETF,REIT,INDEX` completes without errors
3. ✅ `psxsync instruments rankings --as-of <date>` computes and stores ranking data
4. ✅ Streamlit UI displays the two new pages without affecting existing pages
5. ✅ Instruments lacking DPS OHLCV are clearly labeled and excluded from rankings
6. ✅ All existing tests pass; Phase 1–specific tests pass (14/14)
7. ✅ `ruff check .` passes with no fatal errors

### Risks and Mitigation

- **DPS OHLCV coverage variability**
  Some Index, ETF, or REIT symbols may not return OHLCV data via the DPS EOD endpoint.

- **Mitigation**
  - Instruments without DPS data are marked as inactive for analytics
  - Such instruments remain visible in the UI with a "No data available" indicator
  - Rankings and performance metrics exclude instruments without valid OHLCV

### Verification Checklist

- [x] Universe seeded via CLI (8 instruments: 4 INDEX, 2 ETF, 2 REIT)
- [x] EOD sync executed for ETF/REIT/INDEX instruments (5 OK, 3 no data)
- [x] Rankings generated and stored (5 instruments ranked)
- [x] New UI pages visible and populated
- [x] Existing dashboards, analytics, and history pages unchanged

---

## Phase 2: FX Analytics (Read-Only)

**Status**: IMPLEMENTED

### Goal
Add foreign exchange rates (USD/PKR, etc.) from a reliable source to enable macro overlays and currency-adjusted performance analysis.

### Scope

- **Data Model**
  - New `fx_pairs` table for FX pair metadata (USD/PKR, EUR/PKR, GBP/PKR, SAR/PKR, AED/PKR)
  - New `fx_ohlcv` table for FX rate OHLCV data
  - New `fx_adjusted_metrics` table for storing FX-adjusted equity returns
  - New `fx_sync_runs` table for sync audit trail

- **Ingestion**
  - FX data source module (`sources/fx.py`) with support for:
    - SBP API (State Bank of Pakistan)
    - Open exchange rate APIs
    - Sample data fallback for testing/development
  - Sync module (`sync_fx.py`) for incremental FX data updates

- **Analytics**
  - FX return calculations (1W, 1M, 3M periods)
  - FX volatility metrics (annualized)
  - FX trend indicators (50-day MA, direction, strength)
  - FX-adjusted equity returns (equity_return - fx_return)
  - FX impact summary for equities

- **CLI**
  - `psxsync fx seed` - Seed default FX pairs
  - `psxsync fx sync` - Sync FX OHLCV data
  - `psxsync fx show --pair USD/PKR` - Display FX analytics
  - `psxsync fx compute-adjusted` - Compute FX-adjusted equity metrics
  - `psxsync fx status` - Show FX sync status and data summary

- **UI**
  - 🌍 FX Overview page - FX rates, trends, and charts
  - 📊 FX Impact page - FX-adjusted equity performance

### Out of Scope
- Real-time FX streaming
- FX trading signals or forecasts
- Multiple exotic currency pairs (focus on PKR pairs)
- Intraday FX data

### Success Metrics
1. ✅ `psxsync fx seed` successfully populates default FX pairs
2. ✅ `psxsync fx sync` fetches and stores FX OHLCV data
3. ✅ `psxsync fx show --pair USD/PKR` displays FX analytics
4. ✅ `psxsync fx compute-adjusted` computes and stores metrics
5. ✅ Streamlit UI displays FX Overview and FX Impact pages
6. ✅ All existing tests pass; Phase 2–specific tests pass
7. ✅ `ruff check .` passes with no fatal errors

### Risks
- **Data source reliability**: Free FX APIs may have rate limits or outages
- **Frequency**: Daily rates may not capture intraday volatility
- **Mitigation**: Support multiple FX sources with sample data fallback

### Verification Checklist

- [x] FX pairs seeded via CLI (5 pairs: USD/PKR, EUR/PKR, GBP/PKR, SAR/PKR, AED/PKR)
- [x] FX sync executed (sample data available when APIs unavailable)
- [x] FX analytics computed (returns, volatility, trend)
- [x] FX-adjusted metrics computed for equities
- [x] New UI pages visible and populated
- [x] Existing dashboards, analytics, and history pages unchanged

**Verification completed: 2026-01-28 12:50 PKT**

---

## Phase 2.5: Mutual Fund Analytics (MUFAP Integration)

**Status**: IMPLEMENTED

### Goal
Add mutual fund data integration from MUFAP (Mutual Funds Association of Pakistan) to enable comprehensive fund analysis and performance comparison.

### Scope

- **Data Model**
  - New `mutual_funds` table for fund master data (AMC, category, Shariah compliance)
  - New `mutual_fund_nav` table for daily NAV time-series
  - New `mutual_fund_sync_runs` table for sync audit trail
  - Indexes for efficient queries by category, AMC, and fund type

- **Ingestion**
  - MUFAP data source module (`sources/mufap.py`) with sample data fallback
  - 20 default funds across major AMCs (ABL, Alfalah, MCB, NIT, HBL, UBL, Faysal)
  - Categories: Equity, Money Market, Income, Balanced, Islamic variants, VPS

- **Analytics**
  - NAV return calculations (1W, 1M, 3M, 6M, 1Y)
  - Volatility metrics (annualized)
  - Sharpe ratio calculation (vs KIBOR benchmark)
  - Max drawdown analysis
  - Category performance rankings

- **CLI**
  - `psxsync mufap seed` - Seed fund master data
  - `psxsync mufap sync` - Sync NAV data (incremental by default)
  - `psxsync mufap show --fund <symbol>` - Display fund analytics
  - `psxsync mufap list` - List funds with filters
  - `psxsync mufap rankings --category <cat>` - Category rankings
  - `psxsync mufap status` - Show data summary

- **UI**
  - Mutual Funds page - Fund browser with filters and NAV charts
  - Fund Analytics page - Category rankings and multi-fund comparison

### Out of Scope

- Real-time NAV streaming (daily updates only)
- Investment recommendations or trading signals
- Direct MUFAP API integration (uses sample data for now)
- Fund holdings/portfolio breakdown

### Success Metrics

1. `psxsync mufap seed` populates fund master data (20 funds)
2. `psxsync mufap sync` fetches and stores NAV data
3. `psxsync mufap show --fund ABL-ISF` displays fund analytics
4. `psxsync mufap status` shows data summary
5. Streamlit UI displays Mutual Funds and Fund Analytics pages
6. All existing tests pass; `ruff check .` passes

### Risks and Mitigation

- **Data source reliability**: MUFAP website may require authentication
- **Mitigation**: Sample data generator provides realistic NAV series for development

### Verification Checklist

- [x] Database schema created (mutual_funds, mutual_fund_nav, mutual_fund_sync_runs)
- [x] Data source module created (`sources/mufap.py`)
- [x] Sync module created (`sync_mufap.py`)
- [x] Analytics module created (`analytics_mufap.py`)
- [x] CLI commands added (seed, sync, show, list, rankings, status)
- [x] UI pages added (Mutual Funds, Fund Analytics)
- [x] All modules pass ruff check

**Verification completed: 2026-01-28**

---

## Phase 3: Bonds/Sukuk Analytics

**Status**: IMPLEMENTED

### Goal
Add support for fixed income instruments (PIBs, T-Bills, Corporate Sukuk) with yield and duration calculations.

### Scope

- **Data Model**
  - New `bonds_master` table for bond metadata (issuer, type, coupon, maturity)
  - New `bond_quotes` table for price/yield observations
  - New `yield_curve_points` table for term structure
  - New `bond_analytics_snapshots` table for computed metrics
  - New `bond_sync_runs` table for sync audit trail

- **Ingestion**
  - Manual CSV ingestion for bond master data and quotes
  - Sample data generation for development/testing
  - Support for PIB, T-Bill, Sukuk, TFC, and Corporate bonds

- **Analytics**
  - YTM calculation (Newton-Raphson solver)
  - Macaulay and Modified Duration
  - Convexity calculation
  - Accrued interest computation
  - Yield curve construction and interpolation

- **CLI**
  - `psxsync bonds init` - Initialize tables and seed default bonds
  - `psxsync bonds load` - Load data from CSV files
  - `psxsync bonds compute` - Compute analytics and yield curves
  - `psxsync bonds list` - List bonds with filters
  - `psxsync bonds quote --bond <id>` - Show bond analytics
  - `psxsync bonds curve` - Display yield curve
  - `psxsync bonds status` - Show data summary

- **UI**
  - Bonds Screener page - Bond browser with filters and analytics
  - Yield Curve page - Interactive term structure visualization

### Out of Scope
- Credit risk modeling
- Bond trading execution
- Real-time bond prices (mostly OTC market)
- Automated data sourcing from SBP/SECP

### Success Metrics
1. `psxsync bonds init` successfully populates default bonds
2. `psxsync bonds load --sample` generates sample quote data
3. `psxsync bonds compute --curve` builds yield curve
4. `psxsync bonds quote --bond <id>` displays analytics
5. Streamlit UI displays Bonds Screener and Yield Curve pages
6. All existing tests pass; `ruff check .` passes

### Risks
- **Data availability**: Bond market data in Pakistan is less transparent
- **Complexity**: Fixed income modeling differs significantly from equities
- **Mitigation**: CSV-based manual ingestion with sample data fallback

### Verification Checklist

- [x] Database schema created (bonds_master, bond_quotes, yield_curve_points, bond_analytics_snapshots)
- [x] Data source module created (`sources/bonds_manual.py`)
- [x] Sync module created (`sync_bonds.py`)
- [x] Analytics module created (`analytics_bonds.py`)
- [x] CLI commands added (init, load, compute, list, quote, curve, status)
- [x] UI pages added (Bonds Screener, Yield Curve)
- [x] Sample CSV templates created in `data/bonds/`

---

## Phase 4: Options

**Status**: NOT STARTED

### Goal
Add options data modeling and basic Greeks calculation, only after execution/risk/positions infrastructure is solid.

### Scope
- **Data Model**: Options chain, Greeks storage, open interest
- **Ingestion**: Options data from PSX (if available)
- **Analytics**: Greeks calculation (Delta, Gamma, Theta, Vega, Rho)
- **CLI**: Options chain queries
- **UI**: Options chain visualization, Greeks display

### Out of Scope
- Options trading execution
- Exotic options
- Volatility surface modeling (initially)

### Success Metrics
1. Options chain data available for listed options
2. Greeks calculated and displayed
3. Historical options data tracked

### Risks
- **Market maturity**: PSX options market may have limited liquidity
- **Complexity**: Options pricing requires additional infrastructure
- **Mitigation**: Only implement after Phases 1-3 are stable and well-tested

---

## Architecture Principles

### Additive-Only Changes
All phases follow an additive-only approach:
- New tables, never modify existing schema
- New CLI commands, never change existing command behavior
- New UI pages, never alter existing page functionality
- If integration is needed, create views or wrapper functions

### Code Organization
```
src/psx_ohlcv/
├── instruments.py           # Phase 1: Instrument management
├── sync_instruments.py      # Phase 1: Instrument EOD sync
├── analytics_phase1.py      # Phase 1: Performance analytics
├── analytics_fx.py          # Phase 2: FX analytics
├── sync_fx.py               # Phase 2: FX sync operations
├── analytics_mufap.py       # Phase 2.5: Mutual fund analytics
├── sync_mufap.py            # Phase 2.5: Mutual fund sync
├── analytics_bonds.py       # Phase 3: Bond analytics
├── sync_bonds.py            # Phase 3: Bond sync operations
├── sources/
│   ├── instrument_universe.py  # Phase 1: Universe seeding
│   ├── fx.py                   # Phase 2: FX data source
│   ├── mufap.py                # Phase 2.5: MUFAP data source
│   └── bonds_manual.py         # Phase 3: Bond CSV ingestion
└── options/                 # Phase 4: Options module (future)
```

### Data Configuration
Instrument metadata is stored in config files rather than code:
```
data/
├── universe_phase1.json     # ETF/REIT/INDEX seed data
├── fx_config.json           # Phase 2: FX pairs and source config
├── mufap_config.json        # Phase 2.5: Mutual fund config
└── bonds/                   # Phase 3: Bond data
    ├── bonds_master_template.csv
    └── quotes_template.csv
```

---

## Version History

| Version | Phase | Status | Date |
|---------|-------|--------|------|
| v0.9.0  | 1     | Implemented | 2026-01 |
| v0.10.0 | 2     | Implemented | 2026-01 |
| v0.11.0 | 2.5   | Implemented | 2026-01 |
| v0.12.0 | 3     | Implemented | 2026-01 |
| v1.0.0  | 1+2+2.5+3 | Planned | TBD |
| v2.0.0  | 4     | Planned | TBD |
