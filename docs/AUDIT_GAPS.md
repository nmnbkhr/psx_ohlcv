# PSX OHLCV Feature Audit Report

**Date:** 2026-01-21
**Auditor:** Claude (Automated Audit)

This document summarizes the audit of required features against the current codebase implementation.

---

## Summary

| Category | Implemented | Partial | Missing |
|----------|-------------|---------|---------|
| A) Master Symbol + Sector | 4/4 | 0 | 0 |
| B) Regular Market | 5/5 | 0 | 0 |
| C) Sector Display | 3/3 | 0 | 0 |
| D) Market Summary Downloader | 6/6 | 0 | 0 |
| E) Intraday Module | 5/5 | 0 | 0 |
| F) Company Analytics | 4/4 | 0 | 0 |
| G) Analytics + Dashboard | 5/5 | 0 | 0 |
| H) Candlestick Clarity | 3/3 | 0 | 0 |
| **TOTAL** | **35/35** | **0** | **0** |

**Status: ALL FEATURES IMPLEMENTED** (as of 2026-01-21)

---

## A) Master Symbol + Sector Mapping

### ✅ Implemented

1. **symbols table with sector_name column**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L14-L24)
   - Schema includes: symbol, name, sector, sector_name, outstanding_shares, is_active, source
   - Note: Uses `symbols` table (not `symbols_master`) but serves the same purpose

2. **sectors table for mapping**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L89-L95)
   - Schema: sector_code, sector_name, updated_at, source

3. **CLI: master refresh command**
   - File: [src/pakfindata/cli.py](src/pakfindata/cli.py)
   - Commands: `pfsync master refresh`, `pfsync master refresh --deactivate-missing`

4. **CLI: master list/export commands**
   - Commands: `pfsync master list`, `pfsync master list --active-only`, `pfsync master export --out symbols.csv`

---

## B) Regular Market Table Ingestion

### ✅ Implemented

1. **regular_market_current table**
   - File: [src/pakfindata/sources/regular_market.py](src/pakfindata/sources/regular_market.py#L340-L356)
   - Stores live market-watch data with all required fields

2. **regular_market_snapshots table**
   - File: [src/pakfindata/sources/regular_market.py](src/pakfindata/sources/regular_market.py#L358-L380)
   - Time-series history of market snapshots

3. **Status tags (NC, XD, XR, XB, XA, XI, XW)**
   - File: [src/pakfindata/sources/regular_market.py](src/pakfindata/sources/regular_market.py#L84-L97)
   - Parser extracts status markers from symbol names

4. **Smart-save with row hash**
   - File: [src/pakfindata/sources/regular_market.py](src/pakfindata/sources/regular_market.py#L396-L401)
   - `get_current_row_hash()` function checks for changes before insert

5. **CLI commands**
   - Commands: `pfsync regular-market fetch`, `pfsync regular-market show`, `pfsync regular-market listen`

---

## C) Sector Display Everywhere

### ✅ Implemented

1. **sector_name displayed in UI (not sector_code)**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py#L174-L205)
   - `get_sector_names()` and `add_sector_name_column()` helper functions

2. **Symbols page shows sector_name**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py#L1209-L1224)
   - Column selection uses sector_name only

3. **History page sector dropdown uses sector_name**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py#L1832-L1834)
   - `sector_options = {s["sector_name"]: s["sector_code"] for s in sectors}`

---

## D) Market Summary Historical Downloader

### ✅ Implemented

1. **Download .Z files from DPS**
   - File: [src/pakfindata/sources/market_summary.py](src/pakfindata/sources/market_summary.py#L65-L105)
   - `download_market_summary()` function

2. **Extract using uncompress/gzip**
   - File: [src/pakfindata/sources/market_summary.py](src/pakfindata/sources/market_summary.py#L108-L172)
   - `extract_z_file()` with fallback to gzip

3. **Parse pipe-delimited format**
   - File: [src/pakfindata/sources/market_summary.py](src/pakfindata/sources/market_summary.py#L175-L256)
   - `parse_market_summary()` handles 10 or 13 field variants

4. **CLI commands (day, range, last)**
   - Commands: `pfsync market-summary day --date YYYY-MM-DD`
   - Commands: `pfsync market-summary range --start YYYY-MM-DD --end YYYY-MM-DD`
   - Commands: `pfsync market-summary last --days N`
   - Options: `--force`, `--include-weekends`, `--keep-raw`

5. **downloaded_market_summary_dates tracking table** ✅ (Implemented 2026-01-21)
   - File: [src/pakfindata/sources/market_summary.py](src/pakfindata/sources/market_summary.py#L33-L46)
   - Schema: date (PK), status, csv_path, record_count, error_msg, fetched_at
   - Functions: `init_market_summary_tracking()`, `upsert_download_record()`, `get_failed_dates()`, `get_missing_dates()`

6. **--retry-failed and --retry-missing CLI options** ✅ (Implemented 2026-01-21)
   - Commands: `pfsync market-summary retry-failed`, `pfsync market-summary retry-missing`
   - File: [src/pakfindata/cli.py](src/pakfindata/cli.py#L298-L328)
   - Functions: `retry_failed_dates()`, `retry_missing_dates()` in market_summary.py

---

## E) Intraday Module + UI Page

### ✅ Implemented

1. **intraday_bars table**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L62-L79)
   - Schema: symbol, ts, ts_epoch, open, high, low, close, volume, interval

2. **intraday_sync_state table**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L81-L87)
   - Tracks last sync state per symbol

3. **CLI commands**
   - Commands: `pfsync intraday sync --symbol OGDC`
   - Commands: `pfsync intraday show --symbol OGDC`
   - Options: `--no-incremental`, `--max-rows`

4. **Intraday source module**
   - File: [src/pakfindata/sources/intraday.py](src/pakfindata/sources/intraday.py)

5. **UI: Intraday Trend page**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py)
   - Page exists in sidebar navigation

---

## F) Company Analytics Ingestion

### ✅ Implemented

1. **company_profile table**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L97-L110)
   - Fields: company_name, sector_name, business_description, address, website, etc.

2. **company_key_people table**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L112-L119)
   - Stores CEO, Chairman, CFO, etc.

3. **company_quote_snapshots table**
   - File: [src/pakfindata/db.py](src/pakfindata/db.py#L121-L148)
   - Time-series with smart-save using raw_hash

4. **CLI commands**
   - Commands: `pfsync company refresh --symbol OGDC`
   - Commands: `pfsync company snapshot --symbol OGDC`
   - Commands: `pfsync company listen --symbol OGDC --interval 60`
   - Commands: `pfsync company show --symbol OGDC --what profile|people|quotes`

---

## G) Analytics Tables + Dashboard + History UI

### ✅ Implemented

1. **analytics_market_snapshot table**
   - File: [src/pakfindata/analytics.py](src/pakfindata/analytics.py#L22-L32)
   - Market breadth (gainers/losers/unchanged counts), total volume

2. **analytics_symbol_snapshot table**
   - File: [src/pakfindata/analytics.py](src/pakfindata/analytics.py#L35-L51)
   - Top-N rankings (gainers, losers, volume)

3. **analytics_sector_snapshot table**
   - File: [src/pakfindata/analytics.py](src/pakfindata/analytics.py#L54-L66)
   - Sector rollups with avg_change_pct, sum_volume

4. **Dashboard page**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py)
   - Shows KPIs, market breadth, top movers, sector leaderboard

5. **History page with 3 tabs**
   - File: [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py#L1471-L1960)
   - Tab 1: Market History (breadth over time, volume trends)
   - Tab 2: Symbol History (price trends, volume, optional candlestick)
   - Tab 3: Sector History (avg change %, volume, top performers)

---

## H) Candlestick Clarity

### ✅ Implemented

1. **Minimum chart height (650px)**
   - File: [src/pakfindata/ui/charts.py](src/pakfindata/ui/charts.py)
   - `MIN_CANDLESTICK_HEIGHT = 650`

2. **SMA overlays (20, 50)**
   - File: [src/pakfindata/ui/charts.py](src/pakfindata/ui/charts.py)
   - `compute_sma()` function and SMA toggle checkboxes

3. **Readable axis labels**
   - Plotly charts configured with appropriate layout settings

---

## Action Items - ALL COMPLETED

### ✅ Priority 1: Implement Missing Features (DONE)

1. **Create downloaded_market_summary_dates table** - COMPLETED 2026-01-21
   - Added schema to market_summary.py
   - Tracking: date (PK), status, csv_path, record_count, error_msg, fetched_at

2. **Add --retry-failed and --retry-missing CLI options** - COMPLETED 2026-01-21
   - `pfsync market-summary retry-failed` - retry dates with errors
   - `pfsync market-summary retry-missing` - retry dates that returned 404

### ✅ Priority 2: Verification (DONE)

3. **Create scripts/verify_features.py** - COMPLETED 2026-01-21
   - Tests all table joins work correctly
   - Verifies sector_name displays properly
   - Tests analytics computation
   - Returns exit code 0 on success
   - Run: `python scripts/verify_features.py`

4. **Run pytest and ruff** - COMPLETED 2026-01-21
   - pytest: All applicable tests pass (some tests require WSL mount)
   - ruff: All checks passed

---

## Files Audited

- [src/pakfindata/db.py](src/pakfindata/db.py) - Core database schema
- [src/pakfindata/cli.py](src/pakfindata/cli.py) - CLI commands
- [src/pakfindata/query.py](src/pakfindata/query.py) - Query helpers
- [src/pakfindata/analytics.py](src/pakfindata/analytics.py) - Analytics computation
- [src/pakfindata/sources/market_summary.py](src/pakfindata/sources/market_summary.py) - Market summary downloader
- [src/pakfindata/sources/regular_market.py](src/pakfindata/sources/regular_market.py) - Regular market ingestion
- [src/pakfindata/sources/intraday.py](src/pakfindata/sources/intraday.py) - Intraday data
- [src/pakfindata/ui/app.py](src/pakfindata/ui/app.py) - Streamlit UI
- [src/pakfindata/ui/charts.py](src/pakfindata/ui/charts.py) - Chart components
- [src/pakfindata/range_utils.py](src/pakfindata/range_utils.py) - Date range utilities
- [tests/test_market_summary_parser.py](tests/test_market_summary_parser.py) - Market summary tests
