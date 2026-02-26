# PSX OHLCV Database Schema

**Version:** 0.1.0
**Database:** SQLite
**Location:** `/mnt/e/psxdata/psx.sqlite`

---

## Table of Contents

1. [Overview](#overview)
2. [Entity Relationship](#entity-relationship)
3. [Table Categories](#table-categories)
4. [Core Tables](#core-tables)
5. [Company Data Tables](#company-data-tables)
6. [Quant/Bloomberg-Style Tables](#quantbloomberg-style-tables)
7. [System Tables](#system-tables)
8. [Glossary](#glossary)
9. [SQL Creation Scripts](#sql-creation-scripts)

---

## Overview

The PSX OHLCV database stores Pakistan Stock Exchange market data with **23+ tables** organized into four categories:

| Category | Tables | Purpose |
|----------|--------|---------|
| **Core** | 6 | Symbols, EOD prices, intraday bars |
| **Company Data** | 8 | Fundamentals, financials, ratios, payouts |
| **Quant/Bloomberg** | 5 | Deep snapshots, trading sessions, announcements |
| **System** | 4 | Sync tracking, user interactions, job logs |

---

## Entity Relationship

```
                    ┌─────────────────┐
                    │     symbols     │ (Master)
                    │   PRIMARY KEY   │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│    eod_ohlcv    │ │  intraday_bars  │ │company_snapshots│
│ (symbol, date)  │ │   (symbol, ts)  │ │(symbol, date)   │
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │
         ▼
┌─────────────────┐
│trading_sessions │
│(symbol,date,mkt)│
└─────────────────┘
```

---

## Table Categories

### Core Tables
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `symbols` | `symbol` | Master symbol list with metadata |
| `eod_ohlcv` | `(symbol, date)` | End-of-day OHLCV price data |
| `intraday_bars` | `(symbol, ts)` | Intraday time series (1-min bars) |
| `intraday_sync_state` | `symbol` | Last sync timestamp per symbol |
| `sectors` | `sector_code` | Sector master list |

### Company Data Tables
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `company_profile` | `symbol` | Company profile information |
| `company_key_people` | `(symbol, role, name)` | Directors, executives |
| `company_quote_snapshots` | `(symbol, ts)` | Point-in-time quote captures |
| `company_signal_snapshots` | `(symbol, ts, signal_key)` | Derived signals |
| `company_fundamentals` | `symbol` | Latest fundamentals (live) |
| `company_fundamentals_history` | `(symbol, date)` | Historical fundamentals |
| `company_financials` | `(symbol, period_end, period_type)` | Income statement data |
| `company_ratios` | `(symbol, period_end, period_type)` | Financial ratios |
| `company_payouts` | `(symbol, ex_date, payout_type)` | Dividends and bonuses |

### Quant/Bloomberg-Style Tables
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `company_snapshots` | `(symbol, snapshot_date)` | Full JSON document storage |
| `trading_sessions` | `(symbol, session_date, market_type, contract_month)` | Market microstructure |
| `corporate_announcements` | `id` + unique constraint | Company announcements |
| `equity_structure` | `(symbol, as_of_date)` | Ownership and capital structure |
| `scrape_jobs` | `job_id` | Scrape job tracking |

### System Tables
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `sync_runs` | `run_id` | Sync job runs |
| `sync_failures` | N/A | Failed sync records |
| `downloaded_market_summary_dates` | `date` | Market summary download tracking |
| `user_interactions` | `id` | UI analytics tracking |

---

## Core Tables

### symbols
**Master table of all stock symbols**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol` | TEXT | NO | Stock ticker (PRIMARY KEY) |
| `name` | TEXT | YES | Company name |
| `sector` | TEXT | YES | Sector code |
| `sector_name` | TEXT | YES | Full sector name |
| `outstanding_shares` | REAL | YES | Shares outstanding |
| `is_active` | INTEGER | NO | 1=active, 0=delisted |
| `source` | TEXT | NO | Discovery source |
| `discovered_at` | TEXT | NO | First seen timestamp |
| `updated_at` | TEXT | NO | Last update timestamp |

---

### eod_ohlcv
**End-of-day OHLCV price data (main market data table)**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol` | TEXT | NO | Stock ticker |
| `date` | TEXT | NO | Trading date (YYYY-MM-DD) |
| `open` | REAL | YES | Opening price |
| `high` | REAL | YES | Day high |
| `low` | REAL | YES | Day low |
| `close` | REAL | YES | Closing price |
| `volume` | INTEGER | YES | Shares traded |
| `prev_close` | REAL | YES | Previous day close |
| `sector_code` | TEXT | YES | Sector code |
| `company_name` | TEXT | YES | Company name |
| `ingested_at` | TEXT | NO | Ingestion timestamp |

**Primary Key:** `(symbol, date)`
**Indexes:** `idx_eod_ohlcv_date`, `idx_eod_ohlcv_symbol`

---

### intraday_bars
**Intraday time series data (1-minute bars)**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol` | TEXT | NO | Stock ticker |
| `ts` | TEXT | NO | Timestamp (ISO format) |
| `ts_epoch` | INTEGER | NO | Unix epoch timestamp |
| `open` | REAL | YES | Bar open price |
| `high` | REAL | YES | Bar high price |
| `low` | REAL | YES | Bar low price |
| `close` | REAL | YES | Bar close price |
| `volume` | REAL | YES | Bar volume |
| `interval` | TEXT | NO | Bar interval type |
| `ingested_at` | TEXT | NO | Ingestion timestamp |

**Primary Key:** `(symbol, ts)`
**Indexes:** `idx_intraday_bars_symbol`, `idx_intraday_bars_ts`, `idx_intraday_bars_ts_epoch`

---

### sectors
**Sector master table**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `sector_code` | TEXT | NO | Sector code (PRIMARY KEY) |
| `sector_name` | TEXT | NO | Full sector name |
| `updated_at` | TEXT | NO | Last update timestamp |
| `source` | TEXT | NO | Data source |

---

## Company Data Tables

### company_fundamentals
**Latest company fundamentals (live snapshot)**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker (PRIMARY KEY) |
| `company_name` | TEXT | Company name |
| `sector_name` | TEXT | Sector name |
| `price` | REAL | Current price |
| `change` | REAL | Price change |
| `change_pct` | REAL | Price change % |
| `open`, `high`, `low` | REAL | Day OHLC |
| `volume` | INTEGER | Day volume |
| `ldcp` | REAL | Last Day Close Price |
| `bid_price`, `bid_size` | REAL, INT | Best bid |
| `ask_price`, `ask_size` | REAL, INT | Best ask |
| `day_range_low`, `day_range_high` | REAL | Day range |
| `wk52_low`, `wk52_high` | REAL | 52-week range |
| `circuit_low`, `circuit_high` | REAL | Circuit breakers |
| `ytd_change_pct` | REAL | Year-to-date change % |
| `one_year_change_pct` | REAL | 1-year change % |
| `pe_ratio` | REAL | Price/Earnings ratio |
| `market_cap` | REAL | Market cap (thousands) |
| `total_shares` | INTEGER | Total shares |
| `free_float_shares` | INTEGER | Free float shares |
| `free_float_pct` | REAL | Free float % |
| `haircut` | REAL | Margin haircut % |
| `variance` | REAL | VAR % |
| `as_of` | TEXT | Quote timestamp |
| `market_mode` | TEXT | REG, ODD, FUT, SPOT |
| `updated_at` | TEXT | Last update |

---

### company_financials
**Annual and quarterly financial data**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `period_end` | TEXT | Period end date |
| `period_type` | TEXT | 'annual' or 'quarterly' |
| `sales` | REAL | Total revenue |
| `gross_profit` | REAL | Gross profit |
| `operating_profit` | REAL | Operating profit |
| `profit_before_tax` | REAL | PBT |
| `profit_after_tax` | REAL | Net income |
| `eps` | REAL | Earnings per share |
| `total_assets` | REAL | Total assets |
| `total_liabilities` | REAL | Total liabilities |
| `total_equity` | REAL | Shareholder equity |

**Primary Key:** `(symbol, period_end, period_type)`

---

### company_ratios
**Financial ratios**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `period_end` | TEXT | Period end date |
| `period_type` | TEXT | 'annual' or 'quarterly' |
| `gross_profit_margin` | REAL | Gross profit / Sales % |
| `net_profit_margin` | REAL | Net income / Sales % |
| `operating_margin` | REAL | Operating margin % |
| `return_on_equity` | REAL | ROE % |
| `return_on_assets` | REAL | ROA % |
| `sales_growth` | REAL | YoY sales growth % |
| `eps_growth` | REAL | YoY EPS growth % |
| `pe_ratio` | REAL | Price to Earnings |
| `pb_ratio` | REAL | Price to Book |

**Primary Key:** `(symbol, period_end, period_type)`

---

### company_payouts
**Dividend and bonus history**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `ex_date` | TEXT | Ex-dividend date |
| `payout_type` | TEXT | 'cash', 'bonus', 'right' |
| `announcement_date` | TEXT | Date announced |
| `book_closure_from` | TEXT | Book closure start |
| `book_closure_to` | TEXT | Book closure end |
| `amount` | REAL | Dividend/bonus amount |
| `fiscal_year` | TEXT | Fiscal year |

**Primary Key:** `(symbol, ex_date, payout_type)`

---

## Quant/Bloomberg-Style Tables

### company_snapshots
**Full JSON document storage for comprehensive company data**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `snapshot_date` | TEXT | Snapshot date (YYYY-MM-DD) |
| `snapshot_time` | TEXT | Snapshot time (HH:MM:SS) |
| `company_name` | TEXT | Company name |
| `sector_code` | TEXT | Sector code |
| `sector_name` | TEXT | Sector name |
| `quote_data` | TEXT | JSON: price, change, volume, ranges |
| `equity_data` | TEXT | JSON: market cap, shares, float |
| `profile_data` | TEXT | JSON: description, address, key people |
| `financials_data` | TEXT | JSON: annual/quarterly financials |
| `ratios_data` | TEXT | JSON: all financial ratios |
| `trading_data` | TEXT | JSON: bid/ask, circuit breakers, VAR |
| `futures_data` | TEXT | JSON: all futures contracts |
| `announcements_data` | TEXT | JSON: recent announcements |
| `raw_html` | TEXT | Full page HTML (optional) |
| `source_url` | TEXT | Source URL |
| `scraped_at` | TEXT | Scrape timestamp |

**Primary Key:** `(symbol, snapshot_date)`

---

### trading_sessions
**Enhanced trading data with full market microstructure**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `session_date` | TEXT | Trading date |
| `market_type` | TEXT | 'REG', 'FUT', 'CSF', 'ODL' |
| `contract_month` | TEXT | Futures month (JAN, FEB...) |
| `open`, `high`, `low`, `close` | REAL | OHLC prices |
| `volume` | INTEGER | Shares traded |
| `ldcp` | REAL | Last Day Close Price |
| `prev_close` | REAL | Previous close |
| `change_value` | REAL | Price change |
| `change_percent` | REAL | Price change % |
| `bid_price`, `bid_volume` | REAL, INT | Best bid |
| `ask_price`, `ask_volume` | REAL, INT | Best ask |
| `spread` | REAL | Bid-ask spread |
| `circuit_low`, `circuit_high` | REAL | Circuit breakers |
| `week_52_low`, `week_52_high` | REAL | 52-week range |
| `total_trades` | INTEGER | Number of trades |
| `turnover` | REAL | Value traded |
| `vwap` | REAL | Volume-weighted average price |
| `var_percent` | REAL | Value at Risk % |
| `haircut_percent` | REAL | Margin haircut % |
| `pe_ratio_ttm` | REAL | P/E trailing 12 months |
| `ytd_change` | REAL | Year-to-date change |
| `year_1_change` | REAL | 1-year change |

**Primary Key:** `(symbol, session_date, market_type, contract_month)`

---

### corporate_announcements
**Structured corporate announcements**

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment ID |
| `symbol` | TEXT | Stock ticker |
| `announcement_date` | TEXT | Announcement date |
| `announcement_type` | TEXT | Type (see below) |
| `category` | TEXT | quarterly, annual, interim |
| `title` | TEXT | Announcement title |
| `title_hash` | TEXT | Hash for deduplication |
| `document_url` | TEXT | Document link |
| `document_type` | TEXT | pdf, html, xls |
| `summary` | TEXT | Extracted summary |
| `key_figures` | TEXT | JSON: extracted metrics |

**Announcement Types:** `financial_result`, `board_meeting`, `material_info`, `agm`, `dividend`, `other`

---

### equity_structure
**Ownership and capital structure**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | TEXT | Stock ticker |
| `as_of_date` | TEXT | As-of date |
| `authorized_shares` | INTEGER | Authorized shares |
| `issued_shares` | INTEGER | Issued shares |
| `outstanding_shares` | INTEGER | Outstanding shares |
| `treasury_shares` | INTEGER | Treasury shares |
| `free_float_shares` | INTEGER | Free float shares |
| `free_float_percent` | REAL | Free float % |
| `market_cap` | REAL | Market cap (PKR) |
| `market_cap_usd` | REAL | Market cap (USD) |
| `ownership_data` | TEXT | JSON: ownership breakdown |
| `face_value` | REAL | Face value per share |

**Primary Key:** `(symbol, as_of_date)`

---

## System Tables

### sync_runs
**Tracks sync job executions**

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | TEXT | Unique run ID (PRIMARY KEY) |
| `started_at` | TEXT | Start timestamp |
| `ended_at` | TEXT | End timestamp |
| `mode` | TEXT | Sync mode |
| `symbols_total` | INTEGER | Total symbols |
| `symbols_ok` | INTEGER | Successful symbols |
| `symbols_failed` | INTEGER | Failed symbols |
| `rows_upserted` | INTEGER | Records inserted/updated |

---

### user_interactions
**UI analytics tracking**

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment ID |
| `session_id` | TEXT | Session identifier |
| `timestamp` | TEXT | Action timestamp |
| `action_type` | TEXT | page_visit, search, button_click, etc. |
| `page_name` | TEXT | Page name |
| `symbol` | TEXT | Stock symbol (if applicable) |
| `action_detail` | TEXT | Additional details |
| `metadata` | TEXT | JSON extra data |

---

## Glossary

### Market Terms

| Term | Definition |
|------|------------|
| **OHLCV** | Open, High, Low, Close, Volume - standard price bar data |
| **EOD** | End of Day - daily closing data |
| **LDCP** | Last Day Close Price - previous trading day's close |
| **VWAP** | Volume Weighted Average Price |
| **VAR** | Value at Risk - risk metric percentage |
| **Haircut** | Margin collateral discount percentage |
| **Circuit Breaker** | Price limit bands (upper/lower) |
| **Free Float** | Shares available for public trading |

### Market Types

| Code | Description |
|------|-------------|
| **REG** | Regular Market - main trading board |
| **FUT** | Futures Market - derivatives |
| **CSF** | Cash Settled Futures |
| **ODL** | Odd Lot Market - small quantity trades |

### Data Sources

| Source | URL | Data Type |
|--------|-----|-----------|
| **Market Watch** | dps.psx.com.pk/market-watch | Real-time quotes |
| **Company Page** | dps.psx.com.pk/company/{symbol} | Company details |
| **Market Summary** | dps.psx.com.pk/download/mkt_summary/{date}.Z | EOD bulk data |
| **Listed Companies** | dps.psx.com.pk/listed-companies | Symbol master |

### Period Types

| Type | Description |
|------|-------------|
| **annual** | Full fiscal year data |
| **quarterly** | Quarter-end data (Q1, Q2, Q3, Q4) |
| **ttm** | Trailing Twelve Months |
| **ytd** | Year to Date |

### Payout Types

| Type | Description |
|------|-------------|
| **cash** | Cash dividend per share |
| **bonus** | Bonus shares (stock dividend) |
| **right** | Rights issue offering |

---

## SQL Creation Scripts

The complete schema creation SQL is stored in `src/pakfindata/db.py` in the `SCHEMA_SQL` variable.

### Quick Reference

```sql
-- Connect to database
sqlite3 /mnt/e/psxdata/psx.sqlite

-- List all tables
.tables

-- Show table schema
.schema symbols
.schema eod_ohlcv

-- Table row counts
SELECT 'symbols' as tbl, COUNT(*) as cnt FROM symbols
UNION ALL SELECT 'eod_ohlcv', COUNT(*) FROM eod_ohlcv
UNION ALL SELECT 'intraday_bars', COUNT(*) FROM intraday_bars
UNION ALL SELECT 'company_snapshots', COUNT(*) FROM company_snapshots;

-- Recent EOD data
SELECT symbol, date, close, volume
FROM eod_ohlcv
WHERE date = (SELECT MAX(date) FROM eod_ohlcv)
ORDER BY volume DESC
LIMIT 10;

-- Company snapshot JSON extract
SELECT symbol, snapshot_date,
       json_extract(quote_data, '$.price') as price,
       json_extract(quote_data, '$.change_pct') as change_pct
FROM company_snapshots
WHERE snapshot_date = date('now');
```

---

## Full Creation Script

See: [db.py](../src/pakfindata/db.py) - `SCHEMA_SQL` variable (lines 13-638)

The schema is automatically applied when connecting to the database via `db.connect()`.
