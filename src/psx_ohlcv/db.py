"""Database layer for PSX OHLCV."""

import sqlite3
import uuid
from pathlib import Path

import pandas as pd

from .config import ensure_dirs, get_db_path
from .models import now_iso

# SQL schema
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS symbols (
    symbol             TEXT PRIMARY KEY,
    name               TEXT NULL,
    sector             TEXT NULL,
    sector_name        TEXT NULL,
    outstanding_shares REAL NULL,
    is_active          INTEGER DEFAULT 1,
    source             TEXT NOT NULL DEFAULT 'MARKET_WATCH',
    discovered_at      TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS eod_ohlcv (
    symbol       TEXT NOT NULL,
    date         TEXT NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       INTEGER,
    prev_close   REAL,                  -- Previous day close price
    sector_code  TEXT,                  -- Sector code from market summary
    company_name TEXT,                  -- Company name from market summary
    ingested_at  TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_date ON eod_ohlcv(date);
CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_symbol ON eod_ohlcv(symbol);

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id         TEXT PRIMARY KEY,
    started_at     TEXT NOT NULL,
    ended_at       TEXT NULL,
    mode           TEXT NOT NULL,
    symbols_total  INTEGER DEFAULT 0,
    symbols_ok     INTEGER DEFAULT 0,
    symbols_failed INTEGER DEFAULT 0,
    rows_upserted  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sync_failures (
    run_id        TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    error_type    TEXT NOT NULL,
    error_message TEXT,
    created_at    TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES sync_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_failures_run ON sync_failures(run_id);

-- Intraday bars table for storing intraday time series data
CREATE TABLE IF NOT EXISTS intraday_bars (
    symbol      TEXT NOT NULL,
    ts          TEXT NOT NULL,
    ts_epoch    INTEGER NOT NULL,
    open        REAL NULL,
    high        REAL NULL,
    low         REAL NULL,
    close       REAL NULL,
    volume      REAL NULL,
    interval    TEXT NOT NULL DEFAULT 'int',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_intraday_bars_symbol ON intraday_bars(symbol);
CREATE INDEX IF NOT EXISTS idx_intraday_bars_ts ON intraday_bars(ts);
CREATE INDEX IF NOT EXISTS idx_intraday_bars_ts_epoch ON intraday_bars(ts_epoch);

-- Intraday sync state tracking
CREATE TABLE IF NOT EXISTS intraday_sync_state (
    symbol        TEXT PRIMARY KEY,
    last_ts       TEXT NULL,
    last_ts_epoch INTEGER NULL,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Sector master table
CREATE TABLE IF NOT EXISTS sectors (
    sector_code TEXT PRIMARY KEY,
    sector_name TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    source      TEXT NOT NULL DEFAULT 'DPS_SECTOR_SUMMARY'
);

-- Company profile from DPS company pages
CREATE TABLE IF NOT EXISTS company_profile (
    symbol              TEXT PRIMARY KEY,
    company_name        TEXT NULL,
    sector_name         TEXT NULL,
    business_description TEXT NULL,
    address             TEXT NULL,
    website             TEXT NULL,
    registrar           TEXT NULL,
    auditor             TEXT NULL,
    fiscal_year_end     TEXT NULL,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    source_url          TEXT NOT NULL
);

-- Key people from company pages
CREATE TABLE IF NOT EXISTS company_key_people (
    symbol     TEXT NOT NULL,
    role       TEXT NOT NULL,
    name       TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, role, name)
);

-- Quote snapshots from company pages (time series)
CREATE TABLE IF NOT EXISTS company_quote_snapshots (
    symbol          TEXT NOT NULL,
    ts              TEXT NOT NULL,
    as_of           TEXT NULL,
    price           REAL NULL,
    change          REAL NULL,
    change_pct      REAL NULL,
    open            REAL NULL,
    high            REAL NULL,
    low             REAL NULL,
    volume          REAL NULL,
    day_range_low   REAL NULL,
    day_range_high  REAL NULL,
    wk52_low        REAL NULL,
    wk52_high       REAL NULL,
    circuit_low     REAL NULL,
    circuit_high    REAL NULL,
    market_mode     TEXT NULL,
    raw_hash        TEXT NOT NULL,
    ingested_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_quote_snapshots_symbol
    ON company_quote_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_quote_snapshots_ts
    ON company_quote_snapshots(ts);

-- Company signal snapshots (derived metrics per quote snapshot)
CREATE TABLE IF NOT EXISTS company_signal_snapshots (
    symbol       TEXT NOT NULL,
    ts           TEXT NOT NULL,
    signal_key   TEXT NOT NULL,
    signal_value TEXT NOT NULL,
    PRIMARY KEY (symbol, ts, signal_key)
);

CREATE INDEX IF NOT EXISTS idx_signal_snapshots_symbol
    ON company_signal_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_signal_snapshots_ts
    ON company_signal_snapshots(ts);

-- =============================================================================
-- Company Fundamentals: Comprehensive snapshot with all PSX data fields
-- Symbol is the primary key - stores LATEST data
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_fundamentals (
    symbol              TEXT PRIMARY KEY,

    -- Company Identity
    company_name        TEXT,
    sector_name         TEXT,

    -- Price Data
    price               REAL,
    change              REAL,
    change_pct          REAL,
    open                REAL,
    high                REAL,
    low                 REAL,
    volume              INTEGER,
    ldcp                REAL,           -- Last Day Close Price

    -- Bid/Ask (Order Book Top)
    bid_price           REAL,
    bid_size            INTEGER,
    ask_price           REAL,
    ask_size            INTEGER,

    -- Ranges
    day_range_low       REAL,
    day_range_high      REAL,
    wk52_low            REAL,
    wk52_high           REAL,
    circuit_low         REAL,
    circuit_high        REAL,

    -- Performance Metrics
    ytd_change_pct      REAL,
    one_year_change_pct REAL,

    -- Valuation
    pe_ratio            REAL,
    market_cap          REAL,           -- in thousands

    -- Equity Structure
    total_shares        INTEGER,
    free_float_shares   INTEGER,
    free_float_pct      REAL,

    -- Risk Parameters
    haircut             REAL,
    variance            REAL,

    -- Profile Info
    business_description TEXT,
    address             TEXT,
    website             TEXT,
    registrar           TEXT,
    auditor             TEXT,
    fiscal_year_end     TEXT,
    incorporation_date  TEXT,
    listed_in           TEXT,           -- KSE, LSE, ISE

    -- Metadata
    as_of               TEXT,           -- Quote timestamp from PSX
    market_mode         TEXT,           -- REG, ODD, FUT, SPOT
    source_url          TEXT,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- =============================================================================
-- Company Fundamentals History: Daily snapshots with (symbol, date) as PK
-- Keeps historical record of all fundamentals data
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_fundamentals_history (
    symbol              TEXT NOT NULL,
    date                TEXT NOT NULL,  -- YYYY-MM-DD

    -- Company Identity
    company_name        TEXT,
    sector_name         TEXT,

    -- Price Data
    price               REAL,
    change              REAL,
    change_pct          REAL,
    open                REAL,
    high                REAL,
    low                 REAL,
    volume              INTEGER,
    ldcp                REAL,

    -- Bid/Ask
    bid_price           REAL,
    bid_size            INTEGER,
    ask_price           REAL,
    ask_size            INTEGER,

    -- Ranges
    day_range_low       REAL,
    day_range_high      REAL,
    wk52_low            REAL,
    wk52_high           REAL,
    circuit_low         REAL,
    circuit_high        REAL,

    -- Performance Metrics
    ytd_change_pct      REAL,
    one_year_change_pct REAL,

    -- Valuation
    pe_ratio            REAL,
    market_cap          REAL,

    -- Equity Structure
    total_shares        INTEGER,
    free_float_shares   INTEGER,
    free_float_pct      REAL,

    -- Risk Parameters
    haircut             REAL,
    variance            REAL,

    -- Metadata
    as_of               TEXT,
    market_mode         TEXT,
    snapshot_ts         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_fundamentals_history_symbol
    ON company_fundamentals_history(symbol);
CREATE INDEX IF NOT EXISTS idx_fundamentals_history_date
    ON company_fundamentals_history(date);

-- =============================================================================
-- Company Financials: Annual and Quarterly financial data from FINANCIALS tab
-- Primary key: (symbol, period_end, period_type)
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_financials (
    symbol              TEXT NOT NULL,
    period_end          TEXT NOT NULL,      -- YYYY-MM-DD or YYYY
    period_type         TEXT NOT NULL,      -- 'annual' or 'quarterly'

    -- Income Statement
    sales               REAL,               -- Total Revenue/Sales
    gross_profit        REAL,
    operating_profit    REAL,
    profit_before_tax   REAL,
    profit_after_tax    REAL,               -- Net Income
    eps                 REAL,               -- Earnings Per Share

    -- Balance Sheet (optional)
    total_assets        REAL,
    total_liabilities   REAL,
    total_equity        REAL,

    -- Metadata
    currency            TEXT DEFAULT 'PKR',
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, period_end, period_type)
);

CREATE INDEX IF NOT EXISTS idx_company_financials_symbol
    ON company_financials(symbol);

-- =============================================================================
-- Company Ratios: Financial ratios from RATIOS tab
-- Primary key: (symbol, period_end, period_type)
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_ratios (
    symbol              TEXT NOT NULL,
    period_end          TEXT NOT NULL,      -- YYYY-MM-DD or YYYY
    period_type         TEXT NOT NULL,      -- 'annual' or 'quarterly'

    -- Profitability Ratios
    gross_profit_margin REAL,               -- Gross Profit / Sales %
    net_profit_margin   REAL,               -- Net Income / Sales %
    operating_margin    REAL,
    return_on_equity    REAL,               -- ROE %
    return_on_assets    REAL,               -- ROA %

    -- Growth Metrics
    sales_growth        REAL,               -- YoY Sales Growth %
    eps_growth          REAL,               -- YoY EPS Growth %
    profit_growth       REAL,               -- YoY Profit Growth %

    -- Valuation Ratios
    pe_ratio            REAL,               -- Price to Earnings
    pb_ratio            REAL,               -- Price to Book
    peg_ratio           REAL,               -- P/E to Growth

    -- Metadata
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, period_end, period_type)
);

CREATE INDEX IF NOT EXISTS idx_company_ratios_symbol
    ON company_ratios(symbol);

-- =============================================================================
-- Company Payouts: Dividend and bonus history from PAYOUTS tab
-- Primary key: (symbol, ex_date, payout_type)
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_payouts (
    symbol              TEXT NOT NULL,
    ex_date             TEXT NOT NULL,      -- Ex-dividend date YYYY-MM-DD
    payout_type         TEXT NOT NULL,      -- 'cash', 'bonus', 'right'

    -- Payout Details
    announcement_date   TEXT,               -- Date announced
    book_closure_from   TEXT,
    book_closure_to     TEXT,
    amount              REAL,               -- Cash dividend per share or bonus %
    fiscal_year         TEXT,               -- e.g., '2024', '2023'

    -- Metadata
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, ex_date, payout_type)
);

CREATE INDEX IF NOT EXISTS idx_company_payouts_symbol
    ON company_payouts(symbol);
CREATE INDEX IF NOT EXISTS idx_company_payouts_ex_date
    ON company_payouts(ex_date);

-- =============================================================================
-- Financial Announcements: Results announcements from PSX
-- Source: https://www.psx.com.pk/psx/announcement/financial-announcements
-- Primary key: (symbol, announcement_date, fiscal_period)
-- =============================================================================
CREATE TABLE IF NOT EXISTS financial_announcements (
    symbol              TEXT NOT NULL,
    announcement_date   TEXT NOT NULL,      -- YYYY-MM-DD
    fiscal_period       TEXT NOT NULL,      -- e.g., '31/12/2025(YR)', '30/06/2025(HYR)'

    -- Financial Results
    profit_before_tax   REAL,               -- In millions Rs.
    profit_after_tax    REAL,               -- In millions Rs.
    eps                 REAL,               -- Earnings per share

    -- Dividend/Bonus/Right
    dividend_payout     TEXT,               -- Raw string e.g., "83%(i) (D)"
    dividend_amount     REAL,               -- Parsed percentage
    payout_type         TEXT,               -- 'cash', 'bonus', 'right', or NULL

    -- Corporate Events
    agm_date            TEXT,               -- AGM/EOGM date YYYY-MM-DD
    book_closure_from   TEXT,               -- Book closure start
    book_closure_to     TEXT,               -- Book closure end

    -- Metadata
    company_name        TEXT,               -- Company name from announcement
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, announcement_date, fiscal_period)
);

CREATE INDEX IF NOT EXISTS idx_financial_announcements_symbol
    ON financial_announcements(symbol);
CREATE INDEX IF NOT EXISTS idx_financial_announcements_date
    ON financial_announcements(announcement_date);

-- =============================================================================
-- User Interactions: Track user activity for analytics
-- Logs page visits, button clicks, searches, and other actions
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_interactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT NOT NULL,          -- Unique session identifier
    timestamp           TEXT NOT NULL,          -- When action occurred
    action_type         TEXT NOT NULL,          -- 'page_visit', 'search', 'button_click', 'refresh', 'download'
    page_name           TEXT,                   -- Which page the action was on
    symbol              TEXT,                   -- Stock symbol if applicable
    action_detail       TEXT,                   -- Additional details (button name, search query, etc.)
    metadata            TEXT,                   -- JSON string for extra data
    ip_address          TEXT,                   -- Optional: user IP
    user_agent          TEXT                    -- Optional: browser/client info
);

CREATE INDEX IF NOT EXISTS idx_user_interactions_session
    ON user_interactions(session_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_timestamp
    ON user_interactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_user_interactions_action_type
    ON user_interactions(action_type);
CREATE INDEX IF NOT EXISTS idx_user_interactions_symbol
    ON user_interactions(symbol);

-- =============================================================================
-- BLOOMBERG-STYLE QUANT DATA TABLES
-- =============================================================================

-- =============================================================================
-- Company Snapshots: Full JSON document storage for comprehensive company data
-- NoSQL-style flexible storage for all scraped data
-- Primary key: (symbol, snapshot_date)
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_snapshots (
    symbol              TEXT NOT NULL,
    snapshot_date       TEXT NOT NULL,          -- YYYY-MM-DD
    snapshot_time       TEXT,                   -- HH:MM:SS when captured

    -- Core identifiers
    company_name        TEXT,
    sector_code         TEXT,
    sector_name         TEXT,

    -- Full JSON document with all data
    quote_data          TEXT,                   -- JSON: price, change, volume, ranges
    equity_data         TEXT,                   -- JSON: market cap, shares, float
    profile_data        TEXT,                   -- JSON: description, address, key people
    financials_data     TEXT,                   -- JSON: annual/quarterly financials
    ratios_data         TEXT,                   -- JSON: all financial ratios
    trading_data        TEXT,                   -- JSON: bid/ask, circuit breakers, VAR
    futures_data        TEXT,                   -- JSON: all futures contracts
    announcements_data  TEXT,                   -- JSON: recent announcements

    -- Raw HTML for reprocessing
    raw_html            TEXT,                   -- Full page HTML (compressed/encoded)

    -- Metadata
    source_url          TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_company_snapshots_symbol
    ON company_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_company_snapshots_date
    ON company_snapshots(snapshot_date);

-- =============================================================================
-- Trading Sessions: Enhanced intraday/EOD data with full market microstructure
-- Captures all trading metrics for quant analysis
-- Primary key: (symbol, session_date, market_type)
-- =============================================================================
CREATE TABLE IF NOT EXISTS trading_sessions (
    symbol              TEXT NOT NULL,
    session_date        TEXT NOT NULL,          -- YYYY-MM-DD
    market_type         TEXT NOT NULL,          -- 'REG', 'FUT', 'CSF', 'ODL'
    contract_month      TEXT,                   -- For futures: 'JAN', 'FEB', etc.

    -- OHLCV
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL,
    volume              INTEGER,

    -- Price references
    ldcp                REAL,                   -- Last Day Close Price
    prev_close          REAL,
    change_value        REAL,
    change_percent      REAL,

    -- Order book snapshot (end of day)
    bid_price           REAL,
    bid_volume          INTEGER,
    ask_price           REAL,
    ask_volume          INTEGER,
    spread              REAL,                   -- ask - bid

    -- Ranges
    day_range_low       REAL,
    day_range_high      REAL,
    circuit_low         REAL,                   -- Lower circuit breaker
    circuit_high        REAL,                   -- Upper circuit breaker
    week_52_low         REAL,
    week_52_high        REAL,

    -- Trading statistics
    total_trades        INTEGER,
    turnover            REAL,                   -- value traded
    vwap                REAL,                   -- Volume Weighted Average Price

    -- Risk metrics
    var_percent         REAL,                   -- Value at Risk %
    haircut_percent     REAL,                   -- Margin haircut %

    -- Valuation (from quote)
    pe_ratio_ttm        REAL,                   -- P/E Trailing Twelve Months

    -- Performance
    ytd_change          REAL,
    year_1_change       REAL,

    -- Metadata
    last_update         TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, session_date, market_type, contract_month)
);

CREATE INDEX IF NOT EXISTS idx_trading_sessions_symbol
    ON trading_sessions(symbol);
CREATE INDEX IF NOT EXISTS idx_trading_sessions_date
    ON trading_sessions(session_date);
CREATE INDEX IF NOT EXISTS idx_trading_sessions_market
    ON trading_sessions(market_type);

-- =============================================================================
-- Corporate Announcements: Structured storage for all company announcements
-- Primary key: (symbol, announcement_date, announcement_type, title_hash)
-- =============================================================================
CREATE TABLE IF NOT EXISTS corporate_announcements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    announcement_date   TEXT NOT NULL,          -- YYYY-MM-DD
    announcement_type   TEXT NOT NULL,          -- 'financial_result', 'board_meeting', 'material_info', 'agm', 'dividend', 'other'
    category            TEXT,                   -- 'quarterly', 'annual', 'interim', etc.

    title               TEXT NOT NULL,
    title_hash          TEXT NOT NULL,          -- MD5/SHA hash for dedup

    -- Document info
    document_url        TEXT,
    document_type       TEXT,                   -- 'pdf', 'html', 'xls'

    -- Extracted content (if available)
    summary             TEXT,
    key_figures         TEXT,                   -- JSON: extracted numbers/metrics

    -- Metadata
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(symbol, announcement_date, title_hash)
);

CREATE INDEX IF NOT EXISTS idx_announcements_symbol
    ON corporate_announcements(symbol);
CREATE INDEX IF NOT EXISTS idx_announcements_date
    ON corporate_announcements(announcement_date);
CREATE INDEX IF NOT EXISTS idx_announcements_type
    ON corporate_announcements(announcement_type);

-- =============================================================================
-- COMPANY ANNOUNCEMENTS: Raw scraped announcements from PSX DPS
-- Source: POST /announcements endpoint
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_announcements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    company_name        TEXT,
    announcement_date   TEXT NOT NULL,              -- YYYY-MM-DD
    announcement_time   TEXT,                       -- HH:MM

    title               TEXT NOT NULL,
    category            TEXT,                       -- 'results', 'dividend', 'agm', 'board_meeting', 'book_closure', 'corporate_action', 'general'

    -- Document references (PSX IDs)
    image_id            TEXT,                       -- PSX image ID for images
    pdf_id              TEXT,                       -- PSX PDF document ID

    -- Metadata
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(symbol, announcement_date, announcement_time, title)
);

CREATE INDEX IF NOT EXISTS idx_company_announcements_symbol
    ON company_announcements(symbol);
CREATE INDEX IF NOT EXISTS idx_company_announcements_date
    ON company_announcements(announcement_date);
CREATE INDEX IF NOT EXISTS idx_company_announcements_category
    ON company_announcements(category);

-- =============================================================================
-- Equity Structure: Detailed ownership and capital structure
-- Primary key: (symbol, as_of_date)
-- =============================================================================
CREATE TABLE IF NOT EXISTS equity_structure (
    symbol              TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,          -- YYYY-MM-DD

    -- Share capital
    authorized_shares   INTEGER,
    issued_shares       INTEGER,
    outstanding_shares  INTEGER,
    treasury_shares     INTEGER,

    -- Float analysis
    free_float_shares   INTEGER,
    free_float_percent  REAL,

    -- Market cap
    market_cap          REAL,                   -- In local currency (PKR)
    market_cap_usd      REAL,                   -- In USD (if exchange rate available)

    -- Ownership breakdown (JSON for flexibility)
    ownership_data      TEXT,                   -- JSON: {institutions: x%, retail: y%, ...}

    -- Face value
    face_value          REAL,

    -- Metadata
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (symbol, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_equity_structure_symbol
    ON equity_structure(symbol);

-- =============================================================================
-- Scrape Jobs: Track scraping runs for data lineage (with background job support)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scrape_jobs (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT NOT NULL,          -- 'bulk_deep_scrape', 'company_snapshot', 'market_summary'
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'completed', 'failed', 'stopped'

    -- Scope
    symbols_requested   INTEGER DEFAULT 0,
    symbols_completed   INTEGER DEFAULT 0,
    symbols_failed      INTEGER DEFAULT 0,

    -- Results
    records_inserted    INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,

    -- Error tracking
    errors              TEXT,                   -- JSON array of errors

    -- Metadata
    config              TEXT,                   -- JSON: job configuration

    -- Background Job Support
    stop_requested      INTEGER DEFAULT 0,      -- 1 = stop requested by user
    current_symbol      TEXT,                   -- Symbol currently being processed
    current_batch       INTEGER DEFAULT 0,      -- Current batch number
    total_batches       INTEGER DEFAULT 0,      -- Total number of batches
    batch_size          INTEGER DEFAULT 50,     -- Symbols per batch
    batch_pause_sec     INTEGER DEFAULT 30,     -- Pause between batches (seconds)
    pid                 INTEGER,                -- Process ID of worker
    last_heartbeat      TEXT,                   -- Last update timestamp (for monitoring)
    notification_sent   INTEGER DEFAULT 0       -- 1 = completion notification sent
);

CREATE INDEX IF NOT EXISTS idx_scrape_jobs_status
    ON scrape_jobs(status);
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_type
    ON scrape_jobs(job_type);

-- =============================================================================
-- Job Notifications: Store notifications for UI to display
-- =============================================================================
CREATE TABLE IF NOT EXISTS job_notifications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              TEXT NOT NULL,
    notification_type   TEXT NOT NULL,          -- 'completed', 'failed', 'stopped', 'progress'
    title               TEXT NOT NULL,
    message             TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    read_at             TEXT,                   -- NULL = unread
    FOREIGN KEY (job_id) REFERENCES scrape_jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_job_notifications_unread
    ON job_notifications(read_at) WHERE read_at IS NULL;

-- =============================================================================
-- PSX INDICES: KSE-100, KSE-30, KMI-30, and other market indices
-- Stores daily index values with full statistics
-- =============================================================================
CREATE TABLE IF NOT EXISTS psx_indices (
    index_code          TEXT NOT NULL,              -- 'KSE100', 'KSE30', 'KMI30', etc.
    index_date          TEXT NOT NULL,              -- YYYY-MM-DD
    index_time          TEXT,                       -- HH:MM:SS when captured

    -- Core values
    value               REAL NOT NULL,              -- Current index value
    change              REAL,                       -- Point change
    change_pct          REAL,                       -- Percentage change

    -- OHLV for the day
    open                REAL,
    high                REAL,
    low                 REAL,
    volume              INTEGER,                    -- Total market volume

    -- References
    previous_close      REAL,

    -- Extended stats
    ytd_change_pct      REAL,                       -- Year-to-date change
    one_year_change_pct REAL,                       -- 1-year change
    week_52_low         REAL,
    week_52_high        REAL,

    -- Metadata
    trades              INTEGER,                    -- Number of trades
    market_cap          REAL,                       -- Total market cap (if available)
    turnover            REAL,                       -- Total turnover in PKR

    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (index_code, index_date)
);

CREATE INDEX IF NOT EXISTS idx_psx_indices_code
    ON psx_indices(index_code);
CREATE INDEX IF NOT EXISTS idx_psx_indices_date
    ON psx_indices(index_date);

-- =============================================================================
-- PSX MARKET SUMMARY: Daily market-wide statistics
-- =============================================================================
CREATE TABLE IF NOT EXISTS psx_market_stats (
    stat_date           TEXT NOT NULL,              -- YYYY-MM-DD
    stat_time           TEXT,                       -- HH:MM:SS when captured
    board_type          TEXT NOT NULL DEFAULT 'MAIN',  -- 'MAIN', 'GEM', 'DEBT'

    -- Trading segments summary
    reg_trades          INTEGER,
    reg_volume          INTEGER,
    reg_value           REAL,
    reg_state           TEXT,

    fut_trades          INTEGER,
    fut_volume          INTEGER,
    fut_value           REAL,
    fut_state           TEXT,

    csf_trades          INTEGER,
    csf_volume          INTEGER,
    csf_value           REAL,
    csf_state           TEXT,

    odl_trades          INTEGER,
    odl_volume          INTEGER,
    odl_value           REAL,
    odl_state           TEXT,

    squareup_trades     INTEGER,
    squareup_volume     INTEGER,
    squareup_value      REAL,
    squareup_state      TEXT,

    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (stat_date, board_type)
);

CREATE INDEX IF NOT EXISTS idx_psx_market_stats_date
    ON psx_market_stats(stat_date);

-- =============================================================================
-- CORPORATE EVENTS: AGM, EOGM, and other scheduled corporate events
-- Source: PSX DPS /calendar endpoint
-- =============================================================================
CREATE TABLE IF NOT EXISTS corporate_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            INTEGER UNIQUE,             -- PSX event ID from API
    symbol              TEXT NOT NULL,
    company_name        TEXT,

    -- Event details
    event_type          TEXT NOT NULL,              -- 'AGM', 'EOGM', 'BOARD_MEETING', etc.
    event_date          TEXT NOT NULL,              -- YYYY-MM-DD
    event_time          TEXT,                       -- HH:MM:SS
    city                TEXT,                       -- Event location city
    venue               TEXT,                       -- Full venue details

    -- Financial period
    period_end          TEXT,                       -- Fiscal period end date (YYYY-MM-DD)

    -- Status tracking
    status              TEXT DEFAULT 'scheduled',   -- 'scheduled', 'completed', 'cancelled'

    -- Metadata
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(symbol, event_date, event_type)
);

CREATE INDEX IF NOT EXISTS idx_corporate_events_symbol
    ON corporate_events(symbol);
CREATE INDEX IF NOT EXISTS idx_corporate_events_date
    ON corporate_events(event_date);
CREATE INDEX IF NOT EXISTS idx_corporate_events_type
    ON corporate_events(event_type);

-- =============================================================================
-- DIVIDEND PAYOUTS: Historical dividend declarations and payments
-- Source: PSX DPS /company/payouts endpoint
-- =============================================================================
CREATE TABLE IF NOT EXISTS dividend_payouts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,

    -- Announcement info
    announcement_date   TEXT NOT NULL,              -- YYYY-MM-DD
    announcement_time   TEXT,                       -- HH:MM:SS

    -- Dividend details
    fiscal_period       TEXT,                       -- e.g., 'Q1 2024', 'FY 2023'
    dividend_percent    REAL,                       -- Dividend percentage (e.g., 50 for 50%)
    dividend_type       TEXT,                       -- 'cash', 'stock', 'interim', 'final'
    dividend_number     TEXT,                       -- e.g., '1st Interim', '2nd Interim', 'Final'

    -- Book closure dates
    book_closure_from   TEXT,                       -- YYYY-MM-DD
    book_closure_to     TEXT,                       -- YYYY-MM-DD

    -- Payment info (if available)
    record_date         TEXT,                       -- YYYY-MM-DD
    payment_date        TEXT,                       -- YYYY-MM-DD
    dividend_per_share  REAL,                       -- Actual amount per share

    -- Metadata
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(symbol, announcement_date, dividend_number)
);

CREATE INDEX IF NOT EXISTS idx_dividend_payouts_symbol
    ON dividend_payouts(symbol);
CREATE INDEX IF NOT EXISTS idx_dividend_payouts_date
    ON dividend_payouts(announcement_date);
CREATE INDEX IF NOT EXISTS idx_dividend_payouts_type
    ON dividend_payouts(dividend_type);

-- =============================================================================
-- ANNOUNCEMENTS SYNC STATUS: Track sync progress for resumable scraping
-- =============================================================================
CREATE TABLE IF NOT EXISTS announcements_sync_status (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_type           TEXT NOT NULL,              -- 'announcements', 'events', 'dividends'
    symbol              TEXT,                       -- NULL for all-symbols sync
    last_sync_date      TEXT,                       -- Last successful sync date
    last_page           INTEGER DEFAULT 0,          -- Last page processed (for pagination)
    total_records       INTEGER DEFAULT 0,          -- Total records synced
    status              TEXT DEFAULT 'pending',     -- 'pending', 'running', 'completed', 'failed'
    error_message       TEXT,
    started_at          TEXT,
    completed_at        TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(sync_type, symbol)
);

CREATE INDEX IF NOT EXISTS idx_announcements_sync_type
    ON announcements_sync_status(sync_type);

-- =============================================================================
-- PHASE 1: INSTRUMENTS UNIVERSE
-- Extends instrument coverage beyond equities to include ETFs, REITs, and Indexes
-- =============================================================================

-- Master table for all tradeable instruments
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id       TEXT PRIMARY KEY,           -- Stable ID like "PSX:OGDC" or "IDX:KSE100"
    symbol              TEXT NOT NULL,              -- Display symbol used in DPS or internal code
    name                TEXT,                       -- Full instrument name
    instrument_type     TEXT NOT NULL,              -- 'EQUITY'|'ETF'|'REIT'|'INDEX'
    exchange            TEXT NOT NULL DEFAULT 'PSX',
    currency            TEXT NOT NULL DEFAULT 'PKR',
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL,              -- 'DPS'|'MANUAL'
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(exchange, symbol)
);

CREATE INDEX IF NOT EXISTS idx_instruments_type
    ON instruments(instrument_type);
CREATE INDEX IF NOT EXISTS idx_instruments_symbol
    ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_active
    ON instruments(is_active);

-- Index/ETF membership and weights (e.g., KSE-100 constituents)
CREATE TABLE IF NOT EXISTS instrument_membership (
    parent_instrument_id TEXT NOT NULL,             -- Index or ETF ID
    child_instrument_id  TEXT NOT NULL,             -- Constituent instrument ID
    weight               REAL,                      -- Weight in parent (0-1 or percentage)
    effective_date       TEXT NOT NULL DEFAULT '',  -- Date this weight became effective
    source               TEXT NOT NULL DEFAULT 'MANUAL',

    PRIMARY KEY(parent_instrument_id, child_instrument_id, effective_date),
    FOREIGN KEY(parent_instrument_id) REFERENCES instruments(instrument_id),
    FOREIGN KEY(child_instrument_id) REFERENCES instruments(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_membership_parent
    ON instrument_membership(parent_instrument_id);
CREATE INDEX IF NOT EXISTS idx_membership_child
    ON instrument_membership(child_instrument_id);

-- OHLCV data for non-equity instruments (ETFs, REITs, Indexes)
-- Separate from eod_ohlcv to avoid schema conflicts
CREATE TABLE IF NOT EXISTS ohlcv_instruments (
    instrument_id       TEXT NOT NULL,
    date                TEXT NOT NULL,
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL,
    volume              INTEGER,
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(instrument_id, date),
    FOREIGN KEY(instrument_id) REFERENCES instruments(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_instruments_date
    ON ohlcv_instruments(date);
CREATE INDEX IF NOT EXISTS idx_ohlcv_instruments_id
    ON ohlcv_instruments(instrument_id);

-- Performance rankings for instruments (flat structure for easy querying)
CREATE TABLE IF NOT EXISTS instrument_rankings (
    as_of_date          TEXT NOT NULL,
    instrument_id       TEXT NOT NULL,
    instrument_type     TEXT NOT NULL,              -- 'ETF'|'REIT'|'INDEX'
    return_1m           REAL,                       -- 1-month return (decimal, e.g., 0.05 = 5%)
    return_3m           REAL,                       -- 3-month return
    return_6m           REAL,                       -- 6-month return
    return_1y           REAL,                       -- 1-year return
    volatility_30d      REAL,                       -- 30-day annualized volatility
    relative_strength   REAL,                       -- Relative strength vs KSE-100
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(as_of_date, instrument_id),
    FOREIGN KEY(instrument_id) REFERENCES instruments(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_rankings_date
    ON instrument_rankings(as_of_date);
CREATE INDEX IF NOT EXISTS idx_rankings_type
    ON instrument_rankings(instrument_type);
CREATE INDEX IF NOT EXISTS idx_rankings_instrument
    ON instrument_rankings(instrument_id);

-- Sync tracking for instrument EOD data
CREATE TABLE IF NOT EXISTS instruments_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    instrument_types    TEXT NOT NULL,              -- Comma-separated types synced
    instruments_total   INTEGER DEFAULT 0,
    instruments_ok      INTEGER DEFAULT 0,
    instruments_failed  INTEGER DEFAULT 0,
    instruments_no_data INTEGER DEFAULT 0,          -- Instruments with no DPS data
    rows_upserted       INTEGER DEFAULT 0
);

-- =============================================================================
-- Phase 2: FX Analytics Tables
-- =============================================================================

-- FX currency pairs master table
CREATE TABLE IF NOT EXISTS fx_pairs (
    pair                TEXT PRIMARY KEY,           -- e.g. "USD/PKR"
    base_currency       TEXT NOT NULL,              -- e.g. "USD"
    quote_currency      TEXT NOT NULL,              -- e.g. "PKR"
    source              TEXT NOT NULL,              -- e.g. "SBP" | "OPEN_API" | "MANUAL"
    description         TEXT,                       -- Optional description
    is_active           INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- FX OHLCV data (daily rates)
CREATE TABLE IF NOT EXISTS fx_ohlcv (
    pair                TEXT NOT NULL,
    date                TEXT NOT NULL,
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL NOT NULL,              -- Close is required
    volume              REAL,                       -- FX volume (may be null)
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(pair, date),
    FOREIGN KEY(pair) REFERENCES fx_pairs(pair)
);

CREATE INDEX IF NOT EXISTS idx_fx_ohlcv_pair
    ON fx_ohlcv(pair);
CREATE INDEX IF NOT EXISTS idx_fx_ohlcv_date
    ON fx_ohlcv(date);

-- FX-adjusted equity metrics (derived analytics)
CREATE TABLE IF NOT EXISTS fx_adjusted_metrics (
    as_of_date          TEXT NOT NULL,
    symbol              TEXT NOT NULL,              -- Equity/index symbol
    fx_pair             TEXT NOT NULL,              -- e.g. "USD/PKR"
    equity_return       REAL,                       -- Equity return (decimal)
    fx_return           REAL,                       -- FX return (decimal)
    fx_adjusted_return  REAL,                       -- equity_return - fx_return
    period              TEXT NOT NULL DEFAULT '1M', -- '1W', '1M', '3M', etc.
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(as_of_date, symbol, fx_pair, period),
    FOREIGN KEY(fx_pair) REFERENCES fx_pairs(pair)
);

CREATE INDEX IF NOT EXISTS idx_fx_adjusted_date
    ON fx_adjusted_metrics(as_of_date);
CREATE INDEX IF NOT EXISTS idx_fx_adjusted_symbol
    ON fx_adjusted_metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_fx_adjusted_pair
    ON fx_adjusted_metrics(fx_pair);

-- FX sync tracking
CREATE TABLE IF NOT EXISTS fx_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    pairs_synced        TEXT NOT NULL,              -- Comma-separated pairs
    status              TEXT DEFAULT 'running',     -- 'running', 'completed', 'failed'
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);

-- =============================================================================
-- Phase 2.5: MUTUAL FUND TABLES (MUFAP Integration)
-- =============================================================================

-- Mutual fund master table
CREATE TABLE IF NOT EXISTS mutual_funds (
    fund_id             TEXT PRIMARY KEY,           -- e.g., "MUFAP:ABL-ISF"
    symbol              TEXT NOT NULL UNIQUE,       -- Short code e.g., "ABL-ISF"
    fund_name           TEXT NOT NULL,
    amc_code            TEXT NOT NULL,              -- Asset Management Company code
    amc_name            TEXT,                       -- AMC full name
    fund_type           TEXT NOT NULL,              -- 'OPEN_END' | 'VPS' | 'ETF'
    category            TEXT NOT NULL,              -- 'Equity', 'Money Market', etc.
    is_shariah          INTEGER NOT NULL DEFAULT 0, -- 1 = Shariah-compliant
    launch_date         TEXT,                       -- Fund inception date
    expense_ratio       REAL,                       -- Annual expense ratio (%)
    management_fee      REAL,                       -- Management fee (%)
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MUFAP',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_mutual_funds_category
    ON mutual_funds(category);
CREATE INDEX IF NOT EXISTS idx_mutual_funds_amc
    ON mutual_funds(amc_code);
CREATE INDEX IF NOT EXISTS idx_mutual_funds_type
    ON mutual_funds(fund_type);
CREATE INDEX IF NOT EXISTS idx_mutual_funds_shariah
    ON mutual_funds(is_shariah);
CREATE INDEX IF NOT EXISTS idx_mutual_funds_active
    ON mutual_funds(is_active);

-- Mutual fund NAV time-series
CREATE TABLE IF NOT EXISTS mutual_fund_nav (
    fund_id             TEXT NOT NULL,
    date                TEXT NOT NULL,              -- YYYY-MM-DD
    nav                 REAL NOT NULL,              -- Net Asset Value per unit
    offer_price         REAL,                       -- Offer/Sale price
    redemption_price    REAL,                       -- Redemption/Bid price
    aum                 REAL,                       -- Assets Under Management (millions PKR)
    nav_change_pct      REAL,                       -- Daily NAV change %
    source              TEXT NOT NULL DEFAULT 'MUFAP',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(fund_id, date),
    FOREIGN KEY(fund_id) REFERENCES mutual_funds(fund_id)
);

CREATE INDEX IF NOT EXISTS idx_mf_nav_date
    ON mutual_fund_nav(date);
CREATE INDEX IF NOT EXISTS idx_mf_nav_fund
    ON mutual_fund_nav(fund_id);

-- Mutual fund sync tracking
CREATE TABLE IF NOT EXISTS mutual_fund_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    sync_type           TEXT NOT NULL,              -- 'SEED' | 'NAV_SYNC'
    status              TEXT NOT NULL DEFAULT 'running', -- 'running' | 'completed' | 'failed' | 'partial'
    funds_total         INTEGER DEFAULT 0,
    funds_ok            INTEGER DEFAULT 0,
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_mf_sync_status
    ON mutual_fund_sync_runs(status);

-- =============================================================================
-- Phase 3: BONDS/SUKUK TABLES
-- =============================================================================

-- Bond master table
CREATE TABLE IF NOT EXISTS bonds_master (
    bond_id             TEXT PRIMARY KEY,           -- e.g., "PIB:3Y:2026-01-15"
    isin                TEXT UNIQUE,                -- ISIN code (nullable)
    symbol              TEXT NOT NULL,              -- Short display name
    issuer              TEXT NOT NULL,              -- 'GOP' | 'SBP' | corporate issuer
    bond_type           TEXT NOT NULL,              -- 'PIB', 'T-Bill', 'Sukuk', 'TFC', 'Corporate'
    is_islamic          INTEGER NOT NULL DEFAULT 0, -- 1 = Sukuk
    face_value          REAL NOT NULL DEFAULT 100,  -- Usually 100
    coupon_rate         REAL,                       -- Annual coupon rate (NULL for zero-coupon)
    coupon_frequency    INTEGER DEFAULT 2,          -- Payments per year (2=semi-annual)
    issue_date          TEXT,                       -- YYYY-MM-DD
    maturity_date       TEXT NOT NULL,              -- YYYY-MM-DD
    day_count           TEXT DEFAULT 'ACT/ACT',     -- Day count convention
    currency            TEXT NOT NULL DEFAULT 'PKR',
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    notes               TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_bonds_type
    ON bonds_master(bond_type);
CREATE INDEX IF NOT EXISTS idx_bonds_issuer
    ON bonds_master(issuer);
CREATE INDEX IF NOT EXISTS idx_bonds_maturity
    ON bonds_master(maturity_date);
CREATE INDEX IF NOT EXISTS idx_bonds_islamic
    ON bonds_master(is_islamic);
CREATE INDEX IF NOT EXISTS idx_bonds_active
    ON bonds_master(is_active);

-- Bond quotes (price/yield observations)
CREATE TABLE IF NOT EXISTS bond_quotes (
    bond_id             TEXT NOT NULL,
    date                TEXT NOT NULL,              -- YYYY-MM-DD
    price               REAL,                       -- Clean price (% of face)
    dirty_price         REAL,                       -- Price + accrued interest
    ytm                 REAL,                       -- Yield to maturity (decimal)
    bid_yield           REAL,
    ask_yield           REAL,
    bid_price           REAL,
    ask_price           REAL,
    volume              REAL,                       -- Trading volume if available
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(bond_id, date),
    FOREIGN KEY(bond_id) REFERENCES bonds_master(bond_id)
);

CREATE INDEX IF NOT EXISTS idx_bond_quotes_date
    ON bond_quotes(date);
CREATE INDEX IF NOT EXISTS idx_bond_quotes_bond
    ON bond_quotes(bond_id);

-- Yield curve points (interpolated/fitted curve)
CREATE TABLE IF NOT EXISTS yield_curve_points (
    curve_date          TEXT NOT NULL,              -- YYYY-MM-DD
    tenor_months        INTEGER NOT NULL,           -- Tenor in months (3, 6, 12, 24, 36, 60, 120)
    yield_rate          REAL NOT NULL,              -- Yield as decimal
    bond_type           TEXT NOT NULL DEFAULT 'PIB', -- 'PIB', 'T-Bill', 'Sukuk', 'ALL'
    interpolation       TEXT DEFAULT 'LINEAR',      -- 'LINEAR' | 'CUBIC' | 'NS'
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(curve_date, tenor_months, bond_type)
);

CREATE INDEX IF NOT EXISTS idx_yc_date
    ON yield_curve_points(curve_date);
CREATE INDEX IF NOT EXISTS idx_yc_type
    ON yield_curve_points(bond_type);

-- Bond analytics snapshots
CREATE TABLE IF NOT EXISTS bond_analytics_snapshots (
    bond_id             TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,
    price               REAL,
    ytm                 REAL,                       -- Yield to maturity
    duration            REAL,                       -- Macaulay duration (years)
    modified_duration   REAL,                       -- Modified duration
    convexity           REAL,                       -- Convexity
    accrued_interest    REAL,                       -- Accrued interest per face value
    spread_to_benchmark REAL,                       -- Spread vs benchmark curve
    days_to_maturity    INTEGER,
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY(bond_id, as_of_date),
    FOREIGN KEY(bond_id) REFERENCES bonds_master(bond_id)
);

CREATE INDEX IF NOT EXISTS idx_bond_analytics_date
    ON bond_analytics_snapshots(as_of_date);
CREATE INDEX IF NOT EXISTS idx_bond_analytics_bond
    ON bond_analytics_snapshots(bond_id);

-- Bond sync tracking
CREATE TABLE IF NOT EXISTS bond_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    sync_type           TEXT NOT NULL,              -- 'INIT' | 'LOAD_MASTER' | 'LOAD_QUOTES' | 'COMPUTE'
    status              TEXT NOT NULL DEFAULT 'running',
    items_total         INTEGER DEFAULT 0,
    items_ok            INTEGER DEFAULT 0,
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_bond_sync_status
    ON bond_sync_runs(status);

-- =============================================================================
-- Phase 3: SUKUK / DEBT MARKET TABLES (Regulator-Aligned)
-- =============================================================================

-- Sukuk master table (PSX GIS + SBP instruments)
CREATE TABLE IF NOT EXISTS sukuk_master (
    instrument_id       TEXT PRIMARY KEY,           -- e.g., "SUKUK:GOP-IJARA-2027"
    issuer              TEXT NOT NULL,              -- GOVT_OF_PAKISTAN or corporate
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,              -- GOP_SUKUK | CORP_SUKUK | PIB | T-BILL
    currency            TEXT NOT NULL DEFAULT 'PKR',
    issue_date          TEXT,                       -- YYYY-MM-DD
    maturity_date       TEXT NOT NULL,              -- YYYY-MM-DD
    coupon_rate         REAL,                       -- Annual rate (decimal)
    coupon_frequency    INTEGER,                    -- Payments per year
    face_value          REAL DEFAULT 100.0,
    issue_size          REAL,                       -- Total issue size
    shariah_compliant   INTEGER DEFAULT 1,          -- 1 = Sukuk, 0 = conventional
    is_active           INTEGER DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    notes               TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sukuk_category
    ON sukuk_master(category);
CREATE INDEX IF NOT EXISTS idx_sukuk_issuer
    ON sukuk_master(issuer);
CREATE INDEX IF NOT EXISTS idx_sukuk_maturity
    ON sukuk_master(maturity_date);
CREATE INDEX IF NOT EXISTS idx_sukuk_active
    ON sukuk_master(is_active);

-- Sukuk quotes (price/yield observations)
CREATE TABLE IF NOT EXISTS sukuk_quotes (
    instrument_id       TEXT NOT NULL,
    quote_date          TEXT NOT NULL,              -- YYYY-MM-DD
    clean_price         REAL,                       -- Price as % of face
    dirty_price         REAL,                       -- Clean + accrued
    yield_to_maturity   REAL,                       -- YTM as decimal
    bid_yield           REAL,
    ask_yield           REAL,
    volume              REAL,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    ingested_at         TEXT DEFAULT (datetime('now')),

    PRIMARY KEY(instrument_id, quote_date),
    FOREIGN KEY(instrument_id) REFERENCES sukuk_master(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_sukuk_quotes_date
    ON sukuk_quotes(quote_date);
CREATE INDEX IF NOT EXISTS idx_sukuk_quotes_instrument
    ON sukuk_quotes(instrument_id);

-- Sukuk yield curves
CREATE TABLE IF NOT EXISTS sukuk_yield_curve (
    curve_name          TEXT NOT NULL,              -- PKR_GOP_SUKUK | PKR_PIB | PKR_TBILL
    curve_date          TEXT NOT NULL,              -- YYYY-MM-DD
    tenor_days          INTEGER NOT NULL,           -- Days to maturity
    yield_rate          REAL NOT NULL,              -- Yield as decimal
    source              TEXT NOT NULL DEFAULT 'SBP',
    computed_at         TEXT DEFAULT (datetime('now')),

    PRIMARY KEY(curve_name, curve_date, tenor_days)
);

CREATE INDEX IF NOT EXISTS idx_sukuk_yc_name
    ON sukuk_yield_curve(curve_name);
CREATE INDEX IF NOT EXISTS idx_sukuk_yc_date
    ON sukuk_yield_curve(curve_date);

-- Sukuk analytics snapshots
CREATE TABLE IF NOT EXISTS sukuk_analytics_snapshots (
    instrument_id       TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,              -- YYYY-MM-DD
    price               REAL,
    ytm                 REAL,                       -- Yield to maturity
    macaulay_duration   REAL,                       -- Duration in years
    modified_duration   REAL,
    convexity           REAL,
    accrued_interest    REAL,
    days_to_maturity    INTEGER,
    computed_at         TEXT DEFAULT (datetime('now')),

    PRIMARY KEY(instrument_id, as_of_date),
    FOREIGN KEY(instrument_id) REFERENCES sukuk_master(instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_sukuk_analytics_date
    ON sukuk_analytics_snapshots(as_of_date);
CREATE INDEX IF NOT EXISTS idx_sukuk_analytics_instrument
    ON sukuk_analytics_snapshots(instrument_id);

-- SBP Primary Market Documents archive
CREATE TABLE IF NOT EXISTS sbp_primary_market_docs (
    doc_id              TEXT PRIMARY KEY,           -- hash(url + title)
    category            TEXT NOT NULL,              -- T-BILL | PIB | GOP_SUKUK
    title               TEXT NOT NULL,
    doc_date            TEXT,                       -- YYYY-MM-DD if available
    url                 TEXT NOT NULL,
    local_path          TEXT,                       -- Local file path if downloaded
    file_size           INTEGER,
    fetched_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sbp_docs_category
    ON sbp_primary_market_docs(category);
CREATE INDEX IF NOT EXISTS idx_sbp_docs_date
    ON sbp_primary_market_docs(doc_date);

-- Sukuk sync tracking
CREATE TABLE IF NOT EXISTS sukuk_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    sync_type           TEXT NOT NULL,              -- INIT | LOAD_MASTER | LOAD_QUOTES | COMPUTE | SBP_REFRESH
    status              TEXT NOT NULL DEFAULT 'running',
    items_total         INTEGER DEFAULT 0,
    items_ok            INTEGER DEFAULT 0,
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_sukuk_sync_status
    ON sukuk_sync_runs(status);

-- ==========================================================================
-- Phase 3: Fixed Income Tables (Government Debt + Sukuk)
-- ==========================================================================

-- Fixed income instruments master table
CREATE TABLE IF NOT EXISTS fi_instruments (
    instrument_id       TEXT PRIMARY KEY,
    isin                TEXT,
    issuer              TEXT NOT NULL DEFAULT 'GOVT_OF_PAKISTAN',
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,              -- MTB | PIB | GOP_SUKUK | CORP_BOND | CORP_SUKUK
    currency            TEXT NOT NULL DEFAULT 'PKR',
    issue_date          TEXT,
    maturity_date       TEXT NOT NULL,
    coupon_rate         REAL,                       -- annual decimal (0.185 = 18.5%)
    coupon_frequency    INTEGER,                    -- payments per year (2 typical)
    day_count           TEXT NOT NULL DEFAULT 'ACT/365',
    face_value          REAL NOT NULL DEFAULT 100.0,
    shariah_compliant   INTEGER NOT NULL DEFAULT 0,
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fi_instruments_category
    ON fi_instruments(category);

CREATE INDEX IF NOT EXISTS idx_fi_instruments_maturity
    ON fi_instruments(maturity_date);

-- Fixed income daily quotes (yield-first)
CREATE TABLE IF NOT EXISTS fi_quotes (
    instrument_id       TEXT NOT NULL,
    quote_date          TEXT NOT NULL,              -- YYYY-MM-DD
    clean_price         REAL,                       -- per 100 face value
    ytm                 REAL,                       -- yield to maturity (decimal)
    bid                 REAL,
    ask                 REAL,
    volume              REAL,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(instrument_id, quote_date)
);

CREATE INDEX IF NOT EXISTS idx_fi_quotes_date
    ON fi_quotes(quote_date);

-- Fixed income yield curves
CREATE TABLE IF NOT EXISTS fi_curves (
    curve_name          TEXT NOT NULL,              -- PKR_MTB | PKR_PIB | PKR_GOP_SUKUK
    curve_date          TEXT NOT NULL,
    tenor_days          INTEGER NOT NULL,
    rate                REAL NOT NULL,              -- decimal yield
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    PRIMARY KEY(curve_name, curve_date, tenor_days)
);

CREATE INDEX IF NOT EXISTS idx_fi_curves_date
    ON fi_curves(curve_date);

-- Fixed income computed analytics (bond math snapshots)
CREATE TABLE IF NOT EXISTS fi_analytics (
    instrument_id       TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,
    price               REAL,
    ytm                 REAL,
    macaulay_duration   REAL,
    modified_duration   REAL,
    convexity           REAL,
    pvbp                REAL,                       -- price value of 1bp
    PRIMARY KEY(instrument_id, as_of_date)
);

-- SBP Primary Market Activities document archive
CREATE TABLE IF NOT EXISTS sbp_pma_docs (
    doc_id              TEXT PRIMARY KEY,           -- sha256(url + title)
    category            TEXT NOT NULL,              -- MTB | PIB | GOP_SUKUK
    title               TEXT NOT NULL,
    doc_date            TEXT,
    url                 TEXT NOT NULL,
    local_path          TEXT,
    fetched_at          TEXT NOT NULL DEFAULT (datetime('now')),
    source              TEXT NOT NULL DEFAULT 'SBP_PMA'
);

CREATE INDEX IF NOT EXISTS idx_sbp_pma_docs_category
    ON sbp_pma_docs(category);

CREATE INDEX IF NOT EXISTS idx_sbp_pma_docs_date
    ON sbp_pma_docs(doc_date);

-- Fixed income events (structured facts from auction docs)
CREATE TABLE IF NOT EXISTS fi_events (
    event_id            TEXT PRIMARY KEY,           -- sha256(category+date+title)
    category            TEXT NOT NULL,              -- MTB | PIB | GOP_SUKUK
    event_date          TEXT NOT NULL,
    label               TEXT NOT NULL,              -- "Auction Result", "Tender Notice", etc.
    notes               TEXT,
    source_doc_id       TEXT,                       -- link to sbp_pma_docs
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fi_events_category
    ON fi_events(category);

CREATE INDEX IF NOT EXISTS idx_fi_events_date
    ON fi_events(event_date);

-- Fixed income sync runs audit
CREATE TABLE IF NOT EXISTS fi_sync_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    sync_type           TEXT NOT NULL,              -- INIT | LOAD | COMPUTE | SBP_REFRESH
    status              TEXT NOT NULL DEFAULT 'running',
    items_total         INTEGER DEFAULT 0,
    items_ok            INTEGER DEFAULT 0,
    rows_upserted       INTEGER DEFAULT 0,
    error_message       TEXT
);

-- SBP Policy Rates (monetary policy rates)
CREATE TABLE IF NOT EXISTS sbp_policy_rates (
    rate_date           TEXT PRIMARY KEY,           -- YYYY-MM-DD
    policy_rate         REAL,                       -- SBP Policy Rate (decimal)
    ceiling_rate        REAL,                       -- Overnight Reverse Repo ceiling
    floor_rate          REAL,                       -- Overnight Repo floor
    overnight_repo_rate REAL,                       -- Weighted avg overnight repo
    source              TEXT NOT NULL DEFAULT 'SBP_MSM',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

-- KIBOR Rates (interbank offered rates)
CREATE TABLE IF NOT EXISTS kibor_rates (
    rate_date           TEXT NOT NULL,              -- YYYY-MM-DD
    tenor_months        INTEGER NOT NULL,           -- 3, 6, or 12
    bid                 REAL NOT NULL,
    offer               REAL NOT NULL,
    source              TEXT NOT NULL DEFAULT 'SBP_MSM',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(rate_date, tenor_months)
);

CREATE INDEX IF NOT EXISTS idx_kibor_rates_date
    ON kibor_rates(rate_date);
"""


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """
    Connect to SQLite database with WAL mode enabled.

    Args:
        db_path: Path to database file. If None, uses default from config.
                 Use ":memory:" for in-memory database.

    Returns:
        sqlite3.Connection with row_factory set to sqlite3.Row
    """
    if db_path == ":memory:":
        path = ":memory:"
    else:
        path = get_db_path(db_path)
        ensure_dirs(path)
        path = str(path)

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row

    # Enable WAL mode for better concurrent access (not for :memory:)
    if db_path != ":memory:":
        con.execute("PRAGMA journal_mode=WAL")

    return con


def init_schema(con: sqlite3.Connection) -> None:
    """
    Initialize database schema.

    Creates all tables if they don't exist, and runs migrations
    to add any new columns to existing tables.
    """
    con.executescript(SCHEMA_SQL)
    con.commit()

    # Run migrations for new columns in existing tables
    _migrate_symbols_table(con)
    _migrate_eod_ohlcv_table(con)
    _migrate_scrape_jobs_table(con)


def _migrate_symbols_table(con: sqlite3.Connection) -> None:
    """Add new columns to symbols table if they don't exist."""
    # Get existing columns
    cursor = con.execute("PRAGMA table_info(symbols)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # Add sector_name column if missing
    if "sector_name" not in existing_cols:
        con.execute("ALTER TABLE symbols ADD COLUMN sector_name TEXT NULL")

    # Add outstanding_shares column if missing
    if "outstanding_shares" not in existing_cols:
        con.execute("ALTER TABLE symbols ADD COLUMN outstanding_shares REAL NULL")

    # Add source column if missing
    if "source" not in existing_cols:
        con.execute(
            "ALTER TABLE symbols ADD COLUMN source TEXT NOT NULL DEFAULT 'MARKET_WATCH'"
        )

    con.commit()


def _migrate_eod_ohlcv_table(con: sqlite3.Connection) -> None:
    """Add new columns to eod_ohlcv table if they don't exist."""
    cursor = con.execute("PRAGMA table_info(eod_ohlcv)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # Add prev_close column if missing
    if "prev_close" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN prev_close REAL")

    # Add sector_code column if missing
    if "sector_code" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN sector_code TEXT")

    # Add company_name column if missing
    if "company_name" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN company_name TEXT")

    con.commit()


def _migrate_scrape_jobs_table(con: sqlite3.Connection) -> None:
    """Add new columns to scrape_jobs table for background job support."""
    cursor = con.execute("PRAGMA table_info(scrape_jobs)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # New columns for background job support
    new_columns = [
        ("stop_requested", "INTEGER DEFAULT 0"),
        ("current_symbol", "TEXT"),
        ("current_batch", "INTEGER DEFAULT 0"),
        ("total_batches", "INTEGER DEFAULT 0"),
        ("batch_size", "INTEGER DEFAULT 50"),
        ("batch_pause_sec", "INTEGER DEFAULT 30"),
        ("pid", "INTEGER"),
        ("last_heartbeat", "TEXT"),
        ("notification_sent", "INTEGER DEFAULT 0"),
    ]

    for col_name, col_def in new_columns:
        if col_name not in existing_cols:
            con.execute(f"ALTER TABLE scrape_jobs ADD COLUMN {col_name} {col_def}")

    # Create job_notifications table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS job_notifications (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id              TEXT NOT NULL,
            notification_type   TEXT NOT NULL,
            title               TEXT NOT NULL,
            message             TEXT,
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            read_at             TEXT,
            FOREIGN KEY (job_id) REFERENCES scrape_jobs(job_id)
        )
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_notifications_unread
        ON job_notifications(read_at) WHERE read_at IS NULL
    """)

    con.commit()


def upsert_symbols(con: sqlite3.Connection, symbols: list[dict]) -> int:
    """
    Upsert symbols into the symbols table.

    Args:
        con: Database connection
        symbols: List of dicts with keys: symbol, name (optional), sector (optional)

    Returns:
        Number of rows inserted or updated
    """
    if not symbols:
        return 0

    now = now_iso()
    count = 0

    for sym in symbols:
        symbol = sym.get("symbol")
        if not symbol:
            continue

        name = sym.get("name")
        sector = sym.get("sector")

        # Try insert, on conflict update
        cur = con.execute(
            """
            INSERT INTO symbols
                (symbol, name, sector, is_active, discovered_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = COALESCE(excluded.name, symbols.name),
                sector = COALESCE(excluded.sector, symbols.sector),
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (symbol, name, sector, now, now),
        )
        count += cur.rowcount

    con.commit()
    return count


def upsert_eod(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert EOD OHLCV data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: symbol, date, open, high, low, close, volume
            Optional columns: prev_close, sector_code, company_name

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"symbol", "date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        cur = con.execute(
            """
            INSERT INTO eod_ohlcv
                (symbol, date, open, high, low, close, volume,
                 prev_close, sector_code, company_name, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                prev_close = excluded.prev_close,
                sector_code = excluded.sector_code,
                company_name = excluded.company_name,
                ingested_at = excluded.ingested_at
            """,
            (
                row["symbol"],
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row.get("prev_close"),
                row.get("sector_code"),
                row.get("company_name"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def record_sync_run_start(
    con: sqlite3.Connection, mode: str, symbols_total: int
) -> str:
    """
    Record the start of a sync run.

    Args:
        con: Database connection
        mode: Sync mode (e.g., 'full', 'incremental', 'symbols_only')
        symbols_total: Total number of symbols to sync

    Returns:
        run_id (UUID string)
    """
    run_id = str(uuid.uuid4())
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_runs (run_id, started_at, mode, symbols_total)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, now, mode, symbols_total),
    )
    con.commit()

    return run_id


def record_sync_run_end(
    con: sqlite3.Connection,
    run_id: str,
    symbols_ok: int,
    symbols_failed: int,
    rows_upserted: int,
) -> None:
    """
    Record the end of a sync run.

    Args:
        con: Database connection
        run_id: The run ID returned by record_sync_run_start
        symbols_ok: Number of symbols successfully synced
        symbols_failed: Number of symbols that failed
        rows_upserted: Total number of EOD rows upserted
    """
    now = now_iso()

    con.execute(
        """
        UPDATE sync_runs
        SET ended_at = ?,
            symbols_ok = ?,
            symbols_failed = ?,
            rows_upserted = ?
        WHERE run_id = ?
        """,
        (now, symbols_ok, symbols_failed, rows_upserted, run_id),
    )
    con.commit()


def record_failure(
    con: sqlite3.Connection,
    run_id: str,
    symbol: str,
    error_type: str,
    error_message: str | None,
) -> None:
    """
    Record a sync failure for a specific symbol.

    Args:
        con: Database connection
        run_id: The run ID
        symbol: The symbol that failed
        error_type: Type of error (e.g., 'HTTP_ERROR', 'PARSE_ERROR')
        error_message: Detailed error message
    """
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_failures
            (run_id, symbol, error_type, error_message, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, symbol, error_type, error_message, now),
    )
    con.commit()


def get_symbols_list(con: sqlite3.Connection, limit: int | None = None) -> list[str]:
    """
    Get list of active symbols in sorted order.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        List of symbol strings, sorted alphabetically
    """
    query = "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    cur = con.execute(query)
    return [row["symbol"] for row in cur.fetchall()]


def get_symbols_string(con: sqlite3.Connection, limit: int | None = None) -> str:
    """
    Get comma-separated string of active symbols.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        Comma-separated string of symbols, sorted alphabetically
    """
    symbols = get_symbols_list(con, limit)
    return ",".join(symbols)


def get_max_date_for_symbol(con: sqlite3.Connection, symbol: str) -> str | None:
    """
    Get the most recent date for a symbol in eod_ohlcv table.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Max date as string (YYYY-MM-DD) or None if no data exists
    """
    cur = con.execute(
        "SELECT MAX(date) as max_date FROM eod_ohlcv WHERE symbol = ?",
        (symbol,),
    )
    row = cur.fetchone()
    if row and row["max_date"]:
        return row["max_date"]
    return None


def get_date_range_for_symbol(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get date range statistics for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_date, max_date, row_count, or all None if no data
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as row_count
        FROM eod_ohlcv
        WHERE symbol = ?
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if row and row["row_count"] > 0:
        return {
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "row_count": row["row_count"],
        }
    return {"min_date": None, "max_date": None, "row_count": 0}


def get_data_coverage_summary(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Get summary of data coverage across all symbols.

    Returns:
        DataFrame with columns: symbol, min_date, max_date, row_count, days_missing
    """
    cur = con.execute(
        """
        SELECT
            symbol,
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as row_count,
            CAST(
                julianday(MAX(date)) - julianday(MIN(date)) + 1
            AS INTEGER) as days_span
        FROM eod_ohlcv
        GROUP BY symbol
        ORDER BY symbol
        """
    )

    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["symbol", "min_date", "max_date", "row_count", "days_span"]
        )

    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)

    # Calculate days with data vs expected
    df["data_coverage_pct"] = (df["row_count"] / df["days_span"] * 100).round(1)

    return df


def get_global_date_stats(con: sqlite3.Connection) -> dict:
    """
    Get global date statistics across all data.

    Returns:
        Dict with global_min_date, global_max_date, total_rows, unique_symbols
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as global_min_date,
            MAX(date) as global_max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM eod_ohlcv
        """
    )
    row = cur.fetchone()
    if row and row["total_rows"] > 0:
        return {
            "global_min_date": row["global_min_date"],
            "global_max_date": row["global_max_date"],
            "total_rows": row["total_rows"],
            "unique_symbols": row["unique_symbols"],
        }
    return {
        "global_min_date": None,
        "global_max_date": None,
        "total_rows": 0,
        "unique_symbols": 0,
    }


# =============================================================================
# Intraday Functions
# =============================================================================


def _parse_ts_to_epoch(ts: str) -> int:
    """
    Parse a timestamp string to Unix epoch (seconds).

    Handles formats:
    - YYYY-MM-DD HH:MM:SS
    - YYYY-MM-DDTHH:MM:SS
    - ISO format with timezone

    Args:
        ts: Timestamp string

    Returns:
        Unix epoch in seconds
    """
    from datetime import datetime

    ts = str(ts).strip()

    # Try common formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts[:19], fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    # Fallback: try pandas
    try:
        dt = pd.to_datetime(ts)
        return int(dt.timestamp())
    except Exception:
        # Last resort: return 0
        return 0


def upsert_intraday(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert intraday bars data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: symbol, ts, open, high, low, close, volume
            Optionally ts_epoch (will be computed if missing)

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"symbol", "ts"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        # Compute ts_epoch if not provided
        ts_epoch = row.get("ts_epoch")
        if ts_epoch is None or pd.isna(ts_epoch):
            ts_epoch = _parse_ts_to_epoch(row["ts"])

        cur = con.execute(
            """
            INSERT INTO intraday_bars
                (symbol, ts, ts_epoch, open, high, low, close, volume,
                 interval, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'int', ?)
            ON CONFLICT(symbol, ts) DO UPDATE SET
                ts_epoch = excluded.ts_epoch,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                ingested_at = excluded.ingested_at
            """,
            (
                row["symbol"],
                row["ts"],
                int(ts_epoch),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_intraday_sync_state(
    con: sqlite3.Connection, symbol: str
) -> tuple[str | None, int | None]:
    """
    Get the last synced timestamp for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Tuple of (last_ts string, last_ts_epoch integer) or (None, None)
    """
    cur = con.execute(
        "SELECT last_ts, last_ts_epoch FROM intraday_sync_state WHERE symbol = ?",
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row["last_ts"]:
        return row["last_ts"], row["last_ts_epoch"]
    return None, None


def update_intraday_sync_state(
    con: sqlite3.Connection, symbol: str, last_ts: str, last_ts_epoch: int | None = None
) -> None:
    """
    Update the sync state for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol
        last_ts: Latest timestamp that was synced
        last_ts_epoch: Unix epoch of last_ts (computed if not provided)
    """
    if last_ts_epoch is None:
        last_ts_epoch = _parse_ts_to_epoch(last_ts)

    now = now_iso()
    con.execute(
        """
        INSERT INTO intraday_sync_state (symbol, last_ts, last_ts_epoch, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            last_ts = excluded.last_ts,
            last_ts_epoch = excluded.last_ts_epoch,
            updated_at = excluded.updated_at
        """,
        (symbol.upper(), last_ts, last_ts_epoch, now),
    )
    con.commit()


def get_intraday_range(
    con: sqlite3.Connection,
    symbol: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
    start_epoch: int | None = None,
    end_epoch: int | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    """
    Get intraday bars for a symbol within a time range.

    Args:
        con: Database connection
        symbol: Stock symbol
        start_ts: Optional start timestamp string (inclusive)
        end_ts: Optional end timestamp string (inclusive)
        start_epoch: Optional start epoch (takes precedence over start_ts)
        end_epoch: Optional end epoch (takes precedence over end_ts)
        limit: Maximum rows to return (default 2000)

    Returns:
        DataFrame with columns: symbol, ts, ts_epoch, open, high, low, close, volume
        Sorted by ts_epoch ascending (oldest first).
    """
    query = """
        SELECT symbol, ts, ts_epoch, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    # Use epoch for filtering if provided, otherwise convert ts to epoch
    if start_epoch is not None:
        query += " AND ts_epoch >= ?"
        params.append(start_epoch)
    elif start_ts:
        query += " AND ts_epoch >= ?"
        params.append(_parse_ts_to_epoch(start_ts))

    if end_epoch is not None:
        query += " AND ts_epoch <= ?"
        params.append(end_epoch)
    elif end_ts:
        query += " AND ts_epoch <= ?"
        params.append(_parse_ts_to_epoch(end_ts))

    query += " ORDER BY ts_epoch DESC LIMIT ?"
    params.append(limit)

    df = pd.read_sql_query(query, con, params=params)

    # Sort ascending for display
    if not df.empty:
        df = df.sort_values("ts_epoch").reset_index(drop=True)

    return df


def get_intraday_latest(
    con: sqlite3.Connection, symbol: str, limit: int = 500
) -> pd.DataFrame:
    """
    Get the most recent intraday bars for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return (default 500)

    Returns:
        DataFrame with columns: symbol, ts, ts_epoch, open, high, low, close, volume
        Sorted by ts_epoch ascending (oldest first).
    """
    query = """
        SELECT symbol, ts, ts_epoch, open, high, low, close, volume
        FROM intraday_bars
        WHERE symbol = ?
        ORDER BY ts_epoch DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, con, params=[symbol.upper(), limit])

    # Sort ascending for display
    if not df.empty:
        df = df.sort_values("ts_epoch").reset_index(drop=True)

    return df


def get_intraday_stats(con: sqlite3.Connection, symbol: str) -> dict:
    """
    Get statistics for a symbol's intraday data.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with min_ts, max_ts, row_count
    """
    cur = con.execute(
        """
        SELECT
            MIN(ts) as min_ts,
            MAX(ts) as max_ts,
            COUNT(*) as row_count
        FROM intraday_bars
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row and row["row_count"] > 0:
        return {
            "min_ts": row["min_ts"],
            "max_ts": row["max_ts"],
            "row_count": row["row_count"],
        }
    return {"min_ts": None, "max_ts": None, "row_count": 0}


# =============================================================================
# Sector Functions
# =============================================================================


def upsert_sectors(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert sectors data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: sector_code, sector_name

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"sector_code", "sector_name"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        cur = con.execute(
            """
            INSERT INTO sectors (sector_code, sector_name, updated_at, source)
            VALUES (?, ?, ?, 'DPS_SECTOR_SUMMARY')
            ON CONFLICT(sector_code) DO UPDATE SET
                sector_name = excluded.sector_name,
                updated_at = excluded.updated_at
            """,
            (row["sector_code"], row["sector_name"], now),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_sectors(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Get all sectors from the database.

    Args:
        con: Database connection

    Returns:
        DataFrame with columns: sector_code, sector_name, updated_at, source
    """
    query = """
        SELECT sector_code, sector_name, updated_at, source
        FROM sectors
        ORDER BY sector_code
    """
    return pd.read_sql_query(query, con)


def get_sector_name(con: sqlite3.Connection, sector_code: str) -> str | None:
    """
    Get sector name for a given sector code.

    Args:
        con: Database connection
        sector_code: Sector code (e.g., '0101')

    Returns:
        Sector name or None if not found
    """
    cur = con.execute(
        "SELECT sector_name FROM sectors WHERE sector_code = ?",
        (sector_code,),
    )
    row = cur.fetchone()
    return row["sector_name"] if row else None


def get_sector_map(con: sqlite3.Connection) -> dict[str, str]:
    """
    Get a mapping of sector_code -> sector_name.

    Args:
        con: Database connection

    Returns:
        Dict mapping sector codes to sector names
    """
    cur = con.execute("SELECT sector_code, sector_name FROM sectors")
    return {row["sector_code"]: row["sector_name"] for row in cur.fetchall()}


# =============================================================================
# Company Page Functions
# =============================================================================


def upsert_company_profile(con: sqlite3.Connection, profile: dict) -> int:
    """
    Upsert company profile data.

    Args:
        con: Database connection
        profile: Dict with keys matching company_profile columns.
                Required: symbol, source_url

    Returns:
        Number of rows affected (1 for insert/update, 0 if no change)
    """
    symbol = profile.get("symbol")
    if not symbol:
        raise ValueError("profile must include 'symbol'")

    source_url = profile.get("source_url", "")
    now = now_iso()

    cur = con.execute(
        """
        INSERT INTO company_profile (
            symbol, company_name, sector_name, business_description,
            address, website, registrar, auditor, fiscal_year_end,
            updated_at, source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            company_name = excluded.company_name,
            sector_name = excluded.sector_name,
            business_description = excluded.business_description,
            address = excluded.address,
            website = excluded.website,
            registrar = excluded.registrar,
            auditor = excluded.auditor,
            fiscal_year_end = excluded.fiscal_year_end,
            updated_at = excluded.updated_at,
            source_url = excluded.source_url
        """,
        (
            symbol.upper(),
            profile.get("company_name"),
            profile.get("sector_name"),
            profile.get("business_description"),
            profile.get("address"),
            profile.get("website"),
            profile.get("registrar"),
            profile.get("auditor"),
            profile.get("fiscal_year_end"),
            now,
            source_url,
        ),
    )
    con.commit()
    return cur.rowcount


def replace_company_key_people(
    con: sqlite3.Connection, symbol: str, key_people: list[dict]
) -> int:
    """
    Replace key people for a company (delete old, insert new).

    Args:
        con: Database connection
        symbol: Stock symbol
        key_people: List of dicts with 'role' and 'name' keys

    Returns:
        Number of rows inserted
    """
    symbol = symbol.upper()
    now = now_iso()

    # Delete existing key people for this symbol
    con.execute("DELETE FROM company_key_people WHERE symbol = ?", (symbol,))

    # Insert new key people
    count = 0
    for person in key_people:
        role = person.get("role", "").strip()
        name = person.get("name", "").strip()
        if role and name:
            con.execute(
                """
                INSERT INTO company_key_people (symbol, role, name, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (symbol, role, name, now),
            )
            count += 1

    con.commit()
    return count


def insert_quote_snapshot(
    con: sqlite3.Connection, symbol: str, ts: str, quote: dict
) -> bool:
    """
    Insert a quote snapshot. Does not overwrite if same ts exists.

    Args:
        con: Database connection
        symbol: Stock symbol
        ts: Ingestion timestamp (ISO format)
        quote: Dict with quote data including 'raw_hash'

    Returns:
        True if inserted, False if skipped (duplicate ts)
    """
    symbol = symbol.upper()
    raw_hash = quote.get("raw_hash", "")
    now = now_iso()

    try:
        con.execute(
            """
            INSERT INTO company_quote_snapshots (
                symbol, ts, as_of, price, change, change_pct,
                open, high, low, volume,
                day_range_low, day_range_high,
                wk52_low, wk52_high,
                circuit_low, circuit_high,
                market_mode, raw_hash, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                ts,
                quote.get("as_of"),
                quote.get("price"),
                quote.get("change"),
                quote.get("change_pct"),
                quote.get("open"),
                quote.get("high"),
                quote.get("low"),
                quote.get("volume"),
                quote.get("day_range_low"),
                quote.get("day_range_high"),
                quote.get("wk52_low"),
                quote.get("wk52_high"),
                quote.get("circuit_low"),
                quote.get("circuit_high"),
                quote.get("market_mode"),
                raw_hash,
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.IntegrityError:
        # Duplicate ts for this symbol, skip
        return False


def get_last_quote_hash(con: sqlite3.Connection, symbol: str) -> str | None:
    """
    Get the raw_hash of the most recent quote snapshot for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        raw_hash string or None if no snapshots exist
    """
    cur = con.execute(
        """
        SELECT raw_hash FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return row["raw_hash"] if row else None


def get_company_profile(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get company profile for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with profile data or None if not found
    """
    cur = con.execute(
        """
        SELECT symbol, company_name, sector_name, business_description,
               address, website, registrar, auditor, fiscal_year_end,
               updated_at, source_url
        FROM company_profile
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_company_key_people(con: sqlite3.Connection, symbol: str) -> list[dict]:
    """
    Get key people for a company.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        List of dicts with 'role' and 'name' keys
    """
    cur = con.execute(
        """
        SELECT role, name FROM company_key_people
        WHERE symbol = ?
        ORDER BY role
        """,
        (symbol.upper(),),
    )
    return [{"role": row["role"], "name": row["name"]} for row in cur.fetchall()]


def get_quote_snapshots(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get recent quote snapshots for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with quote snapshot data, sorted by ts desc
    """
    query = """
        SELECT symbol, ts, as_of, price, change, change_pct,
               open, high, low, volume,
               day_range_low, day_range_high,
               wk52_low, wk52_high,
               circuit_low, circuit_high,
               market_mode, raw_hash, ingested_at
        FROM company_quote_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT ?
    """
    return pd.read_sql_query(query, con, params=[symbol.upper(), limit])


def sync_sector_names_from_company_profile(con: sqlite3.Connection) -> int:
    """
    Sync sector_name from company_profile to symbols table.

    Updates the symbols.sector_name column with values from company_profile
    for any symbols where company_profile has a sector_name.

    Args:
        con: Database connection

    Returns:
        Number of rows updated
    """
    now = now_iso()
    cur = con.execute(
        """
        UPDATE symbols
        SET sector_name = (
            SELECT company_profile.sector_name
            FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        ),
        updated_at = ?
        WHERE EXISTS (
            SELECT 1 FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        )
        AND (
            symbols.sector_name IS NULL
            OR symbols.sector_name = ''
            OR symbols.sector_name != (
                SELECT company_profile.sector_name
                FROM company_profile
                WHERE company_profile.symbol = symbols.symbol
            )
        )
        """,
        (now,),
    )
    con.commit()
    return cur.rowcount


# =============================================================================
# Company Fundamentals Functions
# =============================================================================


def upsert_company_fundamentals(
    con: sqlite3.Connection,
    symbol: str,
    data: dict,
    save_history: bool = True,
) -> dict:
    """
    Upsert company fundamentals and optionally save to history.

    Args:
        con: Database connection
        symbol: Stock symbol
        data: Dict with all fundamentals fields
        save_history: If True, also insert into history table

    Returns:
        Dict with 'updated' and 'history_saved' status
    """
    symbol = symbol.upper()
    now = now_iso()
    today = now[:10]  # YYYY-MM-DD

    # Build the upsert for company_fundamentals
    fields = [
        "symbol", "company_name", "sector_name",
        "price", "change", "change_pct", "open", "high", "low", "volume", "ldcp",
        "bid_price", "bid_size", "ask_price", "ask_size",
        "day_range_low", "day_range_high", "wk52_low", "wk52_high",
        "circuit_low", "circuit_high",
        "ytd_change_pct", "one_year_change_pct",
        "pe_ratio", "market_cap",
        "total_shares", "free_float_shares", "free_float_pct",
        "haircut", "variance",
        "business_description", "address", "website", "registrar", "auditor",
        "fiscal_year_end", "incorporation_date", "listed_in",
        "as_of", "market_mode", "source_url", "updated_at",
    ]

    values = [
        symbol,
        data.get("company_name"),
        data.get("sector_name"),
        data.get("price"),
        data.get("change"),
        data.get("change_pct"),
        data.get("open"),
        data.get("high"),
        data.get("low"),
        data.get("volume"),
        data.get("ldcp"),
        data.get("bid_price"),
        data.get("bid_size"),
        data.get("ask_price"),
        data.get("ask_size"),
        data.get("day_range_low"),
        data.get("day_range_high"),
        data.get("wk52_low"),
        data.get("wk52_high"),
        data.get("circuit_low"),
        data.get("circuit_high"),
        data.get("ytd_change_pct"),
        data.get("one_year_change_pct"),
        data.get("pe_ratio"),
        data.get("market_cap"),
        data.get("total_shares"),
        data.get("free_float_shares"),
        data.get("free_float_pct"),
        data.get("haircut"),
        data.get("variance"),
        data.get("business_description"),
        data.get("address"),
        data.get("website"),
        data.get("registrar"),
        data.get("auditor"),
        data.get("fiscal_year_end"),
        data.get("incorporation_date"),
        data.get("listed_in"),
        data.get("as_of"),
        data.get("market_mode"),
        data.get("source_url"),
        now,
    ]

    placeholders = ", ".join(["?"] * len(fields))
    field_names = ", ".join(fields)

    # Build ON CONFLICT update clause (exclude symbol)
    update_parts = [f"{f} = excluded.{f}" for f in fields if f != "symbol"]
    update_clause = ", ".join(update_parts)

    con.execute(
        f"""
        INSERT INTO company_fundamentals ({field_names})
        VALUES ({placeholders})
        ON CONFLICT(symbol) DO UPDATE SET {update_clause}
        """,
        values,
    )
    con.commit()

    result = {"updated": True, "history_saved": False}

    # Save to history if requested
    if save_history:
        result["history_saved"] = save_fundamentals_history(con, symbol, today, data)

    return result


def save_fundamentals_history(
    con: sqlite3.Connection,
    symbol: str,
    date: str,
    data: dict,
) -> bool:
    """
    Save fundamentals snapshot to history table.

    Args:
        con: Database connection
        symbol: Stock symbol
        date: Date string (YYYY-MM-DD)
        data: Dict with fundamentals fields

    Returns:
        True if inserted, False if already exists for that date
    """
    symbol = symbol.upper()
    now = now_iso()

    # Check if we already have a record for this symbol+date
    cur = con.execute(
        "SELECT 1 FROM company_fundamentals_history WHERE symbol = ? AND date = ?",
        (symbol, date),
    )
    if cur.fetchone():
        # Update existing record
        con.execute(
            """
            UPDATE company_fundamentals_history SET
                company_name = ?, sector_name = ?,
                price = ?, change = ?, change_pct = ?,
                open = ?, high = ?, low = ?, volume = ?, ldcp = ?,
                bid_price = ?, bid_size = ?, ask_price = ?, ask_size = ?,
                day_range_low = ?, day_range_high = ?,
                wk52_low = ?, wk52_high = ?,
                circuit_low = ?, circuit_high = ?,
                ytd_change_pct = ?, one_year_change_pct = ?,
                pe_ratio = ?, market_cap = ?,
                total_shares = ?, free_float_shares = ?, free_float_pct = ?,
                haircut = ?, variance = ?,
                as_of = ?, market_mode = ?, snapshot_ts = ?
            WHERE symbol = ? AND date = ?
            """,
            (
                data.get("company_name"), data.get("sector_name"),
                data.get("price"), data.get("change"), data.get("change_pct"),
                data.get("open"), data.get("high"), data.get("low"),
                data.get("volume"), data.get("ldcp"),
                data.get("bid_price"), data.get("bid_size"),
                data.get("ask_price"), data.get("ask_size"),
                data.get("day_range_low"), data.get("day_range_high"),
                data.get("wk52_low"), data.get("wk52_high"),
                data.get("circuit_low"), data.get("circuit_high"),
                data.get("ytd_change_pct"), data.get("one_year_change_pct"),
                data.get("pe_ratio"), data.get("market_cap"),
                data.get("total_shares"), data.get("free_float_shares"),
                data.get("free_float_pct"),
                data.get("haircut"), data.get("variance"),
                data.get("as_of"), data.get("market_mode"), now,
                symbol, date,
            ),
        )
        con.commit()
        return True

    # Insert new record
    con.execute(
        """
        INSERT INTO company_fundamentals_history (
            symbol, date, company_name, sector_name,
            price, change, change_pct, open, high, low, volume, ldcp,
            bid_price, bid_size, ask_price, ask_size,
            day_range_low, day_range_high, wk52_low, wk52_high,
            circuit_low, circuit_high,
            ytd_change_pct, one_year_change_pct,
            pe_ratio, market_cap,
            total_shares, free_float_shares, free_float_pct,
            haircut, variance,
            as_of, market_mode, snapshot_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            symbol, date,
            data.get("company_name"), data.get("sector_name"),
            data.get("price"), data.get("change"), data.get("change_pct"),
            data.get("open"), data.get("high"), data.get("low"),
            data.get("volume"), data.get("ldcp"),
            data.get("bid_price"), data.get("bid_size"),
            data.get("ask_price"), data.get("ask_size"),
            data.get("day_range_low"), data.get("day_range_high"),
            data.get("wk52_low"), data.get("wk52_high"),
            data.get("circuit_low"), data.get("circuit_high"),
            data.get("ytd_change_pct"), data.get("one_year_change_pct"),
            data.get("pe_ratio"), data.get("market_cap"),
            data.get("total_shares"), data.get("free_float_shares"),
            data.get("free_float_pct"),
            data.get("haircut"), data.get("variance"),
            data.get("as_of"), data.get("market_mode"), now,
        ),
    )
    con.commit()
    return True


def get_company_fundamentals(con: sqlite3.Connection, symbol: str) -> dict | None:
    """
    Get latest company fundamentals.

    Args:
        con: Database connection
        symbol: Stock symbol

    Returns:
        Dict with all fundamentals fields, or None if not found
    """
    cur = con.execute(
        """
        SELECT symbol, company_name, sector_name,
               price, change, change_pct, open, high, low, volume, ldcp,
               bid_price, bid_size, ask_price, ask_size,
               day_range_low, day_range_high, wk52_low, wk52_high,
               circuit_low, circuit_high,
               ytd_change_pct, one_year_change_pct,
               pe_ratio, market_cap,
               total_shares, free_float_shares, free_float_pct,
               haircut, variance,
               business_description, address, website, registrar, auditor,
               fiscal_year_end, incorporation_date, listed_in,
               as_of, market_mode, source_url, updated_at
        FROM company_fundamentals
        WHERE symbol = ?
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    return {
        "symbol": row[0],
        "company_name": row[1],
        "sector_name": row[2],
        "price": row[3],
        "change": row[4],
        "change_pct": row[5],
        "open": row[6],
        "high": row[7],
        "low": row[8],
        "volume": row[9],
        "ldcp": row[10],
        "bid_price": row[11],
        "bid_size": row[12],
        "ask_price": row[13],
        "ask_size": row[14],
        "day_range_low": row[15],
        "day_range_high": row[16],
        "wk52_low": row[17],
        "wk52_high": row[18],
        "circuit_low": row[19],
        "circuit_high": row[20],
        "ytd_change_pct": row[21],
        "one_year_change_pct": row[22],
        "pe_ratio": row[23],
        "market_cap": row[24],
        "total_shares": row[25],
        "free_float_shares": row[26],
        "free_float_pct": row[27],
        "haircut": row[28],
        "variance": row[29],
        "business_description": row[30],
        "address": row[31],
        "website": row[32],
        "registrar": row[33],
        "auditor": row[34],
        "fiscal_year_end": row[35],
        "incorporation_date": row[36],
        "listed_in": row[37],
        "as_of": row[38],
        "market_mode": row[39],
        "source_url": row[40],
        "updated_at": row[41],
    }


# =============================================================================
# Company Financials Functions
# =============================================================================


def upsert_company_financials(
    con: sqlite3.Connection,
    symbol: str,
    financials: list[dict],
) -> int:
    """
    Upsert company financial data (annual/quarterly).

    Args:
        con: Database connection
        symbol: Stock symbol
        financials: List of dicts with period data

    Returns:
        Number of rows upserted
    """
    if not financials:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in financials:
        period_end = item.get("period_end")
        period_type = item.get("period_type", "annual")

        if not period_end:
            continue

        cur = con.execute(
            """
            INSERT INTO company_financials (
                symbol, period_end, period_type,
                sales, gross_profit, operating_profit,
                profit_before_tax, profit_after_tax, eps,
                total_assets, total_liabilities, total_equity,
                currency, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                sales = excluded.sales,
                gross_profit = excluded.gross_profit,
                operating_profit = excluded.operating_profit,
                profit_before_tax = excluded.profit_before_tax,
                profit_after_tax = excluded.profit_after_tax,
                eps = excluded.eps,
                total_assets = excluded.total_assets,
                total_liabilities = excluded.total_liabilities,
                total_equity = excluded.total_equity,
                currency = excluded.currency,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                period_end,
                period_type,
                item.get("sales"),
                item.get("gross_profit"),
                item.get("operating_profit"),
                item.get("profit_before_tax"),
                item.get("profit_after_tax"),
                item.get("eps"),
                item.get("total_assets"),
                item.get("total_liabilities"),
                item.get("total_equity"),
                item.get("currency", "PKR"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_financials(
    con: sqlite3.Connection,
    symbol: str,
    period_type: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """
    Get company financial data.

    Args:
        con: Database connection
        symbol: Stock symbol
        period_type: 'annual' or 'quarterly', or None for both
        limit: Maximum rows to return

    Returns:
        DataFrame with financial data
    """
    query = """
        SELECT symbol, period_end, period_type,
               sales, gross_profit, operating_profit,
               profit_before_tax, profit_after_tax, eps,
               total_assets, total_liabilities, total_equity,
               currency, updated_at
        FROM company_financials
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if period_type:
        query += " AND period_type = ?"
        params.append(period_type)

    query += " ORDER BY period_end DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Company Ratios Functions
# =============================================================================


def upsert_company_ratios(
    con: sqlite3.Connection,
    symbol: str,
    ratios: list[dict],
) -> int:
    """
    Upsert company ratio data.

    Args:
        con: Database connection
        symbol: Stock symbol
        ratios: List of dicts with ratio data per period

    Returns:
        Number of rows upserted
    """
    if not ratios:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in ratios:
        period_end = item.get("period_end")
        period_type = item.get("period_type", "annual")

        if not period_end:
            continue

        cur = con.execute(
            """
            INSERT INTO company_ratios (
                symbol, period_end, period_type,
                gross_profit_margin, net_profit_margin, operating_margin,
                return_on_equity, return_on_assets,
                sales_growth, eps_growth, profit_growth,
                pe_ratio, pb_ratio, peg_ratio,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                gross_profit_margin = excluded.gross_profit_margin,
                net_profit_margin = excluded.net_profit_margin,
                operating_margin = excluded.operating_margin,
                return_on_equity = excluded.return_on_equity,
                return_on_assets = excluded.return_on_assets,
                sales_growth = excluded.sales_growth,
                eps_growth = excluded.eps_growth,
                profit_growth = excluded.profit_growth,
                pe_ratio = excluded.pe_ratio,
                pb_ratio = excluded.pb_ratio,
                peg_ratio = excluded.peg_ratio,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                period_end,
                period_type,
                item.get("gross_profit_margin"),
                item.get("net_profit_margin"),
                item.get("operating_margin"),
                item.get("return_on_equity"),
                item.get("return_on_assets"),
                item.get("sales_growth"),
                item.get("eps_growth"),
                item.get("profit_growth"),
                item.get("pe_ratio"),
                item.get("pb_ratio"),
                item.get("peg_ratio"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_ratios(
    con: sqlite3.Connection,
    symbol: str,
    period_type: str | None = None,
    limit: int = 20,
) -> pd.DataFrame:
    """
    Get company ratio data.

    Args:
        con: Database connection
        symbol: Stock symbol
        period_type: 'annual' or 'quarterly', or None for both
        limit: Maximum rows to return

    Returns:
        DataFrame with ratio data
    """
    query = """
        SELECT symbol, period_end, period_type,
               gross_profit_margin, net_profit_margin, operating_margin,
               return_on_equity, return_on_assets,
               sales_growth, eps_growth, profit_growth,
               pe_ratio, pb_ratio, peg_ratio,
               updated_at
        FROM company_ratios
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if period_type:
        query += " AND period_type = ?"
        params.append(period_type)

    query += " ORDER BY period_end DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Company Payouts Functions
# =============================================================================


def upsert_company_payouts(
    con: sqlite3.Connection,
    symbol: str,
    payouts: list[dict],
) -> int:
    """
    Upsert company payout/dividend data.

    Args:
        con: Database connection
        symbol: Stock symbol
        payouts: List of dicts with payout data

    Returns:
        Number of rows upserted
    """
    if not payouts:
        return 0

    symbol = symbol.upper()
    now = now_iso()
    count = 0

    for item in payouts:
        ex_date = item.get("ex_date")
        payout_type = item.get("payout_type", "cash")

        if not ex_date:
            continue

        cur = con.execute(
            """
            INSERT INTO company_payouts (
                symbol, ex_date, payout_type,
                announcement_date, book_closure_from, book_closure_to,
                amount, fiscal_year, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ex_date, payout_type) DO UPDATE SET
                announcement_date = excluded.announcement_date,
                book_closure_from = excluded.book_closure_from,
                book_closure_to = excluded.book_closure_to,
                amount = excluded.amount,
                fiscal_year = excluded.fiscal_year,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                ex_date,
                payout_type,
                item.get("announcement_date"),
                item.get("book_closure_from"),
                item.get("book_closure_to"),
                item.get("amount"),
                item.get("fiscal_year"),
                now,
            ),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_company_payouts(
    con: sqlite3.Connection,
    symbol: str,
    payout_type: str | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    """
    Get company payout/dividend history.

    Args:
        con: Database connection
        symbol: Stock symbol
        payout_type: 'cash', 'bonus', 'right', or None for all
        limit: Maximum rows to return

    Returns:
        DataFrame with payout data
    """
    query = """
        SELECT symbol, ex_date, payout_type,
               announcement_date, book_closure_from, book_closure_to,
               amount, fiscal_year, updated_at
        FROM company_payouts
        WHERE symbol = ?
    """
    params: list = [symbol.upper()]

    if payout_type:
        query += " AND payout_type = ?"
        params.append(payout_type)

    query += " ORDER BY ex_date DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# Financial Announcements Functions
# =============================================================================


def upsert_financial_announcement(
    con: sqlite3.Connection,
    symbol: str,
    announcement: dict,
) -> bool:
    """
    Upsert a single financial announcement.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcement: Dict with announcement data

    Returns:
        True if inserted/updated successfully
    """
    now = now_iso()
    symbol = symbol.upper()

    ann_date = announcement.get("announcement_date")
    fiscal_period = announcement.get("fiscal_period") or announcement.get("fiscal_year", "")

    if not ann_date or not fiscal_period:
        return False

    try:
        con.execute(
            """
            INSERT INTO financial_announcements (
                symbol, announcement_date, fiscal_period,
                profit_before_tax, profit_after_tax, eps,
                dividend_payout, dividend_amount, payout_type,
                agm_date, book_closure_from, book_closure_to,
                company_name, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, announcement_date, fiscal_period) DO UPDATE SET
                profit_before_tax = excluded.profit_before_tax,
                profit_after_tax = excluded.profit_after_tax,
                eps = excluded.eps,
                dividend_payout = excluded.dividend_payout,
                dividend_amount = excluded.dividend_amount,
                payout_type = excluded.payout_type,
                agm_date = excluded.agm_date,
                book_closure_from = excluded.book_closure_from,
                book_closure_to = excluded.book_closure_to,
                company_name = excluded.company_name,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                ann_date,
                fiscal_period,
                announcement.get("profit_before_tax"),
                announcement.get("profit_after_tax"),
                announcement.get("eps"),
                announcement.get("dividend_payout") or announcement.get("details_raw"),
                announcement.get("dividend_amount") or announcement.get("amount"),
                announcement.get("payout_type"),
                announcement.get("agm_date"),
                announcement.get("book_closure_from"),
                announcement.get("book_closure_to"),
                announcement.get("company_name"),
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error:
        return False


def upsert_financial_announcements(
    con: sqlite3.Connection,
    symbol: str,
    announcements: list[dict],
) -> int:
    """
    Upsert multiple financial announcements.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcements: List of announcement dicts

    Returns:
        Number of rows upserted
    """
    count = 0
    for ann in announcements:
        if upsert_financial_announcement(con, symbol, ann):
            count += 1
    return count


def get_financial_announcements(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 10,
) -> list[dict]:
    """
    Get financial announcements for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        limit: Maximum rows to return

    Returns:
        List of announcement dicts
    """
    cur = con.execute(
        """
        SELECT symbol, announcement_date, fiscal_period,
               profit_before_tax, profit_after_tax, eps,
               dividend_payout, dividend_amount, payout_type,
               agm_date, book_closure_from, book_closure_to,
               company_name, updated_at
        FROM financial_announcements
        WHERE symbol = ?
        ORDER BY announcement_date DESC
        LIMIT ?
        """,
        (symbol.upper(), limit),
    )

    results = []
    for row in cur.fetchall():
        results.append({
            "symbol": row[0],
            "announcement_date": row[1],
            "fiscal_period": row[2],
            "profit_before_tax": row[3],
            "profit_after_tax": row[4],
            "eps": row[5],
            "dividend_payout": row[6],
            "dividend_amount": row[7],
            "payout_type": row[8],
            "agm_date": row[9],
            "book_closure_from": row[10],
            "book_closure_to": row[11],
            "company_name": row[12],
        })

    return results


# =============================================================================
# User Interaction Tracking Functions
# =============================================================================


def log_interaction(
    con: sqlite3.Connection,
    session_id: str,
    action_type: str,
    page_name: str | None = None,
    symbol: str | None = None,
    action_detail: str | None = None,
    metadata: dict | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> int:
    """
    Log a user interaction to the database.

    Args:
        con: Database connection
        session_id: Unique session identifier
        action_type: Type of action ('page_visit', 'search', 'button_click', 'refresh', 'download')
        page_name: Which page the action was on
        symbol: Stock symbol if applicable
        action_detail: Additional details (button name, search query, etc.)
        metadata: Dict with extra data (will be JSON serialized)
        ip_address: Optional user IP
        user_agent: Optional browser/client info

    Returns:
        ID of the inserted row
    """
    import json

    now = now_iso()
    metadata_json = json.dumps(metadata) if metadata else None

    cur = con.execute(
        """
        INSERT INTO user_interactions (
            session_id, timestamp, action_type, page_name,
            symbol, action_detail, metadata, ip_address, user_agent
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            now,
            action_type,
            page_name,
            symbol.upper() if symbol else None,
            action_detail,
            metadata_json,
            ip_address,
            user_agent,
        ),
    )
    con.commit()
    return cur.lastrowid


def get_session_interactions(
    con: sqlite3.Connection,
    session_id: str,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get interactions for a specific session.

    Args:
        con: Database connection
        session_id: Session identifier
        limit: Maximum rows to return

    Returns:
        DataFrame with interaction history
    """
    query = """
        SELECT id, session_id, timestamp, action_type, page_name,
               symbol, action_detail, metadata
        FROM user_interactions
        WHERE session_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """
    return pd.read_sql_query(query, con, params=[session_id, limit])


def get_recent_interactions(
    con: sqlite3.Connection,
    action_type: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Get recent interactions across all sessions.

    Args:
        con: Database connection
        action_type: Filter by action type
        symbol: Filter by symbol
        limit: Maximum rows to return

    Returns:
        DataFrame with recent interactions
    """
    query = """
        SELECT id, session_id, timestamp, action_type, page_name,
               symbol, action_detail, metadata
        FROM user_interactions
        WHERE 1=1
    """
    params: list = []

    if action_type:
        query += " AND action_type = ?"
        params.append(action_type)

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_interaction_stats(
    con: sqlite3.Connection,
    days: int = 7,
) -> dict:
    """
    Get interaction statistics for analytics.

    Args:
        con: Database connection
        days: Number of days to look back

    Returns:
        Dict with stats: total_interactions, unique_sessions, top_pages,
                        top_symbols, action_breakdown
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    stats: dict = {
        "total_interactions": 0,
        "unique_sessions": 0,
        "top_pages": [],
        "top_symbols": [],
        "action_breakdown": {},
    }

    # Total interactions and unique sessions
    cur = con.execute(
        """
        SELECT COUNT(*) as total, COUNT(DISTINCT session_id) as sessions
        FROM user_interactions
        WHERE timestamp >= ?
        """,
        (cutoff,),
    )
    row = cur.fetchone()
    if row:
        stats["total_interactions"] = row[0]
        stats["unique_sessions"] = row[1]

    # Top pages
    cur = con.execute(
        """
        SELECT page_name, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND page_name IS NOT NULL
        GROUP BY page_name
        ORDER BY count DESC
        LIMIT 10
        """,
        (cutoff,),
    )
    stats["top_pages"] = [{"page": row[0], "count": row[1]} for row in cur.fetchall()]

    # Top symbols
    cur = con.execute(
        """
        SELECT symbol, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND symbol IS NOT NULL
        GROUP BY symbol
        ORDER BY count DESC
        LIMIT 10
        """,
        (cutoff,),
    )
    stats["top_symbols"] = [{"symbol": row[0], "count": row[1]} for row in cur.fetchall()]

    # Action breakdown
    cur = con.execute(
        """
        SELECT action_type, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ?
        GROUP BY action_type
        ORDER BY count DESC
        """,
        (cutoff,),
    )
    stats["action_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

    return stats


def get_symbol_activity(
    con: sqlite3.Connection,
    symbol: str,
    days: int = 30,
) -> dict:
    """
    Get activity statistics for a specific symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        days: Number of days to look back

    Returns:
        Dict with symbol-specific stats
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    symbol = symbol.upper()

    stats: dict = {
        "symbol": symbol,
        "total_views": 0,
        "unique_sessions": 0,
        "action_breakdown": {},
        "recent_activity": [],
    }

    # Total views and unique sessions
    cur = con.execute(
        """
        SELECT COUNT(*) as total, COUNT(DISTINCT session_id) as sessions
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        """,
        (cutoff, symbol),
    )
    row = cur.fetchone()
    if row:
        stats["total_views"] = row[0]
        stats["unique_sessions"] = row[1]

    # Action breakdown
    cur = con.execute(
        """
        SELECT action_type, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        GROUP BY action_type
        """,
        (cutoff, symbol),
    )
    stats["action_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

    # Recent activity
    cur = con.execute(
        """
        SELECT timestamp, action_type, action_detail
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (cutoff, symbol),
    )
    stats["recent_activity"] = [
        {"timestamp": row[0], "action": row[1], "detail": row[2]}
        for row in cur.fetchall()
    ]

    return stats


# =============================================================================
# EOD OHLCV Query Functions
# =============================================================================


def get_eod_ohlcv(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Get EOD OHLCV data with optional filters.

    Args:
        con: Database connection
        symbol: Filter by stock symbol (optional)
        start_date: Start date YYYY-MM-DD (inclusive, optional)
        end_date: End date YYYY-MM-DD (inclusive, optional)
        limit: Maximum rows to return

    Returns:
        DataFrame with OHLCV data
    """
    query = """
        SELECT symbol, date, open, high, low, close, volume,
               prev_close, sector_code, company_name, ingested_at
        FROM eod_ohlcv
        WHERE 1=1
    """
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def get_eod_dates(con: sqlite3.Connection) -> list[str]:
    """
    Get list of all dates with EOD data.

    Args:
        con: Database connection

    Returns:
        List of date strings (YYYY-MM-DD), newest first
    """
    cur = con.execute(
        "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC"
    )
    return [row[0] for row in cur.fetchall()]


def get_eod_date_range(con: sqlite3.Connection) -> dict:
    """
    Get min/max date range and count for EOD data.

    Args:
        con: Database connection

    Returns:
        Dict with min_date, max_date, total_rows, unique_dates, unique_symbols
    """
    cur = con.execute(
        """
        SELECT
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(*) as total_rows,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM eod_ohlcv
        """
    )
    row = cur.fetchone()
    return {
        "min_date": row[0],
        "max_date": row[1],
        "total_rows": row[2] or 0,
        "unique_dates": row[3] or 0,
        "unique_symbols": row[4] or 0,
    }


def check_eod_date_exists(con: sqlite3.Connection, date: str) -> bool:
    """
    Check if EOD data exists for a specific date.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD

    Returns:
        True if data exists, False otherwise
    """
    cur = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE date = ?",
        (date,),
    )
    return cur.fetchone()[0] > 0


def get_eod_date_count(con: sqlite3.Connection, date: str) -> int:
    """
    Get count of EOD records for a specific date.

    Args:
        con: Database connection
        date: Date string YYYY-MM-DD

    Returns:
        Number of records for that date
    """
    cur = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE date = ?",
        (date,),
    )
    return cur.fetchone()[0]


# =============================================================================
# Market Summary CSV Ingestion Functions
# =============================================================================


def ingest_market_summary_csv(
    con: sqlite3.Connection,
    csv_path: str | Path,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest market summary CSV file into eod_ohlcv table.

    The CSV should have columns: date, symbol, sector_code, company_name,
    open, high, low, close, volume, prev_close

    Args:
        con: Database connection
        csv_path: Path to CSV file
        skip_existing: If True, skip if date already has data

    Returns:
        Dict with status, rows_inserted, date, message
    """
    from pathlib import Path

    csv_path = Path(csv_path)

    result = {
        "csv_path": str(csv_path),
        "date": None,
        "status": "failed",
        "rows_inserted": 0,
        "rows_in_csv": 0,
        "message": None,
    }

    if not csv_path.exists():
        result["message"] = f"File not found: {csv_path}"
        return result

    try:
        df = pd.read_csv(csv_path)
        result["rows_in_csv"] = len(df)

        if df.empty:
            result["status"] = "empty"
            result["message"] = "CSV file is empty"
            return result

        # Get date from first row or filename
        if "date" in df.columns and not df["date"].isna().all():
            date_str = df["date"].iloc[0]
        else:
            # Extract date from filename (e.g., 2026-01-20.csv)
            date_str = csv_path.stem

        result["date"] = date_str

        # Check if date already exists
        if skip_existing and check_eod_date_exists(con, date_str):
            existing_count = get_eod_date_count(con, date_str)
            result["status"] = "skipped"
            result["message"] = f"Date {date_str} already has {existing_count} records"
            return result

        # Ensure required columns
        required = {"symbol", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            result["message"] = f"Missing columns: {missing}"
            return result

        # Add date column if missing
        if "date" not in df.columns:
            df["date"] = date_str

        # Upsert the data
        rows = upsert_eod(con, df)
        result["rows_inserted"] = rows
        result["status"] = "ok"
        result["message"] = f"Inserted {rows} rows for {date_str}"

    except Exception as e:
        result["message"] = f"Error: {e}"

    return result


def ingest_all_market_summary_csvs(
    con: sqlite3.Connection,
    csv_dir: str | Path | None = None,
    skip_existing: bool = True,
) -> dict:
    """
    Ingest all market summary CSV files from a directory into eod_ohlcv table.

    Args:
        con: Database connection
        csv_dir: Directory containing CSV files. Defaults to DATA_ROOT/market_summary/csv
        skip_existing: If True, skip dates that already have data

    Returns:
        Dict with summary: total_files, ok, skipped, failed, total_rows
    """
    from pathlib import Path

    from .config import DATA_ROOT

    if csv_dir is None:
        csv_dir = DATA_ROOT / "market_summary" / "csv"
    else:
        csv_dir = Path(csv_dir)

    summary = {
        "csv_dir": str(csv_dir),
        "total_files": 0,
        "ok": 0,
        "skipped": 0,
        "empty": 0,
        "failed": 0,
        "total_rows": 0,
        "errors": [],
    }

    if not csv_dir.exists():
        summary["errors"].append(f"Directory not found: {csv_dir}")
        return summary

    # Get all CSV files sorted by name (date)
    csv_files = sorted(csv_dir.glob("*.csv"))
    summary["total_files"] = len(csv_files)

    for csv_path in csv_files:
        result = ingest_market_summary_csv(con, csv_path, skip_existing=skip_existing)

        if result["status"] == "ok":
            summary["ok"] += 1
            summary["total_rows"] += result["rows_inserted"]
        elif result["status"] == "skipped":
            summary["skipped"] += 1
        elif result["status"] == "empty":
            summary["empty"] += 1
        else:
            summary["failed"] += 1
            summary["errors"].append({
                "file": csv_path.name,
                "message": result["message"],
            })

    return summary


# =============================================================================
# Bloomberg-Style Quant Data Functions
# =============================================================================


def upsert_company_snapshot(
    con: sqlite3.Connection,
    symbol: str,
    snapshot_date: str,
    data: dict,
    raw_html: str | None = None,
) -> dict:
    """
    Upsert a full company snapshot with all scraped data.

    This is the main function for storing comprehensive company data
    in a NoSQL-style flexible format.

    Args:
        con: Database connection
        symbol: Stock symbol
        snapshot_date: Date of snapshot (YYYY-MM-DD)
        data: Dict containing all scraped data with keys:
              - company_name, sector_code, sector_name
              - quote_data, equity_data, profile_data
              - financials_data, ratios_data, trading_data
              - futures_data, announcements_data
        raw_html: Optional raw HTML for reprocessing

    Returns:
        Dict with status and row count
    """
    import json

    symbol = symbol.upper()
    now = now_iso()

    # Serialize nested dicts to JSON
    def to_json(obj):
        return json.dumps(obj) if obj else None

    try:
        con.execute(
            """
            INSERT INTO company_snapshots (
                symbol, snapshot_date, snapshot_time,
                company_name, sector_code, sector_name,
                quote_data, equity_data, profile_data,
                financials_data, ratios_data, trading_data,
                futures_data, announcements_data,
                raw_html, source_url, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                company_name = excluded.company_name,
                sector_code = excluded.sector_code,
                sector_name = excluded.sector_name,
                quote_data = excluded.quote_data,
                equity_data = excluded.equity_data,
                profile_data = excluded.profile_data,
                financials_data = excluded.financials_data,
                ratios_data = excluded.ratios_data,
                trading_data = excluded.trading_data,
                futures_data = excluded.futures_data,
                announcements_data = excluded.announcements_data,
                raw_html = excluded.raw_html,
                source_url = excluded.source_url,
                scraped_at = excluded.scraped_at
            """,
            (
                symbol,
                snapshot_date,
                data.get("snapshot_time"),
                data.get("company_name"),
                data.get("sector_code"),
                data.get("sector_name"),
                to_json(data.get("quote_data")),
                to_json(data.get("equity_data")),
                to_json(data.get("profile_data")),
                to_json(data.get("financials_data")),
                to_json(data.get("ratios_data")),
                to_json(data.get("trading_data")),
                to_json(data.get("futures_data")),
                to_json(data.get("announcements_data")),
                raw_html,
                data.get("source_url"),
                now,
            ),
        )
        con.commit()
        return {"status": "ok", "symbol": symbol, "date": snapshot_date}
    except Exception as e:
        return {"status": "error", "symbol": symbol, "error": str(e)}


def get_company_snapshot(
    con: sqlite3.Connection,
    symbol: str,
    snapshot_date: str | None = None,
) -> dict | None:
    """
    Get a company snapshot, optionally for a specific date.

    Args:
        con: Database connection
        symbol: Stock symbol
        snapshot_date: Specific date or None for latest

    Returns:
        Dict with all snapshot data (JSON fields parsed)
    """
    import json

    symbol = symbol.upper()

    if snapshot_date:
        query = """
            SELECT * FROM company_snapshots
            WHERE symbol = ? AND snapshot_date = ?
        """
        cur = con.execute(query, (symbol, snapshot_date))
    else:
        query = """
            SELECT * FROM company_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """
        cur = con.execute(query, (symbol,))

    row = cur.fetchone()
    if not row:
        return None

    # Convert Row to dict and parse JSON fields
    result = dict(row)
    json_fields = [
        "quote_data", "equity_data", "profile_data",
        "financials_data", "ratios_data", "trading_data",
        "futures_data", "announcements_data"
    ]
    for field in json_fields:
        if result.get(field):
            try:
                result[field] = json.loads(result[field])
            except json.JSONDecodeError:
                pass

    return result


def upsert_trading_session(
    con: sqlite3.Connection,
    symbol: str,
    session_date: str,
    market_type: str,
    data: dict,
) -> int:
    """
    Upsert trading session data with full market microstructure.

    Args:
        con: Database connection
        symbol: Stock symbol
        session_date: Trading date (YYYY-MM-DD)
        market_type: 'REG', 'FUT', 'CSF', 'ODL'
        data: Dict with all trading metrics

    Returns:
        Number of rows affected
    """
    symbol = symbol.upper()
    now = now_iso()
    contract_month = data.get("contract_month", "")

    cur = con.execute(
        """
        INSERT INTO trading_sessions (
            symbol, session_date, market_type, contract_month,
            open, high, low, close, volume,
            ldcp, prev_close, change_value, change_percent,
            bid_price, bid_volume, ask_price, ask_volume, spread,
            day_range_low, day_range_high, circuit_low, circuit_high,
            week_52_low, week_52_high,
            total_trades, turnover, vwap,
            var_percent, haircut_percent, pe_ratio_ttm,
            ytd_change, year_1_change,
            last_update, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, session_date, market_type, contract_month) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            ldcp = excluded.ldcp,
            prev_close = excluded.prev_close,
            change_value = excluded.change_value,
            change_percent = excluded.change_percent,
            bid_price = excluded.bid_price,
            bid_volume = excluded.bid_volume,
            ask_price = excluded.ask_price,
            ask_volume = excluded.ask_volume,
            spread = excluded.spread,
            day_range_low = excluded.day_range_low,
            day_range_high = excluded.day_range_high,
            circuit_low = excluded.circuit_low,
            circuit_high = excluded.circuit_high,
            week_52_low = excluded.week_52_low,
            week_52_high = excluded.week_52_high,
            total_trades = excluded.total_trades,
            turnover = excluded.turnover,
            vwap = excluded.vwap,
            var_percent = excluded.var_percent,
            haircut_percent = excluded.haircut_percent,
            pe_ratio_ttm = excluded.pe_ratio_ttm,
            ytd_change = excluded.ytd_change,
            year_1_change = excluded.year_1_change,
            last_update = excluded.last_update,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol, session_date, market_type, contract_month,
            data.get("open"), data.get("high"), data.get("low"),
            data.get("close"), data.get("volume"),
            data.get("ldcp"), data.get("prev_close"),
            data.get("change_value"), data.get("change_percent"),
            data.get("bid_price"), data.get("bid_volume"),
            data.get("ask_price"), data.get("ask_volume"),
            data.get("spread"),
            data.get("day_range_low"), data.get("day_range_high"),
            data.get("circuit_low"), data.get("circuit_high"),
            data.get("week_52_low"), data.get("week_52_high"),
            data.get("total_trades"), data.get("turnover"),
            data.get("vwap"),
            data.get("var_percent"), data.get("haircut_percent"),
            data.get("pe_ratio_ttm"),
            data.get("ytd_change"), data.get("year_1_change"),
            data.get("last_update"), now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_trading_sessions(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    market_type: str | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Query trading sessions with filters.

    Args:
        con: Database connection
        symbol: Filter by symbol
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        market_type: Filter by market type ('REG', 'FUT', etc.)
        limit: Max rows

    Returns:
        DataFrame with trading session data
    """
    query = "SELECT * FROM trading_sessions WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if start_date:
        query += " AND session_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND session_date <= ?"
        params.append(end_date)
    if market_type:
        query += " AND market_type = ?"
        params.append(market_type)

    query += " ORDER BY session_date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def upsert_corporate_announcement(
    con: sqlite3.Connection,
    symbol: str,
    announcement_date: str,
    announcement_type: str,
    title: str,
    data: dict | None = None,
) -> int:
    """
    Upsert a corporate announcement.

    Args:
        con: Database connection
        symbol: Stock symbol
        announcement_date: Date of announcement
        announcement_type: Type (financial_result, board_meeting, etc.)
        title: Announcement title
        data: Optional additional data (document_url, summary, etc.)

    Returns:
        Number of rows affected
    """
    import hashlib
    import json

    symbol = symbol.upper()
    now = now_iso()
    data = data or {}

    # Create hash for deduplication
    title_hash = hashlib.md5(title.encode()).hexdigest()

    cur = con.execute(
        """
        INSERT INTO corporate_announcements (
            symbol, announcement_date, announcement_type, category,
            title, title_hash, document_url, document_type,
            summary, key_figures, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, announcement_date, title_hash) DO UPDATE SET
            category = excluded.category,
            document_url = excluded.document_url,
            document_type = excluded.document_type,
            summary = excluded.summary,
            key_figures = excluded.key_figures,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol,
            announcement_date,
            announcement_type,
            data.get("category"),
            title,
            title_hash,
            data.get("document_url"),
            data.get("document_type"),
            data.get("summary"),
            json.dumps(data.get("key_figures")) if data.get("key_figures") else None,
            now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_corporate_announcements(
    con: sqlite3.Connection,
    symbol: str | None = None,
    announcement_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Query corporate announcements with filters.

    Args:
        con: Database connection
        symbol: Filter by symbol
        announcement_type: Filter by type
        start_date: Start date
        end_date: End date
        limit: Max rows

    Returns:
        DataFrame with announcements
    """
    query = "SELECT * FROM corporate_announcements WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if announcement_type:
        query += " AND announcement_type = ?"
        params.append(announcement_type)
    if start_date:
        query += " AND announcement_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND announcement_date <= ?"
        params.append(end_date)

    query += " ORDER BY announcement_date DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)


def upsert_equity_structure(
    con: sqlite3.Connection,
    symbol: str,
    as_of_date: str,
    data: dict,
) -> int:
    """
    Upsert equity structure data.

    Args:
        con: Database connection
        symbol: Stock symbol
        as_of_date: Date of the data
        data: Dict with equity structure fields

    Returns:
        Number of rows affected
    """
    import json

    symbol = symbol.upper()
    now = now_iso()

    cur = con.execute(
        """
        INSERT INTO equity_structure (
            symbol, as_of_date,
            authorized_shares, issued_shares, outstanding_shares, treasury_shares,
            free_float_shares, free_float_percent,
            market_cap, market_cap_usd,
            ownership_data, face_value, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            authorized_shares = excluded.authorized_shares,
            issued_shares = excluded.issued_shares,
            outstanding_shares = excluded.outstanding_shares,
            treasury_shares = excluded.treasury_shares,
            free_float_shares = excluded.free_float_shares,
            free_float_percent = excluded.free_float_percent,
            market_cap = excluded.market_cap,
            market_cap_usd = excluded.market_cap_usd,
            ownership_data = excluded.ownership_data,
            face_value = excluded.face_value,
            scraped_at = excluded.scraped_at
        """,
        (
            symbol, as_of_date,
            data.get("authorized_shares"),
            data.get("issued_shares"),
            data.get("outstanding_shares"),
            data.get("treasury_shares"),
            data.get("free_float_shares"),
            data.get("free_float_percent"),
            data.get("market_cap"),
            data.get("market_cap_usd"),
            json.dumps(data.get("ownership_data")) if data.get("ownership_data") else None,
            data.get("face_value"),
            now,
        ),
    )
    con.commit()
    return cur.rowcount


def get_equity_structure(
    con: sqlite3.Connection,
    symbol: str,
    as_of_date: str | None = None,
) -> dict | None:
    """
    Get equity structure for a symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        as_of_date: Specific date or None for latest

    Returns:
        Dict with equity structure data
    """
    import json

    symbol = symbol.upper()

    if as_of_date:
        query = """
            SELECT * FROM equity_structure
            WHERE symbol = ? AND as_of_date = ?
        """
        cur = con.execute(query, (symbol, as_of_date))
    else:
        query = """
            SELECT * FROM equity_structure
            WHERE symbol = ?
            ORDER BY as_of_date DESC
            LIMIT 1
        """
        cur = con.execute(query, (symbol,))

    row = cur.fetchone()
    if not row:
        return None

    result = dict(row)
    if result.get("ownership_data"):
        try:
            result["ownership_data"] = json.loads(result["ownership_data"])
        except json.JSONDecodeError:
            pass

    return result


def create_scrape_job(
    con: sqlite3.Connection,
    job_type: str,
    config: dict | None = None,
) -> str:
    """
    Create a new scrape job for tracking.

    Args:
        con: Database connection
        job_type: Type of job
        config: Optional job configuration

    Returns:
        Job ID
    """
    import json

    job_id = str(uuid.uuid4())[:8]
    now = now_iso()

    con.execute(
        """
        INSERT INTO scrape_jobs (job_id, job_type, started_at, status, config)
        VALUES (?, ?, ?, 'running', ?)
        """,
        (job_id, job_type, now, json.dumps(config) if config else None),
    )
    con.commit()
    return job_id


def update_scrape_job(
    con: sqlite3.Connection,
    job_id: str,
    status: str | None = None,
    symbols_requested: int | None = None,
    symbols_completed: int | None = None,
    symbols_failed: int | None = None,
    records_inserted: int | None = None,
    records_updated: int | None = None,
    errors: list | None = None,
) -> None:
    """
    Update a scrape job with progress.

    Args:
        con: Database connection
        job_id: Job ID
        status: New status ('completed', 'failed')
        symbols_requested: Total symbols to process
        symbols_completed: Number completed
        symbols_failed: Number failed
        records_inserted: Records inserted
        records_updated: Records updated
        errors: List of error dicts
    """
    import json

    updates = []
    params = []

    if status:
        updates.append("status = ?")
        params.append(status)
        if status in ("completed", "failed"):
            updates.append("ended_at = ?")
            params.append(now_iso())

    if symbols_requested is not None:
        updates.append("symbols_requested = ?")
        params.append(symbols_requested)
    if symbols_completed is not None:
        updates.append("symbols_completed = ?")
        params.append(symbols_completed)
    if symbols_failed is not None:
        updates.append("symbols_failed = ?")
        params.append(symbols_failed)
    if records_inserted is not None:
        updates.append("records_inserted = ?")
        params.append(records_inserted)
    if records_updated is not None:
        updates.append("records_updated = ?")
        params.append(records_updated)
    if errors is not None:
        updates.append("errors = ?")
        params.append(json.dumps(errors))

    if updates:
        params.append(job_id)
        query = f"UPDATE scrape_jobs SET {', '.join(updates)} WHERE job_id = ?"
        con.execute(query, params)
        con.commit()


def get_scrape_job(con: sqlite3.Connection, job_id: str) -> dict | None:
    """Get scrape job by ID."""
    import json

    cur = con.execute("SELECT * FROM scrape_jobs WHERE job_id = ?", (job_id,))
    row = cur.fetchone()
    if not row:
        return None

    result = dict(row)
    for field in ("errors", "config"):
        if result.get(field):
            try:
                result[field] = json.loads(result[field])
            except json.JSONDecodeError:
                pass
    return result


# =============================================================================
# Background Job Management Functions
# =============================================================================


def create_background_job(
    con: sqlite3.Connection,
    job_type: str,
    symbols: list[str],
    batch_size: int = 50,
    batch_pause_sec: int = 30,
    config: dict | None = None,
) -> str:
    """Create a new background scrape job.

    Args:
        con: Database connection
        job_type: Type of job ('bulk_deep_scrape', etc.)
        symbols: List of symbols to process
        batch_size: Symbols per batch
        batch_pause_sec: Pause between batches
        config: Optional configuration dict

    Returns:
        Job ID
    """
    import json
    import math
    import uuid

    job_id = str(uuid.uuid4())[:8]
    total_batches = math.ceil(len(symbols) / batch_size)

    config_data = config or {}
    config_data["symbols"] = symbols

    con.execute(
        """
        INSERT INTO scrape_jobs (
            job_id, job_type, started_at, status,
            symbols_requested, batch_size, batch_pause_sec,
            total_batches, config
        ) VALUES (?, ?, datetime('now'), 'pending', ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            job_type,
            len(symbols),
            batch_size,
            batch_pause_sec,
            total_batches,
            json.dumps(config_data),
        ),
    )
    con.commit()
    return job_id


def update_job_progress(
    con: sqlite3.Connection,
    job_id: str,
    current_symbol: str | None = None,
    current_batch: int | None = None,
    symbols_completed: int | None = None,
    symbols_failed: int | None = None,
    records_inserted: int | None = None,
    status: str | None = None,
    pid: int | None = None,
) -> None:
    """Update job progress (called by worker)."""
    updates = ["last_heartbeat = datetime('now')"]
    params = []

    if current_symbol is not None:
        updates.append("current_symbol = ?")
        params.append(current_symbol)
    if current_batch is not None:
        updates.append("current_batch = ?")
        params.append(current_batch)
    if symbols_completed is not None:
        updates.append("symbols_completed = ?")
        params.append(symbols_completed)
    if symbols_failed is not None:
        updates.append("symbols_failed = ?")
        params.append(symbols_failed)
    if records_inserted is not None:
        updates.append("records_inserted = ?")
        params.append(records_inserted)
    if status is not None:
        updates.append("status = ?")
        params.append(status)
        if status in ("completed", "failed", "stopped"):
            updates.append("ended_at = datetime('now')")
    if pid is not None:
        updates.append("pid = ?")
        params.append(pid)

    params.append(job_id)
    con.execute(
        f"UPDATE scrape_jobs SET {', '.join(updates)} WHERE job_id = ?",
        params,
    )
    con.commit()


def request_job_stop(con: sqlite3.Connection, job_id: str) -> bool:
    """Request a job to stop (called by UI)."""
    con.execute(
        "UPDATE scrape_jobs SET stop_requested = 1 WHERE job_id = ?",
        (job_id,),
    )
    con.commit()
    return True


def is_job_stop_requested(con: sqlite3.Connection, job_id: str) -> bool:
    """Check if stop was requested for a job (called by worker)."""
    cur = con.execute(
        "SELECT stop_requested FROM scrape_jobs WHERE job_id = ?",
        (job_id,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def get_running_jobs(con: sqlite3.Connection) -> list[dict]:
    """Get all running/pending jobs."""
    import json

    cur = con.execute(
        """
        SELECT * FROM scrape_jobs
        WHERE status IN ('pending', 'running')
        ORDER BY started_at DESC
        """
    )
    jobs = []
    for row in cur.fetchall():
        job = dict(row)
        for field in ("errors", "config"):
            if job.get(field):
                try:
                    job[field] = json.loads(job[field])
                except json.JSONDecodeError:
                    pass
        jobs.append(job)
    return jobs


def get_recent_jobs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent jobs (all statuses)."""
    import json

    cur = con.execute(
        """
        SELECT * FROM scrape_jobs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    jobs = []
    for row in cur.fetchall():
        job = dict(row)
        for field in ("errors", "config"):
            if job.get(field):
                try:
                    job[field] = json.loads(job[field])
                except json.JSONDecodeError:
                    pass
        jobs.append(job)
    return jobs


def add_job_notification(
    con: sqlite3.Connection,
    job_id: str,
    notification_type: str,
    title: str,
    message: str | None = None,
) -> None:
    """Add a notification for a job."""
    con.execute(
        """
        INSERT INTO job_notifications (job_id, notification_type, title, message)
        VALUES (?, ?, ?, ?)
        """,
        (job_id, notification_type, title, message),
    )
    con.execute(
        "UPDATE scrape_jobs SET notification_sent = 1 WHERE job_id = ?",
        (job_id,),
    )
    con.commit()


def get_unread_notifications(con: sqlite3.Connection) -> list[dict]:
    """Get all unread notifications."""
    cur = con.execute(
        """
        SELECT n.*, j.job_type, j.symbols_completed, j.symbols_failed
        FROM job_notifications n
        JOIN scrape_jobs j ON n.job_id = j.job_id
        WHERE n.read_at IS NULL
        ORDER BY n.created_at DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def mark_notification_read(con: sqlite3.Connection, notification_id: int) -> None:
    """Mark a notification as read."""
    con.execute(
        "UPDATE job_notifications SET read_at = datetime('now') WHERE id = ?",
        (notification_id,),
    )
    con.commit()


def mark_all_notifications_read(con: sqlite3.Connection) -> None:
    """Mark all notifications as read."""
    con.execute("UPDATE job_notifications SET read_at = datetime('now') WHERE read_at IS NULL")
    con.commit()


# =============================================================================
# Unified Data Access (Hybrid Model)
# =============================================================================


def get_company_unified(
    con: sqlite3.Connection,
    symbol: str,
    include_history: bool = False,
) -> dict | None:
    """Get unified company data from Deep Data tables.

    This is the primary function for accessing company data in the hybrid model.
    It reads from company_snapshots, trading_sessions, and corporate_announcements.

    Args:
        con: Database connection
        symbol: Stock symbol
        include_history: If True, include historical snapshots

    Returns:
        Dict with unified company data or None if not found
    """
    import json
    from datetime import datetime

    symbol = symbol.upper()
    today = datetime.now().strftime("%Y-%m-%d")

    # Get latest snapshot
    cur = con.execute(
        """
        SELECT * FROM company_snapshots
        WHERE symbol = ?
        ORDER BY snapshot_date DESC, scraped_at DESC
        LIMIT 1
        """,
        (symbol,),
    )
    snapshot_row = cur.fetchone()

    if not snapshot_row:
        return None

    snapshot = dict(snapshot_row)

    # Parse JSON fields
    json_fields = [
        "quote_data", "equity_data", "profile_data", "financials_data",
        "ratios_data", "trading_data", "futures_data", "announcements_data"
    ]
    for field in json_fields:
        if snapshot.get(field):
            try:
                snapshot[field] = json.loads(snapshot[field])
            except json.JSONDecodeError:
                snapshot[field] = {}

    # Get today's trading sessions (all market types)
    cur = con.execute(
        """
        SELECT * FROM trading_sessions
        WHERE symbol = ? AND session_date = ?
        ORDER BY market_type
        """,
        (symbol, today),
    )
    trading_rows = cur.fetchall()
    trading_sessions = {}
    for row in trading_rows:
        session = dict(row)
        market_type = session.get("market_type", "REG")
        contract = session.get("contract_month", "")
        key = f"{market_type}_{contract}" if contract else market_type
        trading_sessions[key] = session

    # Get recent announcements
    cur = con.execute(
        """
        SELECT * FROM corporate_announcements
        WHERE symbol = ?
        ORDER BY announcement_date DESC
        LIMIT 20
        """,
        (symbol,),
    )
    announcements = [dict(row) for row in cur.fetchall()]

    # Get equity structure
    cur = con.execute(
        """
        SELECT * FROM equity_structure
        WHERE symbol = ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (symbol,),
    )
    equity_row = cur.fetchone()
    equity_structure = dict(equity_row) if equity_row else {}

    # Build unified response
    quote_data = snapshot.get("quote_data", {})
    trading_data = snapshot.get("trading_data", {})
    equity_data = snapshot.get("equity_data", {})
    reg_trading = trading_data.get("REG", {}) if trading_data else {}

    # Get price for calculations
    price = reg_trading.get("close") or quote_data.get("close")
    total_shares = equity_structure.get("outstanding_shares") or equity_data.get("outstanding_shares")

    # Calculate market cap if not available
    market_cap = equity_structure.get("market_cap") or equity_data.get("market_cap")
    if (not market_cap or market_cap == 0) and price and total_shares:
        market_cap = price * total_shares

    result = {
        # Core info
        "symbol": symbol,
        "company_name": snapshot.get("company_name") or quote_data.get("company_name"),
        "sector_code": snapshot.get("sector_code"),
        "sector_name": snapshot.get("sector_name") or quote_data.get("sector_name"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "scraped_at": snapshot.get("scraped_at"),

        # Current quote (from snapshot or trading session)
        "price": price,
        "open": reg_trading.get("open") or quote_data.get("open"),
        "high": reg_trading.get("high") or quote_data.get("high"),
        "low": reg_trading.get("low") or quote_data.get("low"),
        "close": price,
        "volume": reg_trading.get("volume") or quote_data.get("volume"),
        "ldcp": reg_trading.get("ldcp") or quote_data.get("ldcp"),
        "change": quote_data.get("change_value") or quote_data.get("change"),
        "change_pct": reg_trading.get("change_percent") or quote_data.get("change_percent") or quote_data.get("change_pct"),

        # Ranges
        "day_range_low": reg_trading.get("day_range_low") or quote_data.get("day_range_low"),
        "day_range_high": reg_trading.get("day_range_high") or quote_data.get("day_range_high"),
        "wk52_low": reg_trading.get("week_52_low") or quote_data.get("wk52_low"),
        "wk52_high": reg_trading.get("week_52_high") or quote_data.get("wk52_high"),
        "circuit_low": reg_trading.get("circuit_low") or quote_data.get("circuit_low"),
        "circuit_high": reg_trading.get("circuit_high") or quote_data.get("circuit_high"),

        # Valuation
        "pe_ratio": reg_trading.get("pe_ratio_ttm") or quote_data.get("pe_ratio"),
        "market_cap": market_cap,

        # Performance
        "ytd_change_pct": reg_trading.get("ytd_change"),
        "one_year_change_pct": reg_trading.get("year_1_change"),

        # Risk
        "haircut": reg_trading.get("haircut_percent"),
        "variance": reg_trading.get("var_percent"),

        # Equity
        "total_shares": total_shares,
        "free_float_shares": equity_structure.get("free_float_shares") or equity_data.get("free_float_shares"),
        "free_float_pct": equity_structure.get("free_float_percent") or equity_data.get("free_float_percent"),

        # Full data objects
        "quote_data": quote_data,
        "trading_data": trading_data,
        "equity_data": snapshot.get("equity_data", {}),
        "profile_data": snapshot.get("profile_data", {}),
        "financials_data": snapshot.get("financials_data", {}),
        "ratios_data": snapshot.get("ratios_data", {}),
        "futures_data": snapshot.get("futures_data", {}),

        # Live trading sessions (today)
        "trading_sessions": trading_sessions,

        # Announcements
        "announcements": announcements,
        "announcements_count": len(announcements),

        # Equity structure
        "equity_structure": equity_structure,
    }

    # Include historical snapshots if requested
    if include_history:
        cur = con.execute(
            """
            SELECT snapshot_date, scraped_at,
                   json_extract(quote_data, '$.close') as close,
                   json_extract(quote_data, '$.volume') as volume
            FROM company_snapshots
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 30
            """,
            (symbol,),
        )
        result["history"] = [dict(row) for row in cur.fetchall()]

    return result


def get_unified_symbols_list(con: sqlite3.Connection) -> list[str]:
    """Get list of symbols available in Deep Data tables."""
    cur = con.execute(
        "SELECT DISTINCT symbol FROM company_snapshots ORDER BY symbol"
    )
    return [row[0] for row in cur.fetchall()]


def get_unified_symbol_count(con: sqlite3.Connection) -> int:
    """Get count of symbols in Deep Data tables."""
    cur = con.execute("SELECT COUNT(DISTINCT symbol) FROM company_snapshots")
    return cur.fetchone()[0]


# =============================================================================
# PSX Index Functions
# =============================================================================


def get_latest_kse100(con: sqlite3.Connection) -> dict | None:
    """
    Get the latest KSE-100 index data.

    Returns:
        Dict with index data or None if not available
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_indices
            WHERE index_code = 'KSE100'
            ORDER BY index_date DESC, index_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_latest_index(con: sqlite3.Connection, index_code: str = "KSE100") -> dict | None:
    """
    Get the latest data for any index.

    Args:
        con: Database connection
        index_code: Index code (KSE100, KSE30, KMI30, etc.)

    Returns:
        Dict with index data or None
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_indices
            WHERE index_code = ?
            ORDER BY index_date DESC, index_time DESC
            LIMIT 1
        """, (index_code,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_all_latest_indices(con: sqlite3.Connection) -> list[dict]:
    """
    Get latest data for all indices.

    Returns:
        List of dicts with index data
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_indices pi
            WHERE (index_code, index_date, index_time) IN (
                SELECT index_code, MAX(index_date), MAX(index_time)
                FROM psx_indices
                GROUP BY index_code
            )
            ORDER BY
                CASE index_code
                    WHEN 'KSE100' THEN 1
                    WHEN 'KSE30' THEN 2
                    WHEN 'KMI30' THEN 3
                    ELSE 4
                END
        """)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_index_history(
    con: sqlite3.Connection,
    index_code: str = "KSE100",
    days: int = 30
) -> list[dict]:
    """
    Get index history for a specified number of days.

    Args:
        con: Database connection
        index_code: Index code
        days: Number of days of history

    Returns:
        List of dicts with daily index values
    """
    try:
        cur = con.execute("""
            SELECT DISTINCT index_date, value, change, change_pct, high, low, volume
            FROM psx_indices
            WHERE index_code = ?
            ORDER BY index_date DESC
            LIMIT ?
        """, (index_code, days))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_market_stats(con: sqlite3.Connection) -> dict | None:
    """
    Get the latest market stats (trading segments).

    Returns:
        Dict with segment data or None
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_market_stats
            ORDER BY stat_date DESC, stat_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_index_data(con: sqlite3.Connection, index_data: dict) -> bool:
    """
    Insert or update index data.

    Args:
        con: Database connection
        index_data: Dict with index_code, index_date, value, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT OR REPLACE INTO psx_indices (
                index_code, index_date, index_time,
                value, change, change_pct,
                open, high, low, volume,
                previous_close,
                ytd_change_pct, one_year_change_pct,
                week_52_low, week_52_high,
                trades, market_cap, turnover,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            index_data.get("index_code"),
            index_data.get("index_date"),
            index_data.get("index_time"),
            index_data.get("value"),
            index_data.get("change"),
            index_data.get("change_pct"),
            index_data.get("open"),
            index_data.get("high"),
            index_data.get("low"),
            index_data.get("volume"),
            index_data.get("previous_close"),
            index_data.get("ytd_change_pct"),
            index_data.get("one_year_change_pct"),
            index_data.get("week_52_low"),
            index_data.get("week_52_high"),
            index_data.get("trades"),
            index_data.get("market_cap"),
            index_data.get("turnover"),
        ))
        con.commit()
        return True
    except Exception:
        return False


# =============================================================================
# PHASE 1: INSTRUMENTS UNIVERSE FUNCTIONS
# =============================================================================

def upsert_instrument(con: sqlite3.Connection, instrument: dict) -> bool:
    """
    Insert or update an instrument record.

    Args:
        con: Database connection
        instrument: Dict with instrument_id, symbol, name, instrument_type, etc.

    Returns:
        True if successful, False otherwise
    """
    try:
        con.execute("""
            INSERT INTO instruments (
                instrument_id, symbol, name, instrument_type,
                exchange, currency, is_active, source,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                symbol = excluded.symbol,
                name = excluded.name,
                instrument_type = excluded.instrument_type,
                exchange = excluded.exchange,
                currency = excluded.currency,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            instrument.get("name"),
            instrument.get("instrument_type"),
            instrument.get("exchange", "PSX"),
            instrument.get("currency", "PKR"),
            instrument.get("is_active", 1),
            instrument.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def upsert_instruments_batch(con: sqlite3.Connection, instruments: list[dict]) -> dict:
    """
    Batch upsert multiple instruments.

    Args:
        con: Database connection
        instruments: List of instrument dicts

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    counts = {"inserted": 0, "updated": 0, "failed": 0}

    for inst in instruments:
        try:
            # Check if exists
            cur = con.execute(
                "SELECT 1 FROM instruments WHERE instrument_id = ?",
                (inst.get("instrument_id"),)
            )
            exists = cur.fetchone() is not None

            if upsert_instrument(con, inst):
                if exists:
                    counts["updated"] += 1
                else:
                    counts["inserted"] += 1
            else:
                counts["failed"] += 1
        except Exception:
            counts["failed"] += 1

    return counts


def get_instruments(
    con: sqlite3.Connection,
    instrument_type: str | None = None,
    active_only: bool = True
) -> list[dict]:
    """
    Get instruments, optionally filtered by type.

    Args:
        con: Database connection
        instrument_type: Filter by type ('EQUITY', 'ETF', 'REIT', 'INDEX'), or None for all
        active_only: If True, only return active instruments

    Returns:
        List of instrument dicts
    """
    try:
        query = "SELECT * FROM instruments WHERE 1=1"
        params = []

        if instrument_type:
            query += " AND instrument_type = ?"
            params.append(instrument_type)

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY instrument_type, symbol"

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_instrument_by_id(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single instrument by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_instrument_by_symbol(
    con: sqlite3.Connection,
    symbol: str,
    exchange: str = "PSX"
) -> dict | None:
    """Get instrument by symbol and exchange."""
    try:
        cur = con.execute(
            "SELECT * FROM instruments WHERE symbol = ? AND exchange = ?",
            (symbol, exchange)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def resolve_instrument_id(exchange: str, symbol: str) -> str:
    """
    Generate a standardized instrument ID.

    Args:
        exchange: Exchange code (e.g., 'PSX', 'IDX')
        symbol: Instrument symbol

    Returns:
        Standardized ID like "PSX:HBL" or "IDX:KSE100"
    """
    return f"{exchange}:{symbol}"


def upsert_ohlcv_instrument(con: sqlite3.Connection, instrument_id: str, df: pd.DataFrame) -> int:
    """
    Upsert OHLCV data for an instrument.

    Args:
        con: Database connection
        instrument_id: Instrument ID
        df: DataFrame with date, open, high, low, close, volume columns

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO ohlcv_instruments (
                    instrument_id, date, open, high, low, close, volume, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(instrument_id, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    ingested_at = datetime('now')
            """, (
                instrument_id,
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            ))
            count += 1
        except Exception:
            pass

    con.commit()
    return count


def get_ohlcv_instrument(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> pd.DataFrame:
    """
    Get OHLCV data for an instrument.

    Args:
        con: Database connection
        instrument_id: Instrument ID
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        limit: Max rows to return

    Returns:
        DataFrame with date, open, high, low, close, volume
    """
    query = "SELECT date, open, high, low, close, volume FROM ohlcv_instruments WHERE instrument_id = ?"
    params = [instrument_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


def get_instrument_latest_date(con: sqlite3.Connection, instrument_id: str) -> str | None:
    """Get the latest OHLCV date for an instrument."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM ohlcv_instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def upsert_instrument_ranking(con: sqlite3.Connection, ranking: dict) -> bool:
    """
    Insert or update an instrument ranking.

    Args:
        con: Database connection
        ranking: Dict with as_of_date, instrument_id, instrument_type, and metrics

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO instrument_rankings (
                as_of_date, instrument_id, instrument_type,
                return_1m, return_3m, return_6m, return_1y,
                volatility_30d, relative_strength, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(as_of_date, instrument_id) DO UPDATE SET
                instrument_type = excluded.instrument_type,
                return_1m = excluded.return_1m,
                return_3m = excluded.return_3m,
                return_6m = excluded.return_6m,
                return_1y = excluded.return_1y,
                volatility_30d = excluded.volatility_30d,
                relative_strength = excluded.relative_strength,
                computed_at = datetime('now')
        """, (
            ranking.get("as_of_date"),
            ranking.get("instrument_id"),
            ranking.get("instrument_type"),
            ranking.get("return_1m"),
            ranking.get("return_3m"),
            ranking.get("return_6m"),
            ranking.get("return_1y"),
            ranking.get("volatility_30d"),
            ranking.get("relative_strength"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_instrument_rankings(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    instrument_type: str | None = None,
    top_n: int = 10
) -> list[dict]:
    """
    Get instrument rankings.

    Args:
        con: Database connection
        as_of_date: Date for rankings (default: most recent)
        instrument_type: Filter by type
        top_n: Number of top rankings to return

    Returns:
        List of ranking dicts with instrument details
    """
    if as_of_date:
        date_clause = "r.as_of_date = ?"
        params = [as_of_date]
    else:
        date_clause = "r.as_of_date = (SELECT MAX(as_of_date) FROM instrument_rankings)"
        params = []

    query = f"""
        SELECT r.*, i.symbol, i.name
        FROM instrument_rankings r
        JOIN instruments i ON r.instrument_id = i.instrument_id
        WHERE {date_clause}
    """

    if instrument_type:
        query += " AND r.instrument_type = ?"
        params.append(instrument_type)

    query += " ORDER BY r.return_1m DESC NULLS LAST LIMIT ?"
    params.append(top_n)

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_ranking_date(con: sqlite3.Connection) -> str | None:
    """Get the latest date for which rankings exist."""
    try:
        cur = con.execute("SELECT MAX(as_of_date) FROM instrument_rankings")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def create_instruments_sync_run(con: sqlite3.Connection, run_id: str, instrument_types: str) -> bool:
    """Create a new sync run record."""
    try:
        con.execute("""
            INSERT INTO instruments_sync_runs (run_id, started_at, instrument_types)
            VALUES (?, datetime('now'), ?)
        """, (run_id, instrument_types))
        con.commit()
        return True
    except Exception:
        return False


def update_instruments_sync_run(con: sqlite3.Connection, run_id: str, stats: dict) -> bool:
    """Update a sync run with final stats."""
    try:
        con.execute("""
            UPDATE instruments_sync_runs SET
                ended_at = datetime('now'),
                instruments_total = ?,
                instruments_ok = ?,
                instruments_failed = ?,
                instruments_no_data = ?,
                rows_upserted = ?
            WHERE run_id = ?
        """, (
            stats.get("total", 0),
            stats.get("ok", 0),
            stats.get("failed", 0),
            stats.get("no_data", 0),
            stats.get("rows", 0),
            run_id,
        ))
        con.commit()
        return True
    except Exception:
        return False


# =============================================================================
# Phase 2: FX Database Functions
# =============================================================================

def upsert_fx_pair(con: sqlite3.Connection, pair_data: dict) -> bool:
    """
    Insert or update an FX pair.

    Args:
        con: Database connection
        pair_data: Dict with pair, base_currency, quote_currency, source, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO fx_pairs (
                pair, base_currency, quote_currency, source, description,
                is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(pair) DO UPDATE SET
                base_currency = excluded.base_currency,
                quote_currency = excluded.quote_currency,
                source = excluded.source,
                description = excluded.description,
                is_active = excluded.is_active,
                updated_at = datetime('now')
        """, (
            pair_data.get("pair"),
            pair_data.get("base_currency"),
            pair_data.get("quote_currency"),
            pair_data.get("source", "MANUAL"),
            pair_data.get("description"),
            pair_data.get("is_active", 1),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_pairs(
    con: sqlite3.Connection,
    active_only: bool = True
) -> list[dict]:
    """
    Get all FX pairs.

    Args:
        con: Database connection
        active_only: If True, only return active pairs

    Returns:
        List of pair dicts
    """
    query = "SELECT * FROM fx_pairs"
    if active_only:
        query += " WHERE is_active = 1"
    query += " ORDER BY pair"

    try:
        cur = con.execute(query)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fx_pair(con: sqlite3.Connection, pair: str) -> dict | None:
    """Get a single FX pair by name."""
    try:
        cur = con.execute("SELECT * FROM fx_pairs WHERE pair = ?", (pair,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fx_ohlcv(
    con: sqlite3.Connection,
    pair: str,
    df: "pd.DataFrame"
) -> int:
    """
    Upsert FX OHLCV data.

    Args:
        con: Database connection
        pair: FX pair (e.g., "USD/PKR")
        df: DataFrame with date, open, high, low, close, volume columns

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    rows = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO fx_ohlcv (pair, date, open, high, low, close, volume, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(pair, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    ingested_at = datetime('now')
            """, (
                pair,
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            ))
            rows += 1
        except Exception:
            pass

    con.commit()
    return rows


def get_fx_ohlcv(
    con: sqlite3.Connection,
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> "pd.DataFrame":
    """
    Get FX OHLCV data.

    Args:
        con: Database connection
        pair: FX pair
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows to return

    Returns:
        DataFrame with OHLCV data
    """
    import pandas as pd

    query = "SELECT * FROM fx_ohlcv WHERE pair = ?"
    params = [pair]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_fx_latest_date(con: sqlite3.Connection, pair: str) -> str | None:
    """Get the latest date for an FX pair."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM fx_ohlcv WHERE pair = ?",
            (pair,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_fx_latest_rate(con: sqlite3.Connection, pair: str) -> dict | None:
    """Get the latest FX rate for a pair."""
    try:
        cur = con.execute("""
            SELECT * FROM fx_ohlcv
            WHERE pair = ?
            ORDER BY date DESC
            LIMIT 1
        """, (pair,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fx_adjusted_metric(con: sqlite3.Connection, metric: dict) -> bool:
    """
    Insert or update an FX-adjusted metric.

    Args:
        con: Database connection
        metric: Dict with as_of_date, symbol, fx_pair, equity_return, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO fx_adjusted_metrics (
                as_of_date, symbol, fx_pair, equity_return, fx_return,
                fx_adjusted_return, period, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(as_of_date, symbol, fx_pair, period) DO UPDATE SET
                equity_return = excluded.equity_return,
                fx_return = excluded.fx_return,
                fx_adjusted_return = excluded.fx_adjusted_return,
                computed_at = datetime('now')
        """, (
            metric.get("as_of_date"),
            metric.get("symbol"),
            metric.get("fx_pair"),
            metric.get("equity_return"),
            metric.get("fx_return"),
            metric.get("fx_adjusted_return"),
            metric.get("period", "1M"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_adjusted_metrics(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    symbol: str | None = None,
    fx_pair: str | None = None,
    period: str | None = None,
    limit: int = 50
) -> list[dict]:
    """
    Get FX-adjusted metrics.

    Args:
        con: Database connection
        as_of_date: Filter by date
        symbol: Filter by symbol
        fx_pair: Filter by FX pair
        period: Filter by period
        limit: Max results

    Returns:
        List of metric dicts
    """
    query = "SELECT * FROM fx_adjusted_metrics WHERE 1=1"
    params = []

    if as_of_date:
        query += " AND as_of_date = ?"
        params.append(as_of_date)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if fx_pair:
        query += " AND fx_pair = ?"
        params.append(fx_pair)
    if period:
        query += " AND period = ?"
        params.append(period)

    query += " ORDER BY as_of_date DESC, fx_adjusted_return DESC LIMIT ?"
    params.append(limit)

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def record_fx_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    pairs: list[str]
) -> bool:
    """Record the start of an FX sync run."""
    try:
        con.execute("""
            INSERT INTO fx_sync_runs (run_id, started_at, pairs_synced, status)
            VALUES (?, datetime('now'), ?, 'running')
        """, (run_id, ",".join(pairs)))
        con.commit()
        return True
    except Exception:
        return False


def update_fx_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update an FX sync run."""
    try:
        con.execute("""
            UPDATE fx_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent FX sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM fx_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


# =============================================================================
# Phase 2.5: Mutual Fund CRUD Functions (MUFAP Integration)
# =============================================================================


def upsert_mutual_fund(con: sqlite3.Connection, fund_data: dict) -> bool:
    """
    Insert or update a mutual fund.

    Args:
        con: Database connection
        fund_data: Dict with fund_id, symbol, fund_name, amc_code, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO mutual_funds (
                fund_id, symbol, fund_name, amc_code, amc_name,
                fund_type, category, is_shariah, launch_date,
                expense_ratio, management_fee, is_active, source,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(fund_id) DO UPDATE SET
                symbol = excluded.symbol,
                fund_name = excluded.fund_name,
                amc_code = excluded.amc_code,
                amc_name = excluded.amc_name,
                fund_type = excluded.fund_type,
                category = excluded.category,
                is_shariah = excluded.is_shariah,
                launch_date = excluded.launch_date,
                expense_ratio = excluded.expense_ratio,
                management_fee = excluded.management_fee,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            fund_data.get("fund_id"),
            fund_data.get("symbol"),
            fund_data.get("fund_name"),
            fund_data.get("amc_code"),
            fund_data.get("amc_name"),
            fund_data.get("fund_type", "OPEN_END"),
            fund_data.get("category"),
            fund_data.get("is_shariah", 0),
            fund_data.get("launch_date"),
            fund_data.get("expense_ratio"),
            fund_data.get("management_fee"),
            fund_data.get("is_active", 1),
            fund_data.get("source", "MUFAP"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_mutual_funds(
    con: sqlite3.Connection,
    category: str | None = None,
    fund_type: str | None = None,
    is_shariah: bool | None = None,
    active_only: bool = True,
    search: str | None = None,
) -> list[dict]:
    """
    Get mutual funds with optional filters.

    Args:
        con: Database connection
        category: Filter by category (e.g., 'Equity', 'Money Market')
        fund_type: Filter by fund type ('OPEN_END', 'VPS', 'ETF')
        is_shariah: Filter by Shariah compliance
        active_only: If True, only return active funds
        search: Search term for fund name or symbol

    Returns:
        List of fund dicts
    """
    query = "SELECT * FROM mutual_funds WHERE 1=1"
    params = []

    if category:
        query += " AND category = ?"
        params.append(category)
    if fund_type:
        query += " AND fund_type = ?"
        params.append(fund_type)
    if is_shariah is not None:
        query += " AND is_shariah = ?"
        params.append(1 if is_shariah else 0)
    if active_only:
        query += " AND is_active = 1"
    if search:
        query += " AND (fund_name LIKE ? OR symbol LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY category, fund_name"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_mutual_fund(con: sqlite3.Connection, fund_id: str) -> dict | None:
    """Get a single mutual fund by fund_id."""
    try:
        cur = con.execute("SELECT * FROM mutual_funds WHERE fund_id = ?", (fund_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_mutual_fund_by_symbol(con: sqlite3.Connection, symbol: str) -> dict | None:
    """Get a single mutual fund by symbol."""
    try:
        cur = con.execute("SELECT * FROM mutual_funds WHERE symbol = ?", (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_mf_nav(
    con: sqlite3.Connection,
    fund_id: str,
    df: "pd.DataFrame"
) -> int:
    """
    Upsert mutual fund NAV data.

    Args:
        con: Database connection
        fund_id: Mutual fund ID
        df: DataFrame with date, nav, offer_price, redemption_price, aum, nav_change_pct

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    rows = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO mutual_fund_nav (
                    fund_id, date, nav, offer_price, redemption_price,
                    aum, nav_change_pct, source, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(fund_id, date) DO UPDATE SET
                    nav = excluded.nav,
                    offer_price = excluded.offer_price,
                    redemption_price = excluded.redemption_price,
                    aum = excluded.aum,
                    nav_change_pct = excluded.nav_change_pct,
                    source = excluded.source,
                    ingested_at = datetime('now')
            """, (
                fund_id,
                row.get("date"),
                row.get("nav"),
                row.get("offer_price"),
                row.get("redemption_price"),
                row.get("aum"),
                row.get("nav_change_pct"),
                row.get("source", "MUFAP"),
            ))
            rows += 1
        except Exception:
            pass

    con.commit()
    return rows


def get_mf_nav(
    con: sqlite3.Connection,
    fund_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> "pd.DataFrame":
    """
    Get mutual fund NAV data.

    Args:
        con: Database connection
        fund_id: Mutual fund ID
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows to return

    Returns:
        DataFrame with NAV data
    """
    import pandas as pd

    query = "SELECT * FROM mutual_fund_nav WHERE fund_id = ?"
    params = [fund_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_mf_latest_date(con: sqlite3.Connection, fund_id: str) -> str | None:
    """Get the latest NAV date for a mutual fund."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM mutual_fund_nav WHERE fund_id = ?",
            (fund_id,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_mf_latest_nav(con: sqlite3.Connection, fund_id: str) -> dict | None:
    """Get the latest NAV for a mutual fund."""
    try:
        cur = con.execute("""
            SELECT * FROM mutual_fund_nav
            WHERE fund_id = ?
            ORDER BY date DESC
            LIMIT 1
        """, (fund_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def record_mf_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    funds_total: int = 0
) -> bool:
    """Record the start of a mutual fund sync run."""
    try:
        con.execute("""
            INSERT INTO mutual_fund_sync_runs (
                run_id, started_at, sync_type, status, funds_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, funds_total))
        con.commit()
        return True
    except Exception:
        return False


def update_mf_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    funds_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a mutual fund sync run."""
    try:
        con.execute("""
            UPDATE mutual_fund_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                funds_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, funds_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_mf_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent mutual fund sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM mutual_fund_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_mf_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of mutual fund data in database.

    Returns:
        Dict with fund counts, date ranges, category breakdown, etc.
    """
    summary = {
        "total_funds": 0,
        "active_funds": 0,
        "funds_with_nav": 0,
        "total_nav_rows": 0,
        "categories": {},
        "fund_types": {},
        "latest_nav_date": None,
        "earliest_nav_date": None,
    }

    try:
        # Total and active funds
        cur = con.execute("SELECT COUNT(*) FROM mutual_funds")
        summary["total_funds"] = cur.fetchone()[0]

        cur = con.execute("SELECT COUNT(*) FROM mutual_funds WHERE is_active = 1")
        summary["active_funds"] = cur.fetchone()[0]

        # Funds with NAV data
        cur = con.execute("""
            SELECT COUNT(DISTINCT fund_id) FROM mutual_fund_nav
        """)
        summary["funds_with_nav"] = cur.fetchone()[0]

        # Total NAV rows
        cur = con.execute("SELECT COUNT(*) FROM mutual_fund_nav")
        summary["total_nav_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM mutual_funds
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Fund type breakdown
        cur = con.execute("""
            SELECT fund_type, COUNT(*) as count
            FROM mutual_funds
            WHERE is_active = 1
            GROUP BY fund_type
            ORDER BY count DESC
        """)
        summary["fund_types"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute("SELECT MAX(date), MIN(date) FROM mutual_fund_nav")
        row = cur.fetchone()
        if row:
            summary["latest_nav_date"] = row[0]
            summary["earliest_nav_date"] = row[1]

    except Exception:
        pass

    return summary


# =============================================================================
# Phase 3: Bonds/Sukuk Functions
# =============================================================================


def upsert_bond(con: sqlite3.Connection, bond_data: dict) -> bool:
    """
    Upsert a bond into the bonds_master table.

    Args:
        con: Database connection
        bond_data: Dict with bond fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bonds_master (
                bond_id, isin, symbol, issuer, bond_type, is_islamic,
                face_value, coupon_rate, coupon_frequency, issue_date,
                maturity_date, day_count, currency, is_active, source,
                notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      datetime('now'), datetime('now'))
            ON CONFLICT(bond_id) DO UPDATE SET
                isin = excluded.isin,
                symbol = excluded.symbol,
                issuer = excluded.issuer,
                bond_type = excluded.bond_type,
                is_islamic = excluded.is_islamic,
                face_value = excluded.face_value,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                day_count = excluded.day_count,
                currency = excluded.currency,
                is_active = excluded.is_active,
                source = excluded.source,
                notes = excluded.notes,
                updated_at = datetime('now')
        """, (
            bond_data.get("bond_id"),
            bond_data.get("isin"),
            bond_data.get("symbol"),
            bond_data.get("issuer"),
            bond_data.get("bond_type"),
            bond_data.get("is_islamic", 0),
            bond_data.get("face_value", 100),
            bond_data.get("coupon_rate"),
            bond_data.get("coupon_frequency", 2),
            bond_data.get("issue_date"),
            bond_data.get("maturity_date"),
            bond_data.get("day_count", "ACT/ACT"),
            bond_data.get("currency", "PKR"),
            bond_data.get("is_active", 1),
            bond_data.get("source", "MANUAL"),
            bond_data.get("notes"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_bonds(
    con: sqlite3.Connection,
    bond_type: str | None = None,
    issuer: str | None = None,
    is_islamic: bool | None = None,
    active_only: bool = True,
) -> list[dict]:
    """
    Get bonds with optional filters.

    Args:
        con: Database connection
        bond_type: Filter by bond type
        issuer: Filter by issuer
        is_islamic: Filter by Islamic/conventional
        active_only: Only return active bonds

    Returns:
        List of bond dicts
    """
    query = "SELECT * FROM bonds_master WHERE 1=1"
    params = []

    if active_only:
        query += " AND is_active = 1"
    if bond_type:
        query += " AND bond_type = ?"
        params.append(bond_type)
    if issuer:
        query += " AND issuer = ?"
        params.append(issuer)
    if is_islamic is not None:
        query += " AND is_islamic = ?"
        params.append(1 if is_islamic else 0)

    query += " ORDER BY maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_bond(con: sqlite3.Connection, bond_id: str) -> dict | None:
    """Get a single bond by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM bonds_master WHERE bond_id = ?",
            (bond_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_bond_by_symbol(con: sqlite3.Connection, symbol: str) -> dict | None:
    """Get a bond by symbol."""
    try:
        cur = con.execute(
            "SELECT * FROM bonds_master WHERE symbol = ?",
            (symbol,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_bond_quote(con: sqlite3.Connection, quote_data: dict) -> bool:
    """
    Upsert a bond quote.

    Args:
        con: Database connection
        quote_data: Dict with bond_id, date, price/yield fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bond_quotes (
                bond_id, date, price, dirty_price, ytm,
                bid_yield, ask_yield, bid_price, ask_price,
                volume, source, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(bond_id, date) DO UPDATE SET
                price = excluded.price,
                dirty_price = excluded.dirty_price,
                ytm = excluded.ytm,
                bid_yield = excluded.bid_yield,
                ask_yield = excluded.ask_yield,
                bid_price = excluded.bid_price,
                ask_price = excluded.ask_price,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            quote_data.get("bond_id"),
            quote_data.get("date"),
            quote_data.get("price"),
            quote_data.get("dirty_price"),
            quote_data.get("ytm"),
            quote_data.get("bid_yield"),
            quote_data.get("ask_yield"),
            quote_data.get("bid_price"),
            quote_data.get("ask_price"),
            quote_data.get("volume"),
            quote_data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def upsert_bond_quotes_batch(
    con: sqlite3.Connection,
    quotes: list[dict]
) -> int:
    """
    Upsert multiple bond quotes.

    Args:
        con: Database connection
        quotes: List of quote dicts

    Returns:
        Number of rows upserted
    """
    count = 0
    for quote in quotes:
        if upsert_bond_quote(con, quote):
            count += 1
    return count


def get_bond_quotes(
    con: sqlite3.Connection,
    bond_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> "pd.DataFrame":
    """
    Get bond quotes.

    Args:
        con: Database connection
        bond_id: Bond ID
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows

    Returns:
        DataFrame with quotes
    """
    import pandas as pd

    query = "SELECT * FROM bond_quotes WHERE bond_id = ?"
    params = [bond_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_bond_latest_quote(con: sqlite3.Connection, bond_id: str) -> dict | None:
    """Get the latest quote for a bond."""
    try:
        cur = con.execute("""
            SELECT * FROM bond_quotes
            WHERE bond_id = ?
            ORDER BY date DESC
            LIMIT 1
        """, (bond_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_all_latest_quotes(
    con: sqlite3.Connection,
    bond_type: str | None = None,
) -> list[dict]:
    """Get latest quotes for all bonds with optional type filter."""
    query = """
        SELECT bq.*, bm.symbol, bm.issuer, bm.bond_type, bm.coupon_rate,
               bm.maturity_date, bm.is_islamic
        FROM bond_quotes bq
        JOIN bonds_master bm ON bq.bond_id = bm.bond_id
        WHERE bq.date = (
            SELECT MAX(date) FROM bond_quotes WHERE bond_id = bq.bond_id
        )
        AND bm.is_active = 1
    """
    params = []

    if bond_type:
        query += " AND bm.bond_type = ?"
        params.append(bond_type)

    query += " ORDER BY bm.maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_yield_curve_point(con: sqlite3.Connection, point_data: dict) -> bool:
    """
    Upsert a yield curve point.

    Args:
        con: Database connection
        point_data: Dict with curve_date, tenor_months, yield_rate, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO yield_curve_points (
                curve_date, tenor_months, yield_rate, bond_type,
                interpolation, computed_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(curve_date, tenor_months, bond_type) DO UPDATE SET
                yield_rate = excluded.yield_rate,
                interpolation = excluded.interpolation,
                computed_at = datetime('now')
        """, (
            point_data.get("curve_date"),
            point_data.get("tenor_months"),
            point_data.get("yield_rate"),
            point_data.get("bond_type", "PIB"),
            point_data.get("interpolation", "LINEAR"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_yield_curve(
    con: sqlite3.Connection,
    curve_date: str,
    bond_type: str = "PIB",
) -> list[dict]:
    """
    Get yield curve points for a date.

    Args:
        con: Database connection
        curve_date: Date for the curve
        bond_type: Bond type filter

    Returns:
        List of curve points sorted by tenor
    """
    try:
        cur = con.execute("""
            SELECT * FROM yield_curve_points
            WHERE curve_date = ? AND bond_type = ?
            ORDER BY tenor_months ASC
        """, (curve_date, bond_type))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_yield_curve(
    con: sqlite3.Connection,
    bond_type: str = "PIB",
) -> tuple[str | None, list[dict]]:
    """
    Get the most recent yield curve.

    Args:
        con: Database connection
        bond_type: Bond type filter

    Returns:
        Tuple of (curve_date, list of points)
    """
    try:
        # Get latest date
        cur = con.execute("""
            SELECT MAX(curve_date) FROM yield_curve_points
            WHERE bond_type = ?
        """, (bond_type,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None, []

        curve_date = row[0]
        points = get_yield_curve(con, curve_date, bond_type)
        return curve_date, points
    except Exception:
        return None, []


def upsert_bond_analytics(con: sqlite3.Connection, analytics: dict) -> bool:
    """
    Upsert bond analytics snapshot.

    Args:
        con: Database connection
        analytics: Dict with analytics fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bond_analytics_snapshots (
                bond_id, as_of_date, price, ytm, duration,
                modified_duration, convexity, accrued_interest,
                spread_to_benchmark, days_to_maturity, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(bond_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                duration = excluded.duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                accrued_interest = excluded.accrued_interest,
                spread_to_benchmark = excluded.spread_to_benchmark,
                days_to_maturity = excluded.days_to_maturity,
                computed_at = datetime('now')
        """, (
            analytics.get("bond_id"),
            analytics.get("as_of_date"),
            analytics.get("price"),
            analytics.get("ytm"),
            analytics.get("duration"),
            analytics.get("modified_duration"),
            analytics.get("convexity"),
            analytics.get("accrued_interest"),
            analytics.get("spread_to_benchmark"),
            analytics.get("days_to_maturity"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_bond_analytics(
    con: sqlite3.Connection,
    bond_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get bond analytics snapshot."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM bond_analytics_snapshots
                WHERE bond_id = ? AND as_of_date = ?
            """, (bond_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM bond_analytics_snapshots
                WHERE bond_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (bond_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def record_bond_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0
) -> bool:
    """Record the start of a bond sync run."""
    try:
        con.execute("""
            INSERT INTO bond_sync_runs (
                run_id, started_at, sync_type, status, items_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_bond_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a bond sync run."""
    try:
        con.execute("""
            UPDATE bond_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_bond_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent bond sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM bond_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_bond_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of bond data in database.

    Returns:
        Dict with bond counts, date ranges, type breakdown, etc.
    """
    summary = {
        "total_bonds": 0,
        "active_bonds": 0,
        "bonds_with_quotes": 0,
        "total_quote_rows": 0,
        "bond_types": {},
        "issuers": {},
        "islamic_count": 0,
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "yield_curve_dates": 0,
    }

    try:
        # Total and active bonds
        cur = con.execute("SELECT COUNT(*) FROM bonds_master")
        summary["total_bonds"] = cur.fetchone()[0]

        cur = con.execute("SELECT COUNT(*) FROM bonds_master WHERE is_active = 1")
        summary["active_bonds"] = cur.fetchone()[0]

        # Islamic bonds
        cur = con.execute(
            "SELECT COUNT(*) FROM bonds_master WHERE is_islamic = 1 AND is_active = 1"
        )
        summary["islamic_count"] = cur.fetchone()[0]

        # Bonds with quotes
        cur = con.execute("SELECT COUNT(DISTINCT bond_id) FROM bond_quotes")
        summary["bonds_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM bond_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Bond type breakdown
        cur = con.execute("""
            SELECT bond_type, COUNT(*) as count
            FROM bonds_master
            WHERE is_active = 1
            GROUP BY bond_type
            ORDER BY count DESC
        """)
        summary["bond_types"] = {row[0]: row[1] for row in cur.fetchall()}

        # Issuer breakdown
        cur = con.execute("""
            SELECT issuer, COUNT(*) as count
            FROM bonds_master
            WHERE is_active = 1
            GROUP BY issuer
            ORDER BY count DESC
        """)
        summary["issuers"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute("SELECT MAX(date), MIN(date) FROM bond_quotes")
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Yield curve dates
        cur = con.execute("SELECT COUNT(DISTINCT curve_date) FROM yield_curve_points")
        summary["yield_curve_dates"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# Phase 3: Sukuk/Debt Market Functions
# =============================================================================


def upsert_sukuk(con: sqlite3.Connection, sukuk_data: dict) -> bool:
    """
    Upsert a sukuk into the sukuk_master table.

    Args:
        con: Database connection
        sukuk_data: Dict with sukuk fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO sukuk_master (
                instrument_id, issuer, name, category, currency,
                issue_date, maturity_date, coupon_rate, coupon_frequency,
                face_value, issue_size, shariah_compliant, is_active,
                source, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                issuer = excluded.issuer,
                name = excluded.name,
                category = excluded.category,
                currency = excluded.currency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                face_value = excluded.face_value,
                issue_size = excluded.issue_size,
                shariah_compliant = excluded.shariah_compliant,
                is_active = excluded.is_active,
                source = excluded.source,
                notes = excluded.notes
        """, (
            sukuk_data.get("instrument_id"),
            sukuk_data.get("issuer"),
            sukuk_data.get("name"),
            sukuk_data.get("category"),
            sukuk_data.get("currency", "PKR"),
            sukuk_data.get("issue_date"),
            sukuk_data.get("maturity_date"),
            sukuk_data.get("coupon_rate"),
            sukuk_data.get("coupon_frequency"),
            sukuk_data.get("face_value", 100.0),
            sukuk_data.get("issue_size"),
            sukuk_data.get("shariah_compliant", 1),
            sukuk_data.get("is_active", 1),
            sukuk_data.get("source", "MANUAL"),
            sukuk_data.get("notes"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_list(
    con: sqlite3.Connection,
    category: str | None = None,
    issuer: str | None = None,
    shariah_only: bool = False,
    active_only: bool = True,
) -> list[dict]:
    """
    Get sukuk instruments with optional filters.

    Args:
        con: Database connection
        category: Filter by category (GOP_SUKUK, PIB, T-BILL, etc.)
        issuer: Filter by issuer
        shariah_only: Only return Shariah-compliant instruments
        active_only: Only return active instruments

    Returns:
        List of sukuk dicts
    """
    query = "SELECT * FROM sukuk_master WHERE 1=1"
    params = []

    if active_only:
        query += " AND is_active = 1"
    if category:
        query += " AND category = ?"
        params.append(category)
    if issuer:
        query += " AND issuer = ?"
        params.append(issuer)
    if shariah_only:
        query += " AND shariah_compliant = 1"

    query += " ORDER BY maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single sukuk by instrument ID."""
    try:
        cur = con.execute(
            "SELECT * FROM sukuk_master WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sukuk_quote(con: sqlite3.Connection, quote_data: dict) -> bool:
    """
    Upsert a sukuk quote.

    Args:
        con: Database connection
        quote_data: Dict with quote fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO sukuk_quotes (
                instrument_id, quote_date, clean_price, dirty_price,
                yield_to_maturity, bid_yield, ask_yield, volume,
                source, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id, quote_date) DO UPDATE SET
                clean_price = excluded.clean_price,
                dirty_price = excluded.dirty_price,
                yield_to_maturity = excluded.yield_to_maturity,
                bid_yield = excluded.bid_yield,
                ask_yield = excluded.ask_yield,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            quote_data.get("instrument_id"),
            quote_data.get("quote_date"),
            quote_data.get("clean_price"),
            quote_data.get("dirty_price"),
            quote_data.get("yield_to_maturity"),
            quote_data.get("bid_yield"),
            quote_data.get("ask_yield"),
            quote_data.get("volume"),
            quote_data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_quotes(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> "pd.DataFrame":
    """Get sukuk quotes as DataFrame."""
    import pandas as pd

    query = "SELECT * FROM sukuk_quotes WHERE instrument_id = ?"
    params = [instrument_id]

    if start_date:
        query += " AND quote_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND quote_date <= ?"
        params.append(end_date)

    query += " ORDER BY quote_date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_sukuk_latest_quote(
    con: sqlite3.Connection,
    instrument_id: str
) -> dict | None:
    """Get the latest quote for a sukuk."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_quotes
            WHERE instrument_id = ?
            ORDER BY quote_date DESC
            LIMIT 1
        """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sukuk_yield_curve_point(
    con: sqlite3.Connection,
    point_data: dict
) -> bool:
    """Upsert a yield curve point."""
    try:
        con.execute("""
            INSERT INTO sukuk_yield_curve (
                curve_name, curve_date, tenor_days, yield_rate,
                source, computed_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(curve_name, curve_date, tenor_days) DO UPDATE SET
                yield_rate = excluded.yield_rate,
                source = excluded.source,
                computed_at = datetime('now')
        """, (
            point_data.get("curve_name"),
            point_data.get("curve_date"),
            point_data.get("tenor_days"),
            point_data.get("yield_rate"),
            point_data.get("source", "SBP"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_yield_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str,
) -> list[dict]:
    """Get yield curve points for a specific curve and date."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_yield_curve
            WHERE curve_name = ? AND curve_date = ?
            ORDER BY tenor_days ASC
        """, (curve_name, curve_date))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk_latest_yield_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str | None = None,
) -> list[dict]:
    """
    Get yield curve for a curve name.

    Args:
        con: Database connection
        curve_name: Name of the curve (e.g., 'GOP_SUKUK')
        curve_date: Specific date (None = latest available)

    Returns:
        List of curve point dicts
    """
    try:
        if curve_date is None:
            # Get latest date
            cur = con.execute("""
                SELECT MAX(curve_date) FROM sukuk_yield_curve
                WHERE curve_name = ?
            """, (curve_name,))
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            curve_date = row[0]

        points = get_sukuk_yield_curve(con, curve_name, curve_date)
        return points
    except Exception:
        return []


def get_available_curve_dates(
    con: sqlite3.Connection,
    curve_name: str,
    limit: int = 30,
) -> list[str]:
    """Get available curve dates for a curve name."""
    try:
        cur = con.execute("""
            SELECT DISTINCT curve_date FROM sukuk_yield_curve
            WHERE curve_name = ?
            ORDER BY curve_date DESC
            LIMIT ?
        """, (curve_name, limit))
        return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def upsert_sukuk_analytics(con: sqlite3.Connection, analytics: dict) -> bool:
    """Upsert sukuk analytics snapshot."""
    try:
        con.execute("""
            INSERT INTO sukuk_analytics_snapshots (
                instrument_id, as_of_date, price, ytm,
                macaulay_duration, modified_duration, convexity,
                accrued_interest, days_to_maturity, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                macaulay_duration = excluded.macaulay_duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                accrued_interest = excluded.accrued_interest,
                days_to_maturity = excluded.days_to_maturity,
                computed_at = datetime('now')
        """, (
            analytics.get("instrument_id"),
            analytics.get("as_of_date"),
            analytics.get("price"),
            analytics.get("ytm"),
            analytics.get("macaulay_duration"),
            analytics.get("modified_duration"),
            analytics.get("convexity"),
            analytics.get("accrued_interest"),
            analytics.get("days_to_maturity"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_analytics(
    con: sqlite3.Connection,
    instrument_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get sukuk analytics snapshot."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM sukuk_analytics_snapshots
                WHERE instrument_id = ? AND as_of_date = ?
            """, (instrument_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM sukuk_analytics_snapshots
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sbp_document(con: sqlite3.Connection, doc_data: dict) -> bool:
    """Upsert an SBP primary market document."""
    try:
        con.execute("""
            INSERT INTO sbp_primary_market_docs (
                doc_id, category, title, doc_date, url,
                local_path, file_size, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(doc_id) DO UPDATE SET
                category = excluded.category,
                title = excluded.title,
                doc_date = excluded.doc_date,
                url = excluded.url,
                local_path = excluded.local_path,
                file_size = excluded.file_size,
                fetched_at = datetime('now')
        """, (
            doc_data.get("doc_id"),
            doc_data.get("category"),
            doc_data.get("title"),
            doc_data.get("doc_date"),
            doc_data.get("url"),
            doc_data.get("local_path"),
            doc_data.get("file_size"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sbp_documents(
    con: sqlite3.Connection,
    category: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Get SBP primary market documents."""
    query = "SELECT * FROM sbp_primary_market_docs WHERE 1=1"
    params = []

    if category:
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY doc_date DESC, fetched_at DESC"
    query += f" LIMIT {limit}"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def record_sukuk_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0
) -> bool:
    """Record the start of a sukuk sync run."""
    try:
        con.execute("""
            INSERT INTO sukuk_sync_runs (
                run_id, started_at, sync_type, status, items_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_sukuk_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a sukuk sync run."""
    try:
        con.execute("""
            UPDATE sukuk_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent sukuk sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of sukuk data in database.

    Returns:
        Dict with sukuk counts, date ranges, category breakdown, etc.
    """
    summary = {
        "total_instruments": 0,
        "active_instruments": 0,
        "shariah_compliant": 0,
        "instruments_with_quotes": 0,
        "total_quote_rows": 0,
        "categories": {},
        "issuers": {},
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "yield_curves": 0,
        "sbp_documents": 0,
    }

    try:
        # Total and active instruments
        cur = con.execute("SELECT COUNT(*) FROM sukuk_master")
        summary["total_instruments"] = cur.fetchone()[0]

        cur = con.execute(
            "SELECT COUNT(*) FROM sukuk_master WHERE is_active = 1"
        )
        summary["active_instruments"] = cur.fetchone()[0]

        # Shariah compliant
        cur = con.execute(
            "SELECT COUNT(*) FROM sukuk_master WHERE shariah_compliant = 1"
        )
        summary["shariah_compliant"] = cur.fetchone()[0]

        # Instruments with quotes
        cur = con.execute(
            "SELECT COUNT(DISTINCT instrument_id) FROM sukuk_quotes"
        )
        summary["instruments_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM sukuk_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM sukuk_master
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Issuer breakdown
        cur = con.execute("""
            SELECT issuer, COUNT(*) as count
            FROM sukuk_master
            WHERE is_active = 1
            GROUP BY issuer
            ORDER BY count DESC
        """)
        summary["issuers"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute(
            "SELECT MAX(quote_date), MIN(quote_date) FROM sukuk_quotes"
        )
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Yield curve count
        cur = con.execute(
            "SELECT COUNT(DISTINCT curve_name || curve_date) FROM sukuk_yield_curve"
        )
        summary["yield_curves"] = cur.fetchone()[0]

        # SBP documents
        cur = con.execute("SELECT COUNT(*) FROM sbp_primary_market_docs")
        summary["sbp_documents"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# Phase 3: Fixed Income CRUD Functions
# =============================================================================


def upsert_fi_instrument(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income instrument."""
    try:
        con.execute("""
            INSERT INTO fi_instruments (
                instrument_id, isin, issuer, name, category, currency,
                issue_date, maturity_date, coupon_rate, coupon_frequency,
                day_count, face_value, shariah_compliant, is_active, source,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                isin = excluded.isin,
                issuer = excluded.issuer,
                name = excluded.name,
                category = excluded.category,
                currency = excluded.currency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                day_count = excluded.day_count,
                face_value = excluded.face_value,
                shariah_compliant = excluded.shariah_compliant,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            data.get("instrument_id"),
            data.get("isin"),
            data.get("issuer", "GOVT_OF_PAKISTAN"),
            data.get("name"),
            data.get("category"),
            data.get("currency", "PKR"),
            data.get("issue_date"),
            data.get("maturity_date"),
            data.get("coupon_rate"),
            data.get("coupon_frequency"),
            data.get("day_count", "ACT/365"),
            data.get("face_value", 100.0),
            1 if data.get("shariah_compliant") else 0,
            1 if data.get("is_active", True) else 0,
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_instruments(
    con: sqlite3.Connection,
    category: str | None = None,
    active_only: bool = True,
    issuer: str | None = None,
) -> list[dict]:
    """Get fixed income instruments with optional filters."""
    try:
        query = "SELECT * FROM fi_instruments WHERE 1=1"
        params = []

        if active_only:
            query += " AND is_active = 1"

        if category:
            query += " AND category = ?"
            params.append(category)

        if issuer:
            query += " AND issuer LIKE ?"
            params.append(f"%{issuer}%")

        query += " ORDER BY maturity_date ASC"

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_instrument(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single fixed income instrument by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM fi_instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fi_quote(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income quote."""
    try:
        con.execute("""
            INSERT INTO fi_quotes (
                instrument_id, quote_date, clean_price, ytm, bid, ask,
                volume, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, quote_date) DO UPDATE SET
                clean_price = excluded.clean_price,
                ytm = excluded.ytm,
                bid = excluded.bid,
                ask = excluded.ask,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            data.get("instrument_id"),
            data.get("quote_date"),
            data.get("clean_price"),
            data.get("ytm"),
            data.get("bid"),
            data.get("ask"),
            data.get("volume"),
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_quotes(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 365,
) -> list[dict]:
    """Get quotes for an instrument."""
    try:
        query = "SELECT * FROM fi_quotes WHERE instrument_id = ?"
        params = [instrument_id]

        if start_date:
            query += " AND quote_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND quote_date <= ?"
            params.append(end_date)

        query += " ORDER BY quote_date DESC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_latest_quote(
    con: sqlite3.Connection,
    instrument_id: str,
) -> dict | None:
    """Get latest quote for an instrument."""
    try:
        cur = con.execute("""
            SELECT * FROM fi_quotes
            WHERE instrument_id = ?
            ORDER BY quote_date DESC
            LIMIT 1
        """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fi_curve_point(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a yield curve point."""
    try:
        con.execute("""
            INSERT INTO fi_curves (curve_name, curve_date, tenor_days, rate, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(curve_name, curve_date, tenor_days) DO UPDATE SET
                rate = excluded.rate,
                source = excluded.source
        """, (
            data.get("curve_name"),
            data.get("curve_date"),
            data.get("tenor_days"),
            data.get("rate"),
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str | None = None,
) -> list[dict]:
    """
    Get yield curve points.

    Args:
        con: Database connection
        curve_name: Name of curve (PKR_MTB, PKR_PIB, etc.)
        curve_date: Specific date (None = latest)

    Returns:
        List of curve points sorted by tenor
    """
    try:
        if curve_date is None:
            # Get latest date for this curve
            cur = con.execute("""
                SELECT MAX(curve_date) FROM fi_curves
                WHERE curve_name = ?
            """, (curve_name,))
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            curve_date = row[0]

        cur = con.execute("""
            SELECT * FROM fi_curves
            WHERE curve_name = ? AND curve_date = ?
            ORDER BY tenor_days ASC
        """, (curve_name, curve_date))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_curve_dates(
    con: sqlite3.Connection,
    curve_name: str | None = None,
    limit: int = 30,
) -> list:
    """
    Get available curve dates.

    If curve_name is provided, returns list of date strings for that curve.
    If curve_name is None, returns list of dicts with curve summaries.
    """
    try:
        if curve_name:
            cur = con.execute("""
                SELECT DISTINCT curve_date FROM fi_curves
                WHERE curve_name = ?
                ORDER BY curve_date DESC
                LIMIT ?
            """, (curve_name, limit))
            return [row[0] for row in cur.fetchall()]
        else:
            # Return summary of all curves
            cur = con.execute("""
                SELECT curve_name,
                       MAX(curve_date) as latest_date,
                       COUNT(*) as count
                FROM fi_curves
                GROUP BY curve_name
                ORDER BY curve_name
            """)
            return [
                {"curve_name": row[0], "latest_date": row[1], "count": row[2]}
                for row in cur.fetchall()
            ]
    except Exception:
        return []


def upsert_fi_analytics(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert fixed income analytics snapshot."""
    try:
        con.execute("""
            INSERT INTO fi_analytics (
                instrument_id, as_of_date, price, ytm,
                macaulay_duration, modified_duration, convexity, pvbp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                macaulay_duration = excluded.macaulay_duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                pvbp = excluded.pvbp
        """, (
            data.get("instrument_id"),
            data.get("as_of_date"),
            data.get("price"),
            data.get("ytm"),
            data.get("macaulay_duration"),
            data.get("modified_duration"),
            data.get("convexity"),
            data.get("pvbp"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_analytics(
    con: sqlite3.Connection,
    instrument_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get analytics for an instrument."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM fi_analytics
                WHERE instrument_id = ? AND as_of_date = ?
            """, (instrument_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM fi_analytics
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sbp_pma_doc(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert an SBP PMA document record."""
    try:
        con.execute("""
            INSERT INTO sbp_pma_docs (
                doc_id, category, title, doc_date, url, local_path, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
                category = excluded.category,
                title = excluded.title,
                doc_date = excluded.doc_date,
                url = excluded.url,
                local_path = excluded.local_path,
                fetched_at = datetime('now')
        """, (
            data.get("doc_id"),
            data.get("category"),
            data.get("title"),
            data.get("doc_date"),
            data.get("url"),
            data.get("local_path"),
            data.get("source", "SBP_PMA"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sbp_pma_docs(
    con: sqlite3.Connection,
    category: str | None = None,
    doc_type: str | None = None,
    since: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Get SBP PMA documents with optional filters."""
    try:
        query = "SELECT * FROM sbp_pma_docs WHERE 1=1"
        params = []

        if category:
            query += " AND category = ?"
            params.append(category)

        if doc_type:
            query += " AND doc_type = ?"
            params.append(doc_type)

        if since:
            query += " AND doc_date >= ?"
            params.append(since)

        query += " ORDER BY doc_date DESC, fetched_at DESC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_fi_event(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income event."""
    try:
        con.execute("""
            INSERT INTO fi_events (
                event_id, category, event_date, label, notes, source_doc_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                category = excluded.category,
                event_date = excluded.event_date,
                label = excluded.label,
                notes = excluded.notes,
                source_doc_id = excluded.source_doc_id
        """, (
            data.get("event_id"),
            data.get("category"),
            data.get("event_date"),
            data.get("label"),
            data.get("notes"),
            data.get("source_doc_id"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def record_fi_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0,
) -> bool:
    """Record start of a fixed income sync run."""
    try:
        con.execute("""
            INSERT INTO fi_sync_runs (run_id, started_at, sync_type, items_total)
            VALUES (?, datetime('now'), ?, ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_fi_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error_message: str | None = None,
) -> bool:
    """Update a fixed income sync run."""
    try:
        con.execute("""
            UPDATE fi_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error_message, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent fixed income sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM fi_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_data_summary(con: sqlite3.Connection) -> dict:
    """Get summary of fixed income data in database."""
    summary = {
        "total_instruments": 0,
        "active_instruments": 0,
        "instruments_with_quotes": 0,
        "total_quote_rows": 0,
        "categories": {},
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "curve_count": 0,
        "sbp_doc_count": 0,
    }

    try:
        # Total and active instruments
        cur = con.execute("SELECT COUNT(*) FROM fi_instruments")
        summary["total_instruments"] = cur.fetchone()[0]

        cur = con.execute(
            "SELECT COUNT(*) FROM fi_instruments WHERE is_active = 1"
        )
        summary["active_instruments"] = cur.fetchone()[0]

        # Instruments with quotes
        cur = con.execute(
            "SELECT COUNT(DISTINCT instrument_id) FROM fi_quotes"
        )
        summary["instruments_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM fi_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM fi_instruments
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute(
            "SELECT MAX(quote_date), MIN(quote_date) FROM fi_quotes"
        )
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Curve count
        cur = con.execute(
            "SELECT COUNT(DISTINCT curve_name || curve_date) FROM fi_curves"
        )
        summary["curve_count"] = cur.fetchone()[0]

        # SBP documents
        cur = con.execute("SELECT COUNT(*) FROM sbp_pma_docs")
        summary["sbp_doc_count"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# SBP Policy Rates and KIBOR CRUD Functions
# =============================================================================

def upsert_policy_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert SBP policy rate data."""
    try:
        con.execute("""
            INSERT INTO sbp_policy_rates (
                rate_date, policy_rate, ceiling_rate, floor_rate,
                overnight_repo_rate, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(rate_date) DO UPDATE SET
                policy_rate = excluded.policy_rate,
                ceiling_rate = excluded.ceiling_rate,
                floor_rate = excluded.floor_rate,
                overnight_repo_rate = excluded.overnight_repo_rate,
                source = excluded.source
        """, (
            data.get("rate_date"),
            data.get("policy_rate"),
            data.get("ceiling_rate"),
            data.get("floor_rate"),
            data.get("overnight_repo_rate"),
            data.get("source", "SBP_MSM"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_latest_policy_rate(con: sqlite3.Connection) -> dict | None:
    """Get the latest SBP policy rate."""
    try:
        cur = con.execute("""
            SELECT * FROM sbp_policy_rates
            ORDER BY rate_date DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_policy_rates(
    con: sqlite3.Connection,
    since: str | None = None,
    limit: int = 30,
) -> list[dict]:
    """Get SBP policy rate history."""
    try:
        if since:
            cur = con.execute("""
                SELECT * FROM sbp_policy_rates
                WHERE rate_date >= ?
                ORDER BY rate_date DESC
                LIMIT ?
            """, (since, limit))
        else:
            cur = con.execute("""
                SELECT * FROM sbp_policy_rates
                ORDER BY rate_date DESC
                LIMIT ?
            """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_kibor_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert KIBOR rate data."""
    try:
        con.execute("""
            INSERT INTO kibor_rates (
                rate_date, tenor_months, bid, offer, source
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(rate_date, tenor_months) DO UPDATE SET
                bid = excluded.bid,
                offer = excluded.offer,
                source = excluded.source
        """, (
            data.get("rate_date"),
            data.get("tenor_months"),
            data.get("bid"),
            data.get("offer"),
            data.get("source", "SBP_MSM"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_kibor_rates(
    con: sqlite3.Connection,
    rate_date: str | None = None,
    tenor_months: int | None = None,
    limit: int = 30,
) -> list[dict]:
    """Get KIBOR rates with optional filters."""
    try:
        query = "SELECT * FROM kibor_rates WHERE 1=1"
        params = []

        if rate_date:
            query += " AND rate_date = ?"
            params.append(rate_date)

        if tenor_months:
            query += " AND tenor_months = ?"
            params.append(tenor_months)

        query += " ORDER BY rate_date DESC, tenor_months ASC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_kibor_rates(con: sqlite3.Connection) -> list[dict]:
    """Get the latest KIBOR rates for all tenors."""
    try:
        cur = con.execute("""
            SELECT * FROM kibor_rates
            WHERE rate_date = (SELECT MAX(rate_date) FROM kibor_rates)
            ORDER BY tenor_months ASC
        """)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
