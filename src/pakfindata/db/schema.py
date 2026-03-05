"""Database schema definitions and DDL statements."""

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
    turnover     REAL,                  -- PKR traded value from post_close
    prev_close   REAL,                  -- Previous day close price
    sector_code  TEXT,                  -- Sector code from market summary
    company_name TEXT,                  -- Company name from market summary
    ingested_at  TEXT NOT NULL,
    source       TEXT,                  -- Data source: market_summary, closing_rates_pdf, per_symbol_api
    processname  TEXT,                  -- Process type: eodfile (csv/pdf), per_symbol_api
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_date ON eod_ohlcv(date);
CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_symbol ON eod_ohlcv(symbol);
CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_source ON eod_ohlcv(source);
CREATE INDEX IF NOT EXISTS idx_eod_ohlcv_processname ON eod_ohlcv(processname);

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

-- Data freshness tracking per domain
CREATE TABLE IF NOT EXISTS data_freshness (
    domain         TEXT PRIMARY KEY,
    display_name   TEXT NOT NULL,
    source_table   TEXT NOT NULL,
    date_column    TEXT NOT NULL DEFAULT 'date',
    last_sync_at   TEXT,
    last_row_date  TEXT,
    row_count      INTEGER DEFAULT 0,
    status         TEXT DEFAULT 'unknown'
);

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
    operation   TEXT NOT NULL DEFAULT 'insert',
    process_ts  TEXT NOT NULL DEFAULT (datetime('now')),
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, ts, close)
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
    sales               REAL,               -- Total Revenue/Sales (non-banks) or Total Income (banks)
    gross_profit        REAL,               -- Gross Profit (non-banks) or Net Interest Income (banks)
    operating_profit    REAL,
    profit_before_tax   REAL,
    profit_after_tax    REAL,               -- Net Income
    eps                 REAL,               -- Earnings Per Share

    -- Banking-specific (interest income/expense for gross margin)
    markup_earned       REAL,               -- Mark-up/Interest earned (banks: top-line)
    markup_expensed     REAL,               -- Mark-up/Interest expensed (banks: cost of funds)

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
    mufap_fund_id       TEXT,                       -- MUFAP GUID (FundID field)
    mufap_int_id        TEXT,                       -- MUFAP integer ID (fund field) for historical NAV API
    mufap_amc_id        TEXT,                       -- MUFAP AMC ID for API calls
    front_load          REAL,                       -- Front-end load (%)
    back_load           REAL,                       -- Back-end load (%)
    risk_profile        TEXT,                       -- Risk classification
    benchmark           TEXT,                       -- Benchmark index name
    rating              TEXT,                       -- Fund rating
    trustee             TEXT,                       -- Trustee name
    fund_manager        TEXT,                       -- Fund manager name
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
CREATE INDEX IF NOT EXISTS idx_mf_nav_fund_date
    ON mutual_fund_nav(fund_id, date DESC);

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

-- =============================================================================
-- COMPOSITE / COVERING INDEXES for common query patterns
-- =============================================================================

-- EOD: symbol + date range queries (most common pattern)
CREATE INDEX IF NOT EXISTS idx_eod_symbol_date
    ON eod_ohlcv(symbol, date);

-- Company fundamentals queried by sector
CREATE INDEX IF NOT EXISTS idx_fundamentals_sector
    ON company_fundamentals(sector_name);

-- Financial announcements queried by date range
CREATE INDEX IF NOT EXISTS idx_fin_ann_date
    ON financial_announcements(announcement_date);

-- Company quote snapshots queried by symbol + timestamp
CREATE INDEX IF NOT EXISTS idx_quote_snap_symbol_ts
    ON company_quote_snapshots(symbol, ts);
"""
