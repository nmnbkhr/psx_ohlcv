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
