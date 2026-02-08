# PSX OHLCV Database Schema Documentation

**Database:** SQLite 3
**Location:** `/mnt/e/psxdata/psx.sqlite`
**Size:** ~106 MB
**Tables:** 70 tables
**Generated:** 2026-01-30

---

## Table Categories Overview

| Category | Tables | Purpose |
|----------|--------|---------|
| **Core Market Data** | 8 | EOD OHLCV, intraday bars, symbols, sectors |
| **Company Data** | 14 | Profiles, fundamentals, financials, ratios, payouts |
| **Instruments Universe** | 6 | ETFs, REITs, indices, rankings, memberships |
| **Fixed Income** | 14 | Sukuk, bonds, yield curves, SBP data |
| **FX Data** | 4 | Currency pairs, OHLCV, adjusted metrics |
| **Mutual Funds** | 3 | Fund master, NAV history |
| **Analytics** | 5 | Market/sector/symbol snapshots |
| **Sync & Operations** | 10 | Sync runs, failures, job tracking |
| **System** | 6 | User interactions, LLM cache, notifications |

---

## 1. Core Market Data Tables

### `symbols`
Primary symbol registry from PSX Market Watch.

```sql
CREATE TABLE symbols (
    symbol            TEXT PRIMARY KEY,
    name              TEXT NULL,
    sector            TEXT NULL,
    sector_name       TEXT NULL,
    is_active         INTEGER DEFAULT 1,
    outstanding_shares REAL NULL,
    source            TEXT NOT NULL DEFAULT 'MARKET_WATCH',
    discovered_at     TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);
```

### `eod_ohlcv`
End-of-day OHLCV data for all equities and indices.

```sql
CREATE TABLE eod_ohlcv (
    symbol        TEXT NOT NULL,
    date          TEXT NOT NULL,
    open          REAL,
    high          REAL,
    low           REAL,
    close         REAL,
    volume        INTEGER,
    prev_close    REAL,
    sector_code   TEXT,
    company_name  TEXT,
    ingested_at   TEXT NOT NULL,
    PRIMARY KEY (symbol, date)
);
-- Indexes: idx_eod_ohlcv_date, idx_eod_ohlcv_symbol
```

### `intraday_bars`
1-minute intraday bars for real-time analysis.

```sql
CREATE TABLE intraday_bars (
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
-- Indexes: idx_intraday_bars_symbol, idx_intraday_bars_ts, idx_intraday_bars_ts_epoch
```

### `intraday_sync_state`
Tracks last sync timestamp per symbol for incremental sync.

```sql
CREATE TABLE intraday_sync_state (
    symbol        TEXT PRIMARY KEY,
    last_ts       TEXT NULL,
    last_ts_epoch INTEGER NULL,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `sectors`
Sector master data.

```sql
CREATE TABLE sectors (
    sector_code TEXT PRIMARY KEY,
    sector_name TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    source      TEXT NOT NULL DEFAULT 'DPS_SECTOR_SUMMARY'
);
```

### `regular_market_current`
Current market snapshot (latest prices).

```sql
CREATE TABLE regular_market_current (
    symbol      TEXT PRIMARY KEY,
    ts          TEXT NOT NULL,
    status      TEXT,
    sector_code TEXT,
    listed_in   TEXT,
    ldcp        REAL,
    open        REAL,
    high        REAL,
    low         REAL,
    current     REAL,
    change      REAL,
    change_pct  REAL,
    volume      REAL,
    row_hash    TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `regular_market_snapshots`
Historical market snapshots for trending.

```sql
CREATE TABLE regular_market_snapshots (
    ts          TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    status      TEXT,
    sector_code TEXT,
    listed_in   TEXT,
    ldcp        REAL,
    open        REAL,
    high        REAL,
    low         REAL,
    current     REAL,
    change      REAL,
    change_pct  REAL,
    volume      REAL,
    row_hash    TEXT NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (ts, symbol)
);
-- Index: idx_rm_snapshots_symbol, idx_rm_snapshots_ts
```

### `psx_indices`
PSX index data (KSE-100, KSE-30, KMI-30, etc.).

```sql
CREATE TABLE psx_indices (
    index_code          TEXT NOT NULL,
    index_date          TEXT NOT NULL,
    index_time          TEXT,
    value               REAL NOT NULL,
    change              REAL,
    change_pct          REAL,
    open                REAL,
    high                REAL,
    low                 REAL,
    volume              INTEGER,
    previous_close      REAL,
    ytd_change_pct      REAL,
    one_year_change_pct REAL,
    week_52_low         REAL,
    week_52_high        REAL,
    trades              INTEGER,
    market_cap          REAL,
    turnover            REAL,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (index_code, index_date)
);
```

---

## 2. Company Data Tables

### `company_profile`
Basic company profile information.

```sql
CREATE TABLE company_profile (
    symbol               TEXT PRIMARY KEY,
    company_name         TEXT NULL,
    sector_name          TEXT NULL,
    business_description TEXT NULL,
    address              TEXT NULL,
    website              TEXT NULL,
    registrar            TEXT NULL,
    auditor              TEXT NULL,
    fiscal_year_end      TEXT NULL,
    updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
    source_url           TEXT NOT NULL
);
```

### `company_fundamentals`
Current company fundamentals (latest snapshot).

```sql
CREATE TABLE company_fundamentals (
    symbol              TEXT PRIMARY KEY,
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
    -- Performance
    ytd_change_pct      REAL,
    one_year_change_pct REAL,
    -- Valuation
    pe_ratio            REAL,
    market_cap          REAL,
    -- Equity Structure
    total_shares        INTEGER,
    free_float_shares   INTEGER,
    free_float_pct      REAL,
    -- Risk
    haircut             REAL,
    variance            REAL,
    -- Profile
    business_description TEXT,
    address             TEXT,
    website             TEXT,
    registrar           TEXT,
    auditor             TEXT,
    fiscal_year_end     TEXT,
    incorporation_date  TEXT,
    listed_in           TEXT,
    -- Metadata
    as_of               TEXT,
    market_mode         TEXT,
    source_url          TEXT,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `company_fundamentals_history`
Historical fundamentals for time-series analysis.

```sql
CREATE TABLE company_fundamentals_history (
    symbol              TEXT NOT NULL,
    date                TEXT NOT NULL,
    -- Same structure as company_fundamentals
    ...
    snapshot_ts         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date)
);
```

### `company_financials`
Financial statements (income statement, balance sheet).

```sql
CREATE TABLE company_financials (
    symbol              TEXT NOT NULL,
    period_end          TEXT NOT NULL,
    period_type         TEXT NOT NULL,  -- 'annual' or 'quarterly'
    -- Income Statement
    sales               REAL,
    gross_profit        REAL,
    operating_profit    REAL,
    profit_before_tax   REAL,
    profit_after_tax    REAL,
    eps                 REAL,
    -- Balance Sheet
    total_assets        REAL,
    total_liabilities   REAL,
    total_equity        REAL,
    currency            TEXT DEFAULT 'PKR',
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, period_end, period_type)
);
```

### `company_ratios`
Financial ratios and growth metrics.

```sql
CREATE TABLE company_ratios (
    symbol              TEXT NOT NULL,
    period_end          TEXT NOT NULL,
    period_type         TEXT NOT NULL,
    -- Profitability
    gross_profit_margin REAL,
    net_profit_margin   REAL,
    operating_margin    REAL,
    return_on_equity    REAL,
    return_on_assets    REAL,
    -- Growth
    sales_growth        REAL,
    eps_growth          REAL,
    profit_growth       REAL,
    -- Valuation
    pe_ratio            REAL,
    pb_ratio            REAL,
    peg_ratio           REAL,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, period_end, period_type)
);
```

### `company_payouts`
Dividend and bonus payouts.

```sql
CREATE TABLE company_payouts (
    symbol              TEXT NOT NULL,
    ex_date             TEXT NOT NULL,
    payout_type         TEXT NOT NULL,  -- 'cash', 'bonus', 'right'
    announcement_date   TEXT,
    book_closure_from   TEXT,
    book_closure_to     TEXT,
    amount              REAL,
    fiscal_year         TEXT,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, ex_date, payout_type)
);
```

### `company_key_people`
Key executives and board members.

```sql
CREATE TABLE company_key_people (
    symbol     TEXT NOT NULL,
    role       TEXT NOT NULL,
    name       TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, role, name)
);
```

### `company_quote_snapshots`
Historical quote snapshots.

```sql
CREATE TABLE company_quote_snapshots (
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
```

### `company_snapshots`
Full company snapshots with JSON data.

```sql
CREATE TABLE company_snapshots (
    symbol              TEXT NOT NULL,
    snapshot_date       TEXT NOT NULL,
    snapshot_time       TEXT,
    company_name        TEXT,
    sector_code         TEXT,
    sector_name         TEXT,
    -- JSON Documents
    quote_data          TEXT,        -- JSON: price, change, volume
    equity_data         TEXT,        -- JSON: market cap, shares
    profile_data        TEXT,        -- JSON: description, address
    financials_data     TEXT,        -- JSON: annual/quarterly
    ratios_data         TEXT,        -- JSON: all ratios
    trading_data        TEXT,        -- JSON: bid/ask, circuits
    futures_data        TEXT,        -- JSON: futures contracts
    announcements_data  TEXT,        -- JSON: recent announcements
    raw_html            TEXT,        -- Compressed HTML
    source_url          TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, snapshot_date)
);
```

### `equity_structure`
Share capital and ownership structure.

```sql
CREATE TABLE equity_structure (
    symbol              TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,
    authorized_shares   INTEGER,
    issued_shares       INTEGER,
    outstanding_shares  INTEGER,
    treasury_shares     INTEGER,
    free_float_shares   INTEGER,
    free_float_percent  REAL,
    market_cap          REAL,
    market_cap_usd      REAL,
    ownership_data      TEXT,  -- JSON
    face_value          REAL,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, as_of_date)
);
```

### `trading_sessions`
Detailed trading session data.

```sql
CREATE TABLE trading_sessions (
    symbol              TEXT NOT NULL,
    session_date        TEXT NOT NULL,
    market_type         TEXT NOT NULL,  -- 'REG', 'FUT', 'CSF', 'ODL'
    contract_month      TEXT,
    -- OHLCV
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL,
    volume              INTEGER,
    -- References
    ldcp                REAL,
    prev_close          REAL,
    change_value        REAL,
    change_percent      REAL,
    -- Order Book
    bid_price           REAL,
    bid_volume          INTEGER,
    ask_price           REAL,
    ask_volume          INTEGER,
    spread              REAL,
    -- Ranges
    day_range_low       REAL,
    day_range_high      REAL,
    circuit_low         REAL,
    circuit_high        REAL,
    week_52_low         REAL,
    week_52_high        REAL,
    -- Statistics
    total_trades        INTEGER,
    turnover            REAL,
    vwap                REAL,
    -- Risk
    var_percent         REAL,
    haircut_percent     REAL,
    pe_ratio_ttm        REAL,
    ytd_change          REAL,
    year_1_change       REAL,
    last_update         TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, session_date, market_type, contract_month)
);
```

---

## 3. Corporate Events & Announcements

### `company_announcements`
Corporate announcements from PSX.

```sql
CREATE TABLE company_announcements (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    company_name        TEXT,
    announcement_date   TEXT NOT NULL,
    announcement_time   TEXT,
    title               TEXT NOT NULL,
    category            TEXT,  -- 'results', 'dividend', 'agm', etc.
    image_id            TEXT,
    pdf_id              TEXT,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(symbol, announcement_date, announcement_time, title)
);
```

### `corporate_events`
AGM, EOGM, board meetings.

```sql
CREATE TABLE corporate_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            INTEGER UNIQUE,
    symbol              TEXT NOT NULL,
    company_name        TEXT,
    event_type          TEXT NOT NULL,  -- 'AGM', 'EOGM', 'BOARD_MEETING'
    event_date          TEXT NOT NULL,
    event_time          TEXT,
    city                TEXT,
    venue               TEXT,
    period_end          TEXT,
    status              TEXT DEFAULT 'scheduled',
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(symbol, event_date, event_type)
);
```

### `dividend_payouts`
Dividend announcements and payouts.

```sql
CREATE TABLE dividend_payouts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    announcement_date   TEXT NOT NULL,
    announcement_time   TEXT,
    fiscal_period       TEXT,
    dividend_percent    REAL,
    dividend_type       TEXT,  -- 'cash', 'stock', 'interim', 'final'
    dividend_number     TEXT,
    book_closure_from   TEXT,
    book_closure_to     TEXT,
    record_date         TEXT,
    payment_date        TEXT,
    dividend_per_share  REAL,
    scraped_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(symbol, announcement_date, dividend_number)
);
```

### `financial_announcements`
Financial result announcements.

```sql
CREATE TABLE financial_announcements (
    symbol              TEXT NOT NULL,
    announcement_date   TEXT NOT NULL,
    fiscal_period       TEXT NOT NULL,
    profit_before_tax   REAL,
    profit_after_tax    REAL,
    eps                 REAL,
    dividend_payout     TEXT,
    dividend_amount     REAL,
    payout_type         TEXT,
    agm_date            TEXT,
    book_closure_from   TEXT,
    book_closure_to     TEXT,
    company_name        TEXT,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, announcement_date, fiscal_period)
);
```

---

## 4. Instruments Universe Tables

### `instruments`
Central instrument registry (equities, ETFs, REITs, indices).

```sql
CREATE TABLE instruments (
    instrument_id       TEXT PRIMARY KEY,  -- "PSX:OGDC" or "IDX:KSE100"
    symbol              TEXT NOT NULL,
    name                TEXT,
    instrument_type     TEXT NOT NULL,     -- 'EQUITY'|'ETF'|'REIT'|'INDEX'
    exchange            TEXT NOT NULL DEFAULT 'PSX',
    currency            TEXT NOT NULL DEFAULT 'PKR',
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL,     -- 'DPS'|'MANUAL'
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(exchange, symbol)
);
```

### `ohlcv_instruments`
OHLCV data for instruments (ETFs, REITs, indices).

```sql
CREATE TABLE ohlcv_instruments (
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
```

### `instrument_rankings`
Performance rankings for ETFs, REITs, indices.

```sql
CREATE TABLE instrument_rankings (
    as_of_date          TEXT NOT NULL,
    instrument_id       TEXT NOT NULL,
    instrument_type     TEXT NOT NULL,
    return_1m           REAL,
    return_3m           REAL,
    return_6m           REAL,
    return_1y           REAL,
    volatility_30d      REAL,
    relative_strength   REAL,
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(as_of_date, instrument_id),
    FOREIGN KEY(instrument_id) REFERENCES instruments(instrument_id)
);
```

### `instrument_membership`
Index/ETF constituents and weights.

```sql
CREATE TABLE instrument_membership (
    parent_instrument_id TEXT NOT NULL,
    child_instrument_id  TEXT NOT NULL,
    weight               REAL,
    effective_date       TEXT NOT NULL DEFAULT '',
    source               TEXT NOT NULL DEFAULT 'MANUAL',
    PRIMARY KEY(parent_instrument_id, child_instrument_id, effective_date),
    FOREIGN KEY(parent_instrument_id) REFERENCES instruments(instrument_id),
    FOREIGN KEY(child_instrument_id) REFERENCES instruments(instrument_id)
);
```

---

## 5. Fixed Income Tables

### `sukuk_master`
Sukuk and Islamic bond master data.

```sql
CREATE TABLE sukuk_master (
    instrument_id       TEXT PRIMARY KEY,  -- "SUKUK:GOP-IJARA-2027"
    issuer              TEXT NOT NULL,
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,     -- GOP_SUKUK | CORP_SUKUK | PIB | T-BILL
    currency            TEXT NOT NULL DEFAULT 'PKR',
    issue_date          TEXT,
    maturity_date       TEXT NOT NULL,
    coupon_rate         REAL,
    coupon_frequency    INTEGER,
    face_value          REAL DEFAULT 100.0,
    issue_size          REAL,
    shariah_compliant   INTEGER DEFAULT 1,
    is_active           INTEGER DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    notes               TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);
```

### `sukuk_quotes`
Sukuk price and yield quotes.

```sql
CREATE TABLE sukuk_quotes (
    instrument_id       TEXT NOT NULL,
    quote_date          TEXT NOT NULL,
    clean_price         REAL,
    dirty_price         REAL,
    yield_to_maturity   REAL,
    bid_yield           REAL,
    ask_yield           REAL,
    volume              REAL,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    ingested_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(instrument_id, quote_date),
    FOREIGN KEY(instrument_id) REFERENCES sukuk_master(instrument_id)
);
```

### `sukuk_analytics_snapshots`
Computed sukuk analytics (YTM, duration, convexity).

```sql
CREATE TABLE sukuk_analytics_snapshots (
    instrument_id       TEXT NOT NULL,
    as_of_date          TEXT NOT NULL,
    price               REAL,
    ytm                 REAL,
    macaulay_duration   REAL,
    modified_duration   REAL,
    convexity           REAL,
    accrued_interest    REAL,
    days_to_maturity    INTEGER,
    computed_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(instrument_id, as_of_date),
    FOREIGN KEY(instrument_id) REFERENCES sukuk_master(instrument_id)
);
```

### `sukuk_yield_curve`
Sukuk yield curve data.

```sql
CREATE TABLE sukuk_yield_curve (
    curve_name          TEXT NOT NULL,  -- PKR_GOP_SUKUK | PKR_PIB | PKR_TBILL
    curve_date          TEXT NOT NULL,
    tenor_days          INTEGER NOT NULL,
    yield_rate          REAL NOT NULL,
    source              TEXT NOT NULL DEFAULT 'SBP',
    computed_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY(curve_name, curve_date, tenor_days)
);
```

### `bonds_master`
Conventional bonds master data.

```sql
CREATE TABLE bonds_master (
    bond_id             TEXT PRIMARY KEY,  -- "PIB:3Y:2026-01-15"
    isin                TEXT UNIQUE,
    symbol              TEXT NOT NULL,
    issuer              TEXT NOT NULL,
    bond_type           TEXT NOT NULL,     -- 'PIB', 'T-Bill', 'Sukuk', 'TFC'
    is_islamic          INTEGER NOT NULL DEFAULT 0,
    face_value          REAL NOT NULL DEFAULT 100,
    coupon_rate         REAL,
    coupon_frequency    INTEGER DEFAULT 2,
    issue_date          TEXT,
    maturity_date       TEXT NOT NULL,
    day_count           TEXT DEFAULT 'ACT/ACT',
    currency            TEXT NOT NULL DEFAULT 'PKR',
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    notes               TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `fi_instruments`
Fixed income instruments (general).

```sql
CREATE TABLE fi_instruments (
    instrument_id       TEXT PRIMARY KEY,
    isin                TEXT,
    issuer              TEXT NOT NULL DEFAULT 'GOVT_OF_PAKISTAN',
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,  -- MTB | PIB | GOP_SUKUK | CORP_BOND
    currency            TEXT NOT NULL DEFAULT 'PKR',
    issue_date          TEXT,
    maturity_date       TEXT NOT NULL,
    coupon_rate         REAL,
    coupon_frequency    INTEGER,
    day_count           TEXT NOT NULL DEFAULT 'ACT/365',
    face_value          REAL NOT NULL DEFAULT 100.0,
    shariah_compliant   INTEGER NOT NULL DEFAULT 0,
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `fi_curves`
Yield curves (T-Bill, PIB, Sukuk).

```sql
CREATE TABLE fi_curves (
    curve_name          TEXT NOT NULL,  -- PKR_MTB | PKR_PIB | PKR_GOP_SUKUK
    curve_date          TEXT NOT NULL,
    tenor_days          INTEGER NOT NULL,
    rate                REAL NOT NULL,
    source              TEXT NOT NULL DEFAULT 'MANUAL',
    PRIMARY KEY(curve_name, curve_date, tenor_days)
);
```

### `yield_curve_points`
Interpolated yield curve points.

```sql
CREATE TABLE yield_curve_points (
    curve_date          TEXT NOT NULL,
    tenor_months        INTEGER NOT NULL,
    yield_rate          REAL NOT NULL,
    bond_type           TEXT NOT NULL DEFAULT 'PIB',
    interpolation       TEXT DEFAULT 'LINEAR',
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(curve_date, tenor_months, bond_type)
);
```

### `kibor_rates`
KIBOR (Karachi Interbank Offered Rate).

```sql
CREATE TABLE kibor_rates (
    rate_date           TEXT NOT NULL,
    tenor_months        INTEGER NOT NULL,  -- 3, 6, or 12
    bid                 REAL NOT NULL,
    offer               REAL NOT NULL,
    source              TEXT NOT NULL DEFAULT 'SBP_MSM',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(rate_date, tenor_months)
);
```

### `sbp_policy_rates`
SBP policy rates.

```sql
CREATE TABLE sbp_policy_rates (
    rate_date           TEXT PRIMARY KEY,
    policy_rate         REAL,
    ceiling_rate        REAL,
    floor_rate          REAL,
    overnight_repo_rate REAL,
    source              TEXT NOT NULL DEFAULT 'SBP_MSM',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now'))
);
```

---

## 6. FX Data Tables

### `fx_pairs`
Currency pair registry.

```sql
CREATE TABLE fx_pairs (
    pair                TEXT PRIMARY KEY,  -- "USD/PKR"
    base_currency       TEXT NOT NULL,
    quote_currency      TEXT NOT NULL,
    source              TEXT NOT NULL,     -- "SBP" | "OPEN_API" | "MANUAL"
    description         TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `fx_ohlcv`
FX OHLCV data.

```sql
CREATE TABLE fx_ohlcv (
    pair                TEXT NOT NULL,
    date                TEXT NOT NULL,
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL NOT NULL,
    volume              REAL,
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(pair, date),
    FOREIGN KEY(pair) REFERENCES fx_pairs(pair)
);
```

### `fx_adjusted_metrics`
FX-adjusted equity returns.

```sql
CREATE TABLE fx_adjusted_metrics (
    as_of_date          TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    fx_pair             TEXT NOT NULL,
    equity_return       REAL,
    fx_return           REAL,
    fx_adjusted_return  REAL,
    period              TEXT NOT NULL DEFAULT '1M',
    computed_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(as_of_date, symbol, fx_pair, period),
    FOREIGN KEY(fx_pair) REFERENCES fx_pairs(pair)
);
```

---

## 7. Mutual Funds Tables

### `mutual_funds`
Mutual fund master data.

```sql
CREATE TABLE mutual_funds (
    fund_id             TEXT PRIMARY KEY,  -- "MUFAP:ABL-ISF"
    symbol              TEXT NOT NULL UNIQUE,
    fund_name           TEXT NOT NULL,
    amc_code            TEXT NOT NULL,
    amc_name            TEXT,
    fund_type           TEXT NOT NULL,     -- 'OPEN_END' | 'VPS' | 'ETF'
    category            TEXT NOT NULL,     -- 'Equity', 'Money Market', etc.
    is_shariah          INTEGER NOT NULL DEFAULT 0,
    launch_date         TEXT,
    expense_ratio       REAL,
    management_fee      REAL,
    is_active           INTEGER NOT NULL DEFAULT 1,
    source              TEXT NOT NULL DEFAULT 'MUFAP',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `mutual_fund_nav`
Daily NAV history.

```sql
CREATE TABLE mutual_fund_nav (
    fund_id             TEXT NOT NULL,
    date                TEXT NOT NULL,
    nav                 REAL NOT NULL,
    offer_price         REAL,
    redemption_price    REAL,
    aum                 REAL,
    nav_change_pct      REAL,
    source              TEXT NOT NULL DEFAULT 'MUFAP',
    ingested_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(fund_id, date),
    FOREIGN KEY(fund_id) REFERENCES mutual_funds(fund_id)
);
```

---

## 8. Analytics Tables

### `analytics_market_snapshot`
Market-wide analytics snapshots.

```sql
CREATE TABLE analytics_market_snapshot (
    ts                  TEXT PRIMARY KEY,
    gainers_count       INTEGER NOT NULL DEFAULT 0,
    losers_count        INTEGER NOT NULL DEFAULT 0,
    unchanged_count     INTEGER NOT NULL DEFAULT 0,
    total_symbols       INTEGER NOT NULL DEFAULT 0,
    total_volume        REAL DEFAULT 0,
    top_gainer_symbol   TEXT,
    top_loser_symbol    TEXT,
    computed_at         TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### `analytics_symbol_snapshot`
Top movers snapshots.

```sql
CREATE TABLE analytics_symbol_snapshot (
    ts          TEXT NOT NULL,
    rank_type   TEXT NOT NULL,  -- 'gainers', 'losers', 'volume'
    rank        INTEGER NOT NULL,
    symbol      TEXT NOT NULL,
    company_name TEXT,
    sector_name TEXT,
    current     REAL,
    change_pct  REAL,
    volume      REAL,
    PRIMARY KEY (ts, rank_type, rank)
);
```

### `analytics_sector_snapshot`
Sector-level analytics.

```sql
CREATE TABLE analytics_sector_snapshot (
    ts              TEXT NOT NULL,
    sector_code     TEXT NOT NULL,
    sector_name     TEXT NOT NULL,
    symbols_count   INTEGER NOT NULL DEFAULT 0,
    avg_change_pct  REAL,
    sum_volume      REAL,
    top_symbol      TEXT,
    PRIMARY KEY (ts, sector_code)
);
```

---

## 9. Sync & Operations Tables

### `sync_runs`
EOD sync run tracking.

```sql
CREATE TABLE sync_runs (
    run_id         TEXT PRIMARY KEY,
    started_at     TEXT NOT NULL,
    ended_at       TEXT NULL,
    mode           TEXT NOT NULL,
    symbols_total  INTEGER DEFAULT 0,
    symbols_ok     INTEGER DEFAULT 0,
    symbols_failed INTEGER DEFAULT 0,
    rows_upserted  INTEGER DEFAULT 0
);
```

### `sync_failures`
Individual symbol sync failures.

```sql
CREATE TABLE sync_failures (
    run_id        TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    error_type    TEXT NOT NULL,
    error_message TEXT,
    created_at    TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES sync_runs(run_id)
);
```

### `scrape_jobs`
Background scraping jobs.

```sql
CREATE TABLE scrape_jobs (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    ended_at            TEXT,
    status              TEXT NOT NULL DEFAULT 'running',
    symbols_requested   INTEGER DEFAULT 0,
    symbols_completed   INTEGER DEFAULT 0,
    symbols_failed      INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,
    errors              TEXT,
    config              TEXT,
    stop_requested      INTEGER DEFAULT 0,
    current_symbol      TEXT,
    current_batch       INTEGER DEFAULT 0,
    total_batches       INTEGER DEFAULT 0,
    batch_size          INTEGER DEFAULT 50,
    batch_pause_sec     INTEGER DEFAULT 30,
    pid                 INTEGER,
    last_heartbeat      TEXT,
    notification_sent   INTEGER DEFAULT 0
);
```

---

## 10. System Tables

### `user_interactions`
User interaction tracking.

```sql
CREATE TABLE user_interactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT NOT NULL,
    timestamp           TEXT NOT NULL,
    action_type         TEXT NOT NULL,
    page_name           TEXT,
    symbol              TEXT,
    action_detail       TEXT,
    metadata            TEXT,
    ip_address          TEXT,
    user_agent          TEXT
);
```

### `llm_cache`
LLM response cache for AI insights.

```sql
CREATE TABLE llm_cache (
    prompt_hash       TEXT PRIMARY KEY,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at        TEXT NOT NULL,
    response_text     TEXT NOT NULL,
    meta_json         TEXT,
    symbol            TEXT,
    mode              TEXT,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    model             TEXT,
    CONSTRAINT valid_expiry CHECK (expires_at > created_at)
);
```

### `job_notifications`
Job completion notifications.

```sql
CREATE TABLE job_notifications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              TEXT NOT NULL,
    notification_type   TEXT NOT NULL,
    title               TEXT NOT NULL,
    message             TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    read_at             TEXT,
    FOREIGN KEY (job_id) REFERENCES scrape_jobs(job_id)
);
```

---

## Index Summary

The database has 110+ indexes for optimized queries:

- **Primary indexes:** Auto-created for all primary keys
- **Date indexes:** All time-series tables indexed by date
- **Symbol indexes:** Quick symbol lookups across all tables
- **Type/category indexes:** Instrument types, event types, announcement categories
- **Status indexes:** Sync status, job status for operational queries

---

## Entity Relationship Diagram (Key Relationships)

```
symbols ─────────────────────── eod_ohlcv
    │                               │
    └─── company_profile            │
    │        │                      │
    │        └─── company_fundamentals
    │        │
    │        └─── company_financials
    │        │
    │        └─── company_ratios
    │
    └─── company_announcements
    │
    └─── dividend_payouts

instruments ─────────────────── ohlcv_instruments
    │                               │
    └─── instrument_rankings        │
    │                               │
    └─── instrument_membership ─────┘

sukuk_master ───────────────── sukuk_quotes
    │                               │
    └─── sukuk_analytics_snapshots  │
    │                               │
    └─── sukuk_yield_curve ─────────┘

fx_pairs ──────────────────── fx_ohlcv
    │                             │
    └─── fx_adjusted_metrics ─────┘

mutual_funds ─────────────── mutual_fund_nav
```

---

## Data Volume Estimates

| Table | Estimated Rows | Growth Rate |
|-------|---------------|-------------|
| eod_ohlcv | 1M+ | ~500/day |
| intraday_bars | 5M+ | ~50K/day (trading days) |
| company_fundamentals | ~600 | Weekly updates |
| instruments | ~700 | Infrequent |
| sukuk_master | ~100 | Monthly |
| mutual_funds | ~300 | Weekly |
| fx_ohlcv | ~20K | Daily |

---

## Notes for Agentic AI Implementation

1. **Query Patterns:** Most queries are time-series with symbol filters
2. **Aggregations:** Market breadth, sector rollups computed on-demand
3. **JSON Storage:** Flexible schema for company snapshots, ownership data
4. **Caching:** LLM responses cached with TTL for AI features
5. **Audit Trail:** All sync operations logged for debugging
