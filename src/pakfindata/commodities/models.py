"""SQLite schema for commodity data tables.

All tables use CREATE IF NOT EXISTS so they can be called on every startup.
Data is stored in the same database as PSX equities (/mnt/e/psxdata/psx.sqlite).
"""

import sqlite3


COMMODITY_SCHEMA_SQL = """
-- Master commodity list with ticker mappings per data source
CREATE TABLE IF NOT EXISTS commodity_symbols (
    symbol          TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    unit            TEXT NOT NULL,
    pk_relevance    TEXT NOT NULL DEFAULT 'MEDIUM',
    yf_ticker       TEXT,
    yf_etf          TEXT,
    fred_series     TEXT,
    wb_column       TEXT,
    pk_unit         TEXT,
    pk_conversion   TEXT,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_commodity_symbols_category
    ON commodity_symbols(category);
CREATE INDEX IF NOT EXISTS idx_commodity_symbols_pk_relevance
    ON commodity_symbols(pk_relevance);

-- Daily OHLCV from yfinance / scrapers (primary daily data)
CREATE TABLE IF NOT EXISTS commodity_eod (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      INTEGER,
    adj_close   REAL,
    source      TEXT NOT NULL DEFAULT 'yfinance',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date, source)
);

CREATE INDEX IF NOT EXISTS idx_commodity_eod_date ON commodity_eod(date);
CREATE INDEX IF NOT EXISTS idx_commodity_eod_symbol ON commodity_eod(symbol);
CREATE INDEX IF NOT EXISTS idx_commodity_eod_source ON commodity_eod(source);

-- Monthly reference prices from FRED / World Bank (gap-fill & backfill)
CREATE TABLE IF NOT EXISTS commodity_monthly (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    price       REAL NOT NULL,
    source      TEXT NOT NULL DEFAULT 'fred',
    series_id   TEXT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date, source)
);

CREATE INDEX IF NOT EXISTS idx_commodity_monthly_symbol ON commodity_monthly(symbol);

-- PKR-denominated prices in local units (tola, maund, bori, litre)
CREATE TABLE IF NOT EXISTS commodity_pkr (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    pkr_price   REAL NOT NULL,
    pk_unit     TEXT NOT NULL,
    usd_price   REAL,
    usd_pkr     REAL,
    source      TEXT NOT NULL DEFAULT 'computed',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_commodity_pkr_symbol ON commodity_pkr(symbol);

-- Daily FX rates synced alongside commodities
CREATE TABLE IF NOT EXISTS commodity_fx_rates (
    pair        TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      INTEGER,
    source      TEXT NOT NULL DEFAULT 'yfinance',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (pair, date)
);

CREATE INDEX IF NOT EXISTS idx_commodity_fx_rates_date ON commodity_fx_rates(date);

-- khistocks.com Pakistan local market data (PMEX, Sarafa, Mandi, LME)
CREATE TABLE IF NOT EXISTS khistocks_prices (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    feed        TEXT NOT NULL,
    name        TEXT,
    quotation   TEXT,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    rate        REAL,
    cash_buyer  REAL,
    cash_seller REAL,
    three_month_buyer REAL,
    three_month_seller REAL,
    net_change  REAL,
    change_pct  TEXT,
    source      TEXT NOT NULL DEFAULT 'khistocks',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date, feed)
);

CREATE INDEX IF NOT EXISTS idx_khistocks_prices_feed ON khistocks_prices(feed);
CREATE INDEX IF NOT EXISTS idx_khistocks_prices_date ON khistocks_prices(date);
CREATE INDEX IF NOT EXISTS idx_khistocks_prices_symbol ON khistocks_prices(symbol);

-- PMEX Market Watch (direct portal API: 134 instruments, 9 categories)
CREATE TABLE IF NOT EXISTS pmex_market_watch (
    contract        TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    category        TEXT NOT NULL,
    bid             REAL,
    ask             REAL,
    open            REAL,
    close           REAL,
    high            REAL,
    low             REAL,
    last_price      REAL,
    last_vol        INTEGER,
    total_vol       INTEGER,
    total_volume    INTEGER,
    change          REAL,
    change_pct      REAL,
    bid_diff        REAL,
    ask_diff        REAL,
    state           TEXT,
    snapshot_ts     TEXT,
    source          TEXT NOT NULL DEFAULT 'pmex_portal',
    ingested_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (contract, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_pmex_mw_category ON pmex_market_watch(category);
CREATE INDEX IF NOT EXISTS idx_pmex_mw_date ON pmex_market_watch(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_pmex_mw_contract ON pmex_market_watch(contract);

-- Sync run tracking for commodity operations
CREATE TABLE IF NOT EXISTS commodity_sync_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    mode            TEXT NOT NULL DEFAULT 'commodity',
    source          TEXT,
    symbols_total   INTEGER DEFAULT 0,
    symbols_ok      INTEGER DEFAULT 0,
    symbols_failed  INTEGER DEFAULT 0,
    rows_upserted   INTEGER DEFAULT 0,
    error_summary   TEXT
);
"""


def init_commodity_schema(con: sqlite3.Connection) -> None:
    """Create commodity tables if they don't exist."""
    con.executescript(COMMODITY_SCHEMA_SQL)
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Repository functions — upsert helpers
# ─────────────────────────────────────────────────────────────────────────────

def upsert_commodity_symbol(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a commodity symbol definition."""
    con.execute(
        """
        INSERT INTO commodity_symbols (symbol, name, category, unit, pk_relevance,
            yf_ticker, yf_etf, fred_series, wb_column, pk_unit, pk_conversion, updated_at)
        VALUES (:symbol, :name, :category, :unit, :pk_relevance,
            :yf_ticker, :yf_etf, :fred_series, :wb_column, :pk_unit, :pk_conversion, datetime('now'))
        ON CONFLICT(symbol) DO UPDATE SET
            name=excluded.name, category=excluded.category, unit=excluded.unit,
            pk_relevance=excluded.pk_relevance, yf_ticker=excluded.yf_ticker,
            yf_etf=excluded.yf_etf, fred_series=excluded.fred_series,
            wb_column=excluded.wb_column, pk_unit=excluded.pk_unit,
            pk_conversion=excluded.pk_conversion, updated_at=datetime('now')
        """,
        data,
    )
    return True


def upsert_commodity_eod(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert daily OHLCV commodity data. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO commodity_eod (symbol, date, open, high, low, close, volume, adj_close, source)
        VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :adj_close, :source)
        ON CONFLICT(symbol, date, source) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume, adj_close=excluded.adj_close,
            ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def upsert_commodity_monthly(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert monthly commodity data. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO commodity_monthly (symbol, date, price, source, series_id)
        VALUES (:symbol, :date, :price, :source, :series_id)
        ON CONFLICT(symbol, date, source) DO UPDATE SET
            price=excluded.price, series_id=excluded.series_id,
            ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def upsert_commodity_pkr(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert PKR-denominated commodity prices. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO commodity_pkr (symbol, date, pkr_price, pk_unit, usd_price, usd_pkr, source)
        VALUES (:symbol, :date, :pkr_price, :pk_unit, :usd_price, :usd_pkr, :source)
        ON CONFLICT(symbol, date) DO UPDATE SET
            pkr_price=excluded.pkr_price, pk_unit=excluded.pk_unit,
            usd_price=excluded.usd_price, usd_pkr=excluded.usd_pkr,
            source=excluded.source, ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def upsert_commodity_fx(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert FX rate data. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO commodity_fx_rates (pair, date, open, high, low, close, volume, source)
        VALUES (:pair, :date, :open, :high, :low, :close, :volume, :source)
        ON CONFLICT(pair, date) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume,
            source=excluded.source, ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def upsert_khistocks_prices(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert khistocks.com local market price data. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO khistocks_prices (symbol, date, feed, name, quotation,
            open, high, low, close, rate,
            cash_buyer, cash_seller, three_month_buyer, three_month_seller,
            net_change, change_pct, source)
        VALUES (:symbol, :date, :feed, :name, :quotation,
            :open, :high, :low, :close, :rate,
            :cash_buyer, :cash_seller, :three_month_buyer, :three_month_seller,
            :net_change, :change_pct, :source)
        ON CONFLICT(symbol, date, feed) DO UPDATE SET
            name=excluded.name, quotation=excluded.quotation,
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, rate=excluded.rate,
            cash_buyer=excluded.cash_buyer, cash_seller=excluded.cash_seller,
            three_month_buyer=excluded.three_month_buyer,
            three_month_seller=excluded.three_month_seller,
            net_change=excluded.net_change, change_pct=excluded.change_pct,
            source=excluded.source, ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def get_khistocks_latest(con: sqlite3.Connection, feed: str | None = None) -> list[dict]:
    """Get latest khistocks prices, optionally filtered by feed."""
    if feed:
        rows = con.execute(
            """
            SELECT kp.* FROM khistocks_prices kp
            INNER JOIN (
                SELECT symbol, feed, MAX(date) as max_date FROM khistocks_prices
                WHERE feed=? GROUP BY symbol, feed
            ) latest ON kp.symbol=latest.symbol AND kp.feed=latest.feed AND kp.date=latest.max_date
            WHERE kp.feed=?
            ORDER BY kp.symbol
            """,
            (feed, feed),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT kp.* FROM khistocks_prices kp
            INNER JOIN (
                SELECT symbol, feed, MAX(date) as max_date FROM khistocks_prices
                GROUP BY symbol, feed
            ) latest ON kp.symbol=latest.symbol AND kp.feed=latest.feed AND kp.date=latest.max_date
            ORDER BY kp.feed, kp.symbol
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def get_khistocks_history(
    con: sqlite3.Connection, symbol: str, feed: str | None = None, limit: int = 90,
) -> list[dict]:
    """Get price history for a khistocks symbol."""
    sql = "SELECT * FROM khistocks_prices WHERE symbol=?"
    params: list = [symbol]
    if feed:
        sql += " AND feed=?"
        params.append(feed)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in con.execute(sql, params).fetchall()]


def record_commodity_sync_start(con: sqlite3.Connection, run_id: str, mode: str, source: str | None = None) -> None:
    """Record the start of a commodity sync run."""
    con.execute(
        """
        INSERT INTO commodity_sync_runs (run_id, started_at, mode, source)
        VALUES (?, datetime('now'), ?, ?)
        """,
        (run_id, mode, source),
    )
    con.commit()


def record_commodity_sync_end(
    con: sqlite3.Connection, run_id: str,
    symbols_total: int = 0, symbols_ok: int = 0, symbols_failed: int = 0,
    rows_upserted: int = 0, error_summary: str | None = None,
) -> None:
    """Record the end of a commodity sync run."""
    con.execute(
        """
        UPDATE commodity_sync_runs SET
            ended_at=datetime('now'), symbols_total=?, symbols_ok=?,
            symbols_failed=?, rows_upserted=?, error_summary=?
        WHERE run_id=?
        """,
        (symbols_total, symbols_ok, symbols_failed, rows_upserted, error_summary, run_id),
    )
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_commodity_latest(con: sqlite3.Connection, symbol: str, source: str = "yfinance") -> dict | None:
    """Get the latest EOD record for a commodity."""
    row = con.execute(
        """
        SELECT * FROM commodity_eod
        WHERE symbol=? AND source=?
        ORDER BY date DESC LIMIT 1
        """,
        (symbol, source),
    ).fetchone()
    return dict(row) if row else None


def get_commodity_max_date(con: sqlite3.Connection, symbol: str, source: str = "yfinance") -> str | None:
    """Get the max date for a commodity in a given source."""
    row = con.execute(
        "SELECT MAX(date) as max_date FROM commodity_eod WHERE symbol=? AND source=?",
        (symbol, source),
    ).fetchone()
    return row["max_date"] if row and row["max_date"] else None


def get_commodity_eod_range(
    con: sqlite3.Connection, symbol: str,
    start: str | None = None, end: str | None = None,
    source: str = "yfinance", limit: int = 365,
) -> list[dict]:
    """Get EOD data for a commodity within a date range."""
    sql = "SELECT * FROM commodity_eod WHERE symbol=? AND source=?"
    params: list = [symbol, source]
    if start:
        sql += " AND date >= ?"
        params.append(start)
    if end:
        sql += " AND date <= ?"
        params.append(end)
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in con.execute(sql, params).fetchall()]


def get_all_commodity_symbols(con: sqlite3.Connection, category: str | None = None) -> list[dict]:
    """Get all commodity symbols, optionally filtered by category."""
    if category:
        rows = con.execute(
            "SELECT * FROM commodity_symbols WHERE category=? AND is_active=1 ORDER BY symbol",
            (category,),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM commodity_symbols WHERE is_active=1 ORDER BY symbol",
        ).fetchall()
    return [dict(r) for r in rows]


def get_commodity_pkr_latest(con: sqlite3.Connection, symbols: list[str] | None = None) -> list[dict]:
    """Get latest PKR prices for commodities."""
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        rows = con.execute(
            f"""
            SELECT cp.* FROM commodity_pkr cp
            INNER JOIN (
                SELECT symbol, MAX(date) as max_date FROM commodity_pkr
                WHERE symbol IN ({placeholders})
                GROUP BY symbol
            ) latest ON cp.symbol=latest.symbol AND cp.date=latest.max_date
            """,
            symbols,
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT cp.* FROM commodity_pkr cp
            INNER JOIN (
                SELECT symbol, MAX(date) as max_date FROM commodity_pkr
                GROUP BY symbol
            ) latest ON cp.symbol=latest.symbol AND cp.date=latest.max_date
            """,
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# PMEX Market Watch helpers
# ─────────────────────────────────────────────────────────────────────────────

def upsert_pmex_market_watch(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert PMEX market watch data. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO pmex_market_watch (contract, snapshot_date, category,
            bid, ask, open, close, high, low, last_price,
            last_vol, total_vol, total_volume,
            change, change_pct, bid_diff, ask_diff,
            state, snapshot_ts, source)
        VALUES (:contract, :snapshot_date, :category,
            :bid, :ask, :open, :close, :high, :low, :last_price,
            :last_vol, :total_vol, :total_volume,
            :change, :change_pct, :bid_diff, :ask_diff,
            :state, :snapshot_ts, :source)
        ON CONFLICT(contract, snapshot_date) DO UPDATE SET
            category=excluded.category,
            bid=excluded.bid, ask=excluded.ask,
            open=excluded.open, close=excluded.close,
            high=excluded.high, low=excluded.low,
            last_price=excluded.last_price,
            last_vol=excluded.last_vol, total_vol=excluded.total_vol,
            total_volume=excluded.total_volume,
            change=excluded.change, change_pct=excluded.change_pct,
            bid_diff=excluded.bid_diff, ask_diff=excluded.ask_diff,
            state=excluded.state, snapshot_ts=excluded.snapshot_ts,
            source=excluded.source, ingested_at=datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def get_pmex_latest(con: sqlite3.Connection, category: str | None = None) -> list[dict]:
    """Get latest PMEX market watch data, optionally filtered by category."""
    if category:
        rows = con.execute(
            """
            SELECT p.* FROM pmex_market_watch p
            INNER JOIN (
                SELECT contract, MAX(snapshot_date) as max_date FROM pmex_market_watch
                WHERE category=? GROUP BY contract
            ) latest ON p.contract=latest.contract AND p.snapshot_date=latest.max_date
            WHERE p.category=?
            ORDER BY p.contract
            """,
            (category, category),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT p.* FROM pmex_market_watch p
            INNER JOIN (
                SELECT contract, MAX(snapshot_date) as max_date FROM pmex_market_watch
                GROUP BY contract
            ) latest ON p.contract=latest.contract AND p.snapshot_date=latest.max_date
            ORDER BY p.category, p.contract
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def get_pmex_history(
    con: sqlite3.Connection, contract: str, limit: int = 90,
) -> list[dict]:
    """Get price history for a PMEX contract."""
    return [dict(r) for r in con.execute(
        "SELECT * FROM pmex_market_watch WHERE contract=? ORDER BY snapshot_date DESC LIMIT ?",
        (contract, limit),
    ).fetchall()]
