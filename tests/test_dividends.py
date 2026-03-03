"""Tests for dividend analytics repository."""

import sqlite3

import pandas as pd
import pytest


@pytest.fixture
def con():
    """In-memory SQLite with company_payouts + eod_ohlcv tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE company_payouts (
            symbol TEXT NOT NULL,
            ex_date TEXT NOT NULL,
            payout_type TEXT NOT NULL,
            announcement_date TEXT,
            book_closure_from TEXT,
            book_closure_to TEXT,
            amount REAL,
            fiscal_year TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (symbol, ex_date, payout_type)
        );
        CREATE TABLE eod_ohlcv (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, date)
        );
    """)
    conn.commit()
    yield conn
    conn.close()


def _seed_dividends(con, symbol, payouts):
    """Insert dividend data."""
    for p in payouts:
        con.execute(
            """INSERT INTO company_payouts (symbol, ex_date, payout_type, amount, fiscal_year)
               VALUES (?, ?, 'cash', ?, ?)""",
            (symbol, p["date"], p["amount"], p.get("fy", "")),
        )
    con.commit()


def _seed_price(con, symbol, date, close):
    """Insert price data."""
    con.execute(
        "INSERT INTO eod_ohlcv (symbol, date, close) VALUES (?, ?, ?)",
        (symbol, date, close),
    )
    con.commit()


# ── get_dividend_history ────────────────────────────────────────────

class TestGetDividendHistory:
    def test_empty(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_history
        df = get_dividend_history(con, "OGDC")
        assert df.empty

    def test_returns_history(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_history
        _seed_dividends(con, "OGDC", [
            {"date": "2025-06-01", "amount": 5.0, "fy": "2025"},
            {"date": "2025-12-01", "amount": 6.0, "fy": "2025"},
        ])
        df = get_dividend_history(con, "OGDC")
        assert len(df) == 2
        assert df.iloc[0]["ex_date"] == "2025-12-01"  # newest first

    def test_filter_by_years(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_history
        _seed_dividends(con, "OGDC", [
            {"date": "2020-06-01", "amount": 3.0},
            {"date": "2025-06-01", "amount": 5.0},
            {"date": "2025-12-01", "amount": 6.0},
        ])
        df = get_dividend_history(con, "OGDC", years=2)
        assert len(df) == 2  # only 2025 entries


# ── get_dividend_yield ──────────────────────────────────────────────

class TestGetDividendYield:
    def test_no_data(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_yield
        result = get_dividend_yield(con, "OGDC")
        assert result is None

    def test_no_price(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_yield
        _seed_dividends(con, "OGDC", [
            {"date": "2025-12-01", "amount": 10.0},
        ])
        result = get_dividend_yield(con, "OGDC")
        assert result is None

    def test_computes_yield(self, con):
        from pakfindata.db.repositories.dividends import get_dividend_yield
        _seed_dividends(con, "OGDC", [
            {"date": "2025-12-01", "amount": 5.0},
            {"date": "2026-01-15", "amount": 5.0},
        ])
        _seed_price(con, "OGDC", "2026-02-08", 100.0)
        yld = get_dividend_yield(con, "OGDC", years=1)
        assert yld == pytest.approx(10.0, abs=0.1)


# ── get_ex_dividend_dates ───────────────────────────────────────────

class TestGetExDividendDates:
    def test_empty(self, con):
        from pakfindata.db.repositories.dividends import get_ex_dividend_dates
        dates = get_ex_dividend_dates(con, "OGDC")
        assert dates == []

    def test_returns_dates(self, con):
        from pakfindata.db.repositories.dividends import get_ex_dividend_dates
        _seed_dividends(con, "OGDC", [
            {"date": "2025-06-01", "amount": 5.0},
            {"date": "2025-12-01", "amount": 6.0},
        ])
        dates = get_ex_dividend_dates(con, "OGDC")
        assert len(dates) == 2
        assert dates[0] == "2025-12-01"  # newest first


# ── get_highest_dividend_stocks ─────────────────────────────────────

class TestGetHighestDividendStocks:
    def test_empty(self, con):
        from pakfindata.db.repositories.dividends import get_highest_dividend_stocks
        df = get_highest_dividend_stocks(con)
        assert df.empty

    def test_ranks_by_yield(self, con):
        from pakfindata.db.repositories.dividends import get_highest_dividend_stocks
        # Stock A: DPS=10, Price=100 → yield 10%
        _seed_dividends(con, "STOCKA", [{"date": "2026-01-01", "amount": 10.0}])
        _seed_price(con, "STOCKA", "2026-02-08", 100.0)
        # Stock B: DPS=20, Price=100 → yield 20%
        _seed_dividends(con, "STOCKB", [{"date": "2026-01-01", "amount": 20.0}])
        _seed_price(con, "STOCKB", "2026-02-08", 100.0)

        df = get_highest_dividend_stocks(con, n=10)
        assert len(df) == 2
        assert df.iloc[0]["symbol"] == "STOCKB"  # higher yield first
        assert df.iloc[0]["yield_pct"] == pytest.approx(20.0, abs=0.1)


# ── get_upcoming_dividends ──────────────────────────────────────────

class TestGetUpcomingDividends:
    def test_empty(self, con):
        from pakfindata.db.repositories.dividends import get_upcoming_dividends
        df = get_upcoming_dividends(con)
        assert df.empty

    def test_filters_future(self, con):
        from datetime import datetime, timedelta
        from pakfindata.db.repositories.dividends import get_upcoming_dividends
        future = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d")
        past = "2020-01-01"
        _seed_dividends(con, "OGDC", [
            {"date": past, "amount": 5.0},
            {"date": future, "amount": 6.0},
        ])
        df = get_upcoming_dividends(con)
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "OGDC"
