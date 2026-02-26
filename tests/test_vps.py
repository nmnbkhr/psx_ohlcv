"""Tests for VPS pension fund query functions."""

import sqlite3

import pandas as pd
import pytest


@pytest.fixture
def con():
    """In-memory SQLite with mutual_funds + mutual_fund_nav tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE mutual_funds (
            fund_id TEXT PRIMARY KEY,
            symbol TEXT,
            fund_name TEXT,
            amc_code TEXT,
            amc_name TEXT,
            fund_type TEXT,
            category TEXT,
            is_shariah INTEGER DEFAULT 0,
            launch_date TEXT,
            expense_ratio REAL,
            management_fee REAL,
            is_active INTEGER DEFAULT 1,
            source TEXT DEFAULT 'MUFAP',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE mutual_fund_nav (
            fund_id TEXT NOT NULL,
            date TEXT NOT NULL,
            nav REAL NOT NULL,
            offer_price REAL,
            redemption_price REAL,
            aum REAL,
            nav_change_pct REAL,
            source TEXT DEFAULT 'MUFAP',
            ingested_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (fund_id, date)
        );
    """)
    conn.commit()
    yield conn
    conn.close()


def _seed_vps(con):
    """Insert sample VPS fund data."""
    con.executescript("""
        INSERT INTO mutual_funds (fund_id, symbol, fund_name, fund_type, category, amc_name)
        VALUES
            ('MUFAP:ABL-VPS-EQ', 'ABL-VPS-EQ', 'ABL Pension Fund - Equity', 'VPS', 'VPS', 'ABL AMC'),
            ('MUFAP:MCB-VPS-ISL', 'MCB-VPS-ISL', 'MCB Islamic Pension Fund', 'VPS', 'VPS', 'MCB AMC'),
            ('MUFAP:HBL-MF', 'HBL-MF', 'HBL Money Market Fund', 'OPEN_END', 'Money Market', 'HBL AMC');

        INSERT INTO mutual_fund_nav (fund_id, date, nav, nav_change_pct)
        VALUES
            ('MUFAP:ABL-VPS-EQ', '2025-12-01', 100.0, NULL),
            ('MUFAP:ABL-VPS-EQ', '2026-01-15', 105.0, 0.5),
            ('MUFAP:ABL-VPS-EQ', '2026-02-08', 110.0, 0.3),
            ('MUFAP:MCB-VPS-ISL', '2025-12-01', 50.0, NULL),
            ('MUFAP:MCB-VPS-ISL', '2026-02-08', 52.0, 0.2);
    """)
    con.commit()


# ── get_vps_funds ───────────────────────────────────────────────────

class TestGetVpsFunds:
    def test_empty(self, con):
        from pakfindata.db.repositories.vps import get_vps_funds
        df = get_vps_funds(con)
        assert df.empty

    def test_returns_only_vps(self, con):
        from pakfindata.db.repositories.vps import get_vps_funds
        _seed_vps(con)
        df = get_vps_funds(con)
        assert len(df) == 2  # Only VPS, not HBL-MF (OPEN_END)
        assert "MUFAP:HBL-MF" not in df["fund_id"].values


# ── get_vps_nav_history ─────────────────────────────────────────────

class TestGetVpsNavHistory:
    def test_empty(self, con):
        from pakfindata.db.repositories.vps import get_vps_nav_history
        df = get_vps_nav_history(con, "MUFAP:ABL-VPS-EQ")
        assert df.empty

    def test_returns_history(self, con):
        from pakfindata.db.repositories.vps import get_vps_nav_history
        _seed_vps(con)
        df = get_vps_nav_history(con, "MUFAP:ABL-VPS-EQ")
        assert len(df) == 3
        assert df.iloc[0]["date"] == "2026-02-08"  # newest first

    def test_date_filter(self, con):
        from pakfindata.db.repositories.vps import get_vps_nav_history
        _seed_vps(con)
        df = get_vps_nav_history(con, "MUFAP:ABL-VPS-EQ", start_date="2026-01-01")
        assert len(df) == 2  # Only Jan + Feb


# ── compare_vps_performance ─────────────────────────────────────────

class TestCompareVpsPerformance:
    def test_empty(self, con):
        from pakfindata.db.repositories.vps import compare_vps_performance
        df = compare_vps_performance(con)
        assert df.empty

    def test_computes_returns(self, con):
        from pakfindata.db.repositories.vps import compare_vps_performance
        _seed_vps(con)
        df = compare_vps_performance(con, days=365)
        assert len(df) == 2
        # ABL: 100 → 110 = 10% return
        abl = df[df["fund_id"] == "MUFAP:ABL-VPS-EQ"].iloc[0]
        assert abl["return_pct"] == pytest.approx(10.0, abs=0.1)


# ── get_vps_summary ────────────────────────────────────────────────

class TestGetVpsSummary:
    def test_empty(self, con):
        from pakfindata.db.repositories.vps import get_vps_summary
        s = get_vps_summary(con)
        assert s["total_funds"] == 0

    def test_with_data(self, con):
        from pakfindata.db.repositories.vps import get_vps_summary
        _seed_vps(con)
        s = get_vps_summary(con)
        assert s["total_funds"] == 2
        assert s["total_nav_records"] == 5
