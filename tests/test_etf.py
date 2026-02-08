"""Tests for ETF repository and scraper."""

import sqlite3

import pandas as pd
import pytest

from psx_ohlcv.db.repositories.etf import (
    get_all_etf_latest_nav,
    get_etf_detail,
    get_etf_list,
    get_etf_nav_history,
    init_etf_schema,
    upsert_etf_master,
    upsert_etf_nav,
)


@pytest.fixture
def con():
    """In-memory SQLite connection with ETF schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_etf_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def seeded_con(con):
    """Connection with sample ETF data."""
    upsert_etf_master(con, {
        "symbol": "MZNPETF",
        "name": "Meezan Pakistan ETF",
        "amc": "Al Meezan Investment Management",
        "benchmark_index": "Meezan Pakistan Index",
        "inception_date": "2020-10-06",
        "expense_ratio": 0.5,
        "management_fee": "Up to 0.50% p.a.",
        "shariah_compliant": True,
        "trustee": "CDC",
        "fiscal_year_end": "June",
    })
    upsert_etf_master(con, {
        "symbol": "NBPGETF",
        "name": "NBP Gold ETF",
        "amc": "NBP Fund Management",
        "shariah_compliant": False,
    })
    # NAV records
    upsert_etf_nav(con, "MZNPETF", "2026-02-06", 21.50, 21.80, 1089.0, 49610000)
    upsert_etf_nav(con, "MZNPETF", "2026-02-07", 21.97, 22.04, 1090.0, 49610000)
    upsert_etf_nav(con, "NBPGETF", "2026-02-07", 10.50, 10.55, 500.0, 10000000)
    return con


class TestInitSchema:
    def test_creates_tables(self, con):
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "etf_master" in table_names
        assert "etf_nav" in table_names

    def test_creates_indexes(self, con):
        indexes = con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_etf%'"
        ).fetchall()
        idx_names = [i["name"] for i in indexes]
        assert "idx_etf_nav_date" in idx_names
        assert "idx_etf_nav_symbol" in idx_names

    def test_idempotent(self, con):
        """Calling init_etf_schema twice should not error."""
        init_etf_schema(con)
        init_etf_schema(con)


class TestUpsertEtfMaster:
    def test_insert(self, con):
        result = upsert_etf_master(con, {
            "symbol": "MZNPETF",
            "name": "Meezan Pakistan ETF",
        })
        assert result is True
        rows = get_etf_list(con)
        assert len(rows) == 1
        assert rows[0]["symbol"] == "MZNPETF"

    def test_upsert_updates(self, con):
        upsert_etf_master(con, {"symbol": "TEST", "name": "Test ETF"})
        upsert_etf_master(con, {"symbol": "TEST", "name": "Updated ETF"})
        rows = get_etf_list(con)
        assert len(rows) == 1
        assert rows[0]["name"] == "Updated ETF"


class TestUpsertEtfNav:
    def test_insert_nav(self, con):
        result = upsert_etf_nav(con, "MZNPETF", "2026-02-07", 21.97, 22.04, 1090.0)
        assert result is True

    def test_premium_discount_calculated(self, con):
        upsert_etf_nav(con, "MZNPETF", "2026-02-07", 20.0, 21.0)
        row = con.execute(
            "SELECT premium_discount FROM etf_nav WHERE symbol='MZNPETF'"
        ).fetchone()
        # (21 - 20) / 20 * 100 = 5.0%
        assert row["premium_discount"] == pytest.approx(5.0, abs=0.01)

    def test_upsert_overwrites(self, con):
        upsert_etf_nav(con, "MZNPETF", "2026-02-07", 20.0, 21.0)
        upsert_etf_nav(con, "MZNPETF", "2026-02-07", 22.0, 23.0)
        row = con.execute(
            "SELECT nav FROM etf_nav WHERE symbol='MZNPETF'"
        ).fetchone()
        assert row["nav"] == 22.0


class TestGetEtfNavHistory:
    def test_returns_all(self, seeded_con):
        df = get_etf_nav_history(seeded_con, "MZNPETF")
        assert len(df) == 2

    def test_date_range_filter(self, seeded_con):
        df = get_etf_nav_history(
            seeded_con, "MZNPETF",
            start_date="2026-02-07", end_date="2026-02-07"
        )
        assert len(df) == 1
        assert df.iloc[0]["date"] == "2026-02-07"

    def test_empty_for_unknown_symbol(self, seeded_con):
        df = get_etf_nav_history(seeded_con, "UNKNOWN")
        assert df.empty


class TestGetEtfDetail:
    def test_returns_combined(self, seeded_con):
        detail = get_etf_detail(seeded_con, "MZNPETF")
        assert detail is not None
        assert detail["symbol"] == "MZNPETF"
        assert detail["name"] == "Meezan Pakistan ETF"
        assert detail["latest_nav"] is not None
        assert detail["latest_nav"]["nav"] == 21.97

    def test_returns_none_for_unknown(self, seeded_con):
        detail = get_etf_detail(seeded_con, "UNKNOWN")
        assert detail is None


class TestGetAllEtfLatestNav:
    def test_returns_all_etfs(self, seeded_con):
        df = get_all_etf_latest_nav(seeded_con)
        assert len(df) == 2
        symbols = df["symbol"].tolist()
        assert "MZNPETF" in symbols
        assert "NBPGETF" in symbols
