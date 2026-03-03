"""Tests for IPO listings repository and scraper."""

import sqlite3

import pandas as pd
import pytest


@pytest.fixture
def con():
    """In-memory SQLite with IPO schema."""
    from pakfindata.db.repositories.ipo import init_ipo_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_ipo_schema(conn)
    yield conn
    conn.close()


# ── Schema tests ────────────────────────────────────────────────────

class TestInitIpoSchema:
    def test_table_created(self, con):
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = [r["name"] for r in tables]
        assert "ipo_listings" in names

    def test_idempotent(self, con):
        from pakfindata.db.repositories.ipo import init_ipo_schema
        init_ipo_schema(con)
        init_ipo_schema(con)


# ── Upsert tests ───────────────────────────────────────────────────

class TestUpsertIpoListing:
    def test_insert(self, con):
        from pakfindata.db.repositories.ipo import upsert_ipo_listing
        ok = upsert_ipo_listing(con, {
            "symbol": "NEWCO",
            "company_name": "New Company Ltd",
            "board": "main",
            "status": "upcoming",
            "offer_price": 25.0,
            "listing_date": "2026-03-15",
        })
        assert ok is True

        row = con.execute(
            "SELECT * FROM ipo_listings WHERE symbol=?", ("NEWCO",)
        ).fetchone()
        assert row["company_name"] == "New Company Ltd"
        assert row["offer_price"] == 25.0

    def test_upsert_updates(self, con):
        from pakfindata.db.repositories.ipo import upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "NEWCO", "listing_date": "2026-03-15",
            "status": "upcoming", "offer_price": 25.0,
        })
        upsert_ipo_listing(con, {
            "symbol": "NEWCO", "listing_date": "2026-03-15",
            "status": "listed", "offer_price": 25.0,
        })
        row = con.execute(
            "SELECT * FROM ipo_listings WHERE symbol=?", ("NEWCO",)
        ).fetchone()
        assert row["status"] == "listed"


# ── Query tests ─────────────────────────────────────────────────────

class TestGetIpoListings:
    def test_empty(self, con):
        from pakfindata.db.repositories.ipo import get_ipo_listings
        df = get_ipo_listings(con)
        assert df.empty

    def test_filter_by_status(self, con):
        from pakfindata.db.repositories.ipo import get_ipo_listings, upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "A", "status": "listed", "listing_date": "2026-01-01",
        })
        upsert_ipo_listing(con, {
            "symbol": "B", "status": "upcoming", "listing_date": "2026-03-01",
        })
        df = get_ipo_listings(con, status="upcoming")
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "B"

    def test_filter_by_board(self, con):
        from pakfindata.db.repositories.ipo import get_ipo_listings, upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "A", "board": "main", "listing_date": "2026-01-01",
        })
        upsert_ipo_listing(con, {
            "symbol": "B", "board": "gem", "listing_date": "2026-02-01",
        })
        df = get_ipo_listings(con, board="gem")
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "B"


class TestGetUpcomingIpos:
    def test_empty(self, con):
        from pakfindata.db.repositories.ipo import get_upcoming_ipos
        df = get_upcoming_ipos(con)
        assert df.empty

    def test_returns_upcoming(self, con):
        from pakfindata.db.repositories.ipo import get_upcoming_ipos, upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "NEWCO", "status": "upcoming",
            "subscription_close": "2030-12-31", "listing_date": "2031-01-15",
        })
        df = get_upcoming_ipos(con)
        assert len(df) == 1


class TestGetRecentListings:
    def test_returns_recent(self, con):
        from pakfindata.db.repositories.ipo import get_recent_listings, upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "A", "listing_date": "2026-01-01",
        })
        upsert_ipo_listing(con, {
            "symbol": "B", "listing_date": "2026-02-01",
        })
        df = get_recent_listings(con, n=10)
        assert len(df) == 2
        assert df.iloc[0]["symbol"] == "B"  # newest first


class TestGetIpoBySymbol:
    def test_not_found(self, con):
        from pakfindata.db.repositories.ipo import get_ipo_by_symbol
        result = get_ipo_by_symbol(con, "XYZ")
        assert result is None

    def test_found(self, con):
        from pakfindata.db.repositories.ipo import get_ipo_by_symbol, upsert_ipo_listing
        upsert_ipo_listing(con, {
            "symbol": "NEWCO", "company_name": "New Co",
            "board": "main", "listing_date": "2026-03-15",
        })
        result = get_ipo_by_symbol(con, "NEWCO")
        assert result is not None
        assert result["company_name"] == "New Co"


# ── Scraper tests ──────────────────────────────────────────────────

class TestIPOScraper:
    def test_sync_with_mock(self, con):
        from unittest.mock import patch
        from pakfindata.sources.ipo_scraper import IPOScraper

        scraper = IPOScraper()
        mock_data = [
            {"symbol": "NEWCO", "company_name": "New Company", "board": "main", "status": "listed"},
        ]
        with patch.object(scraper, "scrape_listings", return_value=mock_data):
            result = scraper.sync_listings(con)
        # mock is called for each board so we override to return once
        assert result["ok"] >= 1

    def test_parse_table(self, con):
        from pakfindata.sources.ipo_scraper import IPOScraper
        html = """
        <table>
            <tr><th>Symbol</th><th>Company</th><th>Sector</th></tr>
            <tr><td><a href="/company/OGDC">OGDC</a></td><td>Oil and Gas Dev.</td><td>E&amp;P</td></tr>
            <tr><td><a href="/company/PPL">PPL</a></td><td>Pakistan Petroleum</td><td>E&amp;P</td></tr>
        </table>
        """
        scraper = IPOScraper()
        results = scraper._parse_table(html, "main")
        assert len(results) == 2
        assert results[0]["symbol"] == "OGDC"
        assert results[0]["board"] == "main"
