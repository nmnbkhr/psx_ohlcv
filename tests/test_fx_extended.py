"""Tests for extended FX rate repository and scrapers."""

import sqlite3

import pandas as pd
import pytest


@pytest.fixture
def con():
    """In-memory SQLite with FX extended schema."""
    from psx_ohlcv.db.repositories.fx_extended import init_fx_extended_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_fx_extended_schema(conn)
    yield conn
    conn.close()


# ── Schema tests ────────────────────────────────────────────────────

class TestInitFxExtendedSchema:
    def test_tables_created(self, con):
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in tables]
        assert "sbp_fx_interbank" in names
        assert "sbp_fx_open_market" in names
        assert "forex_kerb" in names

    def test_idempotent(self, con):
        from psx_ohlcv.db.repositories.fx_extended import init_fx_extended_schema
        init_fx_extended_schema(con)
        init_fx_extended_schema(con)


# ── Interbank tests ─────────────────────────────────────────────────

class TestUpsertFxInterbank:
    def test_insert(self, con):
        from psx_ohlcv.db.repositories.fx_extended import upsert_fx_interbank
        ok = upsert_fx_interbank(con, {
            "date": "2026-02-08",
            "currency": "USD",
            "buying": 278.50,
            "selling": 279.00,
        })
        assert ok is True

        row = con.execute(
            "SELECT * FROM sbp_fx_interbank WHERE date=? AND currency=?",
            ("2026-02-08", "USD"),
        ).fetchone()
        assert row["buying"] == 278.50
        assert row["selling"] == 279.00
        # Mid should be computed
        assert row["mid"] == pytest.approx(278.75, abs=0.01)

    def test_upsert_updates(self, con):
        from psx_ohlcv.db.repositories.fx_extended import upsert_fx_interbank
        upsert_fx_interbank(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 278.50, "selling": 279.00,
        })
        upsert_fx_interbank(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 279.00, "selling": 279.50,
        })
        row = con.execute(
            "SELECT * FROM sbp_fx_interbank WHERE date=? AND currency=?",
            ("2026-02-08", "USD"),
        ).fetchone()
        assert row["buying"] == 279.00
        assert row["selling"] == 279.50


# ── Open market tests ───────────────────────────────────────────────

class TestUpsertFxOpenMarket:
    def test_insert(self, con):
        from psx_ohlcv.db.repositories.fx_extended import upsert_fx_open_market
        ok = upsert_fx_open_market(con, {
            "date": "2026-02-08",
            "currency": "USD",
            "buying": 279.00,
            "selling": 280.00,
        })
        assert ok is True

        row = con.execute(
            "SELECT * FROM sbp_fx_open_market WHERE date=? AND currency=?",
            ("2026-02-08", "USD"),
        ).fetchone()
        assert row["buying"] == 279.00


# ── Kerb tests ──────────────────────────────────────────────────────

class TestUpsertFxKerb:
    def test_insert(self, con):
        from psx_ohlcv.db.repositories.fx_extended import upsert_fx_kerb
        ok = upsert_fx_kerb(con, {
            "date": "2026-02-08",
            "currency": "EUR",
            "buying": 290.00,
            "selling": 292.00,
            "source": "forex.pk",
        })
        assert ok is True

        row = con.execute(
            "SELECT * FROM forex_kerb WHERE date=? AND currency=? AND source=?",
            ("2026-02-08", "EUR", "forex.pk"),
        ).fetchone()
        assert row["buying"] == 290.00

    def test_multiple_currencies(self, con):
        from psx_ohlcv.db.repositories.fx_extended import upsert_fx_kerb
        for code, buy, sell in [("USD", 279, 280), ("EUR", 290, 292), ("GBP", 352, 355)]:
            upsert_fx_kerb(con, {
                "date": "2026-02-08", "currency": code,
                "buying": buy, "selling": sell, "source": "forex.pk",
            })
        count = con.execute("SELECT COUNT(*) FROM forex_kerb").fetchone()[0]
        assert count == 3


# ── Query tests ─────────────────────────────────────────────────────

class TestGetFxRate:
    def test_get_latest(self, con):
        from psx_ohlcv.db.repositories.fx_extended import (
            get_fx_rate,
            upsert_fx_interbank,
        )
        upsert_fx_interbank(con, {
            "date": "2026-02-07", "currency": "USD",
            "buying": 278.00, "selling": 278.50,
        })
        upsert_fx_interbank(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 279.00, "selling": 279.50,
        })
        rate = get_fx_rate(con, "USD", "interbank")
        assert rate["date"] == "2026-02-08"
        assert rate["buying"] == 279.00

    def test_get_by_date(self, con):
        from psx_ohlcv.db.repositories.fx_extended import (
            get_fx_rate,
            upsert_fx_interbank,
        )
        upsert_fx_interbank(con, {
            "date": "2026-02-07", "currency": "USD",
            "buying": 278.00, "selling": 278.50,
        })
        rate = get_fx_rate(con, "USD", "interbank", date="2026-02-07")
        assert rate is not None
        assert rate["buying"] == 278.00

    def test_not_found(self, con):
        from psx_ohlcv.db.repositories.fx_extended import get_fx_rate
        rate = get_fx_rate(con, "XYZ", "interbank")
        assert rate is None


class TestGetFxHistory:
    def test_empty(self, con):
        from psx_ohlcv.db.repositories.fx_extended import get_fx_history
        df = get_fx_history(con, "USD", "interbank")
        assert df.empty

    def test_with_data(self, con):
        from psx_ohlcv.db.repositories.fx_extended import (
            get_fx_history,
            upsert_fx_interbank,
        )
        upsert_fx_interbank(con, {
            "date": "2026-02-07", "currency": "USD",
            "buying": 278.00, "selling": 278.50,
        })
        upsert_fx_interbank(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 279.00, "selling": 279.50,
        })
        df = get_fx_history(con, "USD", "interbank")
        assert len(df) == 2


class TestGetAllFxLatest:
    def test_latest_per_currency(self, con):
        from psx_ohlcv.db.repositories.fx_extended import (
            get_all_fx_latest,
            upsert_fx_kerb,
        )
        for code, buy, sell in [("USD", 279, 280), ("EUR", 290, 292), ("GBP", 352, 355)]:
            upsert_fx_kerb(con, {
                "date": "2026-02-08", "currency": code,
                "buying": buy, "selling": sell, "source": "forex.pk",
            })
        df = get_all_fx_latest(con, source="kerb")
        assert len(df) == 3


class TestGetFxSpread:
    def test_spread(self, con):
        from psx_ohlcv.db.repositories.fx_extended import (
            get_fx_spread,
            upsert_fx_interbank,
            upsert_fx_kerb,
        )
        upsert_fx_interbank(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 278.50, "selling": 279.00,
        })
        upsert_fx_kerb(con, {
            "date": "2026-02-08", "currency": "USD",
            "buying": 279.50, "selling": 280.50, "source": "forex.pk",
        })
        spread = get_fx_spread(con, "USD")
        assert spread["currency"] == "USD"
        assert spread["interbank"] is not None
        assert spread["kerb"] is not None
        assert spread["open_market"] is None  # No open market data inserted
        # Kerb should be higher than interbank
        assert spread["kerb"]["buying"] > spread["interbank"]["buying"]


# ── Scraper tests ───────────────────────────────────────────────────

class TestSBPFXScraper:
    def test_sync_interbank(self, con):
        """Test sync with mocked interbank data."""
        from unittest.mock import patch
        from psx_ohlcv.sources.sbp_fx import SBPFXScraper

        scraper = SBPFXScraper()
        mock_rates = [
            {"date": "2026-02-08", "currency": "USD", "buying": 278.50, "selling": 279.00},
        ]
        with patch.object(scraper, "scrape_interbank", return_value=mock_rates):
            result = scraper.sync_interbank(con)
        assert result["ok"] == 1
        assert result["failed"] == 0


class TestForexPKScraper:
    def test_sync_kerb(self, con):
        """Test sync with mocked kerb data."""
        from unittest.mock import patch
        from psx_ohlcv.sources.forex_scraper import ForexPKScraper

        scraper = ForexPKScraper()
        mock_rates = [
            {"date": "2026-02-08", "currency": "USD", "buying": 279.50, "selling": 280.50, "source": "forex.pk"},
            {"date": "2026-02-08", "currency": "EUR", "buying": 290.00, "selling": 292.00, "source": "forex.pk"},
        ]
        with patch.object(scraper, "scrape_open_market", return_value=mock_rates):
            result = scraper.sync_kerb(con)
        assert result["ok"] == 2
        assert result["failed"] == 0

    def test_parse_number(self):
        """Test number parsing helper."""
        from psx_ohlcv.sources.forex_scraper import _parse_number
        assert _parse_number("279.41") == 279.41
        assert _parse_number("1,089.93") == 1089.93
        assert _parse_number("") is None
        assert _parse_number(None) is None
