"""Tests for treasury repository and SBP treasury scraper."""

import sqlite3

import pandas as pd
import pytest


# ── fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def con():
    """In-memory SQLite connection with treasury schema."""
    from pakfindata.db.repositories.treasury import init_treasury_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_treasury_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_tbill():
    return {
        "auction_date": "2026-02-04",
        "tenor": "3M",
        "target_amount_billions": 200.0,
        "bids_received_billions": 350.0,
        "amount_accepted_billions": 200.0,
        "cutoff_yield": 10.1983,
        "cutoff_price": 97.50,
        "weighted_avg_yield": 10.15,
        "maturity_date": "2026-05-04",
        "settlement_date": "2026-02-06",
    }


@pytest.fixture
def sample_pib():
    return {
        "auction_date": "2026-02-04",
        "tenor": "5Y",
        "pib_type": "Fixed",
        "target_amount_billions": 50.0,
        "bids_received_billions": 80.0,
        "amount_accepted_billions": 50.0,
        "cutoff_yield": 10.525,
        "cutoff_price": 95.00,
        "coupon_rate": 10.0,
        "maturity_date": "2031-02-04",
    }


@pytest.fixture
def sample_gis():
    return {
        "auction_date": "2026-01-15",
        "gis_type": "Ijarah Sukuk 3Y",
        "tenor": "3Y",
        "target_amount_billions": 30.0,
        "amount_accepted_billions": 25.0,
        "cutoff_rental_rate": 10.25,
        "maturity_date": "2029-01-15",
    }


# ── schema tests ────────────────────────────────────────────────────

class TestInitTreasurySchema:
    def test_tables_created(self, con):
        """All three treasury tables should exist."""
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [r["name"] for r in tables]
        assert "tbill_auctions" in table_names
        assert "pib_auctions" in table_names
        assert "gis_auctions" in table_names

    def test_indexes_created(self, con):
        """Indexes should be created for auction tables."""
        indexes = con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        ).fetchall()
        index_names = [r["name"] for r in indexes]
        assert "idx_tbill_date" in index_names
        assert "idx_tbill_tenor" in index_names
        assert "idx_pib_date" in index_names
        assert "idx_gis_date" in index_names

    def test_idempotent(self, con):
        """init_treasury_schema should be safe to call multiple times."""
        from pakfindata.db.repositories.treasury import init_treasury_schema
        init_treasury_schema(con)
        init_treasury_schema(con)
        # No error means success


# ── T-Bill tests ────────────────────────────────────────────────────

class TestUpsertTbillAuction:
    def test_insert(self, con, sample_tbill):
        from pakfindata.db.repositories.treasury import upsert_tbill_auction
        result = upsert_tbill_auction(con, sample_tbill)
        assert result is True

        row = con.execute(
            "SELECT * FROM tbill_auctions WHERE auction_date=? AND tenor=?",
            (sample_tbill["auction_date"], sample_tbill["tenor"]),
        ).fetchone()
        assert row is not None
        assert row["cutoff_yield"] == 10.1983
        assert row["tenor"] == "3M"

    def test_upsert_updates(self, con, sample_tbill):
        from pakfindata.db.repositories.treasury import upsert_tbill_auction
        upsert_tbill_auction(con, sample_tbill)

        # Update yield
        updated = {**sample_tbill, "cutoff_yield": 10.25}
        result = upsert_tbill_auction(con, updated)
        assert result is True

        row = con.execute(
            "SELECT * FROM tbill_auctions WHERE auction_date=? AND tenor=?",
            (sample_tbill["auction_date"], sample_tbill["tenor"]),
        ).fetchone()
        assert row["cutoff_yield"] == 10.25

    def test_minimal_data(self, con):
        """Upsert with only required fields should work."""
        from pakfindata.db.repositories.treasury import upsert_tbill_auction
        data = {"auction_date": "2026-01-01", "tenor": "6M"}
        result = upsert_tbill_auction(con, data)
        assert result is True


class TestGetTbillAuctions:
    def test_empty(self, con):
        from pakfindata.db.repositories.treasury import get_tbill_auctions
        df = get_tbill_auctions(con)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_with_data(self, con, sample_tbill):
        from pakfindata.db.repositories.treasury import (
            get_tbill_auctions,
            upsert_tbill_auction,
        )
        upsert_tbill_auction(con, sample_tbill)
        df = get_tbill_auctions(con)
        assert len(df) == 1
        assert df.iloc[0]["tenor"] == "3M"

    def test_filter_by_tenor(self, con, sample_tbill):
        from pakfindata.db.repositories.treasury import (
            get_tbill_auctions,
            upsert_tbill_auction,
        )
        upsert_tbill_auction(con, sample_tbill)
        upsert_tbill_auction(con, {**sample_tbill, "tenor": "6M"})

        df = get_tbill_auctions(con, tenor="3M")
        assert len(df) == 1

        df = get_tbill_auctions(con, tenor="6M")
        assert len(df) == 1

    def test_filter_by_date_range(self, con):
        from pakfindata.db.repositories.treasury import (
            get_tbill_auctions,
            upsert_tbill_auction,
        )
        upsert_tbill_auction(con, {"auction_date": "2026-01-01", "tenor": "3M"})
        upsert_tbill_auction(con, {"auction_date": "2026-02-01", "tenor": "3M"})
        upsert_tbill_auction(con, {"auction_date": "2026-03-01", "tenor": "3M"})

        df = get_tbill_auctions(con, start_date="2026-02-01")
        assert len(df) == 2

        df = get_tbill_auctions(con, end_date="2026-01-31")
        assert len(df) == 1


class TestGetLatestTbillYields:
    def test_empty(self, con):
        from pakfindata.db.repositories.treasury import get_latest_tbill_yields
        result = get_latest_tbill_yields(con)
        assert result == {}

    def test_returns_latest_per_tenor(self, con):
        from pakfindata.db.repositories.treasury import (
            get_latest_tbill_yields,
            upsert_tbill_auction,
        )
        # Insert two auctions for 3M
        upsert_tbill_auction(con, {
            "auction_date": "2026-01-01", "tenor": "3M", "cutoff_yield": 10.0,
        })
        upsert_tbill_auction(con, {
            "auction_date": "2026-02-04", "tenor": "3M", "cutoff_yield": 10.2,
        })
        # Insert one for 6M
        upsert_tbill_auction(con, {
            "auction_date": "2026-02-04", "tenor": "6M", "cutoff_yield": 10.3,
        })

        yields = get_latest_tbill_yields(con)
        assert "3M" in yields
        assert "6M" in yields
        assert yields["3M"]["cutoff_yield"] == 10.2
        assert yields["6M"]["cutoff_yield"] == 10.3


# ── PIB tests ───────────────────────────────────────────────────────

class TestUpsertPibAuction:
    def test_insert(self, con, sample_pib):
        from pakfindata.db.repositories.treasury import upsert_pib_auction
        result = upsert_pib_auction(con, sample_pib)
        assert result is True

        row = con.execute(
            "SELECT * FROM pib_auctions WHERE auction_date=? AND tenor=? AND pib_type=?",
            (sample_pib["auction_date"], sample_pib["tenor"], sample_pib["pib_type"]),
        ).fetchone()
        assert row is not None
        assert row["cutoff_yield"] == 10.525

    def test_upsert_updates(self, con, sample_pib):
        from pakfindata.db.repositories.treasury import upsert_pib_auction
        upsert_pib_auction(con, sample_pib)

        updated = {**sample_pib, "cutoff_yield": 10.6}
        result = upsert_pib_auction(con, updated)
        assert result is True

        row = con.execute(
            "SELECT * FROM pib_auctions WHERE auction_date=? AND tenor=? AND pib_type=?",
            (sample_pib["auction_date"], sample_pib["tenor"], sample_pib["pib_type"]),
        ).fetchone()
        assert row["cutoff_yield"] == 10.6


class TestGetLatestPibYields:
    def test_returns_latest(self, con, sample_pib):
        from pakfindata.db.repositories.treasury import (
            get_latest_pib_yields,
            upsert_pib_auction,
        )
        upsert_pib_auction(con, sample_pib)
        yields = get_latest_pib_yields(con)
        assert "5Y_Fixed" in yields
        assert yields["5Y_Fixed"]["cutoff_yield"] == 10.525


# ── GIS tests ───────────────────────────────────────────────────────

class TestUpsertGisAuction:
    def test_insert(self, con, sample_gis):
        from pakfindata.db.repositories.treasury import upsert_gis_auction
        result = upsert_gis_auction(con, sample_gis)
        assert result is True

        row = con.execute(
            "SELECT * FROM gis_auctions WHERE auction_date=? AND gis_type=?",
            (sample_gis["auction_date"], sample_gis["gis_type"]),
        ).fetchone()
        assert row is not None
        assert row["cutoff_rental_rate"] == 10.25


class TestGetGisAuctions:
    def test_empty(self, con):
        from pakfindata.db.repositories.treasury import get_gis_auctions
        df = get_gis_auctions(con)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_with_data(self, con, sample_gis):
        from pakfindata.db.repositories.treasury import (
            get_gis_auctions,
            upsert_gis_auction,
        )
        upsert_gis_auction(con, sample_gis)
        df = get_gis_auctions(con)
        assert len(df) == 1


# ── Yield trend tests ───────────────────────────────────────────────

class TestGetYieldTrend:
    def test_empty(self, con):
        from pakfindata.db.repositories.treasury import get_yield_trend
        df = get_yield_trend(con, "3M")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_returns_limited(self, con):
        from pakfindata.db.repositories.treasury import (
            get_yield_trend,
            upsert_tbill_auction,
        )
        # Insert 5 auctions
        for i in range(5):
            upsert_tbill_auction(con, {
                "auction_date": f"2026-0{i+1}-01",
                "tenor": "3M",
                "cutoff_yield": 10.0 + i * 0.1,
                "weighted_avg_yield": 9.9 + i * 0.1,
            })

        df = get_yield_trend(con, "3M", n_auctions=3)
        assert len(df) == 3
        # Should be descending by date
        assert df.iloc[0]["auction_date"] == "2026-05-01"


# ── Scraper unit tests ──────────────────────────────────────────────

class TestSBPTreasuryScraper:
    def test_parse_rate_valid(self):
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
        assert SBPTreasuryScraper._parse_rate("10.1977%") == 10.1977
        assert SBPTreasuryScraper._parse_rate("10.1977") == 10.1977
        assert SBPTreasuryScraper._parse_rate("  10.525 % ") == 10.525

    def test_parse_rate_invalid(self):
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
        assert SBPTreasuryScraper._parse_rate("") is None
        assert SBPTreasuryScraper._parse_rate(None) is None
        assert SBPTreasuryScraper._parse_rate("N/A") is None

    def test_parse_rate_out_of_range(self):
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
        assert SBPTreasuryScraper._parse_rate("150.0%") is None
        assert SBPTreasuryScraper._parse_rate("0.0%") is None

    def test_try_parse_date(self):
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
        assert SBPTreasuryScraper._try_parse_date("February 04, 2026") == "2026-02-04"
        assert SBPTreasuryScraper._try_parse_date("04-Feb-2026") == "2026-02-04"
        assert SBPTreasuryScraper._try_parse_date("2026-02-04") == "2026-02-04"

    def test_try_parse_date_invalid(self):
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
        assert SBPTreasuryScraper._try_parse_date("not a date") is None
        assert SBPTreasuryScraper._try_parse_date("") is None

    def test_sync_treasury_to_db(self, con):
        """Test that sync_treasury can write to the DB (with mocked scrape)."""
        from unittest.mock import patch
        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper

        mock_pma = {
            "tbills": [
                {"tenor": "3M", "cutoff_yield": 10.2, "auction_date": "2026-02-04"},
                {"tenor": "6M", "cutoff_yield": 10.3, "auction_date": "2026-02-04"},
            ],
            "pibs": [
                {"tenor": "5Y", "cutoff_yield": 10.5, "auction_date": "2026-02-04"},
            ],
            "auction_date": "2026-02-04",
            "raw_html_length": 5000,
        }

        scraper = SBPTreasuryScraper()
        with patch.object(scraper, "scrape_pma_page", return_value=mock_pma):
            result = scraper.sync_treasury(con)

        assert result["tbills_ok"] == 2
        assert result["pibs_ok"] == 1
        assert result["failed"] == 0
        assert result["auction_date"] == "2026-02-04"

        # Verify data in DB
        from pakfindata.db.repositories.treasury import get_latest_tbill_yields
        yields = get_latest_tbill_yields(con)
        assert "3M" in yields
        assert "6M" in yields


# ── GSP scraper tests ───────────────────────────────────────────────

class TestGSPScraper:
    def test_extract_gis_tenors(self):
        from pakfindata.sources.sbp_gsp import GSPScraper
        section = "Tenor Cut-off Rental Rate/Price 3-Y 100.2842 5-Y 100.0022"
        results = GSPScraper._extract_gis_tenors(section, "GIS FRR")
        assert len(results) == 2
        tenors = {r["tenor"] for r in results}
        assert "3Y" in tenors
        assert "5Y" in tenors
        # gis_type should include tenor
        types = {r["gis_type"] for r in results}
        assert "GIS FRR 3Y" in types
        assert "GIS FRR 5Y" in types

    def test_extract_gis_tenors_empty(self):
        from pakfindata.sources.sbp_gsp import GSPScraper
        results = GSPScraper._extract_gis_tenors("no data here", "GIS FRR")
        assert results == []

    def test_sync_gis_to_db(self, con):
        """Test GIS sync with mocked scrape."""
        from unittest.mock import patch
        from pakfindata.sources.sbp_gsp import GSPScraper

        mock_records = [
            {
                "auction_date": "2023-12-21",
                "gis_type": "GIS FRR 3Y",
                "tenor": "3Y",
                "cutoff_rental_rate": 100.2842,
            },
            {
                "auction_date": "2023-12-21",
                "gis_type": "GIS VRR 5Y",
                "tenor": "5Y",
                "cutoff_rental_rate": 98.76,
            },
        ]

        scraper = GSPScraper()
        with patch.object(scraper, "scrape_gis_auctions", return_value=mock_records):
            result = scraper.sync_gis(con)

        assert result["ok"] == 2
        assert result["failed"] == 0

        from pakfindata.db.repositories.treasury import get_gis_auctions
        df = get_gis_auctions(con)
        assert len(df) == 2
