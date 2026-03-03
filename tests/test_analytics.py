"""Tests for analytics module."""

import sqlite3

import pytest

from pakfindata.analytics import (
    compute_all_analytics,
    compute_market_analytics,
    compute_sector_rollups,
    compute_top_lists,
    get_current_market_with_sectors,
    get_latest_market_analytics,
    get_sector_leaderboard,
    get_top_list,
    init_analytics_schema,
    store_market_analytics,
    store_top_lists,
)
from pakfindata.db import init_schema
from pakfindata.sources.regular_market import init_regular_market_schema


@pytest.fixture
def db_connection():
    """Create in-memory database with schema initialized."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    init_schema(con)
    init_regular_market_schema(con)
    init_analytics_schema(con)
    return con


@pytest.fixture
def populated_db(db_connection):
    """Database with sample data for testing."""
    con = db_connection

    # Add symbols with sector info
    con.execute(
        """
        INSERT INTO symbols (symbol, name, sector, sector_name, is_active,
            source, discovered_at, updated_at)
        VALUES
            ('OGDC', 'Oil & Gas Development', '0816', 'OIL & GAS', 1,
             'LISTED_CMP', '2026-01-21', '2026-01-21'),
            ('HBL', 'Habib Bank Limited', '0807', 'COMMERCIAL BANKS', 1,
             'LISTED_CMP', '2026-01-21', '2026-01-21'),
            ('UBL', 'United Bank Limited', '0807', 'COMMERCIAL BANKS', 1,
             'LISTED_CMP', '2026-01-21', '2026-01-21'),
            ('PSO', 'Pakistan State Oil', '0817', 'OIL & GAS MARKETING', 1,
             'LISTED_CMP', '2026-01-21', '2026-01-21'),
            ('LUCK', 'Lucky Cement', '0804', 'CEMENT', 1,
             'LISTED_CMP', '2026-01-21', '2026-01-21')
        """
    )

    # Add regular market current data
    con.execute(
        """
        INSERT INTO regular_market_current
            (symbol, ts, sector_code, ldcp, open, high, low, current,
             change, change_pct, volume, row_hash, updated_at)
        VALUES
            ('OGDC', '2026-01-21T15:30:00', '0816', 100, 100, 105, 98, 103,
             3, 3.0, 1000000, 'hash1', '2026-01-21T15:30:00'),
            ('HBL', '2026-01-21T15:30:00', '0807', 200, 200, 210, 195, 205,
             5, 2.5, 500000, 'hash2', '2026-01-21T15:30:00'),
            ('UBL', '2026-01-21T15:30:00', '0807', 150, 150, 155, 145, 145,
             -5, -3.33, 300000, 'hash3', '2026-01-21T15:30:00'),
            ('PSO', '2026-01-21T15:30:00', '0817', 300, 300, 310, 295, 300,
             0, 0.0, 200000, 'hash4', '2026-01-21T15:30:00'),
            ('LUCK', '2026-01-21T15:30:00', '0804', 500, 500, 520, 490, 515,
             15, 3.0, 400000, 'hash5', '2026-01-21T15:30:00')
        """
    )

    con.commit()
    return con


class TestInitAnalyticsSchema:
    """Tests for init_analytics_schema."""

    def test_creates_tables(self, db_connection):
        """Should create all analytics tables."""
        tables = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        assert "analytics_market_snapshot" in table_names
        assert "analytics_symbol_snapshot" in table_names
        assert "analytics_sector_snapshot" in table_names

    def test_idempotent(self, db_connection):
        """Should be safe to call multiple times."""
        init_analytics_schema(db_connection)
        init_analytics_schema(db_connection)
        # No error means success


class TestComputeMarketAnalytics:
    """Tests for compute_market_analytics."""

    def test_counts_gainers(self, populated_db):
        """Should count symbols with positive change_pct."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        # OGDC +3.0, HBL +2.5, LUCK +3.0 = 3 gainers
        assert result["gainers_count"] == 3

    def test_counts_losers(self, populated_db):
        """Should count symbols with negative change_pct."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        # UBL -3.33 = 1 loser
        assert result["losers_count"] == 1

    def test_counts_unchanged(self, populated_db):
        """Should count symbols with zero change_pct."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        # PSO 0.0 = 1 unchanged
        assert result["unchanged_count"] == 1

    def test_total_symbols(self, populated_db):
        """Should count all symbols."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        assert result["total_symbols"] == 5

    def test_total_volume(self, populated_db):
        """Should sum all volume."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        # 1000000 + 500000 + 300000 + 200000 + 400000 = 2400000
        assert result["total_volume"] == 2400000

    def test_top_gainer(self, populated_db):
        """Should identify the symbol with highest change_pct."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        # OGDC and LUCK both have 3.0, either is acceptable
        assert result["top_gainer_symbol"] in ["OGDC", "LUCK"]

    def test_top_loser(self, populated_db):
        """Should identify the symbol with lowest change_pct."""
        result = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        assert result["top_loser_symbol"] == "UBL"

    def test_empty_db(self, db_connection):
        """Should handle empty database."""
        result = compute_market_analytics(db_connection, "2026-01-21T15:30:00")
        assert result["gainers_count"] == 0
        assert result["losers_count"] == 0
        assert result["total_symbols"] == 0


class TestStoreMarketAnalytics:
    """Tests for store_market_analytics."""

    def test_stores_analytics(self, populated_db):
        """Should store analytics in database."""
        analytics = compute_market_analytics(populated_db, "2026-01-21T15:30:00")
        store_market_analytics(populated_db, analytics)

        row = populated_db.execute(
            "SELECT * FROM analytics_market_snapshot WHERE ts = ?",
            ("2026-01-21T15:30:00",),
        ).fetchone()

        assert row is not None
        assert row["gainers_count"] == 3
        assert row["losers_count"] == 1

    def test_upsert(self, populated_db):
        """Should update on conflict."""
        analytics = {
            "ts": "2026-01-21T15:30:00",
            "gainers_count": 10,
            "losers_count": 5,
            "unchanged_count": 2,
            "total_symbols": 17,
            "total_volume": 1000.0,
            "top_gainer_symbol": "ABC",
            "top_loser_symbol": "XYZ",
        }
        store_market_analytics(populated_db, analytics)

        # Update with new values
        analytics["gainers_count"] = 20
        store_market_analytics(populated_db, analytics)

        row = populated_db.execute(
            "SELECT gainers_count FROM analytics_market_snapshot WHERE ts = ?",
            ("2026-01-21T15:30:00",),
        ).fetchone()

        assert row["gainers_count"] == 20


class TestComputeTopLists:
    """Tests for compute_top_lists."""

    def test_returns_three_lists(self, populated_db):
        """Should return gainers, losers, and volume lists."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=3)

        assert "gainers" in result
        assert "losers" in result
        assert "volume" in result

    def test_gainers_sorted_desc(self, populated_db):
        """Gainers should be sorted by change_pct descending."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=5)

        gainers = result["gainers"]
        if len(gainers) > 1:
            assert (
                gainers.iloc[0]["change_pct"] >= gainers.iloc[1]["change_pct"]
            )

    def test_losers_sorted_asc(self, populated_db):
        """Losers should be sorted by change_pct ascending."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=5)

        losers = result["losers"]
        if len(losers) > 1:
            assert losers.iloc[0]["change_pct"] <= losers.iloc[1]["change_pct"]

    def test_volume_sorted_desc(self, populated_db):
        """Volume list should be sorted by volume descending."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=5)

        volume = result["volume"]
        if len(volume) > 1:
            assert volume.iloc[0]["volume"] >= volume.iloc[1]["volume"]

    def test_includes_sector_name(self, populated_db):
        """Should include sector_name from joined symbols table."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=5)

        gainers = result["gainers"]
        # OGDC should have sector_name from symbols table
        ogdc_row = gainers[gainers["symbol"] == "OGDC"]
        if not ogdc_row.empty:
            assert ogdc_row.iloc[0]["sector_name"] == "OIL & GAS"

    def test_respects_top_n(self, populated_db):
        """Should limit results to top_n."""
        result = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=2)

        assert len(result["gainers"]) <= 2
        assert len(result["volume"]) <= 2


class TestStoreTopLists:
    """Tests for store_top_lists."""

    def test_stores_all_lists(self, populated_db):
        """Should store all top lists."""
        top_lists = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=3)
        count = store_top_lists(populated_db, "2026-01-21T15:30:00", top_lists)

        assert count > 0

        # Verify data exists
        rows = populated_db.execute(
            "SELECT COUNT(*) FROM analytics_symbol_snapshot WHERE ts = ?",
            ("2026-01-21T15:30:00",),
        ).fetchone()

        assert rows[0] > 0

    def test_clears_previous(self, populated_db):
        """Should clear previous entries for same ts."""
        top_lists = compute_top_lists(populated_db, "2026-01-21T15:30:00", top_n=3)
        store_top_lists(populated_db, "2026-01-21T15:30:00", top_lists)

        # Store again
        store_top_lists(populated_db, "2026-01-21T15:30:00", top_lists)

        # Should not have duplicates
        rows = populated_db.execute(
            """
            SELECT COUNT(*) FROM analytics_symbol_snapshot
            WHERE ts = ? AND rank_type = 'gainers' AND rank = 1
            """,
            ("2026-01-21T15:30:00",),
        ).fetchone()

        assert rows[0] == 1


class TestComputeSectorRollups:
    """Tests for compute_sector_rollups."""

    def test_groups_by_sector(self, populated_db):
        """Should group symbols by sector."""
        result = compute_sector_rollups(populated_db, "2026-01-21T15:30:00")

        # Should have COMMERCIAL BANKS with 2 symbols (HBL, UBL)
        banks = result[result["sector_name"] == "COMMERCIAL BANKS"]
        if not banks.empty:
            assert banks.iloc[0]["symbols_count"] == 2

    def test_calculates_avg_change(self, populated_db):
        """Should calculate average change_pct."""
        result = compute_sector_rollups(populated_db, "2026-01-21T15:30:00")

        # COMMERCIAL BANKS: (2.5 + -3.33) / 2 = -0.415
        banks = result[result["sector_name"] == "COMMERCIAL BANKS"]
        if not banks.empty:
            avg = banks.iloc[0]["avg_change_pct"]
            assert abs(avg - (-0.415)) < 0.1

    def test_sums_volume(self, populated_db):
        """Should sum volume per sector."""
        result = compute_sector_rollups(populated_db, "2026-01-21T15:30:00")

        # COMMERCIAL BANKS: 500000 + 300000 = 800000
        banks = result[result["sector_name"] == "COMMERCIAL BANKS"]
        if not banks.empty:
            assert banks.iloc[0]["sum_volume"] == 800000

    def test_identifies_top_symbol(self, populated_db):
        """Should identify top performing symbol in sector."""
        result = compute_sector_rollups(populated_db, "2026-01-21T15:30:00")

        # COMMERCIAL BANKS: HBL (+2.5) > UBL (-3.33)
        banks = result[result["sector_name"] == "COMMERCIAL BANKS"]
        if not banks.empty:
            assert banks.iloc[0]["top_symbol"] == "HBL"


class TestComputeAllAnalytics:
    """Tests for compute_all_analytics."""

    def test_computes_all(self, populated_db):
        """Should compute and store all analytics."""
        result = compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        assert "market_analytics" in result
        assert "top_lists_count" in result
        assert "sectors_count" in result

        # Verify data was stored
        market = populated_db.execute(
            "SELECT * FROM analytics_market_snapshot WHERE ts = ?",
            ("2026-01-21T15:30:00",),
        ).fetchone()
        assert market is not None

        symbols = populated_db.execute(
            "SELECT COUNT(*) FROM analytics_symbol_snapshot WHERE ts = ?",
            ("2026-01-21T15:30:00",),
        ).fetchone()
        assert symbols[0] > 0


class TestGetLatestMarketAnalytics:
    """Tests for get_latest_market_analytics."""

    def test_returns_latest(self, populated_db):
        """Should return the most recent analytics."""
        compute_all_analytics(populated_db, "2026-01-21T15:00:00")
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_latest_market_analytics(populated_db)

        assert result is not None
        assert result["ts"] == "2026-01-21T15:30:00"

    def test_returns_none_if_empty(self, db_connection):
        """Should return None if no data."""
        result = get_latest_market_analytics(db_connection)
        assert result is None


class TestGetTopList:
    """Tests for get_top_list."""

    def test_returns_gainers(self, populated_db):
        """Should return gainers list."""
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_top_list(populated_db, "gainers")

        assert not result.empty
        assert "symbol" in result.columns
        assert "change_pct" in result.columns

    def test_returns_losers(self, populated_db):
        """Should return losers list."""
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_top_list(populated_db, "losers")

        assert not result.empty

    def test_returns_volume(self, populated_db):
        """Should return volume list."""
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_top_list(populated_db, "volume")

        assert not result.empty


class TestGetSectorLeaderboard:
    """Tests for get_sector_leaderboard."""

    def test_returns_sectors(self, populated_db):
        """Should return sector rollups."""
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_sector_leaderboard(populated_db)

        assert not result.empty
        assert "sector_name" in result.columns
        assert "avg_change_pct" in result.columns

    def test_sorted_by_default(self, populated_db):
        """Should be sorted by avg_change_pct desc by default."""
        compute_all_analytics(populated_db, "2026-01-21T15:30:00")

        result = get_sector_leaderboard(populated_db)

        if len(result) > 1:
            # First should have higher avg_change_pct
            first_avg = result.iloc[0]["avg_change_pct"]
            second_avg = result.iloc[1]["avg_change_pct"]
            if first_avg is not None and second_avg is not None:
                assert first_avg >= second_avg


class TestGetCurrentMarketWithSectors:
    """Tests for get_current_market_with_sectors."""

    def test_joins_sector_names(self, populated_db):
        """Should include sector_name from symbols table."""
        result = get_current_market_with_sectors(populated_db)

        assert not result.empty
        assert "sector_name" in result.columns

        # OGDC should have its sector name
        ogdc = result[result["symbol"] == "OGDC"]
        if not ogdc.empty:
            assert ogdc.iloc[0]["sector_name"] == "OIL & GAS"

    def test_includes_company_name(self, populated_db):
        """Should include company_name from symbols table."""
        result = get_current_market_with_sectors(populated_db)

        assert "company_name" in result.columns

        # HBL should have its company name
        hbl = result[result["symbol"] == "HBL"]
        if not hbl.empty:
            assert hbl.iloc[0]["company_name"] == "Habib Bank Limited"
