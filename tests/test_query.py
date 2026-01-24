"""Tests for query helpers."""

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema, upsert_eod, upsert_symbols
from psx_ohlcv.analytics import init_analytics_schema
from psx_ohlcv.query import (
    get_latest_close,
    get_market_history,
    get_market_history_stats,
    get_ohlcv_range,
    get_sector_history,
    get_sector_list_from_analytics,
    get_symbol_history_stats,
    get_symbol_snapshot_history,
    get_symbols_list,
    get_symbols_string,
    get_time_range_bounds,
)
from psx_ohlcv.sources.regular_market import init_regular_market_schema


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


@pytest.fixture
def db_with_data(db):
    """Create database with symbols and EOD data."""
    # Add symbols
    upsert_symbols(
        db,
        [
            {"symbol": "HBL", "name": "Habib Bank"},
            {"symbol": "UBL", "name": "United Bank"},
            {"symbol": "MCB", "name": "MCB Bank"},
        ],
    )

    # Add EOD data for HBL
    df_hbl = pd.DataFrame(
        [
            {
                "symbol": "HBL",
                "date": "2024-01-15",
                "open": 150.0,
                "high": 155.0,
                "low": 148.0,
                "close": 153.0,
                "volume": 100000,
            },
            {
                "symbol": "HBL",
                "date": "2024-01-16",
                "open": 153.0,
                "high": 158.0,
                "low": 152.0,
                "close": 156.0,
                "volume": 120000,
            },
            {
                "symbol": "HBL",
                "date": "2024-01-17",
                "open": 156.0,
                "high": 160.0,
                "low": 154.0,
                "close": 159.0,
                "volume": 80000,
            },
        ]
    )
    upsert_eod(db, df_hbl)

    # Add EOD data for UBL
    df_ubl = pd.DataFrame(
        [
            {
                "symbol": "UBL",
                "date": "2024-01-15",
                "open": 200.0,
                "high": 205.0,
                "low": 198.0,
                "close": 203.0,
                "volume": 50000,
            },
            {
                "symbol": "UBL",
                "date": "2024-01-16",
                "open": 203.0,
                "high": 210.0,
                "low": 201.0,
                "close": 208.0,
                "volume": 60000,
            },
        ]
    )
    upsert_eod(db, df_ubl)

    return db


class TestGetSymbolsList:
    """Tests for get_symbols_list."""

    def test_returns_sorted_list(self, db_with_data):
        """Should return symbols in alphabetical order."""
        result = get_symbols_list(db_with_data)
        assert result == ["HBL", "MCB", "UBL"]

    def test_respects_limit(self, db_with_data):
        """Should respect limit parameter."""
        result = get_symbols_list(db_with_data, limit=2)
        assert result == ["HBL", "MCB"]

    def test_active_only_by_default(self, db_with_data):
        """Should only return active symbols by default."""
        # Deactivate one symbol
        db_with_data.execute("UPDATE symbols SET is_active = 0 WHERE symbol = 'MCB'")
        db_with_data.commit()

        result = get_symbols_list(db_with_data)
        assert result == ["HBL", "UBL"]
        assert "MCB" not in result

    def test_include_inactive(self, db_with_data):
        """Should include inactive symbols when is_active_only=False."""
        db_with_data.execute("UPDATE symbols SET is_active = 0 WHERE symbol = 'MCB'")
        db_with_data.commit()

        result = get_symbols_list(db_with_data, is_active_only=False)
        assert result == ["HBL", "MCB", "UBL"]

    def test_empty_db(self, db):
        """Should return empty list for empty database."""
        result = get_symbols_list(db)
        assert result == []


class TestGetSymbolsString:
    """Tests for get_symbols_string."""

    def test_returns_comma_separated(self, db_with_data):
        """Should return comma-separated string."""
        result = get_symbols_string(db_with_data)
        assert result == "HBL,MCB,UBL"

    def test_stable_order(self, db_with_data):
        """Output should be stable (deterministic)."""
        result1 = get_symbols_string(db_with_data)
        result2 = get_symbols_string(db_with_data)
        result3 = get_symbols_string(db_with_data)
        assert result1 == result2 == result3
        assert result1 == "HBL,MCB,UBL"

    def test_respects_limit(self, db_with_data):
        """Should respect limit parameter."""
        result = get_symbols_string(db_with_data, limit=2)
        assert result == "HBL,MCB"

    def test_empty_db(self, db):
        """Should return empty string for empty database."""
        result = get_symbols_string(db)
        assert result == ""


class TestGetLatestClose:
    """Tests for get_latest_close."""

    def test_returns_latest_data(self, db_with_data):
        """Should return the most recent data for a symbol."""
        result = get_latest_close(db_with_data, "HBL")

        assert result is not None
        assert result["symbol"] == "HBL"
        assert result["date"] == "2024-01-17"
        assert result["close"] == 159.0
        assert result["volume"] == 80000

    def test_case_insensitive(self, db_with_data):
        """Should handle lowercase symbol input."""
        result = get_latest_close(db_with_data, "hbl")

        assert result is not None
        assert result["symbol"] == "HBL"

    def test_returns_none_for_unknown(self, db_with_data):
        """Should return None for unknown symbol."""
        result = get_latest_close(db_with_data, "UNKNOWN")
        assert result is None

    def test_returns_all_fields(self, db_with_data):
        """Should return all OHLCV fields."""
        result = get_latest_close(db_with_data, "HBL")

        assert "symbol" in result
        assert "date" in result
        assert "open" in result
        assert "high" in result
        assert "low" in result
        assert "close" in result
        assert "volume" in result


class TestGetOhlcvRange:
    """Tests for get_ohlcv_range."""

    def test_returns_all_data_no_filter(self, db_with_data):
        """Should return all data when no date filters."""
        df = get_ohlcv_range(db_with_data, "HBL")

        assert len(df) == 3
        assert list(df.columns) == [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

    def test_sorted_by_date(self, db_with_data):
        """Should return data sorted by date ascending."""
        df = get_ohlcv_range(db_with_data, "HBL")

        dates = df["date"].tolist()
        assert dates == ["2024-01-15", "2024-01-16", "2024-01-17"]

    def test_filters_by_start_date(self, db_with_data):
        """Should filter by start date."""
        df = get_ohlcv_range(db_with_data, "HBL", start_date="2024-01-16")

        assert len(df) == 2
        assert df["date"].iloc[0] == "2024-01-16"

    def test_filters_by_end_date(self, db_with_data):
        """Should filter by end date."""
        df = get_ohlcv_range(db_with_data, "HBL", end_date="2024-01-16")

        assert len(df) == 2
        assert df["date"].iloc[-1] == "2024-01-16"

    def test_filters_by_both_dates(self, db_with_data):
        """Should filter by both start and end date."""
        df = get_ohlcv_range(
            db_with_data, "HBL", start_date="2024-01-16", end_date="2024-01-16"
        )

        assert len(df) == 1
        assert df["date"].iloc[0] == "2024-01-16"

    def test_case_insensitive(self, db_with_data):
        """Should handle lowercase symbol input."""
        df = get_ohlcv_range(db_with_data, "hbl")

        assert len(df) == 3
        assert df["symbol"].iloc[0] == "HBL"

    def test_returns_empty_for_unknown(self, db_with_data):
        """Should return empty DataFrame for unknown symbol."""
        df = get_ohlcv_range(db_with_data, "UNKNOWN")
        assert df.empty

    def test_returns_empty_for_out_of_range(self, db_with_data):
        """Should return empty DataFrame for dates with no data."""
        df = get_ohlcv_range(
            db_with_data, "HBL", start_date="2025-01-01", end_date="2025-12-31"
        )
        assert df.empty


# =============================================================================
# History Query Helpers Tests
# =============================================================================


@pytest.fixture
def db_with_history(db):
    """Create database with history data in analytics tables."""
    # Initialize all required schemas
    init_regular_market_schema(db)
    init_analytics_schema(db)

    # Insert market analytics snapshots
    db.execute(
        """
        INSERT INTO analytics_market_snapshot (
            ts, gainers_count, losers_count, unchanged_count,
            total_symbols, total_volume, top_gainer_symbol, top_loser_symbol
        ) VALUES
        ('2024-01-15T10:00:00', 100, 50, 30, 180, 1000000, 'HBL', 'UBL'),
        ('2024-01-15T11:00:00', 110, 45, 25, 180, 1200000, 'MCB', 'ABL'),
        ('2024-01-15T12:00:00', 120, 40, 20, 180, 1500000, 'OGDC', 'PPL')
        """
    )

    # Insert regular market snapshots
    db.execute(
        """
        INSERT INTO regular_market_snapshots (
            symbol, ts, status, sector_code, listed_in,
            ldcp, open, high, low, current, change, change_pct, volume, row_hash
        ) VALUES
        ('HBL', '2024-01-15T10:00:00', 'OPEN', '0807', 'KSE100',
         150.0, 151.0, 155.0, 149.0, 153.0, 3.0, 2.0, 50000, 'hash1'),
        ('HBL', '2024-01-15T11:00:00', 'OPEN', '0807', 'KSE100',
         150.0, 151.0, 156.0, 150.0, 155.0, 5.0, 3.33, 60000, 'hash2'),
        ('HBL', '2024-01-15T12:00:00', 'OPEN', '0807', 'KSE100',
         150.0, 151.0, 158.0, 151.0, 157.0, 7.0, 4.67, 70000, 'hash3'),
        ('UBL', '2024-01-15T10:00:00', 'OPEN', '0807', 'KSE100',
         200.0, 201.0, 205.0, 199.0, 203.0, 3.0, 1.5, 30000, 'hash4'),
        ('UBL', '2024-01-15T11:00:00', 'OPEN', '0807', 'KSE100',
         200.0, 201.0, 204.0, 200.0, 202.0, 2.0, 1.0, 35000, 'hash5')
        """
    )

    # Insert sector analytics snapshots
    db.execute(
        """
        INSERT INTO analytics_sector_snapshot (
            ts, sector_code, sector_name, symbols_count,
            avg_change_pct, sum_volume, top_symbol
        ) VALUES
        ('2024-01-15T10:00:00', '0807', 'COMMERCIAL BANKS', 10, 1.5, 500000, 'HBL'),
        ('2024-01-15T11:00:00', '0807', 'COMMERCIAL BANKS', 10, 2.0, 600000, 'MCB'),
        ('2024-01-15T12:00:00', '0807', 'COMMERCIAL BANKS', 10, 2.5, 700000, 'ABL'),
        ('2024-01-15T10:00:00', '0830', 'TEXTILE SPINNING', 20, -0.5, 300000, 'NML'),
        ('2024-01-15T11:00:00', '0830', 'TEXTILE SPINNING', 20, -0.3, 350000, 'NCL')
        """
    )

    db.commit()
    return db


class TestGetTimeRangeBounds:
    """Tests for get_time_range_bounds."""

    def test_all_returns_none(self):
        """Should return (None, None) for 'all' range."""
        start, end = get_time_range_bounds("all")
        assert start is None
        assert end is None

    def test_last_1h_returns_timestamps(self):
        """Should return valid timestamps for last_1h."""
        start, end = get_time_range_bounds("last_1h")
        assert start is not None
        assert end is not None
        assert start < end

    def test_today_starts_at_midnight(self):
        """Should return start time at midnight for today."""
        start, end = get_time_range_bounds("today")
        assert start is not None
        assert "T00:00:00" in start

    def test_invalid_key_returns_none(self):
        """Should return (None, None) for unknown range key."""
        start, end = get_time_range_bounds("invalid")
        assert start is None
        assert end is None


class TestGetMarketHistory:
    """Tests for get_market_history."""

    def test_returns_all_snapshots(self, db_with_history):
        """Should return all market analytics snapshots."""
        df = get_market_history(db_with_history)
        assert len(df) == 3

    def test_sorted_by_ts_ascending(self, db_with_history):
        """Should return data sorted by timestamp ascending."""
        df = get_market_history(db_with_history)
        timestamps = df["ts"].tolist()
        assert timestamps == sorted(timestamps)

    def test_filters_by_start_ts(self, db_with_history):
        """Should filter by start timestamp."""
        df = get_market_history(
            db_with_history, start_ts="2024-01-15T11:00:00"
        )
        assert len(df) == 2
        assert df["ts"].iloc[0] == "2024-01-15T11:00:00"

    def test_filters_by_end_ts(self, db_with_history):
        """Should filter by end timestamp."""
        df = get_market_history(
            db_with_history, end_ts="2024-01-15T11:00:00"
        )
        assert len(df) == 2
        assert df["ts"].iloc[-1] == "2024-01-15T11:00:00"

    def test_respects_limit(self, db_with_history):
        """Should respect limit parameter."""
        df = get_market_history(db_with_history, limit=2)
        assert len(df) == 2

    def test_contains_expected_columns(self, db_with_history):
        """Should contain all expected columns."""
        df = get_market_history(db_with_history)
        expected_cols = [
            "ts", "gainers_count", "losers_count", "unchanged_count",
            "total_symbols", "total_volume", "top_gainer_symbol", "top_loser_symbol"
        ]
        for col in expected_cols:
            assert col in df.columns


class TestGetMarketHistoryStats:
    """Tests for get_market_history_stats."""

    def test_returns_stats(self, db_with_history):
        """Should return statistics about market history."""
        stats = get_market_history_stats(db_with_history)
        assert stats["snapshot_count"] == 3
        assert stats["min_ts"] == "2024-01-15T10:00:00"
        assert stats["max_ts"] == "2024-01-15T12:00:00"

    def test_returns_zeros_for_empty_db(self, db):
        """Should return zeros for empty database."""
        init_analytics_schema(db)
        stats = get_market_history_stats(db)
        assert stats["snapshot_count"] == 0
        assert stats["min_ts"] is None
        assert stats["max_ts"] is None


class TestGetSymbolSnapshotHistory:
    """Tests for get_symbol_snapshot_history."""

    def test_returns_symbol_snapshots(self, db_with_history):
        """Should return snapshots for specified symbol."""
        df = get_symbol_snapshot_history(db_with_history, "HBL")
        assert len(df) == 3
        assert all(df["symbol"] == "HBL")

    def test_sorted_by_ts_ascending(self, db_with_history):
        """Should return data sorted by timestamp ascending."""
        df = get_symbol_snapshot_history(db_with_history, "HBL")
        timestamps = df["ts"].tolist()
        assert timestamps == sorted(timestamps)

    def test_case_insensitive(self, db_with_history):
        """Should handle lowercase symbol input."""
        df = get_symbol_snapshot_history(db_with_history, "hbl")
        assert len(df) == 3
        assert all(df["symbol"] == "HBL")

    def test_filters_by_time_range(self, db_with_history):
        """Should filter by time range."""
        df = get_symbol_snapshot_history(
            db_with_history, "HBL",
            start_ts="2024-01-15T11:00:00",
            end_ts="2024-01-15T11:00:00"
        )
        assert len(df) == 1
        assert df["ts"].iloc[0] == "2024-01-15T11:00:00"

    def test_returns_empty_for_unknown_symbol(self, db_with_history):
        """Should return empty DataFrame for unknown symbol."""
        df = get_symbol_snapshot_history(db_with_history, "UNKNOWN")
        assert df.empty


class TestGetSymbolHistoryStats:
    """Tests for get_symbol_history_stats."""

    def test_returns_stats(self, db_with_history):
        """Should return statistics for symbol history."""
        stats = get_symbol_history_stats(db_with_history, "HBL")
        assert stats["snapshot_count"] == 3
        assert stats["min_ts"] == "2024-01-15T10:00:00"
        assert stats["max_ts"] == "2024-01-15T12:00:00"

    def test_returns_zeros_for_unknown_symbol(self, db_with_history):
        """Should return zeros for unknown symbol."""
        stats = get_symbol_history_stats(db_with_history, "UNKNOWN")
        assert stats["snapshot_count"] == 0
        assert stats["min_ts"] is None


class TestGetSectorHistory:
    """Tests for get_sector_history."""

    def test_returns_sector_snapshots(self, db_with_history):
        """Should return snapshots for specified sector."""
        df = get_sector_history(db_with_history, "0807")
        assert len(df) == 3
        assert all(df["sector_code"] == "0807")

    def test_sorted_by_ts_ascending(self, db_with_history):
        """Should return data sorted by timestamp ascending."""
        df = get_sector_history(db_with_history, "0807")
        timestamps = df["ts"].tolist()
        assert timestamps == sorted(timestamps)

    def test_filters_by_time_range(self, db_with_history):
        """Should filter by time range."""
        df = get_sector_history(
            db_with_history, "0807",
            start_ts="2024-01-15T11:00:00",
            end_ts="2024-01-15T12:00:00"
        )
        assert len(df) == 2

    def test_returns_empty_for_unknown_sector(self, db_with_history):
        """Should return empty DataFrame for unknown sector."""
        df = get_sector_history(db_with_history, "9999")
        assert df.empty


class TestGetSectorListFromAnalytics:
    """Tests for get_sector_list_from_analytics."""

    def test_returns_unique_sectors(self, db_with_history):
        """Should return unique sectors from analytics."""
        sectors = get_sector_list_from_analytics(db_with_history)
        assert len(sectors) == 2
        sector_codes = [s["sector_code"] for s in sectors]
        assert "0807" in sector_codes
        assert "0830" in sector_codes

    def test_sorted_by_sector_name(self, db_with_history):
        """Should return sectors sorted by name."""
        sectors = get_sector_list_from_analytics(db_with_history)
        names = [s["sector_name"] for s in sectors]
        assert names == sorted(names)

    def test_returns_empty_for_no_data(self, db):
        """Should return empty list when no analytics data."""
        init_analytics_schema(db)
        sectors = get_sector_list_from_analytics(db)
        assert sectors == []
