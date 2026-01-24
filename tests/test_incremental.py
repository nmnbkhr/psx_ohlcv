"""Tests for incremental sync filtering logic."""

import pandas as pd
import pytest

from psx_ohlcv.db import connect, get_max_date_for_symbol, init_schema, upsert_eod
from psx_ohlcv.sources.eod import filter_incremental


class TestFilterIncremental:
    """Tests for filter_incremental function."""

    def test_filters_rows_newer_than_max_date(self):
        """Should only keep rows with date > max_date."""
        dates = [
            "2024-01-10", "2024-01-11", "2024-01-12", "2024-01-13", "2024-01-14"
        ]
        df = pd.DataFrame({
            "symbol": ["HBL"] * 5,
            "date": dates,
            "open": [100.0] * 5,
            "high": [105.0] * 5,
            "low": [98.0] * 5,
            "close": [103.0] * 5,
            "volume": [1000] * 5,
        })

        result = filter_incremental(df, "2024-01-12")

        assert len(result) == 2
        assert list(result["date"]) == ["2024-01-13", "2024-01-14"]

    def test_returns_all_rows_when_max_date_none(self):
        """Should return all rows when max_date is None."""
        df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0] * 3,
            "high": [105.0] * 3,
            "low": [98.0] * 3,
            "close": [103.0] * 3,
            "volume": [1000] * 3,
        })

        result = filter_incremental(df, None)

        assert len(result) == 3

    def test_returns_empty_when_all_older(self):
        """Should return empty DataFrame when all rows are older than max_date."""
        df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0] * 3,
            "high": [105.0] * 3,
            "low": [98.0] * 3,
            "close": [103.0] * 3,
            "volume": [1000] * 3,
        })

        result = filter_incremental(df, "2024-01-15")

        assert len(result) == 0

    def test_excludes_rows_equal_to_max_date(self):
        """Should exclude rows where date == max_date (strictly greater than)."""
        df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0] * 3,
            "high": [105.0] * 3,
            "low": [98.0] * 3,
            "close": [103.0] * 3,
            "volume": [1000] * 3,
        })

        result = filter_incremental(df, "2024-01-11")

        assert len(result) == 1
        assert result["date"].iloc[0] == "2024-01-12"

    def test_handles_empty_dataframe(self):
        """Should return empty DataFrame for empty input."""
        cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(columns=cols)

        result = filter_incremental(df, "2024-01-12")

        assert result.empty

    def test_resets_index(self):
        """Should reset index after filtering."""
        dates = [
            "2024-01-10", "2024-01-11", "2024-01-12", "2024-01-13", "2024-01-14"
        ]
        df = pd.DataFrame({
            "symbol": ["HBL"] * 5,
            "date": dates,
            "open": [100.0] * 5,
            "high": [105.0] * 5,
            "low": [98.0] * 5,
            "close": [103.0] * 5,
            "volume": [1000] * 5,
        })

        result = filter_incremental(df, "2024-01-12")

        assert list(result.index) == [0, 1]


class TestGetMaxDateForSymbol:
    """Tests for get_max_date_for_symbol DB function."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database connection."""
        con = connect(":memory:")
        init_schema(con)
        yield con
        con.close()

    def test_returns_max_date_when_data_exists(self, db_conn):
        """Should return the maximum date for a symbol."""
        df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0] * 3,
            "high": [105.0] * 3,
            "low": [98.0] * 3,
            "close": [103.0] * 3,
            "volume": [1000] * 3,
        })
        upsert_eod(db_conn, df)

        result = get_max_date_for_symbol(db_conn, "HBL")

        assert result == "2024-01-12"

    def test_returns_none_when_no_data(self, db_conn):
        """Should return None when no data exists for symbol."""
        result = get_max_date_for_symbol(db_conn, "NONEXISTENT")

        assert result is None

    def test_handles_multiple_symbols(self, db_conn):
        """Should return correct max date per symbol."""
        dates = [
            "2024-01-10", "2024-01-15", "2024-01-08", "2024-01-12", "2024-01-20"
        ]
        df = pd.DataFrame({
            "symbol": ["HBL", "HBL", "UBL", "UBL", "UBL"],
            "date": dates,
            "open": [100.0] * 5,
            "high": [105.0] * 5,
            "low": [98.0] * 5,
            "close": [103.0] * 5,
            "volume": [1000] * 5,
        })
        upsert_eod(db_conn, df)

        hbl_max = get_max_date_for_symbol(db_conn, "HBL")
        ubl_max = get_max_date_for_symbol(db_conn, "UBL")

        assert hbl_max == "2024-01-15"
        assert ubl_max == "2024-01-20"


class TestIncrementalSyncIntegration:
    """Integration tests for incremental sync logic."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database connection."""
        con = connect(":memory:")
        init_schema(con)
        yield con
        con.close()

    def test_incremental_sync_workflow(self, db_conn):
        """Test the full incremental sync workflow."""
        # Initial data load
        initial_df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [98.0, 99.0, 100.0],
            "close": [103.0, 104.0, 105.0],
            "volume": [1000, 1100, 1200],
        })
        upsert_eod(db_conn, initial_df)

        # New data arrives (some overlap, some new)
        new_df = pd.DataFrame({
            "symbol": ["HBL"] * 4,
            "date": ["2024-01-11", "2024-01-12", "2024-01-13", "2024-01-14"],
            "open": [101.0, 102.0, 103.0, 104.0],
            "high": [106.0, 107.0, 108.0, 109.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [104.0, 105.0, 106.0, 107.0],
            "volume": [1100, 1200, 1300, 1400],
        })

        # Get max date and filter
        max_date = get_max_date_for_symbol(db_conn, "HBL")
        assert max_date == "2024-01-12"

        filtered_df = filter_incremental(new_df, max_date)
        assert len(filtered_df) == 2  # Only 2024-01-13 and 2024-01-14

        # Upsert filtered data
        rows_upserted = upsert_eod(db_conn, filtered_df)
        assert rows_upserted == 2

        # Verify final state
        new_max_date = get_max_date_for_symbol(db_conn, "HBL")
        assert new_max_date == "2024-01-14"

        # Verify total rows
        cur = db_conn.execute("SELECT COUNT(*) FROM eod_ohlcv WHERE symbol = 'HBL'")
        total_rows = cur.fetchone()[0]
        assert total_rows == 5  # 3 original + 2 new

    def test_incremental_with_empty_table(self, db_conn):
        """Test incremental filter when table is empty for symbol."""
        new_df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [98.0, 99.0, 100.0],
            "close": [103.0, 104.0, 105.0],
            "volume": [1000, 1100, 1200],
        })

        # No existing data
        max_date = get_max_date_for_symbol(db_conn, "HBL")
        assert max_date is None

        # Filter should return all rows
        filtered_df = filter_incremental(new_df, max_date)
        assert len(filtered_df) == 3

    def test_incremental_no_new_data(self, db_conn):
        """Test incremental filter when all data is old."""
        # Initial data
        initial_df = pd.DataFrame({
            "symbol": ["HBL"] * 3,
            "date": ["2024-01-10", "2024-01-11", "2024-01-12"],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [98.0, 99.0, 100.0],
            "close": [103.0, 104.0, 105.0],
            "volume": [1000, 1100, 1200],
        })
        upsert_eod(db_conn, initial_df)

        # Same data arrives again
        max_date = get_max_date_for_symbol(db_conn, "HBL")
        filtered_df = filter_incremental(initial_df, max_date)

        assert len(filtered_df) == 0
