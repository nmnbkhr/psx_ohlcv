"""Tests for intraday database upsert operations."""

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema
from psx_ohlcv.db import (
    get_intraday_sync_state,
    update_intraday_sync_state,
    upsert_intraday,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


class TestInitSchemaIntraday:
    """Tests for intraday schema initialization."""

    def test_creates_intraday_tables(self, db):
        """Verify intraday tables are created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row["name"] for row in cur.fetchall()]

        assert "intraday_bars" in tables
        assert "intraday_sync_state" in tables


class TestUpsertIntraday:
    """Tests for upsert_intraday function."""

    def test_insert_new_records(self, db):
        """Insert new intraday records."""
        df = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:05:00",
                "open": 100.5,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1500,
            },
        ])

        count = upsert_intraday(db, df)

        assert count == 2
        cur = db.execute("SELECT COUNT(*) as cnt FROM intraday_bars")
        assert cur.fetchone()["cnt"] == 2

    def test_upsert_updates_existing(self, db):
        """Upserting should update existing record."""
        df1 = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ])
        upsert_intraday(db, df1)

        # Update with new close price
        df2 = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 102.0,  # Higher high
                "low": 99.0,
                "close": 101.0,  # Different close
                "volume": 1200,
            },
        ])
        upsert_intraday(db, df2)

        cur = db.execute(
            "SELECT close, volume, high FROM intraday_bars "
            "WHERE symbol='ABOT' AND ts='2024-01-15 10:00:00'"
        )
        row = cur.fetchone()
        assert row["close"] == 101.0
        assert row["volume"] == 1200
        assert row["high"] == 102.0

        # Should still be just one row
        cur = db.execute("SELECT COUNT(*) as cnt FROM intraday_bars")
        assert cur.fetchone()["cnt"] == 1

    def test_upsert_no_duplicates(self, db):
        """Upserting same records twice should not create duplicates."""
        df = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ])

        upsert_intraday(db, df)
        upsert_intraday(db, df)

        cur = db.execute("SELECT COUNT(*) as cnt FROM intraday_bars")
        assert cur.fetchone()["cnt"] == 1

    def test_empty_dataframe(self, db):
        """Empty DataFrame should return 0."""
        df = pd.DataFrame(
            columns=["symbol", "ts", "open", "high", "low", "close", "volume"]
        )
        count = upsert_intraday(db, df)
        assert count == 0

    def test_missing_symbol_column_raises(self, db):
        """Missing symbol column should raise ValueError."""
        df = pd.DataFrame([{"ts": "2024-01-15 10:00:00", "close": 100.0}])

        with pytest.raises(ValueError, match="missing columns"):
            upsert_intraday(db, df)

    def test_missing_ts_column_raises(self, db):
        """Missing ts column should raise ValueError."""
        df = pd.DataFrame([{"symbol": "ABOT", "close": 100.0}])

        with pytest.raises(ValueError, match="missing columns"):
            upsert_intraday(db, df)

    def test_allows_minimal_columns(self, db):
        """DataFrame with just symbol and ts should work."""
        df = pd.DataFrame([{"symbol": "ABOT", "ts": "2024-01-15 10:00:00"}])
        count = upsert_intraday(db, df)
        assert count == 1

    def test_multiple_symbols(self, db):
        """Test upserting data for multiple symbols."""
        df = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "symbol": "HBL",
                "ts": "2024-01-15 10:00:00",
                "open": 150.0,
                "high": 152.0,
                "low": 149.0,
                "close": 151.0,
                "volume": 2000,
            },
        ])

        count = upsert_intraday(db, df)

        assert count == 2
        cur = db.execute("SELECT COUNT(DISTINCT symbol) FROM intraday_bars")
        assert cur.fetchone()[0] == 2

    def test_symbol_case_insensitive(self, db):
        """Symbol should be stored as provided (uppercase expected)."""
        df = pd.DataFrame([
            {
                "symbol": "abot",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ])

        upsert_intraday(db, df)

        cur = db.execute("SELECT symbol FROM intraday_bars")
        assert cur.fetchone()["symbol"] == "abot"


class TestIntradaySyncState:
    """Tests for intraday sync state functions."""

    @staticmethod
    def _ts_to_epoch(ts_str):
        """Helper to convert timestamp string to local epoch."""
        from datetime import datetime
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())

    def test_get_nonexistent_state(self, db):
        """Getting state for nonexistent symbol should return (None, None)."""
        result = get_intraday_sync_state(db, "ABOT")
        assert result == (None, None)

    def test_update_and_get_state(self, db):
        """Update state and retrieve it."""
        ts = "2024-01-15 10:30:00"
        epoch = self._ts_to_epoch(ts)
        update_intraday_sync_state(db, "ABOT", ts, epoch)

        last_ts, last_ts_epoch = get_intraday_sync_state(db, "ABOT")
        assert last_ts == ts
        assert last_ts_epoch == epoch

    def test_update_state_multiple_times(self, db):
        """Updating state multiple times should replace."""
        ts1 = "2024-01-15 10:00:00"
        ts2 = "2024-01-15 11:00:00"
        ts3 = "2024-01-15 12:00:00"
        update_intraday_sync_state(db, "ABOT", ts1, self._ts_to_epoch(ts1))
        update_intraday_sync_state(db, "ABOT", ts2, self._ts_to_epoch(ts2))
        update_intraday_sync_state(db, "ABOT", ts3, self._ts_to_epoch(ts3))

        last_ts, last_ts_epoch = get_intraday_sync_state(db, "ABOT")
        assert last_ts == ts3
        assert last_ts_epoch == self._ts_to_epoch(ts3)

        # Should still be just one row
        cur = db.execute("SELECT COUNT(*) FROM intraday_sync_state")
        assert cur.fetchone()[0] == 1

    def test_state_per_symbol(self, db):
        """Each symbol should have its own state."""
        ts1 = "2024-01-15 10:00:00"
        ts2 = "2024-01-15 11:00:00"
        ts3 = "2024-01-15 12:00:00"
        update_intraday_sync_state(db, "ABOT", ts1, self._ts_to_epoch(ts1))
        update_intraday_sync_state(db, "HBL", ts2, self._ts_to_epoch(ts2))
        update_intraday_sync_state(db, "MCB", ts3, self._ts_to_epoch(ts3))

        last_ts1, _ = get_intraday_sync_state(db, "ABOT")
        last_ts2, _ = get_intraday_sync_state(db, "HBL")
        last_ts3, _ = get_intraday_sync_state(db, "MCB")

        assert last_ts1 == ts1
        assert last_ts2 == ts2
        assert last_ts3 == ts3

    def test_symbol_case_normalized(self, db):
        """Symbol should be normalized to uppercase."""
        ts = "2024-01-15 10:00:00"
        update_intraday_sync_state(db, "abot", ts, self._ts_to_epoch(ts))

        # Both uppercase and lowercase should find it (normalized)
        ts1, _ = get_intraday_sync_state(db, "ABOT")
        ts2, _ = get_intraday_sync_state(db, "abot")
        ts3, _ = get_intraday_sync_state(db, "Abot")

        assert ts1 == ts
        assert ts2 == ts
        assert ts3 == ts


class TestIntradayQueryHelpers:
    """Tests for intraday query helper functions from db.py."""

    def test_get_intraday_range(self, db):
        """Test querying intraday data with time range."""
        from psx_ohlcv.db import get_intraday_range

        # Insert test data
        df = pd.DataFrame([
            {"symbol": "ABOT", "ts": "2024-01-15 09:00:00", "open": 100, "high": 101,
             "low": 99, "close": 100, "volume": 1000},
            {"symbol": "ABOT", "ts": "2024-01-15 10:00:00", "open": 100, "high": 102,
             "low": 99, "close": 101, "volume": 1100},
            {"symbol": "ABOT", "ts": "2024-01-15 11:00:00", "open": 101, "high": 103,
             "low": 100, "close": 102, "volume": 1200},
            {"symbol": "ABOT", "ts": "2024-01-15 12:00:00", "open": 102, "high": 104,
             "low": 101, "close": 103, "volume": 1300},
        ])
        upsert_intraday(db, df)

        # Query with range
        result = get_intraday_range(
            db, "ABOT",
            start_ts="2024-01-15 10:00:00",
            end_ts="2024-01-15 11:00:00"
        )

        assert len(result) == 2
        assert result["ts"].iloc[0] == "2024-01-15 10:00:00"
        assert result["ts"].iloc[1] == "2024-01-15 11:00:00"

    def test_get_intraday_latest(self, db):
        """Test getting latest intraday bars."""
        from psx_ohlcv.db import get_intraday_latest

        # Insert test data
        df = pd.DataFrame([
            {"symbol": "ABOT", "ts": f"2024-01-15 {10+i}:00:00", "open": 100+i,
             "high": 101+i, "low": 99+i, "close": 100+i, "volume": 1000+i}
            for i in range(5)
        ])
        upsert_intraday(db, df)

        # Get latest 3
        result = get_intraday_latest(db, "ABOT", limit=3)

        assert len(result) == 3
        # Should be sorted ascending (oldest to newest)
        assert result["ts"].iloc[0] < result["ts"].iloc[-1]

    def test_get_intraday_stats(self, db):
        """Test getting intraday statistics."""
        from psx_ohlcv.db import get_intraday_stats

        # Insert test data
        df = pd.DataFrame([
            {"symbol": "ABOT", "ts": "2024-01-15 09:00:00", "open": 100, "high": 101,
             "low": 99, "close": 100, "volume": 1000},
            {"symbol": "ABOT", "ts": "2024-01-15 14:00:00", "open": 105, "high": 106,
             "low": 104, "close": 105, "volume": 1500},
        ])
        upsert_intraday(db, df)

        stats = get_intraday_stats(db, "ABOT")

        assert stats["row_count"] == 2
        assert stats["min_ts"] == "2024-01-15 09:00:00"
        assert stats["max_ts"] == "2024-01-15 14:00:00"

    def test_get_intraday_stats_empty(self, db):
        """Stats for nonexistent symbol should return zeros."""
        from psx_ohlcv.db import get_intraday_stats

        stats = get_intraday_stats(db, "NONEXISTENT")

        assert stats["row_count"] == 0
        assert stats["min_ts"] is None
        assert stats["max_ts"] is None
