"""Tests for regular market hash-based change detection (smart saving)."""

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema
from psx_ohlcv.sources.regular_market import (
    _compute_row_hash,
    get_current_hash,
    init_regular_market_schema,
    insert_snapshots,
    upsert_current,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    init_regular_market_schema(con)
    yield con
    con.close()


def make_row_df(
    symbol="HBL",
    ts="2024-01-15T10:30:00+05:00",
    current=100.0,
    volume=1000,
    status=None,
):
    """Create a single-row DataFrame for testing."""
    row = {
        "ts": ts,
        "symbol": symbol,
        "status": status,
        "sector_code": "TEST",
        "listed_in": "KSE100",
        "ldcp": current,
        "open": current,
        "high": current + 1,
        "low": current - 1,
        "current": current + 0.5,
        "change": 0.5,
        "change_pct": 0.5,
        "volume": volume,
    }
    # Compute hash
    row["row_hash"] = _compute_row_hash(row)
    return pd.DataFrame([row])


class TestHashSkipLogic:
    """Tests for hash-based change detection in insert_snapshots."""

    def test_unchanged_row_skipped(self, db):
        """Row with same hash should be skipped (save_unchanged=False)."""
        # Insert initial data into current table
        df1 = make_row_df(ts="2024-01-15T10:00:00+05:00", current=100.0)
        upsert_current(db, df1)

        # Same data (same hash) with different timestamp
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", current=100.0)

        # Hashes should match since data is identical
        assert df1["row_hash"].iloc[0] == df2["row_hash"].iloc[0]

        # Insert with save_unchanged=False
        count = insert_snapshots(db, df2, save_unchanged=False)

        # Should skip since hash matches current
        assert count == 0

    def test_changed_row_saved(self, db):
        """Row with different hash should be saved."""
        # Insert initial data
        df1 = make_row_df(ts="2024-01-15T10:00:00+05:00", current=100.0)
        upsert_current(db, df1)

        # Changed price (different hash)
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", current=101.0)

        # Hashes should differ
        assert df1["row_hash"].iloc[0] != df2["row_hash"].iloc[0]

        # Insert with save_unchanged=False
        count = insert_snapshots(db, df2, save_unchanged=False)

        # Should save since hash changed
        assert count == 1

    def test_save_unchanged_true_always_saves(self, db):
        """With save_unchanged=True, all rows are saved."""
        # Insert initial data
        df1 = make_row_df(ts="2024-01-15T10:00:00+05:00", current=100.0)
        upsert_current(db, df1)

        # Same data (same hash)
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", current=100.0)

        # Insert with save_unchanged=True
        count = insert_snapshots(db, df2, save_unchanged=True)

        # Should save even though hash matches
        assert count == 1

    def test_new_symbol_always_saved(self, db):
        """New symbol (no current hash) should always be saved."""
        df = make_row_df(symbol="NEWSTOCK")

        # No existing record for NEWSTOCK
        assert get_current_hash(db, "NEWSTOCK") is None

        count = insert_snapshots(db, df, save_unchanged=False)

        # Should save since no current hash exists
        assert count == 1

    def test_volume_change_detected(self, db):
        """Change in volume should be detected."""
        df1 = make_row_df(current=100.0, volume=1000)
        upsert_current(db, df1)

        # Only volume changed
        df2 = make_row_df(
            ts="2024-01-15T10:01:00+05:00",
            current=100.0,
            volume=2000
        )

        assert df1["row_hash"].iloc[0] != df2["row_hash"].iloc[0]

        count = insert_snapshots(db, df2, save_unchanged=False)
        assert count == 1

    def test_status_change_detected(self, db):
        """Change in status marker should be detected."""
        df1 = make_row_df(status=None)
        upsert_current(db, df1)

        # Status changed to NC
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", status="NC")

        assert df1["row_hash"].iloc[0] != df2["row_hash"].iloc[0]

        count = insert_snapshots(db, df2, save_unchanged=False)
        assert count == 1

    def test_multiple_symbols_mixed(self, db):
        """Mix of changed and unchanged symbols."""
        # Insert initial data for two symbols
        df1 = pd.concat([
            make_row_df(symbol="HBL", current=100.0),
            make_row_df(symbol="OGDC", current=200.0),
        ], ignore_index=True)
        upsert_current(db, df1)

        # Update: HBL unchanged, OGDC changed
        df2 = pd.concat([
            make_row_df(symbol="HBL", ts="2024-01-15T10:01:00+05:00", current=100.0),
            make_row_df(symbol="OGDC", ts="2024-01-15T10:01:00+05:00", current=205.0),
        ], ignore_index=True)

        count = insert_snapshots(db, df2, save_unchanged=False)

        # Only OGDC should be saved
        assert count == 1

        # Verify only OGDC snapshot exists
        cur = db.execute("SELECT symbol FROM regular_market_snapshots")
        symbols = [row[0] for row in cur.fetchall()]
        assert "OGDC" in symbols
        assert "HBL" not in symbols


class TestHashComputation:
    """Tests for hash computation consistency."""

    def test_hash_excludes_timestamp(self):
        """Timestamp should not affect hash (same data = same hash)."""
        row1 = {
            "symbol": "HBL", "status": None, "sector_code": "BANK",
            "listed_in": "KSE100", "ldcp": 150.0, "open": 150.0,
            "high": 152.0, "low": 149.0, "current": 151.0,
            "change": 1.0, "change_pct": 0.67, "volume": 1000000
        }
        row2 = row1.copy()  # Same data

        hash1 = _compute_row_hash(row1)
        hash2 = _compute_row_hash(row2)

        assert hash1 == hash2

    def test_hash_stable_field_order(self):
        """Hash should be consistent regardless of dict key order."""
        row1 = {
            "symbol": "HBL", "current": 100.0, "volume": 1000,
            "ldcp": 99.0, "open": 99.5
        }
        row2 = {
            "volume": 1000, "symbol": "HBL", "ldcp": 99.0,
            "current": 100.0, "open": 99.5
        }

        hash1 = _compute_row_hash(row1)
        hash2 = _compute_row_hash(row2)

        assert hash1 == hash2

    def test_hash_sensitive_to_small_changes(self):
        """Small value changes should produce different hashes."""
        row1 = {"symbol": "HBL", "current": 100.00}
        row2 = {"symbol": "HBL", "current": 100.01}

        hash1 = _compute_row_hash(row1)
        hash2 = _compute_row_hash(row2)

        assert hash1 != hash2

    def test_hash_handles_missing_fields(self):
        """Missing optional fields should produce valid hash."""
        row = {"symbol": "HBL"}  # Minimal row
        hash_val = _compute_row_hash(row)

        assert hash_val is not None
        assert len(hash_val) == 64


class TestWorkflowScenarios:
    """Test realistic workflow scenarios."""

    def test_market_update_sequence(self, db):
        """Simulate a sequence of market updates.

        Correct workflow: insert_snapshots BEFORE upsert_current
        to detect changes against the PREVIOUS state.
        """
        # 10:00 - Initial state (first time, save all)
        df_1000 = pd.concat([
            make_row_df(symbol="HBL", ts="2024-01-15T10:00:00+05:00", current=150.0),
            make_row_df(symbol="OGDC", ts="2024-01-15T10:00:00+05:00", current=100.0),
        ], ignore_index=True)
        # First time: no current hash exists, so all get saved
        count_1000 = insert_snapshots(db, df_1000, save_unchanged=False)
        assert count_1000 == 2  # All new
        upsert_current(db, df_1000)  # Update baseline

        # 10:01 - No changes
        df_1001 = pd.concat([
            make_row_df(symbol="HBL", ts="2024-01-15T10:01:00+05:00", current=150.0),
            make_row_df(symbol="OGDC", ts="2024-01-15T10:01:00+05:00", current=100.0),
        ], ignore_index=True)
        count_1001 = insert_snapshots(db, df_1001, save_unchanged=False)
        assert count_1001 == 0  # No changes - hashes match current
        upsert_current(db, df_1001)

        # 10:02 - HBL changed
        df_1002 = pd.concat([
            make_row_df(symbol="HBL", ts="2024-01-15T10:02:00+05:00", current=151.0),
            make_row_df(symbol="OGDC", ts="2024-01-15T10:02:00+05:00", current=100.0),
        ], ignore_index=True)
        count_1002 = insert_snapshots(db, df_1002, save_unchanged=False)
        assert count_1002 == 1  # Only HBL changed
        upsert_current(db, df_1002)

        # 10:03 - Both changed
        df_1003 = pd.concat([
            make_row_df(symbol="HBL", ts="2024-01-15T10:03:00+05:00", current=152.0),
            make_row_df(symbol="OGDC", ts="2024-01-15T10:03:00+05:00", current=101.0),
        ], ignore_index=True)
        count_1003 = insert_snapshots(db, df_1003, save_unchanged=False)
        assert count_1003 == 2  # Both changed
        upsert_current(db, df_1003)

        # Verify total snapshots
        cur = db.execute("SELECT COUNT(*) FROM regular_market_snapshots")
        total = cur.fetchone()[0]
        # Initial: 2, 10:01: 0, 10:02: 1, 10:03: 2 = 5
        assert total == 5

    def test_price_bounce_detection(self, db):
        """Detect price returning to previous value.

        Correct workflow: insert_snapshots BEFORE upsert_current.
        """
        # Price at 100
        df1 = make_row_df(ts="2024-01-15T10:00:00+05:00", current=100.0)
        insert_snapshots(db, df1, save_unchanged=False)  # New symbol, gets saved
        upsert_current(db, df1)

        # Price rises to 101
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", current=101.0)
        count2 = insert_snapshots(db, df2, save_unchanged=False)
        assert count2 == 1  # Different from current (100)
        upsert_current(db, df2)

        # Price returns to 100 - should still be detected as change
        df3 = make_row_df(ts="2024-01-15T10:02:00+05:00", current=100.0)
        count3 = insert_snapshots(db, df3, save_unchanged=False)
        assert count3 == 1  # Different from current (101)
        upsert_current(db, df3)

    def test_status_toggle(self, db):
        """Detect status marker toggling.

        Correct workflow: insert_snapshots BEFORE upsert_current.
        """
        # No status
        df1 = make_row_df(ts="2024-01-15T10:00:00+05:00", status=None)
        insert_snapshots(db, df1, save_unchanged=False)  # New symbol
        upsert_current(db, df1)

        # NC status added
        df2 = make_row_df(ts="2024-01-15T10:01:00+05:00", status="NC")
        count2 = insert_snapshots(db, df2, save_unchanged=False)
        assert count2 == 1  # Status changed
        upsert_current(db, df2)

        # NC status removed
        df3 = make_row_df(ts="2024-01-15T10:02:00+05:00", status=None)
        count3 = insert_snapshots(db, df3, save_unchanged=False)
        assert count3 == 1  # Status changed back
        upsert_current(db, df3)
