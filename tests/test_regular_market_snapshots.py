"""Tests for regular market snapshot insertion logic.

These tests verify that:
1. First run inserts snapshots for all symbols (prev_hashes empty)
2. Second run with identical data inserts 0 snapshots
3. Second run with one changed symbol inserts 1 snapshot
"""

import sqlite3

import pandas as pd
import pytest

from psx_ohlcv.sources.regular_market import (
    compute_row_hash,
    get_all_current_hashes,
    init_regular_market_schema,
    insert_snapshots,
    upsert_current,
)


@pytest.fixture
def con() -> sqlite3.Connection:
    """Create in-memory database with regular market schema."""
    conn = sqlite3.connect(":memory:")
    init_regular_market_schema(conn)
    return conn


def make_market_df(data: list[dict]) -> pd.DataFrame:
    """Create a market DataFrame with required columns and computed hashes."""
    if not data:
        cols = ["symbol", "ts", "current", "volume", "change_pct", "row_hash"]
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(data)
    # Compute row_hash for each row (compute_row_hash takes a dict)
    df["row_hash"] = df.apply(
        lambda row: compute_row_hash(row.to_dict()),
        axis=1,
    )
    return df


class TestFirstRunInsertsAllSnapshots:
    """Test that first run inserts snapshots for all symbols."""

    def test_first_run_inserts_all_symbols(self, con):
        """First run with empty DB should insert snapshots for all symbols."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:00:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
            {"symbol": "PSO", "ts": "2025-01-15 10:00:00", "current": 75.0,
             "volume": 750, "change_pct": 0.0},
        ])

        # Load previous hashes (empty on first run)
        prev_hashes = get_all_current_hashes(con)
        assert prev_hashes == {}

        # Insert snapshots with empty prev_hashes
        inserted = insert_snapshots(
            con, df, save_unchanged=False, prev_hashes=prev_hashes
        )

        # All 3 symbols should be inserted
        assert inserted == 3

        # Verify snapshots in DB
        cur = con.execute("SELECT COUNT(*) FROM regular_market_snapshots")
        assert cur.fetchone()[0] == 3

    def test_first_run_with_zero_symbols(self, con):
        """First run with empty DataFrame should insert 0 snapshots."""
        df = make_market_df([])
        prev_hashes = get_all_current_hashes(con)

        inserted = insert_snapshots(
            con, df, save_unchanged=False, prev_hashes=prev_hashes
        )
        assert inserted == 0


class TestSecondRunWithIdenticalData:
    """Test that second run with identical data inserts 0 snapshots."""

    def test_identical_data_inserts_zero(self, con):
        """Second run with identical data should insert 0 snapshots."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:00:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
        ])

        # First run: load hashes, insert snapshots, upsert current
        prev_hashes_1 = get_all_current_hashes(con)
        inserted_1 = insert_snapshots(
            con, df, save_unchanged=False, prev_hashes=prev_hashes_1
        )
        upsert_current(con, df)

        assert inserted_1 == 2  # First run inserts all

        # Second run with SAME data but new timestamp
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:05:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
        ])

        # Load hashes BEFORE upsert (this is the key fix)
        prev_hashes_2 = get_all_current_hashes(con)
        assert len(prev_hashes_2) == 2  # Should have 2 symbols

        inserted_2 = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=prev_hashes_2
        )

        # Second run with identical prices should insert 0
        assert inserted_2 == 0

    def test_identical_data_with_save_unchanged(self, con):
        """With save_unchanged=True, all rows should be inserted."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # First run
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run with save_unchanged=True
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        prev_hashes_2 = get_all_current_hashes(con)
        inserted = insert_snapshots(
            con, df2, save_unchanged=True, prev_hashes=prev_hashes_2
        )

        # With save_unchanged=True, should still insert
        assert inserted == 1


class TestSecondRunWithChangedSymbol:
    """Test that second run with one changed symbol inserts 1 snapshot."""

    def test_one_changed_symbol_inserts_one(self, con):
        """Second run with one changed symbol should insert 1 snapshot."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:00:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
            {"symbol": "PSO", "ts": "2025-01-15 10:00:00", "current": 75.0,
             "volume": 750, "change_pct": 0.0},
        ])

        # First run
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run: only OGDC changed (price changed to 101.0)
        df2 = make_market_df([
            # Price changed from 100.0 to 101.0
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 101.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:05:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
            {"symbol": "PSO", "ts": "2025-01-15 10:05:00", "current": 75.0,
             "volume": 750, "change_pct": 0.0},
        ])

        prev_hashes_2 = get_all_current_hashes(con)
        inserted = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=prev_hashes_2
        )

        # Only 1 symbol changed
        assert inserted == 1

        # Verify it was OGDC that was inserted
        cur = con.execute(
            "SELECT symbol FROM regular_market_snapshots "
            "WHERE ts = '2025-01-15 10:05:00'"
        )
        symbols = [row[0] for row in cur.fetchall()]
        assert symbols == ["OGDC"]

    def test_volume_change_triggers_snapshot(self, con):
        """Change in volume should trigger a new snapshot."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # First run
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run: volume changed from 1000 to 2000
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 100.0,
             "volume": 2000, "change_pct": 1.5},
        ])

        prev_hashes_2 = get_all_current_hashes(con)
        inserted = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=prev_hashes_2
        )

        assert inserted == 1

    def test_new_symbol_triggers_snapshot(self, con):
        """A new symbol should trigger a snapshot."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # First run
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run: new symbol PPL added, OGDC unchanged
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
            {"symbol": "PPL", "ts": "2025-01-15 10:05:00", "current": 50.0,
             "volume": 500, "change_pct": -0.5},
        ])

        prev_hashes_2 = get_all_current_hashes(con)
        inserted = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=prev_hashes_2
        )

        # Only PPL is new, OGDC is unchanged
        assert inserted == 1

        # Verify it was PPL that was inserted
        cur = con.execute(
            "SELECT symbol FROM regular_market_snapshots "
            "WHERE ts = '2025-01-15 10:05:00'"
        )
        symbols = [row[0] for row in cur.fetchall()]
        assert symbols == ["PPL"]


class TestHashComparisonOrder:
    """Test that hash comparison uses pre-loaded hashes, not post-upsert."""

    def test_wrong_order_would_fail(self, con):
        """Demonstrate that upserting BEFORE loading hashes gives wrong result.

        This test documents the bug that existed before the fix.
        If we upsert first, then load hashes, they're already updated and
        we'd incorrectly detect 0 changes.
        """
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # First run: set up initial state
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run with CHANGED data (price 100.0 -> 101.0)
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 101.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # WRONG ORDER: Upsert first, then load hashes (the old buggy behavior)
        upsert_current(con, df2)  # This updates the hash!
        post_upsert_hashes = get_all_current_hashes(con)  # Now hash matches!

        # With wrong order, insert_snapshots would see matching hashes
        # and incorrectly insert 0 rows
        inserted_wrong = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=post_upsert_hashes
        )

        # Bug: this would be 0 because hashes now match (already updated)
        assert inserted_wrong == 0  # Documents the bug

    def test_correct_order_works(self, con):
        """Verify that loading hashes BEFORE upsert gives correct result."""
        df = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:00:00", "current": 100.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # First run
        prev_hashes_1 = get_all_current_hashes(con)
        insert_snapshots(con, df, save_unchanged=False, prev_hashes=prev_hashes_1)
        upsert_current(con, df)

        # Second run with CHANGED data (price 100.0 -> 101.0)
        df2 = make_market_df([
            {"symbol": "OGDC", "ts": "2025-01-15 10:05:00", "current": 101.0,
             "volume": 1000, "change_pct": 1.5},
        ])

        # CORRECT ORDER: Load hashes BEFORE upsert
        prev_hashes_2 = get_all_current_hashes(con)  # Get old hash (100.0)

        # Insert snapshots with pre-loaded hashes
        inserted = insert_snapshots(
            con, df2, save_unchanged=False, prev_hashes=prev_hashes_2
        )

        # Correct: detects the change and inserts 1 row
        assert inserted == 1

        # Then upsert (order matters!)
        upsert_current(con, df2)
