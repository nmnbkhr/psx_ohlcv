"""Tests for regular market database operations."""

import pandas as pd
import pytest

from pakfindata import connect, init_schema
from pakfindata.sources.regular_market import (
    get_current_hash,
    get_current_market,
    get_snapshots,
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


def make_sample_df(
    symbols=None,
    ts="2024-01-15T10:30:00+05:00",
    current=100.0,
    volume=1000,
):
    """Create a sample DataFrame for testing."""
    if symbols is None:
        symbols = ["HBL", "OGDC"]

    rows = []
    for i, sym in enumerate(symbols):
        rows.append({
            "ts": ts,
            "symbol": sym,
            "status": None,
            "sector_code": "TEST",
            "listed_in": "KSE100",
            "ldcp": current + i,
            "open": current + i,
            "high": current + i + 1,
            "low": current + i - 1,
            "current": current + i + 0.5,
            "change": 0.5,
            "change_pct": 0.5,
            "volume": volume + i * 100,
            "row_hash": f"hash_{sym}_{ts}_{current + i}",
        })

    return pd.DataFrame(rows)


class TestInitRegularMarketSchema:
    """Tests for schema initialization."""

    def test_creates_current_table(self, db):
        """Verify regular_market_current table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='regular_market_current'"
        )
        assert cur.fetchone() is not None

    def test_creates_snapshots_table(self, db):
        """Verify regular_market_snapshots table is created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='regular_market_snapshots'"
        )
        assert cur.fetchone() is not None

    def test_creates_indexes(self, db):
        """Verify indexes are created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_rm_%'"
        )
        indexes = [row[0] for row in cur.fetchall()]

        assert "idx_rm_snapshots_symbol" in indexes
        assert "idx_rm_snapshots_ts" in indexes


class TestUpsertCurrent:
    """Tests for upsert_current function."""

    def test_insert_new_records(self, db):
        """Insert new records into current table."""
        df = make_sample_df()

        count = upsert_current(db, df)

        assert count == 2
        cur = db.execute("SELECT COUNT(*) FROM regular_market_current")
        assert cur.fetchone()[0] == 2

    def test_upsert_updates_existing(self, db):
        """Upserting should update existing records."""
        df1 = make_sample_df(symbols=["HBL"], current=100.0)
        upsert_current(db, df1)

        df2 = make_sample_df(symbols=["HBL"], current=150.0)
        upsert_current(db, df2)

        cur = db.execute(
            "SELECT current FROM regular_market_current WHERE symbol='HBL'"
        )
        assert cur.fetchone()[0] == 150.5  # current + 0.5

        # Should still be just one row
        cur = db.execute("SELECT COUNT(*) FROM regular_market_current")
        assert cur.fetchone()[0] == 1

    def test_empty_dataframe(self, db):
        """Empty DataFrame should return 0."""
        df = pd.DataFrame()
        count = upsert_current(db, df)
        assert count == 0

    def test_preserves_all_columns(self, db):
        """All columns should be stored correctly."""
        df = make_sample_df(symbols=["HBL"])
        upsert_current(db, df)

        cur = db.execute(
            "SELECT symbol, status, sector_code, listed_in, ldcp, open, high, "
            "low, current, change, change_pct, volume, row_hash "
            "FROM regular_market_current WHERE symbol='HBL'"
        )
        row = dict(cur.fetchone())

        assert row["symbol"] == "HBL"
        assert row["sector_code"] == "TEST"
        assert row["listed_in"] == "KSE100"
        assert row["ldcp"] == 100.0
        assert row["volume"] == 1000

    def test_updates_timestamp(self, db):
        """Timestamp should be updated on upsert."""
        df1 = make_sample_df(symbols=["HBL"], ts="2024-01-15T10:00:00+05:00")
        upsert_current(db, df1)

        df2 = make_sample_df(symbols=["HBL"], ts="2024-01-15T11:00:00+05:00")
        upsert_current(db, df2)

        cur = db.execute(
            "SELECT ts FROM regular_market_current WHERE symbol='HBL'"
        )
        assert cur.fetchone()[0] == "2024-01-15T11:00:00+05:00"


class TestInsertSnapshots:
    """Tests for insert_snapshots function."""

    def test_insert_snapshots(self, db):
        """Insert snapshot records."""
        df = make_sample_df()

        count = insert_snapshots(db, df, save_unchanged=True)

        assert count == 2
        cur = db.execute("SELECT COUNT(*) FROM regular_market_snapshots")
        assert cur.fetchone()[0] == 2

    def test_insert_different_timestamps(self, db):
        """Multiple snapshots with different timestamps."""
        df1 = make_sample_df(ts="2024-01-15T10:00:00+05:00")
        df2 = make_sample_df(ts="2024-01-15T11:00:00+05:00")

        insert_snapshots(db, df1, save_unchanged=True)
        insert_snapshots(db, df2, save_unchanged=True)

        cur = db.execute("SELECT COUNT(*) FROM regular_market_snapshots")
        assert cur.fetchone()[0] == 4

    def test_duplicate_ts_symbol_ignored(self, db):
        """Duplicate (ts, symbol) should be ignored."""
        df = make_sample_df()

        count1 = insert_snapshots(db, df, save_unchanged=True)
        count2 = insert_snapshots(db, df, save_unchanged=True)

        assert count1 == 2
        assert count2 == 0  # All duplicates

        cur = db.execute("SELECT COUNT(*) FROM regular_market_snapshots")
        assert cur.fetchone()[0] == 2

    def test_empty_dataframe(self, db):
        """Empty DataFrame should return 0."""
        df = pd.DataFrame()
        count = insert_snapshots(db, df, save_unchanged=True)
        assert count == 0


class TestGetCurrentHash:
    """Tests for get_current_hash function."""

    def test_get_existing_hash(self, db):
        """Get hash for existing symbol."""
        df = make_sample_df(symbols=["HBL"])
        upsert_current(db, df)

        hash_val = get_current_hash(db, "HBL")

        assert hash_val is not None
        assert "hash_HBL" in hash_val

    def test_get_nonexistent_hash(self, db):
        """Get hash for nonexistent symbol."""
        hash_val = get_current_hash(db, "NONEXISTENT")
        assert hash_val is None

    def test_case_insensitive(self, db):
        """Symbol lookup should be case insensitive."""
        df = make_sample_df(symbols=["HBL"])
        upsert_current(db, df)

        hash1 = get_current_hash(db, "HBL")
        hash2 = get_current_hash(db, "hbl")
        hash3 = get_current_hash(db, "Hbl")

        assert hash1 == hash2 == hash3


class TestGetCurrentMarket:
    """Tests for get_current_market function."""

    def test_get_all_current(self, db):
        """Get all current market data."""
        df = make_sample_df(symbols=["HBL", "OGDC", "MCB"])
        upsert_current(db, df)

        result = get_current_market(db)

        assert len(result) == 3
        assert "HBL" in result["symbol"].values
        assert "OGDC" in result["symbol"].values
        assert "MCB" in result["symbol"].values

    def test_sorted_by_symbol(self, db):
        """Results should be sorted by symbol."""
        df = make_sample_df(symbols=["ZETA", "ALPHA", "MCB"])
        upsert_current(db, df)

        result = get_current_market(db)

        symbols = result["symbol"].tolist()
        assert symbols == sorted(symbols)

    def test_empty_table(self, db):
        """Empty table should return empty DataFrame."""
        result = get_current_market(db)
        assert result.empty


class TestGetSnapshots:
    """Tests for get_snapshots function."""

    def test_get_all_snapshots(self, db):
        """Get all snapshots."""
        df = make_sample_df()
        insert_snapshots(db, df, save_unchanged=True)

        result = get_snapshots(db)

        assert len(result) == 2

    def test_filter_by_symbol(self, db):
        """Filter snapshots by symbol."""
        df = make_sample_df(symbols=["HBL", "OGDC", "MCB"])
        insert_snapshots(db, df, save_unchanged=True)

        result = get_snapshots(db, symbol="HBL")

        assert len(result) == 1
        assert result["symbol"].iloc[0] == "HBL"

    def test_filter_by_time_range(self, db):
        """Filter snapshots by time range."""
        df1 = make_sample_df(ts="2024-01-15T10:00:00+05:00")
        df2 = make_sample_df(ts="2024-01-15T11:00:00+05:00")
        df3 = make_sample_df(ts="2024-01-15T12:00:00+05:00")

        insert_snapshots(db, df1, save_unchanged=True)
        insert_snapshots(db, df2, save_unchanged=True)
        insert_snapshots(db, df3, save_unchanged=True)

        result = get_snapshots(
            db,
            start_ts="2024-01-15T10:30:00+05:00",
            end_ts="2024-01-15T11:30:00+05:00"
        )

        # Should only get the 11:00 snapshots
        assert len(result) == 2
        assert all("11:00:00" in ts for ts in result["ts"].values)

    def test_limit_results(self, db):
        """Limit number of results."""
        df = make_sample_df(symbols=["A", "B", "C", "D", "E"])
        insert_snapshots(db, df, save_unchanged=True)

        result = get_snapshots(db, limit=3)

        assert len(result) == 3

    def test_empty_table(self, db):
        """Empty table should return empty DataFrame."""
        result = get_snapshots(db)
        assert result.empty


class TestMultipleOperations:
    """Integration tests for multiple operations."""

    def test_upsert_and_snapshot_workflow(self, db):
        """Test typical workflow: upsert current + insert snapshots."""
        # First snapshot
        df1 = make_sample_df(ts="2024-01-15T10:00:00+05:00", current=100.0)
        upsert_current(db, df1)
        insert_snapshots(db, df1, save_unchanged=True)

        # Second snapshot with price change
        df2 = make_sample_df(ts="2024-01-15T10:01:00+05:00", current=101.0)
        upsert_current(db, df2)
        insert_snapshots(db, df2, save_unchanged=True)

        # Current should have latest values
        current = get_current_market(db)
        assert current[current["symbol"] == "HBL"]["current"].iloc[0] == 101.5

        # Snapshots should have both
        snapshots = get_snapshots(db, symbol="HBL")
        assert len(snapshots) == 2
