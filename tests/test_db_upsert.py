"""Tests for database upsert operations."""

import pandas as pd
import pytest

from pakfindata import (
    connect,
    get_symbols_list,
    get_symbols_string,
    init_schema,
    record_failure,
    record_sync_run_end,
    record_sync_run_start,
    upsert_eod,
    upsert_symbols,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    con = connect(":memory:")
    init_schema(con)
    yield con
    con.close()


class TestInitSchema:
    """Tests for schema initialization."""

    def test_creates_tables(self, db):
        """Verify all tables are created."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row["name"] for row in cur.fetchall()]

        assert "symbols" in tables
        assert "eod_ohlcv" in tables
        assert "sync_runs" in tables
        assert "sync_failures" in tables

    def test_idempotent(self, db):
        """Schema init should be idempotent."""
        # Call init_schema again - should not raise
        init_schema(db)
        init_schema(db)

        cur = db.execute("SELECT COUNT(*) as cnt FROM symbols")
        assert cur.fetchone()["cnt"] == 0


class TestUpsertSymbols:
    """Tests for symbol upsert operations."""

    def test_insert_new_symbols(self, db):
        """Insert new symbols."""
        symbols = [
            {"symbol": "ABOT", "name": "Abbott Labs", "sector": "Pharma"},
            {"symbol": "ABL", "name": "Allied Bank", "sector": "Banking"},
        ]

        count = upsert_symbols(db, symbols)

        assert count == 2
        cur = db.execute("SELECT COUNT(*) as cnt FROM symbols")
        assert cur.fetchone()["cnt"] == 2

    def test_upsert_no_duplicates(self, db):
        """Upserting same symbols twice should not create duplicates."""
        symbols = [
            {"symbol": "ABOT", "name": "Abbott Labs", "sector": "Pharma"},
            {"symbol": "ABL", "name": "Allied Bank", "sector": "Banking"},
        ]

        upsert_symbols(db, symbols)
        upsert_symbols(db, symbols)

        cur = db.execute("SELECT COUNT(*) as cnt FROM symbols")
        assert cur.fetchone()["cnt"] == 2

    def test_upsert_updates_existing(self, db):
        """Upserting should update name/sector if provided."""
        symbols1 = [{"symbol": "ABOT", "name": "Abbott", "sector": None}]
        upsert_symbols(db, symbols1)

        symbols2 = [{"symbol": "ABOT", "name": "Abbott Labs", "sector": "Pharma"}]
        upsert_symbols(db, symbols2)

        cur = db.execute("SELECT name, sector FROM symbols WHERE symbol = 'ABOT'")
        row = cur.fetchone()
        assert row["name"] == "Abbott Labs"
        assert row["sector"] == "Pharma"

    def test_upsert_preserves_existing_on_null(self, db):
        """Upserting with null should preserve existing values."""
        symbols1 = [{"symbol": "ABOT", "name": "Abbott Labs", "sector": "Pharma"}]
        upsert_symbols(db, symbols1)

        symbols2 = [{"symbol": "ABOT", "name": None, "sector": None}]
        upsert_symbols(db, symbols2)

        cur = db.execute("SELECT name, sector FROM symbols WHERE symbol = 'ABOT'")
        row = cur.fetchone()
        assert row["name"] == "Abbott Labs"
        assert row["sector"] == "Pharma"

    def test_upsert_sets_is_active(self, db):
        """Upsert should set is_active=1."""
        # Manually insert inactive symbol
        db.execute(
            """
            INSERT INTO symbols (symbol, name, is_active, discovered_at, updated_at)
            VALUES ('ABOT', 'Abbott', 0, '2024-01-01', '2024-01-01')
            """
        )
        db.commit()

        # Upsert should reactivate
        upsert_symbols(db, [{"symbol": "ABOT"}])

        cur = db.execute("SELECT is_active FROM symbols WHERE symbol = 'ABOT'")
        assert cur.fetchone()["is_active"] == 1

    def test_empty_list(self, db):
        """Empty list should return 0."""
        count = upsert_symbols(db, [])
        assert count == 0

    def test_skip_missing_symbol_key(self, db):
        """Entries without symbol key should be skipped."""
        symbols = [{"name": "No Symbol"}, {"symbol": "ABOT"}]
        count = upsert_symbols(db, symbols)

        assert count == 1


class TestUpsertEOD:
    """Tests for EOD OHLCV upsert operations."""

    def test_insert_new_records(self, db):
        """Insert new EOD records."""
        df = pd.DataFrame(
            [
                {
                    "symbol": "ABOT",
                    "date": "2024-01-15",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 50000,
                },
                {
                    "symbol": "ABOT",
                    "date": "2024-01-16",
                    "open": 103.0,
                    "high": 107.0,
                    "low": 102.0,
                    "close": 106.0,
                    "volume": 60000,
                },
            ]
        )

        count = upsert_eod(db, df)

        assert count == 2
        cur = db.execute("SELECT COUNT(*) as cnt FROM eod_ohlcv")
        assert cur.fetchone()["cnt"] == 2

    def test_upsert_updates_existing(self, db):
        """Upserting should update existing record."""
        df1 = pd.DataFrame(
            [
                {
                    "symbol": "ABOT",
                    "date": "2024-01-15",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 50000,
                }
            ]
        )
        upsert_eod(db, df1)

        # Update with new close price
        df2 = pd.DataFrame(
            [
                {
                    "symbol": "ABOT",
                    "date": "2024-01-15",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 104.0,
                    "volume": 55000,
                }
            ]
        )
        upsert_eod(db, df2)

        cur = db.execute(
            "SELECT close, volume FROM eod_ohlcv "
            "WHERE symbol='ABOT' AND date='2024-01-15'"
        )
        row = cur.fetchone()
        assert row["close"] == 104.0
        assert row["volume"] == 55000

        # Should still be just one row
        cur = db.execute("SELECT COUNT(*) as cnt FROM eod_ohlcv")
        assert cur.fetchone()["cnt"] == 1

    def test_empty_dataframe(self, db):
        """Empty DataFrame should return 0."""
        df = pd.DataFrame(
            columns=["symbol", "date", "open", "high", "low", "close", "volume"]
        )
        count = upsert_eod(db, df)
        assert count == 0

    def test_missing_columns_raises(self, db):
        """Missing required columns should raise ValueError."""
        df = pd.DataFrame([{"symbol": "ABOT", "date": "2024-01-15"}])

        with pytest.raises(ValueError, match="missing columns"):
            upsert_eod(db, df)


class TestSyncRuns:
    """Tests for sync run tracking."""

    def test_record_sync_run_lifecycle(self, db):
        """Test full sync run lifecycle."""
        # Start run
        run_id = record_sync_run_start(db, mode="full", symbols_total=100)

        assert run_id is not None
        assert len(run_id) == 36  # UUID format

        cur = db.execute("SELECT * FROM sync_runs WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        assert row["mode"] == "full"
        assert row["symbols_total"] == 100
        assert row["ended_at"] is None

        # End run
        record_sync_run_end(
            db, run_id, symbols_ok=95, symbols_failed=5, rows_upserted=9500
        )

        cur = db.execute("SELECT * FROM sync_runs WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        assert row["ended_at"] is not None
        assert row["symbols_ok"] == 95
        assert row["symbols_failed"] == 5
        assert row["rows_upserted"] == 9500


class TestSyncFailures:
    """Tests for sync failure recording."""

    def test_record_failure(self, db):
        """Test recording a failure."""
        run_id = record_sync_run_start(db, mode="full", symbols_total=10)

        record_failure(
            db,
            run_id=run_id,
            symbol="ABOT",
            error_type="HTTP_ERROR",
            error_message="Connection timeout",
        )

        cur = db.execute("SELECT * FROM sync_failures WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        assert row["symbol"] == "ABOT"
        assert row["error_type"] == "HTTP_ERROR"
        assert row["error_message"] == "Connection timeout"


class TestGetSymbols:
    """Tests for symbol retrieval functions."""

    def test_get_symbols_list_sorted(self, db):
        """Symbols should be returned in sorted order."""
        symbols = [
            {"symbol": "ZIL"},
            {"symbol": "ABOT"},
            {"symbol": "MTL"},
        ]
        upsert_symbols(db, symbols)

        result = get_symbols_list(db)
        assert result == ["ABOT", "MTL", "ZIL"]

    def test_get_symbols_list_with_limit(self, db):
        """Limit should restrict number of symbols."""
        symbols = [{"symbol": f"SYM{i:03d}"} for i in range(10)]
        upsert_symbols(db, symbols)

        result = get_symbols_list(db, limit=3)
        assert len(result) == 3
        assert result == ["SYM000", "SYM001", "SYM002"]

    def test_get_symbols_list_only_active(self, db):
        """Should only return active symbols."""
        upsert_symbols(db, [{"symbol": "ABOT"}, {"symbol": "ABL"}])

        # Deactivate one
        db.execute("UPDATE symbols SET is_active = 0 WHERE symbol = 'ABL'")
        db.commit()

        result = get_symbols_list(db)
        assert result == ["ABOT"]

    def test_get_symbols_string(self, db):
        """Should return comma-separated string."""
        symbols = [
            {"symbol": "ZIL"},
            {"symbol": "ABOT"},
            {"symbol": "MTL"},
        ]
        upsert_symbols(db, symbols)

        result = get_symbols_string(db)
        assert result == "ABOT,MTL,ZIL"

    def test_get_symbols_string_with_limit(self, db):
        """String function should respect limit."""
        symbols = [{"symbol": f"SYM{i:03d}"} for i in range(10)]
        upsert_symbols(db, symbols)

        result = get_symbols_string(db, limit=3)
        assert result == "SYM000,SYM001,SYM002"

    def test_get_symbols_empty(self, db):
        """Empty database should return empty list/string."""
        assert get_symbols_list(db) == []
        assert get_symbols_string(db) == ""
