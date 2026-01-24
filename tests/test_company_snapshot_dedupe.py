"""Tests for company quote snapshot deduplication in listen mode."""

import sqlite3

import pytest

from psx_ohlcv.db import (
    get_last_quote_hash,
    get_quote_snapshots,
    init_schema,
    insert_quote_snapshot,
)
from psx_ohlcv.sources.company_page import (
    _compute_raw_hash,
)


@pytest.fixture
def db_connection():
    """Create in-memory database with schema initialized."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    init_schema(con)
    return con


class TestSnapshotDeduplication:
    """Tests for smart-save deduplication in listen mode."""

    def test_unchanged_hash_skipped(self, db_connection):
        """Unchanged raw_hash should skip insert."""
        # Insert initial snapshot
        quote1 = {
            "price": 100.0,
            "change": 1.0,
            "change_pct": 1.0,
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "volume": 10000,
            "as_of": "2026-01-21 10:00:00",
            "raw_hash": _compute_raw_hash({
                "price": 100.0,
                "change": 1.0,
                "change_pct": 1.0,
                "open": 99.0,
                "high": 101.0,
                "low": 98.0,
                "volume": 10000,
                "as_of": "2026-01-21 10:00:00",
            }),
        }
        insert_quote_snapshot(db_connection, "TEST", "2026-01-21T10:00:00", quote1)

        # Try to insert same data with different timestamp
        last_hash = get_last_quote_hash(db_connection, "TEST")
        assert last_hash == quote1["raw_hash"]

        # Simulate listen mode skip logic
        quote2 = dict(quote1)  # Same data
        quote2["raw_hash"] = _compute_raw_hash({
            "price": 100.0,
            "change": 1.0,
            "change_pct": 1.0,
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "volume": 10000,
            "as_of": "2026-01-21 10:00:00",
        })

        # Hashes should match
        assert quote2["raw_hash"] == last_hash

    def test_changed_hash_inserted(self, db_connection):
        """Changed raw_hash should allow insert."""
        # Insert initial snapshot
        quote1 = {
            "price": 100.0,
            "change": 1.0,
            "raw_hash": _compute_raw_hash({"price": 100.0, "change": 1.0}),
        }
        insert_quote_snapshot(db_connection, "TEST", "2026-01-21T10:00:00", quote1)

        # Insert changed data
        quote2 = {
            "price": 101.0,  # Price changed
            "change": 2.0,
            "raw_hash": _compute_raw_hash({"price": 101.0, "change": 2.0}),
        }
        result = insert_quote_snapshot(
            db_connection, "TEST", "2026-01-21T10:01:00", quote2
        )

        assert result is True
        assert quote2["raw_hash"] != quote1["raw_hash"]

        # Should have 2 snapshots now
        df = get_quote_snapshots(db_connection, "TEST")
        assert len(df) == 2

    def test_hash_consistency(self):
        """Hash is consistent for same data."""
        data = {
            "price": 331.26,
            "change": -2.64,
            "change_pct": -0.79,
            "open": 334.10,
            "high": 336.60,
            "low": 330.00,
            "volume": 8140937,
            "as_of": "Wed, Jan 21, 2026 3:49 PM",
        }

        hash1 = _compute_raw_hash(data)
        hash2 = _compute_raw_hash(data)
        hash3 = _compute_raw_hash(dict(data))  # Copy

        assert hash1 == hash2
        assert hash2 == hash3

    def test_hash_changes_with_price(self):
        """Hash changes when price changes."""
        data1 = {"price": 100.0, "volume": 1000}
        data2 = {"price": 100.01, "volume": 1000}

        assert _compute_raw_hash(data1) != _compute_raw_hash(data2)

    def test_hash_changes_with_volume(self):
        """Hash changes when volume changes."""
        data1 = {"price": 100.0, "volume": 1000}
        data2 = {"price": 100.0, "volume": 1001}

        assert _compute_raw_hash(data1) != _compute_raw_hash(data2)

    def test_multiple_snapshots_same_symbol(self, db_connection):
        """Multiple snapshots for same symbol are stored."""
        for i in range(5):
            quote = {
                "price": 100.0 + i * 0.1,
                "raw_hash": _compute_raw_hash({"price": 100.0 + i * 0.1}),
            }
            insert_quote_snapshot(
                db_connection, "TEST", f"2026-01-21T10:0{i}:00", quote
            )

        df = get_quote_snapshots(db_connection, "TEST", limit=10)
        assert len(df) == 5

    def test_last_hash_is_most_recent(self, db_connection):
        """get_last_quote_hash returns the most recent snapshot's hash."""
        hashes = []
        for i in range(3):
            h = _compute_raw_hash({"price": 100.0 + i})
            hashes.append(h)
            insert_quote_snapshot(
                db_connection, "TEST", f"2026-01-21T10:0{i}:00", {"raw_hash": h}
            )

        last = get_last_quote_hash(db_connection, "TEST")
        assert last == hashes[-1]


class TestHashFieldSelection:
    """Tests for which fields are included in hash computation."""

    def test_hash_includes_price(self):
        """Price affects hash."""
        q1 = {"price": 100}
        q2 = {"price": 101}
        assert _compute_raw_hash(q1) != _compute_raw_hash(q2)

    def test_hash_includes_change(self):
        """Change affects hash."""
        q1 = {"change": 1}
        q2 = {"change": 2}
        assert _compute_raw_hash(q1) != _compute_raw_hash(q2)

    def test_hash_includes_change_pct(self):
        """Change percent affects hash."""
        q1 = {"change_pct": 1.0}
        q2 = {"change_pct": 1.1}
        assert _compute_raw_hash(q1) != _compute_raw_hash(q2)

    def test_hash_includes_volume(self):
        """Volume affects hash."""
        q1 = {"volume": 1000}
        q2 = {"volume": 1001}
        assert _compute_raw_hash(q1) != _compute_raw_hash(q2)

    def test_hash_includes_as_of(self):
        """As-of timestamp affects hash."""
        q1 = {"as_of": "10:00 AM"}
        q2 = {"as_of": "10:01 AM"}
        assert _compute_raw_hash(q1) != _compute_raw_hash(q2)

    def test_hash_ignores_missing_fields(self):
        """Missing fields don't cause errors."""
        q1 = {"price": 100}
        q2 = {"price": 100, "nonexistent": "value"}
        # Both should have valid hashes
        assert len(_compute_raw_hash(q1)) == 16
        assert len(_compute_raw_hash(q2)) == 16
