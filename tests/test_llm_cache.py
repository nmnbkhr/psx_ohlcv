"""Tests for LLM caching functionality.

Verifies:
- Cache hit/miss behavior
- TTL expiry
- Cache key computation
- Cleanup operations
"""

import sqlite3
import time
from datetime import datetime, timedelta

import pytest

from pakfindata.agents.cache import (
    LLMCache,
    CacheEntry,
    init_llm_cache_schema,
    get_db_freshness_marker,
)


@pytest.fixture
def db():
    """Create in-memory database with schema for testing."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    # Initialize basic schema for freshness markers
    con.executescript("""
        CREATE TABLE IF NOT EXISTS eod_ohlcv (
            symbol TEXT, date TEXT, close REAL
        );
        CREATE TABLE IF NOT EXISTS intraday_bars (
            symbol TEXT, timestamp TEXT, close REAL
        );
        CREATE TABLE IF NOT EXISTS company_snapshots (
            symbol TEXT, snapshot_date TEXT
        );
    """)

    # Initialize LLM cache schema
    init_llm_cache_schema(con)

    yield con
    con.close()


class TestLLMCacheSchema:
    """Tests for cache schema initialization."""

    def test_schema_creates_table(self, db):
        """Schema should create llm_cache table."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_cache'"
        )
        assert cur.fetchone() is not None

    def test_schema_creates_index(self, db):
        """Schema should create index for expiry queries."""
        cur = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_llm_cache%'"
        )
        indices = cur.fetchall()
        assert len(indices) >= 1

    def test_schema_idempotent(self, db):
        """Running schema init multiple times should not error."""
        # Should not raise
        init_llm_cache_schema(db)
        init_llm_cache_schema(db)


class TestCacheKeyComputation:
    """Tests for cache key computation."""

    def test_key_deterministic(self, db):
        """Same inputs should produce same key."""
        cache = LLMCache(db)

        key1 = cache.compute_key(symbol="OGDC", mode="company", date_range="2024-01")
        key2 = cache.compute_key(symbol="OGDC", mode="company", date_range="2024-01")

        assert key1 == key2

    def test_key_different_for_different_inputs(self, db):
        """Different inputs should produce different keys."""
        cache = LLMCache(db)

        key1 = cache.compute_key(symbol="OGDC", mode="company")
        key2 = cache.compute_key(symbol="HBL", mode="company")
        key3 = cache.compute_key(symbol="OGDC", mode="intraday")

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3

    def test_key_case_insensitive_symbol(self, db):
        """Symbol should be case-insensitive in key computation."""
        cache = LLMCache(db)

        key1 = cache.compute_key(symbol="ogdc")
        key2 = cache.compute_key(symbol="OGDC")

        assert key1 == key2

    def test_key_includes_freshness(self, db):
        """Different freshness markers should produce different keys."""
        cache = LLMCache(db)

        key1 = cache.compute_key(symbol="OGDC", db_freshness="marker1")
        key2 = cache.compute_key(symbol="OGDC", db_freshness="marker2")

        assert key1 != key2


class TestCacheHitMiss:
    """Tests for cache hit/miss behavior."""

    def test_cache_miss_on_empty(self, db):
        """Cache should miss when empty."""
        cache = LLMCache(db)
        key = cache.compute_key(symbol="OGDC", mode="company")

        result = cache.get(key)
        assert result is None

    def test_cache_hit_after_set(self, db):
        """Cache should hit after setting a value."""
        cache = LLMCache(db)
        key = cache.compute_key(symbol="OGDC", mode="company")

        # Set value
        success = cache.set(key, "Test response", symbol="OGDC", mode="company")
        assert success is True

        # Should hit
        result = cache.get(key)
        assert result is not None
        assert result.response_text == "Test response"

    def test_cache_stores_metadata(self, db):
        """Cache should store and retrieve metadata."""
        cache = LLMCache(db)
        key = cache.compute_key(symbol="OGDC")

        cache.set(
            key,
            "Response",
            symbol="OGDC",
            mode="company",
            prompt_tokens=100,
            completion_tokens=200,
            model="gpt-5.2",
        )

        result = cache.get(key)
        assert result.symbol == "OGDC"
        assert result.mode == "company"
        assert result.prompt_tokens == 100
        assert result.completion_tokens == 200
        assert result.model == "gpt-5.2"


class TestCacheTTL:
    """Tests for cache TTL (time-to-live) behavior."""

    def test_expired_entry_not_returned(self, db):
        """Expired entries should not be returned."""
        cache = LLMCache(db, ttl_hours=1)
        key = cache.compute_key(symbol="OGDC")

        # Insert entry that is already expired (created 2h ago, expired 1h ago)
        db.execute(
            """
            INSERT INTO llm_cache (prompt_hash, created_at, expires_at, response_text)
            VALUES (?, datetime('now', '-2 hours'), datetime('now', '-1 hour'), ?)
            """,
            (key, "Response"),
        )
        db.commit()

        # Should not return expired entry
        result = cache.get(key)
        assert result is None

    def test_fresh_entry_returned(self, db):
        """Non-expired entries should be returned."""
        cache = LLMCache(db, ttl_hours=1)  # 1 hour TTL
        key = cache.compute_key(symbol="OGDC")

        cache.set(key, "Response")

        result = cache.get(key)
        assert result is not None
        assert result.response_text == "Response"

    def test_cache_entry_is_expired_property(self, db):
        """CacheEntry.is_expired property should work correctly."""
        entry = CacheEntry(
            prompt_hash="test",
            created_at=datetime.now() - timedelta(hours=2),
            expires_at=datetime.now() - timedelta(hours=1),
            response_text="Test",
            meta={},
        )
        assert entry.is_expired is True

        entry2 = CacheEntry(
            prompt_hash="test",
            created_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=1),
            response_text="Test",
            meta={},
        )
        assert entry2.is_expired is False


class TestCacheOperations:
    """Tests for cache management operations."""

    def test_invalidate_specific_key(self, db):
        """Should be able to invalidate specific cache entry."""
        cache = LLMCache(db)
        key = cache.compute_key(symbol="OGDC")

        cache.set(key, "Response")
        assert cache.get(key) is not None

        deleted = cache.invalidate(key)
        assert deleted is True
        assert cache.get(key) is None

    def test_invalidate_by_symbol(self, db):
        """Should be able to invalidate all entries for a symbol."""
        cache = LLMCache(db)

        # Add multiple entries for same symbol
        key1 = cache.compute_key(symbol="OGDC", mode="company")
        key2 = cache.compute_key(symbol="OGDC", mode="intraday")
        key3 = cache.compute_key(symbol="HBL", mode="company")

        cache.set(key1, "R1", symbol="OGDC")
        cache.set(key2, "R2", symbol="OGDC")
        cache.set(key3, "R3", symbol="HBL")

        # Invalidate OGDC
        count = cache.invalidate_symbol("OGDC")
        assert count == 2

        # OGDC entries gone, HBL still there
        assert cache.get(key1) is None
        assert cache.get(key2) is None
        assert cache.get(key3) is not None

    def test_cleanup_expired(self, db):
        """Should clean up expired entries."""
        cache = LLMCache(db)

        # Insert an already-expired entry directly (created 2h ago, expired 1h ago)
        db.execute(
            """
            INSERT INTO llm_cache (prompt_hash, created_at, expires_at, response_text)
            VALUES (?, datetime('now', '-2 hours'), datetime('now', '-1 hour'), ?)
            """,
            ("expired_key", "Old response"),
        )
        db.commit()

        # Cleanup should find and remove the expired entry
        count = cache.cleanup_expired()
        assert count >= 1

    def test_clear_all(self, db):
        """Should be able to clear all cache entries."""
        cache = LLMCache(db)

        # Add multiple entries
        for i in range(5):
            key = cache.compute_key(symbol=f"SYM{i}")
            cache.set(key, f"Response {i}")

        # Clear all
        count = cache.clear_all()
        assert count == 5

        # Verify empty
        stats = cache.get_stats()
        assert stats["total_entries"] == 0

    def test_get_stats(self, db):
        """Should return accurate cache statistics."""
        cache = LLMCache(db)

        # Add some entries
        for i in range(3):
            key = cache.compute_key(symbol=f"SYM{i}", mode="company")
            cache.set(
                key,
                f"Response {i}",
                symbol=f"SYM{i}",
                mode="company",
                prompt_tokens=100,
                completion_tokens=50,
            )

        stats = cache.get_stats()

        assert stats["total_entries"] == 3
        assert stats["active_entries"] == 3
        assert stats["total_prompt_tokens"] == 300
        assert stats["total_completion_tokens"] == 150
        assert "company" in stats["entries_by_mode"]


class TestDBFreshnessMarker:
    """Tests for database freshness marker computation."""

    def test_freshness_marker_empty_db(self, db):
        """Should return marker even with empty tables."""
        marker = get_db_freshness_marker(db)

        assert marker is not None
        assert "eod:" in marker
        assert "int:" in marker
        assert "snap:" in marker

    def test_freshness_marker_changes_with_data(self, db):
        """Marker should change when data is added."""
        marker1 = get_db_freshness_marker(db, symbol="OGDC")

        # Add some data
        db.execute(
            "INSERT INTO eod_ohlcv (symbol, date, close) VALUES (?, ?, ?)",
            ("OGDC", "2024-01-15", 100.0),
        )
        db.commit()

        marker2 = get_db_freshness_marker(db, symbol="OGDC")

        assert marker1 != marker2

    def test_freshness_marker_symbol_specific(self, db):
        """Marker should be symbol-specific when symbol provided."""
        # Add data for OGDC only
        db.execute(
            "INSERT INTO eod_ohlcv (symbol, date, close) VALUES (?, ?, ?)",
            ("OGDC", "2024-01-15", 100.0),
        )
        db.commit()

        marker_ogdc = get_db_freshness_marker(db, symbol="OGDC")
        marker_hbl = get_db_freshness_marker(db, symbol="HBL")

        # Should be different because OGDC has data, HBL doesn't
        assert marker_ogdc != marker_hbl
