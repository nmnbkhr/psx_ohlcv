"""LLM response caching using SQLite.

This module provides caching for LLM responses to reduce API costs
and improve response times for repeated queries.

Cache key includes:
- Symbol(s)
- Date range
- Insight mode
- DB freshness marker (max timestamp from relevant tables)

Default TTL: 6 hours (configurable)
"""

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Default cache TTL in hours
DEFAULT_CACHE_TTL_HOURS = 6

# Schema for LLM cache table
LLM_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS llm_cache (
    prompt_hash     TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at      TEXT NOT NULL,
    response_text   TEXT NOT NULL,
    meta_json       TEXT,

    -- Additional fields for debugging and analytics
    symbol          TEXT,
    mode            TEXT,
    prompt_tokens   INTEGER,
    completion_tokens INTEGER,
    model           TEXT,

    -- Index for cleanup queries
    CONSTRAINT valid_expiry CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS idx_llm_cache_expires
    ON llm_cache(expires_at);

CREATE INDEX IF NOT EXISTS idx_llm_cache_symbol
    ON llm_cache(symbol);
"""


def init_llm_cache_schema(con: sqlite3.Connection) -> None:
    """Initialize the LLM cache table schema.

    Args:
        con: SQLite connection.
    """
    con.executescript(LLM_CACHE_SCHEMA)
    con.commit()
    logger.debug("LLM cache schema initialized")


@dataclass
class CacheEntry:
    """Represents a cached LLM response."""

    prompt_hash: str
    created_at: datetime
    expires_at: datetime
    response_text: str
    meta: dict[str, Any]
    symbol: str | None = None
    mode: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    model: str | None = None

    @property
    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        return datetime.now() > self.expires_at

    @property
    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return (datetime.now() - self.created_at).total_seconds()


class LLMCache:
    """SQLite-based cache for LLM responses.

    This cache stores LLM responses with configurable TTL to reduce
    API costs and improve response times.

    Example:
        >>> cache = LLMCache(con, ttl_hours=6)
        >>> cache_key = cache.compute_key(symbol="OGDC", mode="company", ...)
        >>>
        >>> # Check cache
        >>> cached = cache.get(cache_key)
        >>> if cached:
        ...     return cached.response_text
        >>>
        >>> # Generate and cache
        >>> response = llm.generate(prompt)
        >>> cache.set(cache_key, response.content, meta={...})
    """

    def __init__(
        self,
        con: sqlite3.Connection,
        ttl_hours: float = DEFAULT_CACHE_TTL_HOURS,
    ):
        """Initialize the cache.

        Args:
            con: SQLite connection.
            ttl_hours: Time-to-live for cache entries in hours.
        """
        self.con = con
        self.ttl_hours = ttl_hours
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure cache table exists."""
        try:
            init_llm_cache_schema(self.con)
        except sqlite3.Error as e:
            logger.warning("Could not initialize cache schema: %s", e)

    def compute_key(
        self,
        symbol: str | None = None,
        mode: str = "",
        date_range: str = "",
        db_freshness: str = "",
        extra: dict | None = None,
    ) -> str:
        """Compute cache key from input parameters.

        The key is a SHA-256 hash of the normalized inputs.

        Args:
            symbol: Stock symbol(s).
            mode: Insight mode (company, intraday, market, history).
            date_range: Date range string.
            db_freshness: Freshness marker (e.g., max timestamp from tables).
            extra: Additional parameters to include in key.

        Returns:
            Hex string cache key.
        """
        key_parts = {
            "symbol": (symbol or "").upper().strip(),
            "mode": mode.lower().strip(),
            "date_range": date_range,
            "db_freshness": db_freshness,
        }

        if extra:
            key_parts["extra"] = json.dumps(extra, sort_keys=True)

        # Create deterministic string representation
        key_string = json.dumps(key_parts, sort_keys=True)

        # Hash it
        return hashlib.sha256(key_string.encode()).hexdigest()

    def get(self, cache_key: str) -> CacheEntry | None:
        """Retrieve a cached response.

        Args:
            cache_key: The cache key to look up.

        Returns:
            CacheEntry if found and not expired, None otherwise.
        """
        try:
            cur = self.con.execute(
                """
                SELECT prompt_hash, created_at, expires_at, response_text,
                       meta_json, symbol, mode, prompt_tokens, completion_tokens, model
                FROM llm_cache
                WHERE prompt_hash = ?
                AND expires_at > datetime('now')
                """,
                (cache_key,),
            )

            row = cur.fetchone()
            if not row:
                logger.debug("Cache miss for key: %s", cache_key[:16])
                return None

            logger.debug("Cache hit for key: %s", cache_key[:16])

            # Parse meta JSON
            meta = {}
            if row[4]:
                try:
                    meta = json.loads(row[4])
                except json.JSONDecodeError:
                    pass

            return CacheEntry(
                prompt_hash=row[0],
                created_at=datetime.fromisoformat(row[1]),
                expires_at=datetime.fromisoformat(row[2]),
                response_text=row[3],
                meta=meta,
                symbol=row[5],
                mode=row[6],
                prompt_tokens=row[7] or 0,
                completion_tokens=row[8] or 0,
                model=row[9],
            )

        except sqlite3.Error as e:
            logger.warning("Cache read error: %s", e)
            return None

    def set(
        self,
        cache_key: str,
        response_text: str,
        meta: dict | None = None,
        symbol: str | None = None,
        mode: str | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        model: str | None = None,
        ttl_hours: float | None = None,
    ) -> bool:
        """Store a response in the cache.

        Args:
            cache_key: The cache key.
            response_text: The LLM response text.
            meta: Optional metadata dictionary.
            symbol: Stock symbol for indexing.
            mode: Insight mode for indexing.
            prompt_tokens: Token count for analytics.
            completion_tokens: Token count for analytics.
            model: Model name for analytics.
            ttl_hours: Override default TTL.

        Returns:
            True if cached successfully.
        """
        ttl = ttl_hours if ttl_hours is not None else self.ttl_hours
        expires_at = datetime.now() + timedelta(hours=ttl)

        meta_json = json.dumps(meta) if meta else None

        try:
            self.con.execute(
                """
                INSERT OR REPLACE INTO llm_cache
                (prompt_hash, expires_at, response_text, meta_json,
                 symbol, mode, prompt_tokens, completion_tokens, model)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cache_key,
                    expires_at.isoformat(),
                    response_text,
                    meta_json,
                    symbol,
                    mode,
                    prompt_tokens,
                    completion_tokens,
                    model,
                ),
            )
            self.con.commit()
            logger.debug("Cached response for key: %s (expires: %s)", cache_key[:16], expires_at)
            return True

        except sqlite3.Error as e:
            logger.warning("Cache write error: %s", e)
            return False

    def invalidate(self, cache_key: str) -> bool:
        """Invalidate a specific cache entry.

        Args:
            cache_key: The cache key to invalidate.

        Returns:
            True if entry was deleted.
        """
        try:
            cur = self.con.execute(
                "DELETE FROM llm_cache WHERE prompt_hash = ?",
                (cache_key,),
            )
            self.con.commit()
            deleted = cur.rowcount > 0
            if deleted:
                logger.debug("Invalidated cache key: %s", cache_key[:16])
            return deleted
        except sqlite3.Error as e:
            logger.warning("Cache invalidation error: %s", e)
            return False

    def invalidate_symbol(self, symbol: str) -> int:
        """Invalidate all cache entries for a symbol.

        Args:
            symbol: The stock symbol.

        Returns:
            Number of entries invalidated.
        """
        try:
            cur = self.con.execute(
                "DELETE FROM llm_cache WHERE symbol = ?",
                (symbol.upper(),),
            )
            self.con.commit()
            count = cur.rowcount
            if count > 0:
                logger.info("Invalidated %d cache entries for symbol: %s", count, symbol)
            return count
        except sqlite3.Error as e:
            logger.warning("Cache invalidation error: %s", e)
            return 0

    def cleanup_expired(self) -> int:
        """Remove all expired cache entries.

        Returns:
            Number of entries removed.
        """
        try:
            cur = self.con.execute(
                "DELETE FROM llm_cache WHERE expires_at <= datetime('now')"
            )
            self.con.commit()
            count = cur.rowcount
            if count > 0:
                logger.info("Cleaned up %d expired cache entries", count)
            return count
        except sqlite3.Error as e:
            logger.warning("Cache cleanup error: %s", e)
            return 0

    def get_stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics.
        """
        try:
            stats = {}

            # Total entries
            cur = self.con.execute("SELECT COUNT(*) FROM llm_cache")
            stats["total_entries"] = cur.fetchone()[0]

            # Active (non-expired) entries
            cur = self.con.execute(
                "SELECT COUNT(*) FROM llm_cache WHERE expires_at > datetime('now')"
            )
            stats["active_entries"] = cur.fetchone()[0]

            # Expired entries
            stats["expired_entries"] = stats["total_entries"] - stats["active_entries"]

            # Total tokens used
            cur = self.con.execute(
                "SELECT SUM(prompt_tokens), SUM(completion_tokens) FROM llm_cache"
            )
            row = cur.fetchone()
            stats["total_prompt_tokens"] = row[0] or 0
            stats["total_completion_tokens"] = row[1] or 0

            # Entries by mode
            cur = self.con.execute(
                "SELECT mode, COUNT(*) FROM llm_cache WHERE mode IS NOT NULL GROUP BY mode"
            )
            stats["entries_by_mode"] = dict(cur.fetchall())

            # Oldest and newest entries
            cur = self.con.execute(
                "SELECT MIN(created_at), MAX(created_at) FROM llm_cache"
            )
            row = cur.fetchone()
            stats["oldest_entry"] = row[0]
            stats["newest_entry"] = row[1]

            return stats

        except sqlite3.Error as e:
            logger.warning("Error getting cache stats: %s", e)
            return {"error": str(e)}

    def clear_all(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries removed.
        """
        try:
            cur = self.con.execute("DELETE FROM llm_cache")
            self.con.commit()
            count = cur.rowcount
            logger.info("Cleared all %d cache entries", count)
            return count
        except sqlite3.Error as e:
            logger.warning("Cache clear error: %s", e)
            return 0


def get_db_freshness_marker(con: sqlite3.Connection, symbol: str | None = None) -> str:
    """Get a freshness marker based on latest data timestamps.

    This marker changes when new data is added, invalidating stale cache.

    Args:
        con: SQLite connection.
        symbol: Optional symbol to check freshness for.

    Returns:
        String marker representing data freshness.
    """
    try:
        markers = []

        # Latest EOD data timestamp
        if symbol:
            cur = con.execute(
                "SELECT MAX(date) FROM eod_ohlcv WHERE symbol = ?",
                (symbol,)
            )
        else:
            cur = con.execute("SELECT MAX(date) FROM eod_ohlcv")
        row = cur.fetchone()
        markers.append(f"eod:{row[0] if row[0] else 'none'}")

        # Latest intraday timestamp
        if symbol:
            cur = con.execute(
                "SELECT MAX(timestamp) FROM intraday_bars WHERE symbol = ?",
                (symbol,)
            )
        else:
            cur = con.execute("SELECT MAX(timestamp) FROM intraday_bars")
        row = cur.fetchone()
        markers.append(f"int:{row[0] if row[0] else 'none'}")

        # Latest snapshot
        if symbol:
            cur = con.execute(
                "SELECT MAX(snapshot_date) FROM company_snapshots WHERE symbol = ?",
                (symbol,)
            )
        else:
            cur = con.execute("SELECT MAX(snapshot_date) FROM company_snapshots")
        row = cur.fetchone()
        markers.append(f"snap:{row[0] if row[0] else 'none'}")

        return "|".join(markers)

    except sqlite3.Error:
        return "unknown"
