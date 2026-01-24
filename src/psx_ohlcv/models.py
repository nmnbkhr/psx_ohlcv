"""Data models for PSX OHLCV."""

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass
class Symbol:
    """Trading symbol from PSX."""

    symbol: str
    name: str | None = None
    sector: str | None = None
    is_active: int = 1
    discovered_at: str | None = None
    updated_at: str | None = None


@dataclass
class EODRecord:
    """End-of-day OHLCV record."""

    symbol: str
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    ingested_at: str | None = None


@dataclass
class SyncRun:
    """Sync run metadata."""

    run_id: str
    started_at: str
    ended_at: str | None
    mode: str
    symbols_total: int = 0
    symbols_ok: int = 0
    symbols_failed: int = 0
    rows_upserted: int = 0


@dataclass
class SyncFailure:
    """Individual symbol sync failure."""

    run_id: str
    symbol: str
    error_type: str
    error_message: str | None
    created_at: str | None = None


def now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
