"""Database connection management and pooling."""

import sqlite3
from pathlib import Path

from psx_ohlcv.config import ensure_dirs, get_db_path

from .schema import SCHEMA_SQL


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """
    Connect to SQLite database with WAL mode enabled.

    Args:
        db_path: Path to database file. If None, uses default from config.
                 Use ":memory:" for in-memory database.

    Returns:
        sqlite3.Connection with row_factory set to sqlite3.Row
    """
    if db_path == ":memory:":
        path = ":memory:"
    else:
        path = get_db_path(db_path)
        ensure_dirs(path)
        path = str(path)

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row

    # Enable WAL mode for better concurrent access (not for :memory:)
    if db_path != ":memory:":
        con.execute("PRAGMA journal_mode=WAL")

    return con


# Alias for backward compatibility with tools
get_connection = connect


def init_schema(con: sqlite3.Connection) -> None:
    """
    Initialize database schema.

    Creates all tables if they don't exist, and runs migrations
    to add any new columns to existing tables.
    """
    con.executescript(SCHEMA_SQL)
    con.commit()

    # Run migrations for new columns in existing tables
    _migrate_symbols_table(con)
    _migrate_eod_ohlcv_table(con)
    _migrate_scrape_jobs_table(con)


def _migrate_symbols_table(con: sqlite3.Connection) -> None:
    """Add new columns to symbols table if they don't exist."""
    # Get existing columns
    cursor = con.execute("PRAGMA table_info(symbols)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # Add sector_name column if missing
    if "sector_name" not in existing_cols:
        con.execute("ALTER TABLE symbols ADD COLUMN sector_name TEXT NULL")

    # Add outstanding_shares column if missing
    if "outstanding_shares" not in existing_cols:
        con.execute("ALTER TABLE symbols ADD COLUMN outstanding_shares REAL NULL")

    # Add source column if missing
    if "source" not in existing_cols:
        con.execute(
            "ALTER TABLE symbols ADD COLUMN source TEXT NOT NULL DEFAULT 'MARKET_WATCH'"
        )

    con.commit()


def _migrate_eod_ohlcv_table(con: sqlite3.Connection) -> None:
    """Add new columns to eod_ohlcv table if they don't exist."""
    cursor = con.execute("PRAGMA table_info(eod_ohlcv)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # Add prev_close column if missing
    if "prev_close" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN prev_close REAL")

    # Add sector_code column if missing
    if "sector_code" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN sector_code TEXT")

    # Add company_name column if missing
    if "company_name" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN company_name TEXT")

    # Add source column if missing
    if "source" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN source TEXT")

    # Add processname column if missing
    if "processname" not in existing_cols:
        con.execute("ALTER TABLE eod_ohlcv ADD COLUMN processname TEXT")

    con.commit()


def _migrate_scrape_jobs_table(con: sqlite3.Connection) -> None:
    """Add new columns to scrape_jobs table for background job support."""
    cursor = con.execute("PRAGMA table_info(scrape_jobs)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    # New columns for background job support
    new_columns = [
        ("stop_requested", "INTEGER DEFAULT 0"),
        ("current_symbol", "TEXT"),
        ("current_batch", "INTEGER DEFAULT 0"),
        ("total_batches", "INTEGER DEFAULT 0"),
        ("batch_size", "INTEGER DEFAULT 50"),
        ("batch_pause_sec", "INTEGER DEFAULT 30"),
        ("pid", "INTEGER"),
        ("last_heartbeat", "TEXT"),
        ("notification_sent", "INTEGER DEFAULT 0"),
    ]

    for col_name, col_def in new_columns:
        if col_name not in existing_cols:
            con.execute(f"ALTER TABLE scrape_jobs ADD COLUMN {col_name} {col_def}")

    # Create job_notifications table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS job_notifications (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id              TEXT NOT NULL,
            notification_type   TEXT NOT NULL,
            title               TEXT NOT NULL,
            message             TEXT,
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            read_at             TEXT,
            FOREIGN KEY (job_id) REFERENCES scrape_jobs(job_id)
        )
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_notifications_unread
        ON job_notifications(read_at) WHERE read_at IS NULL
    """)

    con.commit()
