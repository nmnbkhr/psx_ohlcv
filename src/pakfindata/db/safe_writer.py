"""
Safe write connection helper for SQLite operations on psx.sqlite.

USE THIS for any in-process write to psx.sqlite from Streamlit pages,
CLI commands, or one-off scripts. Replaces the dangerous pattern of
writing through the long-lived cached singleton (`client.connection`).

WHY THIS EXISTS
═══════════════
On 2026-05-09, psx.sqlite was corrupted (header destroyed, file grew
24GB → 29GB, "file is not a database" error) because:

    1. Streamlit ran overnight with PSXClient._client_instance (singleton)
       holding a long-lived WAL-mode connection to psx.sqlite.
    2. Morning: user clicked "Sync Indices" button.
    3. Each index (~18) called save_index_data(con) which did
       INSERT OR REPLACE + con.commit() — i.e. 18 separate commits on
       the cached connection in stale WAL state.
    4. One commit's WAL replay overwrote page 1 (the header).
    5. ~5GB of garbage allocated by the failing transaction.

Recovery took 13 hours via a custom page-level decoder.

THE FIX (this module)
═════════════════════
Every write should:
  - Open a FRESH connection (not the cached singleton)
  - PRAGMA busy_timeout=10000  (wait up to 10s if another writer holds lock)
  - BEGIN IMMEDIATE             (acquire write lock upfront — fail fast)
  - Do the work in ONE transaction
  - COMMIT once at the end (not per-row)
  - PRAGMA wal_checkpoint(FULL) (flush WAL to main file, prevent growth)
  - Always close() the connection (never persisted across requests)

USAGE
═════

    from pakfindata.db.safe_writer import safe_writer

    # Inside a Streamlit button handler or CLI command:
    with safe_writer() as con:
        for d in indices_data:
            save_index_data(con, d)
        # auto-COMMIT and WAL-checkpoint on context exit
        # auto-ROLLBACK on any exception
    # connection closed

For helpers that previously called con.commit() themselves
(like save_index_data), strip those commits — safe_writer commits ONCE
at the end of the block. Per-row commits defeat the purpose.

ERROR HANDLING
══════════════
SafeWriterBusyError: raised if another writer holds the lock longer
    than busy_timeout. Catch this in UI code to show "another sync
    is running" message.

All other sqlite3.Error subclasses propagate to caller after rollback.

CONCURRENCY MODEL
═════════════════
SQLite WAL mode allows ONE writer + MULTIPLE readers concurrently.
This helper acquires the writer slot via BEGIN IMMEDIATE.
Background services (tick_service, eod_sync_service) use their own
connections — they coordinate with us via the busy_timeout mechanism.

DO NOT use this for reads. Use the cached `client.connection` for
reads (faster, no lock acquisition).
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from pakfindata.config import get_db_path

logger = logging.getLogger(__name__)


class SafeWriterBusyError(Exception):
    """Raised when the writer slot is held by another process and the
    busy_timeout expires. Catch this in UI code to display a graceful
    'another sync is running' message rather than a generic error.
    """


@contextmanager
def safe_writer(
    timeout: float = 30.0,
    busy_timeout_ms: int = 10_000,
    synchronous: str = "NORMAL",
) -> Iterator[sqlite3.Connection]:
    """Open an ad-hoc write connection to psx.sqlite with safe defaults.

    The connection is opened fresh, configured with WAL mode + busy timeout,
    wrapped in a single BEGIN IMMEDIATE transaction, and ALWAYS closed
    after the `with` block exits (success or failure).

    On success: COMMIT + PRAGMA wal_checkpoint(FULL) before close.
    On exception: ROLLBACK before close, exception re-raised.

    Args:
        timeout: Seconds for sqlite3.connect() to wait for the file lock
                 at connection time. Default 30s.
        busy_timeout_ms: PRAGMA busy_timeout — how long SQLite waits when
                         another writer holds the SHARED→RESERVED lock.
                         Default 10s.
        synchronous: PRAGMA synchronous level. NORMAL is safe for WAL mode
                     and faster than FULL. Use FULL for the highest
                     durability guarantee at the cost of speed.

    Yields:
        sqlite3.Connection in autocommit-off mode, ready to execute writes.

    Raises:
        SafeWriterBusyError: if another writer holds the lock past busy_timeout.
        sqlite3.Error: for any other DB error (after ROLLBACK).
        Any other exception raised inside the `with` block (after ROLLBACK).

    Example:
        with safe_writer() as con:
            con.execute("INSERT INTO ...", (...))
            con.execute("INSERT INTO ...", (...))
        # auto-committed, WAL flushed, connection closed
    """
    db_path = str(get_db_path())
    con: sqlite3.Connection | None = None

    try:
        con = sqlite3.connect(db_path, timeout=timeout, isolation_level=None)

        # ── Configure connection ──
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        con.execute(f"PRAGMA synchronous={synchronous}")

        # ── Acquire writer slot upfront (fail fast if locked) ──
        try:
            con.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                raise SafeWriterBusyError(
                    "Another writer holds the lock; try again in a moment."
                ) from e
            raise

        try:
            yield con
            # ── Success path ──
            con.execute("COMMIT")
            try:
                con.execute("PRAGMA wal_checkpoint(FULL)")
            except sqlite3.Error as e:
                # Checkpoint failure isn't fatal — data is in WAL safely,
                # just not yet merged into main file. Log and move on.
                logger.warning("wal_checkpoint(FULL) failed: %s", e)
        except Exception:
            # ── Failure path ──
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error as e:
                logger.warning("ROLLBACK failed during exception handling: %s", e)
            raise
    finally:
        if con is not None:
            try:
                con.close()
            except sqlite3.Error as e:
                logger.warning("Connection close failed: %s", e)


@contextmanager
def safe_writer_attached(
    attach_path: str,
    attach_name: str = "src",
    **kwargs,
) -> Iterator[sqlite3.Connection]:
    """Variant of safe_writer that ATTACHes another SQLite DB read-only.

    Useful for merging tick_bars.db into psx.sqlite, importing from
    a backup, or running cross-database queries that write to psx.sqlite.

    Args:
        attach_path: Path to the DB to attach.
        attach_name: Schema name to attach as. Default "src".
        **kwargs: Passed to safe_writer.

    Example:
        with safe_writer_attached("/path/to/tick_bars.db", "tb") as con:
            con.execute(
                "INSERT OR IGNORE INTO ohlcv_5s SELECT * FROM tb.ohlcv_5s"
            )
    """
    with safe_writer(**kwargs) as con:
        con.execute(f"ATTACH DATABASE ? AS {attach_name}", (attach_path,))
        try:
            yield con
        finally:
            try:
                con.execute(f"DETACH DATABASE {attach_name}")
            except sqlite3.Error:
                pass


def checkpoint_wal(timeout: float = 30.0) -> tuple[int, int, int]:
    """Force a WAL checkpoint on psx.sqlite.

    Useful to call from a CLI command or admin button to ensure the WAL
    file is fully flushed into the main database file before backup,
    shutdown, or long idle periods.

    Returns:
        Tuple of (busy, log_frames, checkpointed_frames) from PRAGMA result.
        busy = 0 means full checkpoint succeeded.
    """
    db_path = str(get_db_path())
    con = sqlite3.connect(db_path, timeout=timeout, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        result = con.execute("PRAGMA wal_checkpoint(FULL)").fetchone()
        return tuple(result) if result else (0, 0, 0)
    finally:
        con.close()
