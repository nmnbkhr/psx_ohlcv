"""Shared FastAPI dependencies for read-side API routes.

Two per-request dependencies:

- ``get_read_db()`` opens a SQLite connection with ``mode=ro`` so writes
  fail at the driver level. This is structural enforcement that the Wave
  A read endpoints can never accidentally write.

- ``get_analytics_con()`` yields the cached in-memory DuckDB connection
  (views over Parquet + read-only SQLite ATTACH). The underlying
  connection is a global singleton wrapped in ``_UnclosableConnection``;
  calling ``.close()`` on it is a no-op by design, so this dep is safe
  to share across requests.

Both deps follow FastAPI's generator pattern so the connection is closed
deterministically when the request scope exits.
"""

from __future__ import annotations

import sqlite3
from typing import Generator

from pakfindata.api.config import get_settings


def get_read_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield a per-request read-only SQLite connection.

    Writes against this connection raise ``sqlite3.OperationalError:
    attempt to write a readonly database`` — structural API safety, not
    convention-based.
    """
    settings = get_settings()
    con = sqlite3.connect(
        f"file:{settings.db_path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


def get_analytics_con() -> Generator:
    """Yield the cached in-memory DuckDB analytics connection.

    Builds once on first call (across the whole process), reuses
    thereafter. Safe across requests because the underlying SQLite
    ATTACH is READ_ONLY and Parquet files are append-only.
    """
    from pakfindata.db.connections import analytics_con

    con = analytics_con()
    try:
        yield con
    finally:
        con.close()
