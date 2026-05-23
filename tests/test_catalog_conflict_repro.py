"""Reproducer for catalog.py ON CONFLICT bug (Bug A).

Affects: rows where ``update_catalog`` is called twice with different
``date_column`` / ``source_table`` / ``display_name`` values for the
same ``domain``. The second call should update those three columns but
silently does not — the row keeps its first-INSERT values forever.

Root cause: ``catalog.py:106-114`` ON CONFLICT SET clause omits
``date_column``, ``source_table``, and ``display_name`` from the
update list. The other columns (last_sync_at, last_row_date,
row_count, status, etc.) are correctly updated.

**NOT the same as the ZUMA/TBILL/MUFAP/WTL pollution (Bug B)**, even
though both surface in ``data_freshness`` rows.

  Bug A (this file): catalog metadata columns frozen on first insert.
    Affects: ``announcements``, ``tick_data`` (rows currently showing
    ``status='failed'`` because ``MAX(date)`` errors against tables
    with no ``date`` column).
    Fix: one-line addition to catalog.py SET clause. Lands in 2.A.2.

  Bug B (separate concern): literal junk strings like 'ZUMA' in source
    tables' date columns themselves.
    Affects: pib_auctions.auction_date, konia_daily.date, etc.
    Fix: STRUCTURAL — Phase 2.A.1's validator framework rolls back
    safe_writer transactions that try to land bad values. Already in
    place (commit b8adb29). Existing polluted rows get a one-shot
    cleanup script in 2.A.2.

These three tests currently FAIL by design. After 2.A.2 lands they
pass and become regression guards. **Do not "fix" them by editing the
asserts** — fix catalog.py instead.
"""

from __future__ import annotations

import sqlite3

import pytest

from pakfindata.db.catalog import update_catalog


# Minimal data_freshness schema — copied verbatim from db/schema.py.
# We use :memory: so the tests are hermetic and never touch the real DB.
DATA_FRESHNESS_SCHEMA = """
CREATE TABLE data_freshness (
    domain           TEXT PRIMARY KEY,
    display_name     TEXT NOT NULL,
    source_table     TEXT NOT NULL,
    date_column      TEXT NOT NULL DEFAULT 'date',
    last_sync_at     TEXT,
    last_row_date    TEXT,
    row_count        INTEGER DEFAULT 0,
    status           TEXT DEFAULT 'unknown',
    last_sync_error  TEXT,
    source           TEXT,
    schema_version   INTEGER NOT NULL DEFAULT 1,
    notes            TEXT,
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


@pytest.fixture
def con():
    """In-memory DB with just the data_freshness table."""
    c = sqlite3.connect(":memory:")
    c.executescript(DATA_FRESHNESS_SCHEMA)
    yield c
    c.close()


def test_on_conflict_updates_date_column(con):
    """date_column should update when a later call passes a new value.

    The realistic trigger (Phase 0.2 backfill): a row was inserted with
    the DEFAULT date_column='date'; later code calls update_catalog
    with the actual date column (e.g. 'announcement_date'); the row
    should now record the correct column.
    """
    domain = "test_announcements"

    update_catalog(con, domain, source="test", date_column="date")
    update_catalog(con, domain, source="test", date_column="announcement_date")

    row = con.execute(
        "SELECT date_column FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[0] == "announcement_date", (
        f"Bug A reproduced: ON CONFLICT did not update date_column. "
        f"Expected 'announcement_date', got {row[0]!r}. Fix lands in 2.A.2."
    )


def test_on_conflict_updates_source_table(con):
    """source_table has the same SET-clause omission as date_column."""
    domain = "test_st"

    update_catalog(con, domain, source="test", source_table="initial_table")
    update_catalog(con, domain, source="test", source_table="corrected_table")

    row = con.execute(
        "SELECT source_table FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[0] == "corrected_table", (
        f"Bug A reproduced: ON CONFLICT did not update source_table. "
        f"Got {row[0]!r}. Fix lands in 2.A.2."
    )


def test_on_conflict_updates_display_name(con):
    """display_name has the same SET-clause omission as date_column."""
    domain = "test_dn"

    update_catalog(con, domain, source="test", display_name="Initial Name")
    update_catalog(con, domain, source="test", display_name="Corrected Name")

    row = con.execute(
        "SELECT display_name FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[0] == "Corrected Name", (
        f"Bug A reproduced: ON CONFLICT did not update display_name. "
        f"Got {row[0]!r}. Fix lands in 2.A.2."
    )


def test_on_conflict_does_update_other_columns(con):
    """Sanity: ON CONFLICT *does* correctly update status, row_count,
    last_row_date. This test passes today — it's here to prove the bug
    is scoped to the three frozen columns above, not a broader breakage.
    """
    domain = "test_other_cols"

    update_catalog(
        con, domain, source="test",
        latest_date="2026-01-01", row_count=5, status="ok",
    )
    update_catalog(
        con, domain, source="test",
        latest_date="2026-05-23", row_count=42, status="partial",
    )

    row = con.execute(
        "SELECT last_row_date, row_count, status "
        "FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[0] == "2026-05-23"
    assert row[1] == 42
    assert row[2] == "partial"
