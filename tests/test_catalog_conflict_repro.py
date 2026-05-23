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

from pakfindata.db.catalog import update_catalog, update_catalog_from_table


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


def test_update_catalog_from_table_preserves_metadata(con):
    """The helper must NOT clobber source_table/date_column/display_name.

    Regression caught after 2.A.2.1 landed: the ON CONFLICT SET fix made
    those three columns writable for the first time, which exposed a
    pre-existing pass-through gap in `update_catalog_from_table` — it
    read the metadata from the row but didn't forward it to the inner
    `update_catalog` call. Post-2.A.2.1, every successful run of the
    helper overwrote source_table with the dataset_id default,
    date_column with 'date', display_name with a Title Cased fallback.

    Fix lands in 2.A.2.1b: helper forwards all three values it read
    from the row into update_catalog. This test would have caught the
    regression had it existed when 2.A.2.1 was reviewed. Lesson: helper
    functions need their own test coverage — exercising the underlying
    primitive (update_catalog) is not sufficient.
    """
    domain = "test_helper_passthrough"

    # Seed a row that uses a non-default date_column AND a source_table
    # whose name diverges from the dataset_id — both must survive the
    # helper roundtrip.
    update_catalog(
        con, domain,
        source="seed",
        source_table="weird_source_tbl",
        date_column="effective_date",
        display_name="Weird Source",
        latest_date="2026-01-01",
        row_count=1,
    )

    # The source table the helper will issue SELECT MAX(effective_date),
    # COUNT(*) against. One row, one date.
    con.executescript(
        "CREATE TABLE weird_source_tbl (effective_date TEXT, sym TEXT);"
        "INSERT INTO weird_source_tbl VALUES ('2026-05-23', 'X');"
    )

    update_catalog_from_table(con, domain, source="helper_test")

    row = con.execute(
        "SELECT source_table, date_column, display_name, last_row_date, row_count "
        "FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[0] == "weird_source_tbl", (
        f"Helper clobbered source_table: {row[0]!r} (expected 'weird_source_tbl'). "
        f"Fix lands in 2.A.2.1b."
    )
    assert row[1] == "effective_date", (
        f"Helper clobbered date_column: {row[1]!r} (expected 'effective_date'). "
        f"Fix lands in 2.A.2.1b."
    )
    assert row[2] == "Weird Source", (
        f"Helper clobbered display_name: {row[2]!r} (expected 'Weird Source'). "
        f"Fix lands in 2.A.2.1b."
    )
    # Sanity — the helper DID compute fresh latest_date / row_count.
    assert row[3] == "2026-05-23"
    assert row[4] == 1


def test_update_catalog_from_table_failed_branch_preserves_metadata(con):
    """Same guarantee on the helper's failed-query branch.

    If the source table has no `date_column` (schema drift / Bug A
    residue), the helper catches OperationalError and calls
    update_catalog with status='failed'. That branch must also forward
    source_table/date_column/display_name through, otherwise a single
    failed sync would wipe the metadata the operator just fixed.
    """
    domain = "test_helper_failed_branch"

    update_catalog(
        con, domain,
        source="seed",
        source_table="empty_tbl_no_date_col",
        date_column="effective_date",
        display_name="Empty Source",
    )

    # Source table exists but lacks the `effective_date` column the
    # catalog claims — mimics the Bug A residue scenario.
    con.executescript(
        "CREATE TABLE empty_tbl_no_date_col (other_col TEXT);"
    )

    update_catalog_from_table(con, domain, source="helper_test")

    row = con.execute(
        "SELECT source_table, date_column, display_name, status "
        "FROM data_freshness WHERE domain = ?", (domain,)
    ).fetchone()
    assert row[3] == "failed"
    assert row[0] == "empty_tbl_no_date_col", (
        f"Helper failed-branch clobbered source_table: {row[0]!r}."
    )
    assert row[1] == "effective_date", (
        f"Helper failed-branch clobbered date_column: {row[1]!r}."
    )
    assert row[2] == "Empty Source", (
        f"Helper failed-branch clobbered display_name: {row[2]!r}."
    )
