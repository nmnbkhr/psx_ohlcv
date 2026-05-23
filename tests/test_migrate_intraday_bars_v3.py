"""Tests for intraday_bars v3 migration script.

Uses a temp SQLite file, populates it with legacy schema + data, runs
the migration, verifies v3 schema + backfilled market + row integrity.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from migrate_intraday_bars_v3 import migrate, is_v3_schema  # noqa: E402

LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_bars (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    ts          TEXT NOT NULL,
    ts_epoch    INTEGER NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      REAL,
    PRIMARY KEY (symbol, ts_epoch)
);
CREATE INDEX idx_ib_date_symbol ON intraday_bars(date, symbol);
CREATE INDEX idx_intraday_bars_ts_epoch ON intraday_bars(ts_epoch);
"""


@pytest.fixture
def temp_db():
    """Temp SQLite file with legacy intraday_bars + sample data."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tf:
        path = Path(tf.name)
    con = sqlite3.connect(str(path))
    con.executescript(LEGACY_SCHEMA)
    # Insert 3 symbols × 3 seconds of bars = 9 rows
    rows = [
        ("OGDC",     "2024-01-15", "2024-01-15 10:00:00", 1705312800, 100.0, 101.0, 99.0, 100.5, 1000),
        ("OGDC",     "2024-01-15", "2024-01-15 10:00:01", 1705312801, 100.5, 101.5, 100.0, 101.0, 1500),
        ("OGDC",     "2024-01-15", "2024-01-15 10:00:02", 1705312802, 101.0, 102.0, 100.5, 101.8, 1200),
        ("OGDC-APR", "2024-01-15", "2024-01-15 10:00:00", 1705312800,  99.5, 100.0, 99.0,  99.8,  500),
        ("OGDC-APR", "2024-01-15", "2024-01-15 10:00:01", 1705312801,  99.8, 100.2, 99.5, 100.0,  600),
        ("UBL-APRB", "2024-01-15", "2024-01-15 10:00:00", 1705312800, 367.0, 368.0, 366.5, 367.55, 200),
        ("KSE100",   "2024-01-15", "2024-01-15 10:00:00", 1705312800, 150000.0, 150100.0, 149900.0, 150050.0, 0),
        ("MZNPETF",  "2024-01-15", "2024-01-15 10:00:00", 1705312800, 20.70, 20.80, 20.65, 20.75, 750000),
        ("MZNPETF",  "2024-01-15", "2024-01-15 10:00:01", 1705312801, 20.75, 20.80, 20.70, 20.78, 800000),
    ]
    con.executemany(
        "INSERT INTO intraday_bars (symbol, date, ts, ts_epoch, open, high, low, close, volume) "
        "VALUES (?,?,?,?,?,?,?,?,?)", rows,
    )
    con.commit()
    con.close()
    yield str(path)
    path.unlink(missing_ok=True)
    # Clean up WAL/SHM files
    for suffix in ("-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


class TestMigration:
    def test_migration_succeeds(self, temp_db):
        rc = migrate(temp_db, dry_run=False)
        assert rc == 0

    def test_legacy_schema_detected_as_not_v3(self, temp_db):
        con = sqlite3.connect(temp_db)
        assert not is_v3_schema(con)
        con.close()

    def test_dry_run_does_not_modify_db(self, temp_db):
        con = sqlite3.connect(temp_db)
        before_count = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
        con.close()

        rc = migrate(temp_db, dry_run=True)
        assert rc == 0

        con = sqlite3.connect(temp_db)
        after_count = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
        assert after_count == before_count
        # Schema unchanged
        assert not is_v3_schema(con)
        con.close()

    def test_new_schema_has_market_column(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        cols = {r[1] for r in con.execute("PRAGMA table_info(intraday_bars)")}
        assert "market" in cols
        assert "interval" in cols
        assert "trade_count" in cols
        assert "vwap" in cols
        assert "source" in cols
        con.close()

    def test_market_backfill_correct(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        markets = dict(con.execute(
            "SELECT symbol, market FROM intraday_bars GROUP BY symbol"
        ).fetchall())
        assert markets["OGDC"] == "REG"
        assert markets["OGDC-APR"] == "FUT"
        assert markets["UBL-APRB"] == "FUT"
        assert markets["KSE100"] == "IDX"
        # MZNPETF cannot be distinguished from symbol alone → REG
        assert markets["MZNPETF"] == "REG"
        con.close()

    def test_all_rows_preserved(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        # Original had 9 rows, all with unique (symbol, ts_epoch) → v3 = 9
        v3_count = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
        assert v3_count == 9
        con.close()

    def test_idempotent(self, temp_db):
        rc1 = migrate(temp_db, dry_run=False)
        assert rc1 == 0
        rc2 = migrate(temp_db, dry_run=False)
        # Second run detects already-migrated → skip
        assert rc2 == 2

    def test_backup_table_created(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        backups = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'intraday_bars_backup_%'"
        )]
        assert len(backups) == 1
        # Backup has original 9 rows
        rows = con.execute(f"SELECT COUNT(*) FROM {backups[0]}").fetchone()[0]
        assert rows == 9
        con.close()

    def test_new_pk_prevents_reg_odl_collision(self, temp_db):
        """After migration, inserting same-ts for different markets must succeed."""
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        # MZNPETF already has ts_epoch=1705312800 with market=REG
        # Now try inserting same symbol+ts with market=ODL
        con.execute(
            "INSERT INTO intraday_bars "
            "(symbol, market, date, ts, ts_epoch, interval, open, high, low, close, volume, source) "
            "VALUES ('MZNPETF','ODL','2024-01-15','2024-01-15 10:00:00',1705312800,'1s',"
            "21.0,21.0,21.0,21.0,153,'test')"
        )
        con.commit()
        rows = con.execute(
            "SELECT market FROM intraday_bars WHERE symbol='MZNPETF' AND ts_epoch=1705312800 "
            "ORDER BY market"
        ).fetchall()
        assert [r[0] for r in rows] == ["ODL", "REG"]
        con.close()

    def test_indexes_preserved_and_renamed(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        idx_names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='intraday_bars' AND sql IS NOT NULL"
        )}
        # Expected v3 indexes
        expected = {
            "idx_ib_date_symbol",
            "idx_ib_symbol_ts",
            "idx_ib_market_date",
            "idx_intraday_bars_ts_epoch",
            "idx_ib_symbol_interval",
        }
        assert expected.issubset(idx_names), f"missing: {expected - idx_names}"
        con.close()


class TestMigrationEdgeCases:
    def test_empty_source_table(self, tmp_path):
        path = tmp_path / "empty.sqlite"
        con = sqlite3.connect(str(path))
        con.executescript(LEGACY_SCHEMA)
        con.close()

        rc = migrate(str(path), dry_run=False)
        assert rc == 0

        con = sqlite3.connect(str(path))
        # intraday_bars should exist with v3 schema
        cols = {r[1] for r in con.execute("PRAGMA table_info(intraday_bars)")}
        assert "market" in cols
        con.close()

    def test_nonexistent_db(self, tmp_path):
        rc = migrate(str(tmp_path / "missing.sqlite"), dry_run=False)
        assert rc == 1
