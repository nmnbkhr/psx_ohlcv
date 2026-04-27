"""Tests for tick_logs v2 migration (tighter PK)."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from migrate_tick_logs_v2 import migrate, already_migrated  # noqa: E402

OLD_SCHEMA = """
CREATE TABLE IF NOT EXISTS tick_logs (
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL,
    timestamp   REAL NOT NULL,
    _ts         TEXT NOT NULL,
    price       REAL,
    open        REAL, high REAL, low REAL,
    change      REAL, change_pct REAL,
    volume      INTEGER DEFAULT 0,
    value       REAL DEFAULT 0,
    trades      INTEGER DEFAULT 0,
    bid         REAL DEFAULT 0, ask REAL DEFAULT 0,
    bid_vol     INTEGER DEFAULT 0, ask_vol INTEGER DEFAULT 0,
    prev_close  REAL,
    source_file TEXT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, timestamp, price)
);
"""


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tf:
        path = Path(tf.name)
    con = sqlite3.connect(str(path))
    con.executescript(OLD_SCHEMA)
    # Insert 6 rows:
    # - 2 unique rows
    # - 2 rows that are "retransmits" (same symbol/market/ts/price but also same vol/trades)
    # - 2 rows same ts but different volume (sequential trades, both valid)
    rows = [
        # (symbol, market, timestamp, _ts, price, ..., volume, value, trades, ..., source_file)
        ("OGDC", "REG", 1776228754.346, "2026-04-15T09:52:34.492+05:00", 302.5, None, None, None, None, None, 1000, 302500.0, 10, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
        ("OGDC", "REG", 1776228755.000, "2026-04-15T09:52:35.000+05:00", 302.6, None, None, None, None, None, 1200, 363120.0, 11, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
        # Retransmit: identical price AND volume AND trades — should collapse
        ("UBL-APRB", "FUT", 1776229162.955, "2026-04-15T09:59:23.145+05:00", 367.55, None, None, None, None, None, 192000, 70570000.0, 140, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
        ("UBL-APRB", "FUT", 1776229162.955, "2026-04-15T09:59:23.147+05:00", 367.55, None, None, None, None, None, 192000, 70570000.0, 140, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
        # Two sequential trades: same price, different volume → both valid under new PK
        ("SSGC-APR", "FUT", 1776228754.346, "2026-04-15T09:52:34.492+05:00", 27.35, None, None, None, None, None, 2944000, 80505400.0, 803, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
        ("SSGC-APR", "FUT", 1776228754.346, "2026-04-15T09:52:34.492+05:00", 27.35, None, None, None, None, None, 2946000, 80560110.0, 805, None, None, None, None, None, "ticks_2026-04-15.jsonl"),
    ]
    # The 2 SSGC-APR rows will collide on old PK (sym, market, ts, price) — old table collapses to 1
    # Let's insert them one at a time and ignore conflicts (simulating real behavior)
    for r in rows:
        try:
            con.execute("""
                INSERT OR IGNORE INTO tick_logs
                (symbol, market, timestamp, _ts, price, open, high, low, change, change_pct,
                 volume, value, trades, bid, ask, bid_vol, ask_vol, prev_close, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, r)
        except sqlite3.IntegrityError:
            pass
    con.commit()
    con.close()
    yield str(path)
    path.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


class TestTickLogsV2Migration:
    def test_migration_succeeds(self, temp_db):
        rc = migrate(temp_db, dry_run=False)
        assert rc == 0

    def test_idempotent(self, temp_db):
        rc1 = migrate(temp_db, dry_run=False)
        assert rc1 == 0
        rc2 = migrate(temp_db, dry_run=False)
        assert rc2 == 2  # already migrated

    def test_new_pk_includes_volume_and_trades(self, temp_db):
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        assert already_migrated(con)
        con.close()

    def test_retransmit_collapsed(self, temp_db):
        """UBL-APRB had 2 identical rows — should be 1 after migration (INSERT OR IGNORE)."""
        # Before migration, the OLD PK already collapsed UBL-APRB to 1 row
        con = sqlite3.connect(temp_db)
        before = con.execute(
            "SELECT COUNT(*) FROM tick_logs WHERE symbol='UBL-APRB'"
        ).fetchone()[0]
        con.close()
        # The old PK (symbol, market, timestamp, price) matches these 2 rows → 1 stored
        assert before == 1

        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        after = con.execute(
            "SELECT COUNT(*) FROM tick_logs WHERE symbol='UBL-APRB'"
        ).fetchone()[0]
        assert after == 1
        con.close()

    def test_new_pk_allows_same_price_different_volume(self, temp_db):
        """After v2 migration, the new PK allows two rows with same
        (sym, market, ts, price) if (volume, trades) differ.
        """
        migrate(temp_db, dry_run=False)
        con = sqlite3.connect(temp_db)
        con.execute("""
            INSERT INTO tick_logs
            (symbol, market, timestamp, _ts, price, volume, trades, source_file)
            VALUES ('FOO', 'REG', 1776228754.346, '2026-04-15T09:52:34.492+05:00',
                    100.0, 1000, 5, 'test.jsonl')
        """)
        con.execute("""
            INSERT INTO tick_logs
            (symbol, market, timestamp, _ts, price, volume, trades, source_file)
            VALUES ('FOO', 'REG', 1776228754.346, '2026-04-15T09:52:34.492+05:00',
                    100.0, 2000, 6, 'test.jsonl')
        """)
        con.commit()
        count = con.execute("SELECT COUNT(*) FROM tick_logs WHERE symbol='FOO'").fetchone()[0]
        assert count == 2
        con.close()

    def test_dry_run_does_not_modify(self, temp_db):
        con = sqlite3.connect(temp_db)
        before = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
        con.close()

        rc = migrate(temp_db, dry_run=True)
        assert rc == 0

        con = sqlite3.connect(temp_db)
        after = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
        assert after == before
        assert not already_migrated(con)
        con.close()
