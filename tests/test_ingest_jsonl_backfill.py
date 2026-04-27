"""Integration tests for ingest_jsonl_backfill.

Tests the end-to-end pipeline: JSONL → tick_logs + intraday_bars.
Uses synthetic JSONL files in a temp directory.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import ingest_jsonl_backfill as ijb  # noqa: E402

TICK_LOGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS tick_logs (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL,
    timestamp    REAL NOT NULL,
    _ts          TEXT NOT NULL,
    price        REAL,
    open         REAL, high REAL, low REAL,
    change       REAL, change_pct REAL,
    volume       INTEGER DEFAULT 0,
    value        REAL DEFAULT 0,
    trades       INTEGER DEFAULT 0,
    bid          REAL DEFAULT 0, ask REAL DEFAULT 0,
    bid_vol      INTEGER DEFAULT 0, ask_vol INTEGER DEFAULT 0,
    prev_close   REAL,
    source_file  TEXT,
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, timestamp, price, volume, trades)
);
"""

INTRADAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_bars (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL DEFAULT 'REG',
    date         TEXT NOT NULL,
    ts           TEXT NOT NULL,
    ts_epoch     INTEGER NOT NULL,
    interval     TEXT NOT NULL DEFAULT '1s',
    open         REAL, high REAL, low REAL, close REAL,
    volume       REAL DEFAULT 0,
    value        REAL DEFAULT 0,
    trade_count  INTEGER DEFAULT 0,
    vwap         REAL,
    source       TEXT DEFAULT 'legacy',
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, ts_epoch, interval)
);
"""


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tf:
        path = Path(tf.name)
    con = sqlite3.connect(str(path))
    con.executescript(TICK_LOGS_SCHEMA + INTRADAY_SCHEMA)
    con.close()
    yield str(path)
    path.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


@pytest.fixture
def temp_jsonl_dir(tmp_path, monkeypatch):
    """Temp dir for JSONL files; monkey-patch module constant."""
    monkeypatch.setattr(ijb, "JSONL_DIR", tmp_path)
    return tmp_path


def _make_processed_jsonl(path: Path, ticks: list[dict]) -> None:
    with open(path, "w") as fp:
        for t in ticks:
            fp.write(json.dumps(t) + "\n")


def _make_raw_jsonl(path: Path, ticks: list[dict]) -> None:
    """Wrap ticks as tickUpdate messages with nested tick object."""
    with open(path, "w") as fp:
        for t in ticks:
            msg = {
                "type": "tickUpdate",
                "symbol": t["symbol"],
                "market": t["market"],
                "timestamp": int(t["timestamp"] * 1000),  # raw uses ms
                "tick": {
                    "s": t["symbol"], "m": t["market"],
                    "c": t.get("price"),
                    "o": t.get("open"), "h": t.get("high"), "l": t.get("low"),
                    "v": t.get("volume", 0),
                    "val": t.get("value", 0),
                    "tr": t.get("trades", 0),
                    "bp": t.get("bid", 0), "ap": t.get("ask", 0),
                    "bv": t.get("bidVol", 0), "av": t.get("askVol", 0),
                    "ldcp": t.get("previousClose"),
                },
            }
            fp.write(json.dumps(msg) + "\n")


class TestProcessedJsonl:
    def test_ingests_all_rows(self, temp_db, temp_jsonl_dir):
        # 3 ticks across 2 seconds for 1 symbol
        ticks = [
            {"symbol": "OGDC", "market": "REG", "timestamp": 1776227524.570,
             "_ts": "2026-04-15T09:32:04.706+05:00",
             "price": 302.87, "volume": 1000, "value": 302870.0, "trades": 5,
             "bid": 302.5, "ask": 302.9, "bidVol": 500, "askVol": 600},
            {"symbol": "OGDC", "market": "REG", "timestamp": 1776227524.900,
             "_ts": "2026-04-15T09:32:05.000+05:00",
             "price": 303.0, "volume": 1200, "value": 363600.0, "trades": 7,
             "bid": 302.7, "ask": 303.1, "bidVol": 400, "askVol": 500},
            {"symbol": "OGDC", "market": "REG", "timestamp": 1776227525.200,
             "_ts": "2026-04-15T09:32:05.500+05:00",
             "price": 303.1, "volume": 1500, "value": 454650.0, "trades": 8,
             "bid": 302.8, "ask": 303.2, "bidVol": 600, "askVol": 400},
        ]
        _make_processed_jsonl(temp_jsonl_dir / "ticks_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        stats = ijb.process_date(con, "2026-04-15", dry_run=False)
        con.close()

        assert stats["ticks"] == 3
        assert stats["bars"] == 2  # 2 distinct seconds

    def test_bar_aggregation(self, temp_db, temp_jsonl_dir):
        """3 ticks in 1 second should aggregate to 1 bar with correct OHLC."""
        ticks = [
            {"symbol": "HBL", "market": "REG", "timestamp": 1776227524.100,
             "_ts": "2026-04-15T09:32:04.100+05:00",
             "price": 100.0, "volume": 1000, "value": 100000.0, "trades": 3},
            {"symbol": "HBL", "market": "REG", "timestamp": 1776227524.500,
             "_ts": "2026-04-15T09:32:04.500+05:00",
             "price": 101.5, "volume": 1500, "value": 152250.0, "trades": 5},
            {"symbol": "HBL", "market": "REG", "timestamp": 1776227524.900,
             "_ts": "2026-04-15T09:32:04.900+05:00",
             "price": 100.5, "volume": 1800, "value": 180900.0, "trades": 7},
        ]
        _make_processed_jsonl(temp_jsonl_dir / "ticks_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        ijb.process_date(con, "2026-04-15", dry_run=False)

        # One bar: O=100, H=101.5, L=100, C=100.5; vol=delta=800; trades=delta=4
        row = con.execute(
            "SELECT open, high, low, close, volume, trade_count, vwap "
            "FROM intraday_bars WHERE symbol='HBL' AND ts_epoch=1776227524"
        ).fetchone()
        assert row is not None
        o, h, l, c, v, tc, vwap = row
        assert o == 100.0
        assert h == 101.5
        assert l == 100.0
        assert c == 100.5
        assert v == 800  # cumulative 1800 - 1000
        assert tc == 4   # cumulative 7 - 3
        # vwap = (152250 + 180900 - 100000) / 800 = ...  actual delta calc
        con.close()

    def test_idempotent_rerun(self, temp_db, temp_jsonl_dir):
        ticks = [
            {"symbol": "ABL", "market": "REG", "timestamp": 1776227524.570,
             "_ts": "2026-04-15T09:32:04.706+05:00",
             "price": 50.0, "volume": 500, "value": 25000.0, "trades": 2},
        ]
        _make_processed_jsonl(temp_jsonl_dir / "ticks_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        s1 = ijb.process_date(con, "2026-04-15")
        s2 = ijb.process_date(con, "2026-04-15")  # re-run

        # Second run should be a skip (source_file seen)
        assert s1.get("ticks") == 1
        assert s2.get("skipped") is True

        count = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
        assert count == 1  # no duplicate
        con.close()

    def test_dry_run_no_writes(self, temp_db, temp_jsonl_dir):
        ticks = [
            {"symbol": "X", "market": "REG", "timestamp": 1776227524.1,
             "_ts": "2026-04-15T09:32:04.1+05:00",
             "price": 1.0, "volume": 1, "value": 1.0, "trades": 1},
        ]
        _make_processed_jsonl(temp_jsonl_dir / "ticks_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        stats = ijb.process_date(con, "2026-04-15", dry_run=True)
        assert stats["dry_run"] is True
        assert stats["tick_count"] == 1

        count = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
        assert count == 0
        con.close()

    def test_missing_source_returns_error(self, temp_db, temp_jsonl_dir):
        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        stats = ijb.process_date(con, "2026-04-15", dry_run=False)
        assert "error" in stats
        con.close()


class TestRawJsonlFallback:
    def test_reads_raw_when_processed_missing(self, temp_db, temp_jsonl_dir):
        """If ticks_*.jsonl missing but raw_ws_*.jsonl present, use raw."""
        ticks = [
            {"symbol": "PSO", "market": "REG", "timestamp": 1776227524.100,
             "_ts": "2026-04-15T09:32:04.100+05:00",
             "price": 200.0, "volume": 500, "value": 100000.0, "trades": 3,
             "bid": 199.5, "ask": 200.5, "bidVol": 100, "askVol": 200},
        ]
        # Only raw exists
        _make_raw_jsonl(temp_jsonl_dir / "raw_ws_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        stats = ijb.process_date(con, "2026-04-15", dry_run=False)

        assert stats.get("ticks") == 1
        row = con.execute(
            "SELECT symbol, market, price, volume FROM tick_logs"
        ).fetchone()
        assert row[0] == "PSO"
        assert row[2] == 200.0
        con.close()


class TestMultiMarket:
    def test_reg_and_odl_same_symbol_both_kept(self, temp_db, temp_jsonl_dir):
        """MZNPETF trades in both REG and ODL — v3 PK keeps both as separate bars."""
        ticks = [
            {"symbol": "MZNPETF", "market": "REG", "timestamp": 1776237499.880,
             "_ts": "2026-04-15T12:18:20.021+05:00",
             "price": 20.75, "volume": 750500, "value": 15568440.0, "trades": 291},
            {"symbol": "MZNPETF", "market": "ODL", "timestamp": 1776237499.880,
             "_ts": "2026-04-15T12:18:20.023+05:00",
             "price": 21.0, "volume": 153, "value": 3129.29, "trades": 6},
        ]
        _make_processed_jsonl(temp_jsonl_dir / "ticks_2026-04-15.jsonl", ticks)

        con = sqlite3.connect(temp_db)
        ijb.apply_pragmas(con)
        ijb.process_date(con, "2026-04-15", dry_run=False)

        # Both ticks kept
        tc = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
        assert tc == 2
        # Both bars kept (different market)
        bars = con.execute(
            "SELECT market FROM intraday_bars WHERE symbol='MZNPETF' ORDER BY market"
        ).fetchall()
        assert [r[0] for r in bars] == ["ODL", "REG"]
        con.close()
