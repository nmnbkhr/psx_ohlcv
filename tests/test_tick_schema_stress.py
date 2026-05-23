"""Stress / load tests for v3 schema — verifies durability under large batches.

Tagged @pytest.mark.slow so they don't run by default.
Run with:  pytest tests/test_tick_schema_stress.py -v --run-slow
"""

from __future__ import annotations

import json
import random
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import ingest_jsonl_backfill as ijb  # noqa: E402

pytestmark = pytest.mark.slow

TICK_LOGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS tick_logs (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL,
    timestamp    REAL NOT NULL,
    _ts          TEXT NOT NULL,
    price        REAL,
    open REAL, high REAL, low REAL,
    change REAL, change_pct REAL,
    volume       INTEGER DEFAULT 0,
    value        REAL DEFAULT 0,
    trades       INTEGER DEFAULT 0,
    bid REAL DEFAULT 0, ask REAL DEFAULT 0,
    bid_vol INTEGER DEFAULT 0, ask_vol INTEGER DEFAULT 0,
    prev_close REAL,
    source_file TEXT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, timestamp, price, volume, trades)
);
"""

INTRADAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_bars (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL DEFAULT 'REG',
    date TEXT NOT NULL, ts TEXT NOT NULL,
    ts_epoch INTEGER NOT NULL,
    interval TEXT NOT NULL DEFAULT '1s',
    open REAL, high REAL, low REAL, close REAL,
    volume REAL DEFAULT 0, value REAL DEFAULT 0,
    trade_count INTEGER DEFAULT 0,
    vwap REAL,
    source TEXT DEFAULT 'legacy',
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
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
def jsonl_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(ijb, "JSONL_DIR", tmp_path)
    return tmp_path


def _gen_ticks(n_ticks: int, n_symbols: int = 50, start_ts: float = 1776227500.0):
    """Generate synthetic tick data."""
    symbols = [f"SYM{i:03d}" for i in range(n_symbols)]
    for i in range(n_ticks):
        sym = symbols[i % n_symbols]
        # Spread ticks across ~1 hour (3600 seconds)
        ts = start_ts + (i / n_ticks) * 3600
        price = 100.0 + random.uniform(-5, 5)
        yield {
            "symbol": sym, "market": "REG",
            "timestamp": ts,
            "_ts": f"2026-04-15T09:32:{i%60:02d}+05:00",
            "price": round(price, 2),
            "volume": 1000 + i * 10,
            "value": round(price * (1000 + i * 10), 2),
            "trades": i // 10,
            "bid": round(price - 0.1, 2), "ask": round(price + 0.1, 2),
            "bidVol": 500, "askVol": 500,
        }


def test_100k_rows_ingest_performance(temp_db, jsonl_dir):
    """Ingest 100K ticks and verify throughput + row count."""
    N = 100_000
    path = jsonl_dir / "ticks_2026-04-15.jsonl"
    with open(path, "w") as fp:
        for t in _gen_ticks(N):
            fp.write(json.dumps(t) + "\n")

    con = sqlite3.connect(temp_db)
    ijb.apply_pragmas(con)
    t0 = time.time()
    stats = ijb.process_date(con, "2026-04-15", dry_run=False)
    elapsed = time.time() - t0

    assert stats["ticks"] == N
    # Expect at least 5K ticks/sec on modest hardware — set a lenient bound
    rate = N / elapsed
    assert rate > 5_000, f"ingest rate too slow: {rate:.0f} rows/s"
    print(f"\n  ingested {N:,} ticks in {elapsed:.1f}s ({rate:,.0f} rows/s)")
    con.close()


def test_retransmit_dedupe_stress(temp_db, jsonl_dir):
    """Send same tick 3x — only 1 should land."""
    tick = {
        "symbol": "OGDC", "market": "REG", "timestamp": 1776227524.100,
        "_ts": "2026-04-15T09:32:04.100+05:00",
        "price": 302.87, "volume": 1000, "value": 302870.0, "trades": 5,
    }
    path = jsonl_dir / "ticks_2026-04-15.jsonl"
    # Write the same tick 100 times
    with open(path, "w") as fp:
        for _ in range(100):
            fp.write(json.dumps(tick) + "\n")

    con = sqlite3.connect(temp_db)
    ijb.apply_pragmas(con)
    stats = ijb.process_date(con, "2026-04-15", dry_run=False)

    # 100 attempted, 1 unique row (all had identical (sym, mkt, ts, price, vol, trades))
    assert stats["ticks"] == 1
    count = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
    assert count == 1
    con.close()


def test_concurrent_writers_wal_safety(temp_db, jsonl_dir):
    """Two parallel inserters with WAL — both should succeed without corruption."""
    import threading

    con_setup = sqlite3.connect(temp_db)
    ijb.apply_pragmas(con_setup)
    con_setup.close()

    errors = []

    def worker(sym_prefix: str, count: int):
        try:
            con = sqlite3.connect(temp_db, timeout=30)
            ijb.apply_pragmas(con)
            rows = []
            for i in range(count):
                rows.append((
                    f"{sym_prefix}{i}", "REG", 1776227500.0 + i,
                    "2026-04-15T09:32:04+05:00",
                    100.0, None, None, None, None, None,
                    1000, 100000.0, 1, 99.0, 101.0, 500, 500, None, "test.jsonl",
                ))
            con.executemany(ijb.TICK_INSERT_SQL, rows)
            con.commit()
            con.close()
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=("A", 5000)),
        threading.Thread(target=worker, args=("B", 5000)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent writer errors: {errors}"
    con = sqlite3.connect(temp_db)
    total = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
    assert total == 10000
    con.close()


def test_bar_aggregation_correctness_under_load(temp_db, jsonl_dir):
    """Verify bar math is exact even at scale."""
    # Generate known data: 1 symbol, 10 ticks in a single second
    start_ts = 1776227500.0
    ticks = []
    total_vol = 0
    total_val = 0.0
    total_trades = 0
    prices = []
    for i in range(10):
        price = 100.0 + i
        vol = 1000 + i * 100  # cumulative
        val = 500_000.0 + i * 50_000  # cumulative
        trades = 10 + i * 2  # cumulative
        ticks.append({
            "symbol": "TEST", "market": "REG",
            "timestamp": start_ts + i * 0.05,  # all within same second
            "_ts": f"2026-04-15T09:32:0{i}+05:00",
            "price": price, "volume": vol, "value": val, "trades": trades,
        })
        prices.append(price)
        total_vol = vol
        total_val = val
        total_trades = trades

    path = jsonl_dir / "ticks_2026-04-15.jsonl"
    with open(path, "w") as fp:
        for t in ticks:
            fp.write(json.dumps(t) + "\n")

    con = sqlite3.connect(temp_db)
    ijb.apply_pragmas(con)
    ijb.process_date(con, "2026-04-15", dry_run=False)

    row = con.execute(
        "SELECT open, high, low, close, volume, trade_count FROM intraday_bars "
        "WHERE symbol='TEST' AND ts_epoch=?", [int(start_ts)]
    ).fetchone()
    assert row is not None
    o, h, l, c, v, tc = row
    assert o == 100.0                    # first price
    assert h == max(prices)              # 109
    assert l == min(prices)              # 100
    assert c == prices[-1]               # 109 (last)
    # volume delta = last - first
    assert v == total_vol - 1000
    assert tc == total_trades - 10
    con.close()
