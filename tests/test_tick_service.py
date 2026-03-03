"""Tests for tick_service: BarBuilder, TickService (memory-only), message parsing, ws_relay."""

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pakfindata.services.tick_service import (
    BAR_INTERVAL,
    BarBuilder,
    TickService,
    TickServiceStatus,
    _get_ram_mb,
    _parse_ws_batch,
    _parse_ws_message,
    _safe_float,
    _safe_int,
)
from pakfindata.services.ws_relay import (
    ConnectionManager,
    broadcast_tick,
    manager,
    set_collector,
)

PKT = timezone(timedelta(hours=5))


# =========================================================================
# BarBuilder tests
# =========================================================================

class TestBarBuilder:
    def test_first_tick_creates_bar(self):
        bb = BarBuilder(interval_seconds=5)
        tick = {"symbol": "HBL", "market": "REG", "price": 100.0,
                "volume": 1000, "timestamp": 1700000000}
        completed = bb.process_tick(tick)
        assert completed == []
        assert len(bb.bars) == 1
        bar = list(bb.bars.values())[0]
        assert bar["open"] == 100.0
        assert bar["high"] == 100.0
        assert bar["low"] == 100.0
        assert bar["close"] == 100.0
        assert bar["trades"] == 1

    def test_same_bucket_updates_ohlcv(self):
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "volume": 1000, "timestamp": ts})
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 105.0, "volume": 2000, "timestamp": ts + 1})
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 98.0, "volume": 3000, "timestamp": ts + 2})

        bar = list(bb.bars.values())[0]
        assert bar["open"] == 100.0
        assert bar["high"] == 105.0
        assert bar["low"] == 98.0
        assert bar["close"] == 98.0
        assert bar["trades"] == 3

    def test_new_bucket_completes_old(self):
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "volume": 1000, "timestamp": ts})

        # Jump ahead 10 seconds (new bucket)
        completed = bb.process_tick({"symbol": "HBL", "market": "REG",
                                      "price": 102.0, "volume": 2000,
                                      "timestamp": ts + 10})
        assert len(completed) == 1
        assert completed[0]["close"] == 100.0

        # New bar is active
        assert len(bb.bars) == 1
        bar = list(bb.bars.values())[0]
        assert bar["open"] == 102.0

    def test_different_symbols_separate_bars(self):
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "timestamp": ts})
        bb.process_tick({"symbol": "OGDC", "market": "REG",
                         "price": 50.0, "timestamp": ts})
        assert len(bb.bars) == 2

    def test_different_markets_separate_bars(self):
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "timestamp": ts})
        bb.process_tick({"symbol": "HBL", "market": "FUT",
                         "price": 101.0, "timestamp": ts})
        assert len(bb.bars) == 2

    def test_flush_stale(self):
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "timestamp": ts})
        assert len(bb.bars) == 1

        # Flush with a very short cutoff (everything is stale vs now)
        stale = bb.flush_stale(cutoff_seconds=0)
        assert len(stale) == 1
        assert stale[0]["symbol"] == "HBL"
        assert len(bb.bars) == 0

    def test_bucket_quantization(self):
        bb = BarBuilder(interval_seconds=5)
        # Timestamps at second 7 and 8 should be in same 5-second bucket (5-9)
        ts_base = 1700000007  # second 7
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "timestamp": ts_base})
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 101.0, "timestamp": ts_base + 1})
        assert len(bb.bars) == 1
        bar = list(bb.bars.values())[0]
        assert bar["trades"] == 2

    def test_flush_all(self):
        """flush_all() closes ALL open bars regardless of age."""
        bb = BarBuilder(interval_seconds=5)
        ts = 1700000000
        # Create bars for 3 different symbols
        bb.process_tick({"symbol": "HBL", "market": "REG",
                         "price": 100.0, "timestamp": ts})
        bb.process_tick({"symbol": "OGDC", "market": "REG",
                         "price": 50.0, "timestamp": ts})
        bb.process_tick({"symbol": "FFC", "market": "REG",
                         "price": 75.0, "timestamp": ts})
        assert len(bb.bars) == 3

        all_bars = bb.flush_all()
        assert len(all_bars) == 3
        assert len(bb.bars) == 0
        symbols = {b["symbol"] for b in all_bars}
        assert symbols == {"HBL", "OGDC", "FFC"}


# =========================================================================
# TickService tests (memory-only architecture)
# =========================================================================

class TestTickService:
    @pytest.fixture
    def tmp_db(self, tmp_path):
        return tmp_path / "test.sqlite"

    def test_init_no_db(self, tmp_db):
        """Memory-only: no DB file created at init."""
        svc = TickService(db_path=tmp_db)
        assert not tmp_db.exists()
        assert svc.tick_count == 0
        assert svc.raw_ticks == []
        assert svc.completed_bars == []

    def test_process_updates_live(self, tmp_db):
        svc = TickService(db_path=tmp_db)
        svc.process({
            "symbol": "HBL", "market": "REG", "price": 100.0,
            "change": 2.0, "changePercent": 0.5, "volume": 1000,
        })
        assert svc.tick_count == 1
        assert "REG:HBL" in svc.live
        assert svc.live["REG:HBL"]["price"] == 100.0

    def test_process_stores_raw_tick(self, tmp_db):
        """Every tick is stored in raw_ticks list."""
        svc = TickService(db_path=tmp_db)
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0})
        svc.process({"symbol": "OGDC", "market": "REG", "price": 50.0})
        assert len(svc.raw_ticks) == 2
        assert svc.raw_ticks[0]["symbol"] == "HBL"

    def test_process_ignores_no_price(self, tmp_db):
        svc = TickService(db_path=tmp_db)
        svc.process({"symbol": "HBL", "market": "REG"})
        assert svc.tick_count == 0
        assert len(svc.raw_ticks) == 0

    def test_completed_bars_in_memory(self, tmp_db):
        """Completed bars accumulate in memory, not written to DB."""
        svc = TickService(db_path=tmp_db)
        ts = 1700000000
        svc.process({"symbol": "HBL", "market": "REG",
                      "price": 100.0, "volume": 1000, "timestamp": ts})
        # Jump ahead → completes first bar
        svc.process({"symbol": "HBL", "market": "REG",
                      "price": 102.0, "volume": 2000, "timestamp": ts + 10})
        assert len(svc.completed_bars) == 1
        assert svc.completed_bars[0]["close"] == 100.0
        assert not tmp_db.exists()  # no DB writes yet

    def test_write_snapshot(self, tmp_db, tmp_path):
        svc = TickService(db_path=tmp_db)
        svc.process({
            "symbol": "HBL", "market": "REG", "price": 100.0,
            "changePercent": 1.5, "volume": 1000,
        })
        svc.process({
            "symbol": "OGDC", "market": "REG", "price": 50.0,
            "changePercent": -0.5, "volume": 500,
        })

        snap_path = tmp_path / "snapshot.json"
        with patch("pakfindata.services.tick_service.SNAPSHOT_PATH", snap_path):
            svc.write_snapshot()

        assert snap_path.exists()
        data = json.loads(snap_path.read_text())
        assert data["symbol_count"] == 2
        assert data["tick_count"] == 2
        assert data["breadth"]["gainers"] == 1
        assert data["breadth"]["losers"] == 1
        # Memory-only fields
        assert "bars_in_memory" in data
        assert "raw_ticks_in_memory" in data
        assert data["raw_ticks_in_memory"] == 2
        assert "ram_mb" in data

    def test_breadth_computation(self, tmp_db, tmp_path):
        svc = TickService(db_path=tmp_db)
        for sym, chg in [("A", 1.0), ("B", 2.0), ("C", 0.5),
                         ("D", -1.0), ("E", -0.3), ("F", 0.0)]:
            svc.process({
                "symbol": sym, "market": "REG",
                "price": 100.0, "changePercent": chg,
            })

        snap_path = tmp_path / "snapshot.json"
        with patch("pakfindata.services.tick_service.SNAPSHOT_PATH", snap_path):
            svc.write_snapshot()

        data = json.loads(snap_path.read_text())
        assert data["breadth"]["gainers"] == 3
        assert data["breadth"]["losers"] == 2
        assert data["breadth"]["unchanged"] == 1

    def test_top_movers(self, tmp_db, tmp_path):
        svc = TickService(db_path=tmp_db)
        for i in range(15):
            svc.process({
                "symbol": f"SYM{i}", "market": "REG",
                "price": 100.0 + i, "changePercent": i - 7,
                "volume": i * 1000,
            })

        snap_path = tmp_path / "snapshot.json"
        with patch("pakfindata.services.tick_service.SNAPSHOT_PATH", snap_path):
            svc.write_snapshot()

        data = json.loads(snap_path.read_text())
        assert len(data["top_gainers"]) == 10
        assert len(data["top_losers"]) == 10
        assert len(data["most_active"]) == 10
        assert data["top_gainers"][0]["changePercent"] > data["top_gainers"][-1]["changePercent"]

    def test_eod_flush(self, tmp_db):
        """eod_flush() writes bars + raw ticks to DB then clears memory."""
        svc = TickService(db_path=tmp_db)
        ts = 1700000000
        # Generate ticks that produce completed bars
        svc.process({"symbol": "HBL", "market": "REG",
                      "price": 100.0, "volume": 1000, "timestamp": ts})
        svc.process({"symbol": "HBL", "market": "REG",
                      "price": 102.0, "volume": 2000, "timestamp": ts + 10})
        assert len(svc.raw_ticks) == 2
        assert len(svc.completed_bars) >= 1

        svc.eod_flush()

        # Data in DB
        con = sqlite3.connect(str(tmp_db))
        bars = con.execute("SELECT * FROM ohlcv_5s").fetchall()
        assert len(bars) >= 1
        ticks = con.execute("SELECT * FROM raw_ticks").fetchall()
        assert len(ticks) == 2
        con.close()

        # Memory cleared
        assert svc.raw_ticks == []
        assert svc.completed_bars == []
        assert svc.tick_count == 0
        assert svc.live == {}

    def test_eod_flush_empty(self, tmp_db):
        """eod_flush() with no data does nothing."""
        svc = TickService(db_path=tmp_db)
        svc.eod_flush()
        assert not tmp_db.exists()

    def test_eod_flush_dedup(self, tmp_db):
        """eod_flush() deduplicates bars by (symbol, market, ts)."""
        svc = TickService(db_path=tmp_db)
        # Add duplicate bars manually
        bar = {
            "symbol": "HBL", "market": "REG",
            "timestamp": "2026-02-20T10:00:00+05:00",
            "open": 100.0, "high": 105.0, "low": 99.0, "close": 102.0,
            "volume": 1000, "trades": 5,
        }
        svc.completed_bars = [bar, bar.copy()]
        svc.raw_ticks = [{"symbol": "HBL", "market": "REG", "price": 100.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(tmp_db))
        rows = con.execute("SELECT COUNT(*) FROM ohlcv_5s").fetchone()[0]
        assert rows == 1  # deduped
        con.close()

    def test_get_status_line(self, tmp_db):
        svc = TickService(db_path=tmp_db)
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0})
        line = svc.get_status_line()
        assert "Ticks: 1" in line
        assert "Symbols: 1" in line
        assert "RAM:" in line


# =========================================================================
# Message parsing tests
# =========================================================================

class TestMessageParsing:
    def test_flat_tick(self):
        msg = json.dumps({"symbol": "HBL", "price": 100.0, "volume": 1000})
        tick = _parse_ws_message(msg)
        assert tick is not None
        assert tick["symbol"] == "HBL"
        assert tick["price"] == 100.0

    def test_wrapped_tick(self):
        msg = json.dumps({
            "type": "update",
            "data": {"symbol": "OGDC", "price": 50.5, "volume": 2000}
        })
        tick = _parse_ws_message(msg)
        assert tick is not None
        assert tick["symbol"] == "OGDC"
        assert tick["price"] == 50.5

    def test_alternative_field_names(self):
        msg = json.dumps({"sym": "ffc", "last": 120.0, "vol": 5000,
                          "mkt": "fut", "changePct": -1.5})
        tick = _parse_ws_message(msg)
        assert tick is not None
        assert tick["symbol"] == "FFC"
        assert tick["market"] == "FUT"
        assert tick["price"] == 120.0
        assert tick["changePercent"] == -1.5

    def test_no_price_returns_none(self):
        msg = json.dumps({"symbol": "HBL", "volume": 1000})
        assert _parse_ws_message(msg) is None

    def test_no_symbol_returns_none(self):
        msg = json.dumps({"price": 100.0, "volume": 1000})
        assert _parse_ws_message(msg) is None

    def test_invalid_json_returns_none(self):
        assert _parse_ws_message("not json") is None

    def test_zero_price_returns_none(self):
        msg = json.dumps({"symbol": "HBL", "price": 0})
        assert _parse_ws_message(msg) is None

    def test_negative_price_returns_none(self):
        msg = json.dumps({"symbol": "HBL", "price": -5.0})
        assert _parse_ws_message(msg) is None

    def test_batch_parse(self):
        msg = json.dumps({
            "type": "batch",
            "data": [
                {"symbol": "HBL", "price": 100.0},
                {"symbol": "OGDC", "price": 50.0},
            ]
        })
        ticks = _parse_ws_batch(msg)
        assert len(ticks) == 2
        assert ticks[0]["symbol"] == "HBL"
        assert ticks[1]["symbol"] == "OGDC"

    def test_batch_skips_invalid(self):
        msg = json.dumps({
            "data": [
                {"symbol": "HBL", "price": 100.0},
                {"symbol": "BAD"},  # no price
                {"price": 50.0},    # no symbol
            ]
        })
        ticks = _parse_ws_batch(msg)
        assert len(ticks) == 1

    def test_batch_empty(self):
        assert _parse_ws_batch("not json") == []
        assert _parse_ws_batch(json.dumps({"data": []})) == []


# =========================================================================
# Helper function tests
# =========================================================================

class TestHelpers:
    def test_safe_float(self):
        assert _safe_float(100.5) == 100.5
        assert _safe_float("1,234.56") == 1234.56
        assert _safe_float(None) == 0.0
        assert _safe_float("") == 0.0
        assert _safe_float("--") == 0.0
        assert _safe_float("abc") == 0.0

    def test_safe_int(self):
        assert _safe_int(100) == 100
        assert _safe_int("1,234") == 1234
        assert _safe_int(None) == 0
        assert _safe_int("") == 0
        assert _safe_int("--") == 0

    def test_status_dataclass(self):
        status = TickServiceStatus(
            running=True, pid=1234,
            tick_count=100, symbol_count=50,
            bars_in_memory=200, raw_ticks_in_memory=5000,
            ram_mb=45.3,
        )
        d = status.to_dict()
        assert d["running"] is True
        assert d["pid"] == 1234
        assert d["bars_in_memory"] == 200
        assert d["raw_ticks_in_memory"] == 5000
        assert d["ram_mb"] == 45.3

        restored = TickServiceStatus.from_dict(d)
        assert restored.running is True
        assert restored.tick_count == 100
        assert restored.bars_in_memory == 200

    def test_get_ram_mb(self):
        """_get_ram_mb() returns a non-negative float."""
        ram = _get_ram_mb()
        assert isinstance(ram, float)
        assert ram >= 0.0


# =========================================================================
# EOD DB schema tests
# =========================================================================

class TestEODSchema:
    def test_eod_creates_tables(self, tmp_path):
        """eod_flush creates ohlcv_5s and raw_ticks tables."""
        db_path = tmp_path / "test.sqlite"
        svc = TickService(db_path=db_path)
        # Add minimal data so flush actually runs
        svc.raw_ticks = [{"symbol": "HBL", "market": "REG", "price": 100.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(db_path))
        tables = {
            r[0] for r in
            con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "ohlcv_5s" in tables
        assert "raw_ticks" in tables
        con.close()

    def test_eod_indexes(self, tmp_path):
        """eod_flush creates indexes on both tables."""
        db_path = tmp_path / "test.sqlite"
        svc = TickService(db_path=db_path)
        svc.raw_ticks = [{"symbol": "HBL", "market": "REG", "price": 100.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(db_path))
        indexes = {
            r[0] for r in
            con.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_bar_sym_ts" in indexes
        assert "idx_tick_sym_ts" in indexes
        con.close()

    def test_raw_ticks_schema(self, tmp_path):
        """raw_ticks table has expected columns."""
        db_path = tmp_path / "test.sqlite"
        svc = TickService(db_path=db_path)
        svc.raw_ticks = [{"symbol": "X", "market": "REG", "price": 1.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(db_path))
        info = con.execute("PRAGMA table_info(raw_ticks)").fetchall()
        cols = {row[1] for row in info}
        assert "symbol" in cols
        assert "market" in cols
        assert "ts" in cols
        assert "price" in cols
        assert "volume" in cols
        assert "bid" in cols
        assert "ask" in cols
        con.close()

    def test_eod_creates_index_tables(self, tmp_path):
        """eod_flush creates index_ohlcv_5s and index_raw_ticks tables."""
        db_path = tmp_path / "test.sqlite"
        svc = TickService(db_path=db_path)
        svc.index_ticks = [{"symbol": "KSE100", "price": 85000.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(db_path))
        tables = {
            r[0] for r in
            con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "index_ohlcv_5s" in tables
        assert "index_raw_ticks" in tables
        con.close()

    def test_eod_index_indexes(self, tmp_path):
        """eod_flush creates indexes on index tables."""
        db_path = tmp_path / "test.sqlite"
        svc = TickService(db_path=db_path)
        svc.index_ticks = [{"symbol": "KSE100", "price": 85000.0}]
        svc.eod_flush()

        con = sqlite3.connect(str(db_path))
        indexes = {
            r[0] for r in
            con.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_idxbar_sym_ts" in indexes
        assert "idx_idxtick_sym_ts" in indexes
        con.close()


# =========================================================================
# Index tracking tests
# =========================================================================

class TestIndexTracking:
    @pytest.fixture
    def tmp_db(self, tmp_path):
        return tmp_path / "test.sqlite"

    def test_idx_tick_routes_to_indices(self, tmp_db):
        """IDX market ticks go to self.indices, not self.live."""
        svc = TickService(db_path=tmp_db)
        svc.process({
            "symbol": "KSE100", "market": "IDX", "price": 85000.0,
            "change": 500.0, "changePercent": 0.006, "volume": 100000,
        })
        assert svc.tick_count == 1
        assert "KSE100" in svc.indices
        assert svc.indices["KSE100"]["value"] == 85000.0
        # Not in live (stock) dict
        assert "IDX:KSE100" not in svc.live
        # Stored in index_ticks, not raw_ticks
        assert len(svc.index_ticks) == 1
        assert len(svc.raw_ticks) == 0

    def test_stock_tick_not_in_indices(self, tmp_db):
        """Stock ticks don't go to indices."""
        svc = TickService(db_path=tmp_db)
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0})
        assert len(svc.indices) == 0
        assert len(svc.raw_ticks) == 1

    def test_idx_tick_builds_bars(self, tmp_db):
        """IDX ticks feed into BarBuilder with market=IDX."""
        svc = TickService(db_path=tmp_db)
        ts = 1700000000
        svc.process({
            "symbol": "KSE100", "market": "IDX", "price": 85000.0,
            "timestamp": ts,
        })
        # Check builder has an IDX bar
        idx_bars = [
            k for k in svc.builder.bars if k[1] == "IDX"
        ]
        assert len(idx_bars) == 1

    def test_idx_sparkline_history(self, tmp_db):
        """Index ticks accumulate in index_history deque."""
        svc = TickService(db_path=tmp_db)
        ts = 1700000000
        for i in range(5):
            svc.process({
                "symbol": "KSE100", "market": "IDX",
                "price": 85000.0 + i * 10,
                "timestamp": ts + i,
            })
        assert "KSE100" in svc.index_history
        assert len(svc.index_history["KSE100"]) == 5

    def test_build_sparklines(self, tmp_db):
        """_build_sparklines() downsamples to 5-min buckets."""
        svc = TickService(db_path=tmp_db)
        from collections import deque
        svc.index_history["KSE100"] = deque(maxlen=360)
        # Add entries in different 5-min buckets
        base_ts = 1700000000
        for i in range(3):
            svc.index_history["KSE100"].append({
                "ts": base_ts + i * 300,  # each 5 min apart
                "value": 85000.0 + i * 100,
            })
        svc._build_sparklines()
        assert "KSE100" in svc.index_sparklines
        assert len(svc.index_sparklines["KSE100"]) == 3

    def test_snapshot_includes_indices(self, tmp_db, tmp_path):
        """write_snapshot() includes indices and index_sparklines."""
        svc = TickService(db_path=tmp_db)
        svc.process({
            "symbol": "KSE100", "market": "IDX", "price": 85000.0,
            "change": 500, "changePercent": 0.006,
        })
        svc.process({
            "symbol": "KSE30", "market": "IDX", "price": 45000.0,
            "change": -200, "changePercent": -0.004,
        })

        snap_path = tmp_path / "snapshot.json"
        with patch("pakfindata.services.tick_service.SNAPSHOT_PATH", snap_path):
            svc.write_snapshot()

        data = json.loads(snap_path.read_text())
        assert data["index_count"] == 2
        assert len(data["indices"]) == 2
        assert "index_sparklines" in data
        # Indices should have sparkline field
        kse = next(i for i in data["indices"] if i["symbol"] == "KSE100")
        assert "sparkline" in kse
        # Stock symbols list should NOT contain index entries
        sym_markets = {s["market"] for s in data.get("symbols", [])}
        assert "IDX" not in sym_markets

    def test_eod_flush_index_data(self, tmp_db):
        """eod_flush() writes index bars and raw ticks to separate tables."""
        svc = TickService(db_path=tmp_db)
        ts = 1700000000
        # Generate index ticks
        svc.process({
            "symbol": "KSE100", "market": "IDX", "price": 85000.0,
            "timestamp": ts,
        })
        svc.process({
            "symbol": "KSE100", "market": "IDX", "price": 85100.0,
            "timestamp": ts + 10,
        })
        # Also a stock tick
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0,
                      "timestamp": ts})

        svc.eod_flush()

        con = sqlite3.connect(str(tmp_db))
        # Index raw ticks
        idx_ticks = con.execute("SELECT * FROM index_raw_ticks").fetchall()
        assert len(idx_ticks) == 2
        # Stock raw ticks
        stock_ticks = con.execute("SELECT * FROM raw_ticks").fetchall()
        assert len(stock_ticks) == 1
        # Index bars (completed from bucket rollover)
        idx_bars = con.execute("SELECT * FROM index_ohlcv_5s").fetchall()
        assert len(idx_bars) >= 1
        con.close()

        # Memory cleared
        assert svc.indices == {}
        assert svc.index_ticks == []
        assert svc.index_history == {}
        assert svc.index_sparklines == {}

    def test_status_line_includes_indices(self, tmp_db):
        """get_status_line() shows index count."""
        svc = TickService(db_path=tmp_db)
        svc.process({"symbol": "KSE100", "market": "IDX", "price": 85000.0})
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0})
        line = svc.get_status_line()
        assert "Indices: 1" in line
        assert "Symbols: 1" in line
        assert "WS Clients:" in line


# =========================================================================
# WebSocket relay tests
# =========================================================================

class TestConnectionManager:
    @pytest.fixture
    def mgr(self):
        return ConnectionManager()

    def test_initial_state(self, mgr):
        assert mgr.client_count == 0
        assert mgr.channel_stats == {}

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, mgr):
        ws = AsyncMock()
        await mgr.connect(ws, "ticks:all")
        assert mgr.client_count == 1
        assert "ticks:all" in mgr.channel_stats
        assert mgr.channel_stats["ticks:all"] == 1

        await mgr.disconnect(ws, "ticks:all")
        assert mgr.client_count == 0

    @pytest.mark.asyncio
    async def test_multiple_channels(self, mgr):
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        ws3 = AsyncMock()
        await mgr.connect(ws1, "ticks:all")
        await mgr.connect(ws2, "indices")
        await mgr.connect(ws3, "firehose")
        assert mgr.client_count == 3
        assert len(mgr.channel_stats) == 3

    @pytest.mark.asyncio
    async def test_broadcast_sends_to_channel(self, mgr):
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        await mgr.connect(ws1, "ticks:all")
        await mgr.connect(ws2, "indices")

        await mgr.broadcast("ticks:all", {"type": "tick", "data": {"symbol": "HBL"}})
        ws1.send_text.assert_called_once()
        ws2.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_cleans_dead(self, mgr):
        ws = AsyncMock()
        ws.send_text.side_effect = Exception("connection closed")
        await mgr.connect(ws, "ticks:all")
        assert mgr.client_count == 1

        await mgr.broadcast("ticks:all", {"data": "test"})
        assert mgr.client_count == 0  # dead connection removed

    @pytest.mark.asyncio
    async def test_broadcast_empty_channel(self, mgr):
        # Should not raise
        await mgr.broadcast("nonexistent", {"data": "test"})


class TestBroadcastTick:
    def test_broadcast_no_loop_noop(self):
        """broadcast_tick does nothing when relay loop is not set."""
        import pakfindata.services.ws_relay as relay
        old_loop = relay._loop
        relay._loop = None
        try:
            broadcast_tick({"symbol": "HBL", "price": 100}, "REG", "HBL")
            # Should not raise
        finally:
            relay._loop = old_loop

    def test_broadcast_no_clients_noop(self):
        """broadcast_tick skips when no clients connected."""
        import pakfindata.services.ws_relay as relay
        old_loop = relay._loop
        relay._loop = MagicMock()
        # manager has no clients (client_count == 0)
        try:
            broadcast_tick({"symbol": "HBL", "price": 100}, "REG", "HBL")
            # Should not raise and should skip (no coroutines scheduled)
        finally:
            relay._loop = old_loop


class TestSetCollector:
    def test_set_collector(self):
        import pakfindata.services.ws_relay as relay
        old = relay._collector
        try:
            mock_svc = MagicMock()
            mock_svc.live = {"REG:HBL": {"symbol": "HBL", "price": 100}}
            set_collector(mock_svc)
            assert relay._collector is mock_svc
        finally:
            relay._collector = old


class TestTickServiceRelay:
    """Test tick_service integration with ws_relay."""

    def test_has_relay_flag(self):
        """HAS_RELAY should be True when ws_relay is importable."""
        from pakfindata.services.tick_service import HAS_RELAY
        assert HAS_RELAY is True

    def test_status_line_ws_clients(self, tmp_path):
        """Status line includes WS Clients count."""
        svc = TickService(db_path=tmp_path / "test.sqlite")
        svc.process({"symbol": "HBL", "market": "REG", "price": 100.0})
        line = svc.get_status_line()
        assert "WS Clients: 0" in line
