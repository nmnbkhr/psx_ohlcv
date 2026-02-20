"""PSX Live Tick Service — connects to psxterminal.com WebSocket,
builds 5-second OHLCV bars, writes live snapshot for Streamlit.

MEMORY-ONLY during market hours. Single EOD flush to SQLite at 15:35 PKT.
Zero DB writes during trading.

Usage:
    python -m psx_ohlcv.services.tick_service              # foreground
    python -m psx_ohlcv.services.tick_service --daemon      # background
    python -m psx_ohlcv.services.tick_service --debug       # print raw WS messages
"""

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# WebSocket relay — real-time push to clients
try:
    from psx_ohlcv.services.ws_relay import (
        broadcast_tick as _broadcast_tick,
        manager as _relay_manager,
        set_collector as _relay_set_collector,
        start_server as _start_relay,
    )
    HAS_RELAY = True
except ImportError:
    HAS_RELAY = False

PKT = timezone(timedelta(hours=5))
MARKETS = ["REG", "FUT", "ODL", "BNB", "IDX"]
BAR_INTERVAL = 5        # seconds per OHLCV bar
SNAPSHOT_INTERVAL = 2    # seconds between snapshot writes
STATUS_PRINT_INTERVAL = 30  # console status line interval
WSS_URL = "wss://psxterminal.com/"

# Paths — derived from config at import time
try:
    from psx_ohlcv.config import DATA_ROOT, DEFAULT_DB_PATH
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")
    DEFAULT_DB_PATH = DATA_ROOT / "psx.sqlite"

SERVICE_DIR = DATA_ROOT / "services"
PID_FILE = SERVICE_DIR / "tick_service.pid"
STATUS_FILE = SERVICE_DIR / "tick_service_status.json"
SNAPSHOT_PATH = DATA_ROOT / "live_snapshot.json"
EOD_DB_PATH = DATA_ROOT / "tick_bars.db"


# =========================================================================
# RAM estimation
# =========================================================================

def _get_ram_mb() -> float:
    """Get current process RSS in MB. Falls back to estimate."""
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    # Linux fallback: /proc/self/status
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # kB → MB
    except (IOError, ValueError):
        pass
    return 0.0


# =========================================================================
# Status dataclass (same pattern as eod_sync_service)
# =========================================================================

@dataclass
class TickServiceStatus:
    running: bool = False
    pid: int | None = None
    started_at: str | None = None
    connected: bool = False
    tick_count: int = 0
    bars_in_memory: int = 0
    raw_ticks_in_memory: int = 0
    symbol_count: int = 0
    ram_mb: float = 0.0
    last_tick: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TickServiceStatus":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# =========================================================================
# Service management (PID file based)
# =========================================================================

def ensure_service_dir():
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def write_status(status: TickServiceStatus):
    ensure_service_dir()
    with open(STATUS_FILE, "w") as f:
        json.dump(status.to_dict(), f, indent=2)


def read_status() -> TickServiceStatus:
    ensure_service_dir()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
            status = TickServiceStatus.from_dict(data)
            if status.running and status.pid:
                if not _is_process_running(status.pid):
                    status.running = False
                    status.pid = None
                    write_status(status)
            return status
        except (json.JSONDecodeError, KeyError):
            pass
    return TickServiceStatus()


def _is_process_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def write_pid(pid: int):
    ensure_service_dir()
    PID_FILE.write_text(str(pid))


def remove_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()


def is_tick_service_running() -> tuple[bool, int | None]:
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if _is_process_running(pid):
                return True, pid
        except (ValueError, IOError):
            pass
    return False, None


def stop_tick_service() -> tuple[bool, str]:
    running, pid = is_tick_service_running()
    if not running:
        return False, "Tick service is not running"
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.5)
            if not _is_process_running(pid):
                remove_pid()
                return True, f"Tick service (PID {pid}) stopped"
        os.kill(pid, signal.SIGKILL)
        remove_pid()
        return True, f"Tick service (PID {pid}) force-killed"
    except OSError as e:
        return False, f"Failed to stop tick service: {e}"


# =========================================================================
# BarBuilder — aggregates ticks into 5-second OHLCV bars
# =========================================================================

class BarBuilder:
    """Aggregates raw ticks into N-second OHLCV bars."""

    def __init__(self, interval_seconds: int = BAR_INTERVAL):
        self.interval = interval_seconds
        self.bars: dict[tuple, dict] = {}  # (symbol, market, bucket_ts) → bar

    def _bucket(self, ts: float) -> datetime:
        """Quantize a Unix timestamp to the nearest bar boundary."""
        dt = datetime.fromtimestamp(ts, tz=PKT)
        total_secs = dt.hour * 3600 + dt.minute * 60 + dt.second
        bucket_secs = (total_secs // self.interval) * self.interval
        h, rem = divmod(bucket_secs, 3600)
        m, s = divmod(rem, 60)
        return dt.replace(hour=h, minute=m, second=s, microsecond=0)

    def process_tick(self, tick: dict) -> list[dict]:
        """Process a tick, return list of completed (closed) bars."""
        symbol = tick["symbol"]
        market = tick.get("market", "REG")
        price = tick["price"]
        volume = tick.get("volume", 0)
        ts = tick.get("timestamp", datetime.now(PKT).timestamp())

        bucket = self._bucket(ts)
        key = (symbol, market, bucket)

        # Close previous buckets for this symbol+market
        completed = []
        for k in list(self.bars):
            if k[0] == symbol and k[1] == market and k[2] < bucket:
                completed.append(self.bars.pop(k))

        # Update current bucket
        if key not in self.bars:
            self.bars[key] = {
                "symbol": symbol,
                "market": market,
                "timestamp": bucket.isoformat(),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "trades": 1,
            }
        else:
            bar = self.bars[key]
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
            bar["volume"] = volume
            bar["trades"] += 1

        return completed

    def flush_stale(self, cutoff_seconds: int = 10) -> list[dict]:
        """Flush bars that haven't received a tick in cutoff_seconds."""
        now = datetime.now(PKT)
        stale = []
        for k in list(self.bars):
            if (now - k[2]).total_seconds() > cutoff_seconds:
                stale.append(self.bars.pop(k))
        return stale

    def flush_all(self) -> list[dict]:
        """Close ALL open bars regardless of age. Used at EOD."""
        all_bars = list(self.bars.values())
        self.bars.clear()
        return all_bars


# =========================================================================
# TickService — MEMORY-ONLY collector
# =========================================================================

class TickService:
    """WebSocket tick collector. ALL data in memory. Single EOD flush."""

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or EOD_DB_PATH
        self.builder = BarBuilder(BAR_INTERVAL)
        self.live: dict[str, dict] = {}      # "MARKET:SYMBOL" → latest tick
        self.raw_ticks: list[dict] = []       # ALL raw ticks for the day
        self.completed_bars: list[dict] = []  # ALL completed 5s bars
        self.tick_count = 0
        self.connected = False
        self.last_tick_time: str | None = None
        self._raw_msg_count = 0
        self._last_checkpoint = time.time()
        self._last_tick_ts = time.time()  # monotonic heartbeat tracker

        # Index-specific tracking (separate from stocks)
        self.indices: dict[str, dict] = {}           # "KSE100" → latest index data
        self.index_history: dict[str, deque] = {}    # "KSE100" → deque(maxlen=360)
        self.index_sparklines: dict[str, list] = {}  # "KSE100" → 5-min downsampled
        self.index_ticks: list[dict] = []            # ALL raw index ticks for EOD

    def process(self, tick: dict):
        """Process a single tick: route IDX to index handler, rest to stock handler."""
        price = tick.get("price")
        if price is None:
            return

        self._last_tick_ts = time.time()  # heartbeat: we got a valid tick
        market = tick.get("market", "REG")
        if market == "IDX":
            self._handle_index_tick(tick)
        else:
            self._handle_stock_tick(tick)

    def _handle_stock_tick(self, tick: dict):
        """Handle a stock tick: update live + store raw + build bars."""
        price = tick["price"]
        self.tick_count += 1
        symbol = tick.get("symbol", "?")
        market = tick.get("market", "REG")
        self.last_tick_time = datetime.now(PKT).isoformat()

        # Store raw tick in memory
        self.raw_ticks.append(tick)

        # Update live snapshot
        key = f"{market}:{symbol}"
        self.live[key] = {
            "symbol": symbol,
            "market": market,
            "price": price,
            "change": tick.get("change", 0),
            "changePercent": tick.get("changePercent", 0),
            "volume": tick.get("volume", 0),
            "value": tick.get("value", 0),
            "trades": tick.get("trades", 0),
            "high": tick.get("high", price),
            "low": tick.get("low", price),
            "bid": tick.get("bid", 0),
            "ask": tick.get("ask", 0),
            "bidVol": tick.get("bidVol", 0),
            "askVol": tick.get("askVol", 0),
            "timestamp": tick.get("timestamp", 0),
            "updated": self.last_tick_time,
        }

        # Build bars — completed bars go to in-memory list
        completed = self.builder.process_tick(tick)
        if completed:
            self.completed_bars.extend(completed)

        # Broadcast to WebSocket relay clients
        if HAS_RELAY:
            _broadcast_tick(tick, market, symbol)

    def _handle_index_tick(self, tick: dict):
        """Handle an index tick — separate from stock ticks."""
        self.tick_count += 1
        self.last_tick_time = datetime.now(PKT).isoformat()
        symbol = tick.get("symbol", "?")
        value = tick.get("price", tick.get("value", 0))

        # Update latest index data
        self.indices[symbol] = {
            "symbol": symbol,
            "value": value,
            "change": tick.get("change", 0),
            "changePercent": tick.get("changePercent", 0),
            "volume": tick.get("volume", 0),
            "turnover": tick.get("turnover", tick.get("value", 0)),
            "high": tick.get("high", 0),
            "low": tick.get("low", 0),
            "open": tick.get("open", 0),
            "previousClose": tick.get("previousClose", 0),
            "timestamp": tick.get("timestamp", 0),
        }

        # Store raw index tick for EOD flush
        self.index_ticks.append(tick)

        # Track for sparkline — append to history deque
        ts_now = tick.get("timestamp", 0) or time.time()
        if symbol not in self.index_history:
            self.index_history[symbol] = deque(maxlen=360)
        self.index_history[symbol].append({"ts": ts_now, "value": value})

        # Also build 5s bars for indices (reuse builder with market=IDX)
        idx_tick = dict(tick)
        idx_tick["market"] = "IDX"
        idx_tick["price"] = value
        completed = self.builder.process_tick(idx_tick)
        if completed:
            self.completed_bars.extend(completed)

        # Broadcast to WebSocket relay clients
        if HAS_RELAY:
            _broadcast_tick(tick, "IDX", tick.get("symbol", ""))

    def _build_sparklines(self):
        """Downsample index history into 5-minute sparkline points."""
        for symbol, history in self.index_history.items():
            if not history:
                continue
            points = []
            for entry in history:
                bucket = int(entry["ts"] // 300) * 300  # 300s = 5 min
                if not points or points[-1]["ts"] != bucket:
                    points.append({"ts": bucket, "value": entry["value"]})
                else:
                    points[-1]["value"] = entry["value"]  # last value wins
            self.index_sparklines[symbol] = points[-60:]

    def write_snapshot(self):
        """Atomic JSON snapshot for Streamlit to read."""
        now = datetime.now(PKT)
        symbols = list(self.live.values())
        reg_symbols = [s for s in symbols if s["market"] == "REG"]

        gainers = sum(1 for s in reg_symbols if (s.get("changePercent") or 0) > 0)
        losers = sum(1 for s in reg_symbols if (s.get("changePercent") or 0) < 0)
        unchanged = len(reg_symbols) - gainers - losers

        by_change = sorted(reg_symbols, key=lambda x: x.get("changePercent", 0))
        top_gainers = by_change[-10:][::-1]
        top_losers = by_change[:10]
        most_active = sorted(
            reg_symbols, key=lambda x: x.get("volume", 0), reverse=True
        )[:10]

        # Flush stale bars during snapshot cycle
        stale = self.builder.flush_stale()
        if stale:
            self.completed_bars.extend(stale)

        # Build index sparklines
        self._build_sparklines()

        # Index data — add sparkline values to each index entry
        indices_list = []
        for idx in self.indices.values():
            entry = dict(idx)
            spark = self.index_sparklines.get(idx["symbol"], [])
            entry["sparkline"] = [p["value"] for p in spark]
            indices_list.append(entry)

        snapshot = {
            "timestamp": now.isoformat(),
            "connected": self.connected,
            "tick_count": self.tick_count,
            "bars_in_memory": len(self.completed_bars) + len(self.builder.bars),
            "raw_ticks_in_memory": len(self.raw_ticks),
            "ram_mb": round(_get_ram_mb(), 1),
            "symbol_count": len(self.live),
            "index_count": len(self.indices),
            "markets": {
                mkt: sum(1 for s in symbols if s["market"] == mkt)
                for mkt in MARKETS
            },
            "breadth": {
                "gainers": gainers,
                "losers": losers,
                "unchanged": unchanged,
            },
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "most_active": most_active,
            "symbols": symbols,
            "indices": indices_list,
            "index_sparklines": {
                sym: pts for sym, pts in self.index_sparklines.items()
            },
        }

        try:
            with open(str(SNAPSHOT_PATH), "w") as f:
                json.dump(snapshot, f, default=str)
        except OSError as e:
            logger.warning("Snapshot write failed: %s", e)

    @staticmethod
    def _ensure_tables(con):
        """Create tick tables with UNIQUE constraints if they don't exist."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_5s (
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                ts TEXT NOT NULL,
                o REAL, h REAL, l REAL, c REAL,
                v INTEGER DEFAULT 0,
                trades INTEGER DEFAULT 0,
                UNIQUE(symbol, market, ts)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_ticks (
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                ts REAL,
                price REAL,
                volume INTEGER DEFAULT 0,
                bid REAL DEFAULT 0,
                ask REAL DEFAULT 0,
                bid_vol INTEGER DEFAULT 0,
                ask_vol INTEGER DEFAULT 0,
                UNIQUE(symbol, market, ts, price)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS index_ohlcv_5s (
                symbol TEXT NOT NULL,
                ts TEXT NOT NULL,
                o REAL, h REAL, l REAL, c REAL,
                v INTEGER DEFAULT 0,
                turnover REAL DEFAULT 0,
                UNIQUE(symbol, ts)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS index_raw_ticks (
                symbol TEXT NOT NULL,
                ts REAL,
                value REAL,
                change REAL DEFAULT 0,
                change_pct REAL DEFAULT 0,
                volume INTEGER DEFAULT 0,
                turnover REAL DEFAULT 0,
                UNIQUE(symbol, ts, value)
            )
        """)

    def _checkpoint_flush(self):
        """Write current data to tick_bars.db without clearing memory.

        Called every 30 minutes during market hours as crash protection.
        Uses INSERT OR IGNORE so duplicate rows from previous checkpoints
        are silently skipped.
        """
        if not self.completed_bars and not self.raw_ticks and not self.index_ticks:
            return

        con = sqlite3.connect(str(self.db_path))
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        self._ensure_tables(con)

        # Stock bars
        stock_bars = [b for b in self.completed_bars if b.get("market") != "IDX"]
        if stock_bars:
            con.executemany(
                "INSERT OR IGNORE INTO ohlcv_5s VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (b["symbol"], b["market"], b["timestamp"],
                     b["open"], b["high"], b["low"], b["close"],
                     b["volume"], b["trades"])
                    for b in stock_bars
                ],
            )

        # Index bars
        index_bars = [b for b in self.completed_bars if b.get("market") == "IDX"]
        if index_bars:
            con.executemany(
                "INSERT OR IGNORE INTO index_ohlcv_5s VALUES (?,?,?,?,?,?,?,?)",
                [
                    (b["symbol"], b["timestamp"],
                     b["open"], b["high"], b["low"], b["close"],
                     b.get("volume", 0), b.get("turnover", 0))
                    for b in index_bars
                ],
            )

        # Raw stock ticks
        if self.raw_ticks:
            con.executemany(
                "INSERT OR IGNORE INTO raw_ticks VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (t.get("symbol", ""), t.get("market", "REG"),
                     t.get("timestamp", 0), t.get("price", 0),
                     t.get("volume", 0), t.get("bid", 0),
                     t.get("ask", 0), t.get("bidVol", 0),
                     t.get("askVol", 0))
                    for t in self.raw_ticks
                ],
            )

        # Raw index ticks
        if self.index_ticks:
            con.executemany(
                "INSERT OR IGNORE INTO index_raw_ticks VALUES (?,?,?,?,?,?,?)",
                [
                    (t.get("symbol", ""), t.get("timestamp", 0),
                     t.get("price", t.get("value", 0)),
                     t.get("change", 0), t.get("changePercent", 0),
                     t.get("volume", 0),
                     t.get("turnover", t.get("value", 0)))
                    for t in self.index_ticks
                ],
            )

        con.commit()
        con.close()
        self._last_checkpoint = time.time()

        bar_count = len(self.completed_bars)
        tick_count = len(self.raw_ticks)
        idx_count = len(self.index_ticks)
        print(
            f"💾 Checkpoint: {bar_count:,} bars, {tick_count:,} ticks, "
            f"{idx_count:,} index ticks saved (memory NOT cleared)"
        )

    def eod_flush(self):
        """Single flush after market close. Writes EVERYTHING to SQLite."""
        # Close all remaining open bars
        final_bars = self.builder.flush_all()
        self.completed_bars.extend(final_bars)

        total_bars = len(self.completed_bars)
        total_ticks = len(self.raw_ticks)
        total_idx_ticks = len(self.index_ticks)
        print(
            f"📊 EOD flush: {total_bars:,} bars, {total_ticks:,} ticks, "
            f"{total_idx_ticks:,} index ticks"
        )

        if total_bars == 0 and total_ticks == 0 and total_idx_ticks == 0:
            print("Nothing to flush")
            return

        con = sqlite3.connect(str(self.db_path))
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA cache_size=-128000")
        con.execute("PRAGMA temp_store=MEMORY")
        con.execute("PRAGMA mmap_size=268435456")

        self._ensure_tables(con)

        # Batch insert bars (stock bars only — market != IDX)
        stock_bars = [b for b in self.completed_bars if b.get("market") != "IDX"]
        if stock_bars:
            con.executemany(
                "INSERT OR IGNORE INTO ohlcv_5s VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        b["symbol"], b["market"], b["timestamp"],
                        b["open"], b["high"], b["low"], b["close"],
                        b["volume"], b["trades"],
                    )
                    for b in stock_bars
                ],
            )

        # Batch insert index bars (market == IDX)
        index_bars = [b for b in self.completed_bars if b.get("market") == "IDX"]
        if index_bars:
            con.executemany(
                "INSERT OR IGNORE INTO index_ohlcv_5s VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        b["symbol"], b["timestamp"],
                        b["open"], b["high"], b["low"], b["close"],
                        b.get("volume", 0), b.get("turnover", 0),
                    )
                    for b in index_bars
                ],
            )

        # Batch insert raw stock ticks
        if self.raw_ticks:
            con.executemany(
                "INSERT OR IGNORE INTO raw_ticks VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        t.get("symbol", ""), t.get("market", "REG"),
                        t.get("timestamp", 0), t.get("price", 0),
                        t.get("volume", 0), t.get("bid", 0),
                        t.get("ask", 0), t.get("bidVol", 0),
                        t.get("askVol", 0),
                    )
                    for t in self.raw_ticks
                ],
            )

        # Batch insert raw index ticks
        if self.index_ticks:
            con.executemany(
                "INSERT OR IGNORE INTO index_raw_ticks VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        t.get("symbol", ""), t.get("timestamp", 0),
                        t.get("price", t.get("value", 0)),
                        t.get("change", 0), t.get("changePercent", 0),
                        t.get("volume", 0),
                        t.get("turnover", t.get("value", 0)),
                    )
                    for t in self.index_ticks
                ],
            )

        con.commit()

        # Build indexes + dedup
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_bar_sym_ts ON ohlcv_5s(symbol, ts)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_tick_sym_ts ON raw_ticks(symbol, ts)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_idxbar_sym_ts ON index_ohlcv_5s(symbol, ts)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_idxtick_sym_ts ON index_raw_ticks(symbol, ts)"
        )
        con.execute("""
            DELETE FROM ohlcv_5s WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ohlcv_5s
                GROUP BY symbol, market, ts
            )
        """)
        con.execute("""
            DELETE FROM index_ohlcv_5s WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM index_ohlcv_5s
                GROUP BY symbol, ts
            )
        """)
        con.execute("PRAGMA optimize")
        con.commit()
        con.close()

        print(
            f"✅ EOD complete: {len(stock_bars):,} stock bars, "
            f"{len(index_bars):,} index bars, "
            f"{total_ticks:,} stock ticks, "
            f"{total_idx_ticks:,} index ticks → {self.db_path}"
        )

        # Clear ALL memory for next day
        self.completed_bars = []
        self.raw_ticks = []
        self.live = {}
        self.builder = BarBuilder(BAR_INTERVAL)
        self.tick_count = 0
        self.indices = {}
        self.index_history = {}
        self.index_sparklines = {}
        self.index_ticks = []

    def _sleep_until_next_session(self):
        """Sleep until 9:15 AM PKT next trading day."""
        now = datetime.now(PKT)

        # Find next trading day (skip weekends)
        next_day = now + timedelta(days=1)
        while next_day.weekday() >= 5:  # 5=Sat, 6=Sun
            next_day += timedelta(days=1)

        # Target: 9:15 AM PKT on next trading day
        target = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
        sleep_seconds = (target - now).total_seconds()

        if sleep_seconds > 0:
            print(
                f"\U0001f4a4 Sleeping until {target.strftime('%A %Y-%m-%d %H:%M')} PKT "
                f"({sleep_seconds / 3600:.1f} hours)"
            )
            time.sleep(sleep_seconds)

    def _is_market_hours(self) -> bool:
        """True if current time is within PSX trading hours (Mon-Fri 9:00-15:35)."""
        now = datetime.now(PKT)
        return (
            now.weekday() < 5
            and now.hour >= 9
            and (now.hour < 15 or (now.hour == 15 and now.minute <= 35))
        )

    def get_status_line(self) -> str:
        """Console status line."""
        ram = _get_ram_mb()
        bars = len(self.completed_bars) + len(self.builder.bars)
        ws_clients = _relay_manager.client_count if HAS_RELAY else 0
        silence = time.time() - self._last_tick_ts
        return (
            f"Ticks: {self.tick_count:,} | "
            f"Bars: {bars:,} | "
            f"Symbols: {len(self.live)} | "
            f"Indices: {len(self.indices)} | "
            f"WS Clients: {ws_clients} | "
            f"RAM: {ram:.0f} MB | "
            f"Last tick: {silence:.0f}s ago"
        )


# =========================================================================
# Message parser — discovers and normalises WebSocket tick format
# =========================================================================

def _parse_ws_message(raw: str) -> dict | None:
    """Parse a WebSocket message into a normalised tick dict.

    Handles:
    1. PSX Terminal tickUpdate format:
       {"type":"tickUpdate","symbol":"KOSM","market":"REG",
        "tick":{"s":"KOSM","m":"REG","c":4.53,...},"timestamp":...}
    2. Flat tick:  {"symbol": "HBL", "price": 100, ...}
    3. Wrapped:    {"type": "update", "data": {"symbol": ..., "price": ...}}
    4. Array:      {"type": "update", "data": [tick, ...]} → returns None (use batch)
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(msg, dict):
        return None

    # PSX Terminal tickUpdate format — the REAL format from psxterminal.com
    if msg.get("type") == "tickUpdate" and "tick" in msg:
        t = msg["tick"]
        if not isinstance(t, dict):
            return None

        # "c" is current/close price — the primary price field
        price = t.get("c") or t.get("x")
        if price is None:
            return None
        try:
            price = float(price)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        # Symbol from outer msg or inner tick
        symbol = str(
            msg.get("symbol") or t.get("s", "")
        ).strip().upper()
        market = str(
            msg.get("market") or t.get("m", "REG")
        ).strip().upper()

        # Timestamp: outer msg has ms epoch, inner tick has seconds epoch
        ts = msg.get("timestamp", 0)
        if ts and ts > 1e12:
            ts = ts / 1000.0  # ms → s
        if not ts:
            ts = t.get("t", 0) or datetime.now(PKT).timestamp()

        return {
            "symbol": symbol,
            "market": market,
            "price": price,
            "open": _safe_float(t.get("o")),
            "change": _safe_float(t.get("ch") or msg.get("change")),
            "changePercent": _safe_float(t.get("pch")),
            "volume": _safe_int(t.get("v")),
            "value": _safe_float(t.get("val")),
            "trades": _safe_int(t.get("tr")),
            "high": _safe_float(t.get("h")),
            "low": _safe_float(t.get("l")),
            "bid": _safe_float(t.get("bp")),
            "ask": _safe_float(t.get("ap")),
            "bidVol": _safe_int(t.get("bv")),
            "askVol": _safe_int(t.get("av")),
            "previousClose": _safe_float(t.get("ldcp")),
            "timestamp": ts,
        }

    # Wrapped format (generic)
    if "data" in msg:
        data = msg["data"]
        if isinstance(data, dict):
            msg = data
        elif isinstance(data, list):
            return None  # handled by _parse_ws_batch

    # Flat tick — must have price and symbol
    if "price" not in msg and "last" not in msg and "current" not in msg:
        return None
    if "symbol" not in msg and "sym" not in msg:
        return None

    price = msg.get("price") or msg.get("last") or msg.get("current")
    if price is None:
        return None

    try:
        price = float(price)
    except (ValueError, TypeError):
        return None

    if price <= 0:
        return None

    return {
        "symbol": str(msg.get("symbol") or msg.get("sym", "")).strip().upper(),
        "market": str(msg.get("market") or msg.get("mkt", "REG")).strip().upper(),
        "price": price,
        "change": _safe_float(msg.get("change")),
        "changePercent": _safe_float(
            msg.get("changePercent") or msg.get("changePct") or msg.get("change_pct")
        ),
        "volume": _safe_int(msg.get("volume") or msg.get("vol")),
        "value": _safe_float(msg.get("value")),
        "trades": _safe_int(msg.get("trades")),
        "high": _safe_float(msg.get("high")),
        "low": _safe_float(msg.get("low")),
        "bid": _safe_float(msg.get("bid")),
        "ask": _safe_float(msg.get("ask") or msg.get("offer")),
        "bidVol": _safe_int(msg.get("bidVol") or msg.get("bid_vol")),
        "askVol": _safe_int(msg.get("askVol") or msg.get("ask_vol")),
        "timestamp": _safe_float(msg.get("timestamp") or msg.get("ts"))
        or datetime.now(PKT).timestamp(),
    }


def _parse_ws_batch(raw: str) -> list[dict]:
    """Parse a message that may contain an array of ticks."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return []

    items = []
    if isinstance(msg, dict) and "data" in msg:
        data = msg["data"]
        if isinstance(data, list):
            items = data
    elif isinstance(msg, list):
        items = msg

    ticks = []
    for item in items:
        if isinstance(item, dict):
            raw_json = json.dumps(item)
            tick = _parse_ws_message(raw_json)
            if tick:
                ticks.append(tick)
    return ticks


def _safe_float(val, default=0.0) -> float:
    if val is None or val == "" or val == "--":
        return default
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    if val is None or val == "" or val == "--":
        return default
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return default


# =========================================================================
# Debug mode — connect, print first N raw messages, exit
# =========================================================================

async def debug_ws(n: int = 10):
    """Connect to WebSocket, print first N raw messages, then exit."""
    try:
        import websockets
    except ImportError:
        print("ERROR: pip install websockets")
        return

    print(f"Connecting to {WSS_URL} ...")
    async with websockets.connect(
        WSS_URL, ping_interval=30, ping_timeout=10, max_size=2**20,
    ) as ws:
        print("Connected. Subscribing...")
        for mkt in MARKETS:
            await ws.send(json.dumps({
                "type": "subscribe",
                "subscriptionType": "marketData",
                "params": {"marketType": mkt},
            }))
            print(f"  Subscribed: {mkt}")

        print(f"\nWaiting for {n} messages...\n")
        for i in range(n):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                # Pretty print
                try:
                    obj = json.loads(raw)
                    formatted = json.dumps(obj, indent=2)
                except json.JSONDecodeError:
                    formatted = raw
                print(f"--- MESSAGE {i + 1} ---")
                print(formatted[:2000])
                print()
            except asyncio.TimeoutError:
                print(f"  Timeout waiting for message {i + 1}")
                break

    print("Done.")


# =========================================================================
# Async main loop — MEMORY ONLY, NO DB WRITES DURING MARKET
# =========================================================================

async def main(db_path: Path | None = None):
    """Main entry point: connect, subscribe, collect in memory, EOD flush."""
    try:
        import websockets
    except ImportError:
        print("ERROR: websockets package required. Install: pip install websockets")
        sys.exit(1)

    service = TickService(db_path=db_path)
    status = TickServiceStatus(
        running=True,
        pid=os.getpid(),
        started_at=datetime.now(PKT).isoformat(),
    )
    write_pid(os.getpid())
    write_status(status)

    # Graceful shutdown
    shutdown = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received")
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            signal.signal(sig, lambda *_: shutdown.set())

    print(f"🚀 PSX Tick Collector (memory mode) — PID {os.getpid()}")
    print(f"  Snapshot: {SNAPSHOT_PATH}")
    print(f"  EOD target: {service.db_path}")
    print(f"  Bars: {BAR_INTERVAL}s | Markets: {', '.join(MARKETS)}")
    print(f"  Zero DB writes during trading. Single EOD flush at 15:35 PKT.")

    # Start WebSocket relay (same process, background thread)
    if HAS_RELAY:
        relay_port = int(os.environ.get("RELAY_PORT", "8765"))
        _relay_set_collector(service)
        _start_relay(port=relay_port)
        print(f"  📡 WS relay on ws://0.0.0.0:{relay_port}")
        print(f"  Docs: http://localhost:{relay_port}/docs")
    else:
        print("  WS relay not available (pip install fastapi uvicorn)")
    print()

    while not shutdown.is_set():
        try:
            async with websockets.connect(
                WSS_URL,
                ping_interval=30,
                ping_timeout=10,
                max_size=2**20,
                additional_headers={"User-Agent": "psx_ohlcv/3.4.0"},
            ) as ws:
                service.connected = True
                status.connected = True
                write_status(status)
                print(f"🔗 Connected to {WSS_URL}")

                # Subscribe to all markets
                for mkt in MARKETS:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "subscriptionType": "marketData",
                        "params": {"marketType": mkt},
                    }))
                print(f"  ✅ Subscribed: {' '.join(MARKETS)}")

                last_snapshot = datetime.now(PKT)
                last_status_print = datetime.now(PKT)
                post_market_done = False

                while not shutdown.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except asyncio.TimeoutError:
                        now = datetime.now(PKT)
                        # Post-market: EOD flush
                        if now.hour >= 15 and now.minute >= 35 and not post_market_done:
                            print("🔔 Market closed")
                            service.eod_flush()
                            service.connected = False
                            service.write_snapshot()
                            post_market_done = True
                            # Sleep until 9:15 AM PKT next trading day
                            service._sleep_until_next_session()
                            continue

                        # Heartbeat: no ticks for 60s during market hours → dead WS
                        if service._is_market_hours():
                            silence = time.time() - service._last_tick_ts
                            if silence > 60:
                                print(
                                    f"💀 No ticks for {silence:.0f}s — "
                                    f"WebSocket may be dead. Forcing reconnect..."
                                )
                                service._last_tick_ts = time.time()  # avoid rapid retries
                                break  # exit inner loop → ws closes → outer loop reconnects

                        service.write_snapshot()
                        continue

                    # Debug: print first 10 raw messages
                    if service._raw_msg_count < 10:
                        service._raw_msg_count += 1
                        print(f"  RAW[{service._raw_msg_count}]: {raw[:500]}")

                    # Parse tick(s)
                    tick = _parse_ws_message(raw)
                    if tick:
                        service.process(tick)
                    else:
                        batch = _parse_ws_batch(raw)
                        for t in batch:
                            service.process(t)

                    now = datetime.now(PKT)

                    # Reset post-market flag on new day
                    if now.hour < 9:
                        post_market_done = False

                    # Snapshot every 2 seconds
                    if (now - last_snapshot).total_seconds() >= SNAPSHOT_INTERVAL:
                        service.write_snapshot()
                        last_snapshot = now

                    # Console status every 30 seconds
                    if (now - last_status_print).total_seconds() >= STATUS_PRINT_INTERVAL:
                        print(f"  ⚡ {service.get_status_line()}")
                        status.tick_count = service.tick_count
                        status.bars_in_memory = (
                            len(service.completed_bars) + len(service.builder.bars)
                        )
                        status.raw_ticks_in_memory = len(service.raw_ticks)
                        status.symbol_count = len(service.live)
                        status.ram_mb = _get_ram_mb()
                        status.last_tick = service.last_tick_time
                        write_status(status)
                        last_status_print = now

                    # Crash-safe checkpoint every 30 minutes
                    if time.time() - service._last_checkpoint >= 1800:
                        service._checkpoint_flush()

                    # Heartbeat: no valid ticks for 60s during market hours
                    if service._is_market_hours():
                        silence = time.time() - service._last_tick_ts
                        if silence > 60:
                            print(
                                f"💀 No ticks for {silence:.0f}s — "
                                f"WebSocket may be dead. Forcing reconnect..."
                            )
                            service._last_tick_ts = time.time()
                            break

        except Exception as e:
            service.connected = False
            status.connected = False
            status.error_message = str(e)
            write_status(status)
            print(f"⚠️ Disconnected: {e}. Reconnecting in 5s...")
            service.write_snapshot()
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    # Graceful shutdown — flush remaining data
    print("🛑 Shutting down...")
    if service.tick_count > 0 and len(service.raw_ticks) > 0:
        print("Flushing remaining data to DB before exit...")
        service.eod_flush()
    service.write_snapshot()
    status.running = False
    status.connected = False
    write_status(status)
    remove_pid()
    print("✅ Stopped.")


def start_tick_service_background(db_path: Path | None = None) -> tuple[bool, str]:
    """Start tick service as a background subprocess."""
    running, pid = is_tick_service_running()
    if running:
        return False, f"Tick service already running (PID {pid})"

    import subprocess

    cmd = [sys.executable, "-m", "psx_ohlcv.services.tick_service"]
    if db_path:
        cmd.extend(["--db", str(db_path)])

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return True, f"Tick service started (PID {proc.pid})"


# =========================================================================
# CLI entry point
# =========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PSX Live Tick Service")
    parser.add_argument("--db", type=Path, default=None, help="Database path")
    parser.add_argument(
        "--daemon", action="store_true", help="Start as background process"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Connect, print first 10 raw WS messages, then exit"
    )
    parser.add_argument(
        "--debug-count", type=int, default=10,
        help="Number of messages to print in debug mode (default: 10)"
    )
    args = parser.parse_args()

    if args.debug:
        asyncio.run(debug_ws(n=args.debug_count))
        sys.exit(0)

    if args.daemon:
        ok, msg = start_tick_service_background(args.db)
        print(msg)
        sys.exit(0 if ok else 1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(main(db_path=args.db))
