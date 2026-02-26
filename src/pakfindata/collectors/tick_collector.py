"""Live Tick OHLCV Builder — polls PSX market-watch and builds real OHLCV from ticks.

Usage:
    collector = TickCollector()
    collector.start()            # background daemon thread
    ...
    ohlcv = collector.get_running_ohlcv()
    collector.stop()             # auto-saves OHLCV to DB
"""

import logging
import threading
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from pakfindata.db.connection import connect, init_schema
from pakfindata.db.repositories.tick import (
    init_tick_schema,
    insert_ticks_batch,
    upsert_tick_ohlcv,
)
from pakfindata.http import create_session

logger = logging.getLogger(__name__)

MARKET_WATCH_URL = "https://dps.psx.com.pk/market-watch"
DEFAULT_INTERVAL = 5  # seconds


class TickCollector:
    """Polls PSX market-watch and incrementally builds OHLCV from ticks.

    In-memory state:
        _last_snapshot:  {symbol: {"price": float, "vol": int}} for dedup
        running_ohlcv:   {symbol: {open, high, low, close, volume, tick_count, first_ts, last_ts}}
        tick_history:    {symbol: [list of tick dicts]}
    """

    def __init__(self, interval: int = DEFAULT_INTERVAL, db_path=None):
        self.interval = interval
        self.db_path = db_path
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        # In-memory state
        self._last_snapshot: dict[str, dict] = {}
        self.running_ohlcv: dict[str, dict] = {}
        self.tick_history: dict[str, list] = {}

        # Stats
        self.poll_count = 0
        self.total_ticks = 0
        self.started_at: datetime | None = None
        self.last_poll_time: datetime | None = None
        self.last_raw_response: list[dict] | None = None

        # HTTP session
        self._session: requests.Session | None = None

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------
    def start(self):
        """Start background polling thread."""
        if self._running:
            return
        self._running = True
        self.started_at = datetime.now()
        self._session = create_session()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("TickCollector started (interval=%ds)", self.interval)

    def stop(self, save_to_db: bool = True):
        """Stop polling and optionally save OHLCV to DB."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        if save_to_db:
            self.save_ohlcv_to_db()
        logger.info("TickCollector stopped (polls=%d, ticks=%d)", self.poll_count, self.total_ticks)

    def poll_once(self) -> dict:
        """Execute a single poll cycle. Returns stats dict.

        Can be called directly (without start()) for manual single-shot polling.
        """
        if self._session is None:
            self._session = create_session()
        return self._poll_once()

    def get_running_ohlcv(self, symbol: str | None = None) -> dict:
        """Get current running OHLCV state.

        Args:
            symbol: If provided, returns only that symbol's OHLCV.
                    If None, returns all symbols.

        Returns:
            Dict of OHLCV data.
        """
        with self._lock:
            if symbol:
                return dict(self.running_ohlcv.get(symbol, {}))
            return {k: dict(v) for k, v in self.running_ohlcv.items()}

    def get_tick_history(self, symbol: str) -> list[dict]:
        """Get tick history for a symbol."""
        with self._lock:
            return list(self.tick_history.get(symbol, []))

    def get_stats(self) -> dict:
        """Get collector statistics."""
        with self._lock:
            return {
                "running": self._running,
                "poll_count": self.poll_count,
                "symbols_tracked": len(self.running_ohlcv),
                "total_ticks": self.total_ticks,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "last_poll_time": self.last_poll_time.isoformat() if self.last_poll_time else None,
            }

    def save_ohlcv_to_db(self) -> int:
        """Persist current running OHLCV to tick_ohlcv table.

        Returns:
            Number of rows upserted.
        """
        with self._lock:
            if not self.running_ohlcv:
                return 0
            ohlcv_rows = self._build_ohlcv_rows()

        con = connect(self.db_path)
        init_schema(con)
        init_tick_schema(con)
        return upsert_tick_ohlcv(con, ohlcv_rows)

    def save_ticks_to_db(self) -> int:
        """Persist all in-memory tick history to tick_data table.

        Returns:
            Number of rows inserted.
        """
        with self._lock:
            all_ticks = []
            for ticks in self.tick_history.values():
                all_ticks.extend(ticks)

        if not all_ticks:
            return 0

        con = connect(self.db_path)
        init_schema(con)
        init_tick_schema(con)
        return insert_ticks_batch(con, all_ticks)

    def reset(self):
        """Clear all in-memory state."""
        with self._lock:
            self._last_snapshot.clear()
            self.running_ohlcv.clear()
            self.tick_history.clear()
            self.poll_count = 0
            self.total_ticks = 0
            self.started_at = None
            self.last_poll_time = None
            self.last_raw_response = None

    # -----------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------
    def _run_loop(self):
        """Background polling loop."""
        while self._running:
            try:
                self._poll_once()
            except Exception:
                logger.exception("TickCollector poll error")
            time.sleep(self.interval)

    def _poll_once(self) -> dict:
        """Fetch market-watch HTML, parse table, dedup, update running OHLCV.

        The PSX market-watch endpoint returns an HTML table with columns:
        SYMBOL, SECTOR, LISTED IN, LDCP, OPEN, HIGH, LOW, CURRENT, CHANGE, CHANGE (%), VOLUME

        Returns:
            Stats dict: {new_ticks, skipped, errors}
        """
        stats = {"new_ticks": 0, "skipped": 0, "errors": 0}

        try:
            resp = self._session.get(MARKET_WATCH_URL, timeout=15)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            logger.warning("Market-watch fetch failed: %s", e)
            stats["errors"] = 1
            return stats

        # Parse HTML table into list of dicts
        try:
            tables = pd.read_html(StringIO(html))
            if not tables:
                logger.warning("No tables found in market-watch HTML")
                stats["errors"] = 1
                return stats
            df = tables[0]
            # Normalize columns
            df.columns = [str(c).strip().upper() for c in df.columns]
            data = df.to_dict("records")
        except Exception as e:
            logger.warning("Market-watch HTML parse failed: %s", e)
            stats["errors"] = 1
            return stats

        now_ts = int(time.time())
        now_dt = datetime.now()
        today = now_dt.strftime("%Y-%m-%d")

        with self._lock:
            self.last_raw_response = data
            self.poll_count += 1
            self.last_poll_time = now_dt

            for item in data:
                symbol = str(item.get("SYMBOL", "")).strip()
                if not symbol:
                    continue

                price = _safe_float(item.get("CURRENT"))
                vol = _safe_int(item.get("VOLUME"))
                if price is None or price <= 0:
                    continue

                # Dedup: skip if price AND volume unchanged
                prev = self._last_snapshot.get(symbol)
                if prev and prev["price"] == price and prev["vol"] == vol:
                    stats["skipped"] += 1
                    continue

                self._last_snapshot[symbol] = {"price": price, "vol": vol}

                # Parse change% — strip trailing '%' if present
                chg_pct_raw = item.get("CHANGE (%)", item.get("CHANGE(%)", 0))
                chg_pct = _safe_float(str(chg_pct_raw).replace("%", ""), 0)

                # Build tick record
                tick = {
                    "symbol": symbol,
                    "timestamp": now_ts,
                    "price": price,
                    "change": _safe_float(item.get("CHANGE"), 0),
                    "change_pct": chg_pct,
                    "cumulative_volume": vol if vol else 0,
                    "mw_high": _safe_float(item.get("HIGH"), 0),
                    "mw_low": _safe_float(item.get("LOW"), 0),
                    "mw_open": _safe_float(item.get("OPEN"), 0),
                }

                # Store tick history
                if symbol not in self.tick_history:
                    self.tick_history[symbol] = []
                self.tick_history[symbol].append(tick)

                # Update running OHLCV
                ohlcv = self.running_ohlcv.get(symbol)
                if ohlcv is None:
                    # First tick: set OPEN
                    self.running_ohlcv[symbol] = {
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "volume": vol if vol else 0,
                        "tick_count": 1,
                        "first_ts": now_ts,
                        "last_ts": now_ts,
                        "date": today,
                        "change": _safe_float(item.get("CHANGE"), 0),
                        "change_pct": chg_pct,
                        "ldcp": _safe_float(item.get("LDCP"), 0),
                    }
                else:
                    ohlcv["high"] = max(ohlcv["high"], price)
                    ohlcv["low"] = min(ohlcv["low"], price)
                    ohlcv["close"] = price
                    ohlcv["volume"] = vol if vol else ohlcv["volume"]
                    ohlcv["tick_count"] += 1
                    ohlcv["last_ts"] = now_ts
                    ohlcv["change"] = _safe_float(item.get("CHANGE"), 0)
                    ohlcv["change_pct"] = chg_pct

                stats["new_ticks"] += 1
                self.total_ticks += 1

        return stats

    def _build_ohlcv_rows(self) -> list[dict]:
        """Convert running_ohlcv to list of dicts for DB upsert."""
        rows = []
        for symbol, ohlcv in self.running_ohlcv.items():
            rows.append({
                "symbol": symbol,
                "date": ohlcv.get("date", datetime.now().strftime("%Y-%m-%d")),
                "open": ohlcv["open"],
                "high": ohlcv["high"],
                "low": ohlcv["low"],
                "close": ohlcv["close"],
                "volume": ohlcv.get("volume", 0),
                "tick_count": ohlcv.get("tick_count", 0),
                "first_tick_ts": ohlcv.get("first_ts"),
                "last_tick_ts": ohlcv.get("last_ts"),
            })
        return rows


# =====================================================================
# Helpers
# =====================================================================

def _safe_float(val, default=None) -> float | None:
    """Convert value to float safely."""
    if val is None or val == "" or val == "--":
        return default
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    """Convert value to int safely."""
    if val is None or val == "" or val == "--":
        return default
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return default
