"""Bulk intraday tick sync for all symbols.

PSX /timeseries/int/{symbol} returns today's tick-level trades only.
Run daily to build up continuous intraday history in intraday_bars.

Usage:
    python -m psx_ohlcv.sync_timeseries           # Sync all symbols
    python -m psx_ohlcv.sync_timeseries OGDC HBL   # Sync specific symbols
"""

import json
import logging
import sqlite3
import sys
import time
import threading
from datetime import datetime
from pathlib import Path

from .config import DATA_ROOT
from .db import connect, init_schema
from .db.repositories.symbols import get_symbols_list
from .http import create_session

log = logging.getLogger("psx_ohlcv.sync_timeseries")

PSX_BASE = "https://dps.psx.com.pk"
INT_URL = f"{PSX_BASE}/timeseries/int/{{symbol}}"

PROGRESS_FILE = DATA_ROOT / "intraday_sync_progress.json"


def _write_progress(data: dict) -> None:
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(PROGRESS_FILE)


def read_intraday_sync_progress() -> dict | None:
    """Read current intraday sync progress. Returns None if no job has run."""
    if not PROGRESS_FILE.exists():
        return None
    try:
        return json.loads(PROGRESS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _upsert_intraday(con: sqlite3.Connection, symbol: str, records: list) -> int:
    """Insert intraday tick records into intraday_bars. Returns rows upserted."""
    if not records:
        return 0
    rows = []
    for item in records:
        if not isinstance(item, list) or len(item) < 2:
            continue
        ts_epoch = int(item[0])
        try:
            dt = datetime.fromtimestamp(ts_epoch)
            ts_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            continue

        price = float(item[1])
        volume = float(item[2]) if len(item) >= 3 else 0
        rows.append((symbol, ts_str, ts_epoch, price, price, price, price, volume, "int"))

    if not rows:
        return 0

    con.executemany(
        """INSERT OR IGNORE INTO intraday_bars
           (symbol, ts, ts_epoch, open, high, low, close, volume, interval)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    con.commit()
    return len(rows)


def _upsert_tick_data(con: sqlite3.Connection, symbol: str, records: list) -> int:
    """Insert intraday tick records into tick_data. Returns rows upserted."""
    if not records:
        return 0

    rows = []
    for item in records:
        if not isinstance(item, list) or len(item) < 2:
            continue
        ts_epoch = int(item[0])
        price = float(item[1])
        volume = int(item[2]) if len(item) >= 3 else 0
        rows.append((symbol, ts_epoch, price, 0, 0, volume, 0, 0, 0))

    if not rows:
        return 0

    con.executemany(
        """INSERT OR IGNORE INTO tick_data
           (symbol, timestamp, price, change, change_pct,
            cumulative_volume, mw_high, mw_low, mw_open)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    con.commit()
    return len(rows)


def sync_intraday_all(
    db_path: Path | str | None = None,
    symbols: list[str] | None = None,
    delay: float = 0.3,
) -> dict:
    """Sync today's intraday ticks for all symbols into intraday_bars."""
    con = connect(db_path)
    init_schema(con)

    if symbols is None:
        symbols = get_symbols_list(con)

    total = len(symbols)
    session = create_session()
    ok = 0
    failed = 0
    total_rows = 0
    errors = []

    progress = {
        "job": "intraday_all",
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "total": total,
        "current": 0,
        "ok": 0,
        "failed": 0,
        "rows_total": 0,
        "current_symbol": "",
        "errors": [],
    }
    _write_progress(progress)

    for i, symbol in enumerate(symbols):
        progress["current"] = i + 1
        progress["current_symbol"] = symbol
        _write_progress(progress)

        try:
            url = INT_URL.format(symbol=symbol)
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()

            data = payload.get("data", payload) if isinstance(payload, dict) else payload
            if not isinstance(data, list):
                data = []

            n1 = _upsert_intraday(con, symbol, data)
            _upsert_tick_data(con, symbol, data)
            ok += 1
            total_rows += n1
        except Exception as e:
            failed += 1
            errors.append(f"{symbol}: {e}")
            errors = errors[-20:]

        progress["ok"] = ok
        progress["failed"] = failed
        progress["rows_total"] = total_rows
        progress["errors"] = errors
        _write_progress(progress)

        if i < total - 1:
            time.sleep(delay)

    con.close()
    progress["status"] = "completed"
    progress["finished_at"] = datetime.now().isoformat()
    _write_progress(progress)

    log.info("Intraday sync: %d/%d ok, %d rows", ok, total, total_rows)
    return progress


# ── Background thread launcher ──────────────────────────────────────────────

_int_thread: threading.Thread | None = None


def start_intraday_sync(db_path=None) -> bool:
    """Launch intraday sync in a background thread. Returns False if already running."""
    global _int_thread
    if _int_thread is not None and _int_thread.is_alive():
        return False
    _int_thread = threading.Thread(
        target=sync_intraday_all, kwargs={"db_path": db_path},
        daemon=True, name="intraday-sync",
    )
    _int_thread.start()
    return True


def is_intraday_sync_running() -> bool:
    """Check if intraday sync is currently running."""
    return _int_thread is not None and _int_thread.is_alive()


# ── CLI entry point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    symbols = sys.argv[1:] if len(sys.argv) > 1 else None
    print(f"Starting intraday sync for {'all' if symbols is None else len(symbols)} symbols...")
    result = sync_intraday_all(symbols=symbols)
    print(f"Done: {result['ok']}/{result['total']} ok, {result['failed']} failed, {result['rows_total']:,} rows")
