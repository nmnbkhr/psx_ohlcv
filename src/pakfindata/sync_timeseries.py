"""Bulk intraday tick sync for all symbols.

PSX /timeseries/int/{symbol} returns today's tick-level trades only.
Run daily to build up continuous intraday history in intraday_bars.

Usage:
    python -m pakfindata.sync_timeseries           # Sync all symbols
    python -m pakfindata.sync_timeseries OGDC HBL   # Sync specific symbols
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

log = logging.getLogger("pakfindata.sync_timeseries")

PSX_BASE = "https://dps.psx.com.pk"
INT_URL = f"{PSX_BASE}/timeseries/int/{{symbol}}"

PROGRESS_FILE = DATA_ROOT / "intraday_sync_progress.json"


def _get_futures_odl_symbols(con: sqlite3.Connection) -> list[str]:
    """Get distinct active futures & ODL symbols from futures_eod.

    Returns only current-month and next-month contracts (active ones),
    plus all ODL symbols that have recent data.
    """
    # Get futures symbols with data in last 60 days (active contracts)
    rows = con.execute(
        """SELECT DISTINCT symbol FROM futures_eod
           WHERE date >= date('now', '-60 days')
             AND market_type IN ('FUT', 'IDX_FUT', 'ODL', 'CONT')
           ORDER BY symbol"""
    ).fetchall()
    return [r[0] for r in rows]


def _write_progress(data: dict) -> None:
    try:
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = PROGRESS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(PROGRESS_FILE)
    except OSError:
        # Fallback: write directly if atomic replace fails
        try:
            PROGRESS_FILE.write_text(json.dumps(data))
        except OSError:
            pass


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
        rows.append((symbol, ts_str, ts_epoch, price, price, price, price, volume, "int", "insert"))

    if not rows:
        return 0

    con.executemany(
        """INSERT INTO intraday_bars
           (symbol, ts, ts_epoch, open, high, low, close, volume, interval, operation)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (symbol, ts, close) DO UPDATE SET
               operation = 'upsert',
               process_ts = datetime('now')""",
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
        rows.append((symbol, ts_epoch, price, 0, 0, volume, 0, 0, 0, "insert"))

    if not rows:
        return 0

    con.executemany(
        """INSERT INTO tick_data
           (symbol, timestamp, price, change, change_pct,
            cumulative_volume, mw_high, mw_low, mw_open, operation)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (symbol, timestamp, price) DO UPDATE SET
               operation = 'upsert',
               process_ts = datetime('now')""",
        rows,
    )
    con.commit()
    return len(rows)


def sync_intraday_all(
    db_path: Path | str | None = None,
    symbols: list[str] | None = None,
    delay: float = 0.3,
    save_json: bool = False,
    include_futures_odl: bool = False,
) -> dict:
    """Sync today's intraday ticks for all symbols into intraday_bars.

    Args:
        db_path: Path to SQLite database (default: settings)
        symbols: List of symbols to sync (default: all)
        delay: Delay between requests in seconds
        save_json: If True, save raw JSON responses to DATA_ROOT/intraday/{date}/{SYMBOL}.json
        include_futures_odl: If True, also sync futures, index futures, ODL, and continuous symbols
    """
    con = connect(db_path)
    init_schema(con)

    if symbols is None:
        symbols = get_symbols_list(con)
        if include_futures_odl:
            existing = set(symbols)
            extra = _get_futures_odl_symbols(con)
            new_syms = [s for s in extra if s not in existing]
            if new_syms:
                log.info("Adding %d futures/ODL symbols to intraday sync", len(new_syms))
                symbols = symbols + new_syms

    total = len(symbols)
    session = create_session()
    ok = 0
    failed = 0
    total_rows = 0
    json_saved = 0
    errors = []

    # Detect actual trading date from first symbol's data (not system date)
    today_str = None  # will be set from first successful fetch
    json_dir = None

    progress = {
        "job": "intraday_all",
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "total": total,
        "current": 0,
        "ok": 0,
        "failed": 0,
        "rows_total": 0,
        "json_saved": 0,
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

            # Detect actual trading date from first symbol with data
            if today_str is None and data:
                for item in data:
                    if isinstance(item, list) and len(item) >= 2:
                        try:
                            detected = datetime.fromtimestamp(int(item[0]))
                            today_str = detected.strftime("%Y-%m-%d")
                            log.info("Detected trading date from data: %s", today_str)
                        except (ValueError, OSError):
                            pass
                        break
                if today_str is None:
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    log.info("Could not detect date from data, using system date: %s", today_str)
                # Create JSON dir now that we know the date
                if save_json:
                    json_dir = DATA_ROOT / "intraday" / today_str
                    json_dir.mkdir(parents=True, exist_ok=True)

            n1 = _upsert_intraday(con, symbol, data)
            _upsert_tick_data(con, symbol, data)
            ok += 1
            total_rows += n1

            # Save raw JSON response
            if save_json and json_dir and data:
                json_path = json_dir / f"{symbol}.json"
                json_path.write_text(json.dumps(payload, indent=2))
                json_saved += 1
        except Exception as e:
            failed += 1
            errors.append(f"{symbol}: {e}")
            errors = errors[-20:]

        progress["ok"] = ok
        progress["failed"] = failed
        progress["rows_total"] = total_rows
        progress["json_saved"] = json_saved
        progress["errors"] = errors
        _write_progress(progress)

        if i < total - 1:
            time.sleep(delay)

    con.close()
    progress["status"] = "completed"
    progress["finished_at"] = datetime.now().isoformat()
    progress["trading_date"] = today_str or datetime.now().strftime("%Y-%m-%d")
    if save_json and json_dir:
        progress["json_dir"] = str(json_dir)
    _write_progress(progress)

    log.info("Intraday sync: %d/%d ok, %d rows", ok, total, total_rows)
    return progress


# ── Background thread launcher ──────────────────────────────────────────────

_int_thread: threading.Thread | None = None


def start_intraday_sync(
    db_path=None, save_json: bool = False, include_futures_odl: bool = False,
) -> bool:
    """Launch intraday sync in a background thread. Returns False if already running."""
    global _int_thread
    if _int_thread is not None and _int_thread.is_alive():
        return False
    _int_thread = threading.Thread(
        target=sync_intraday_all,
        kwargs={
            "db_path": db_path,
            "save_json": save_json,
            "include_futures_odl": include_futures_odl,
        },
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
