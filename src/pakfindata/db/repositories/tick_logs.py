"""Tick logs repository — sync JSONL tick files into tick_logs table."""

import json
import sqlite3
import threading
from pathlib import Path
from datetime import datetime

import pandas as pd

__all__ = [
    "ensure_tick_logs_table",
    "sync_latest_file",
    "backfill_all_files",
    "backfill_background",
    "get_backfill_status",
    "insert_ticks_from_file",
    "upsert_ticks_from_file",
    "get_tick_logs",
    "get_synced_files",
    "get_tick_logs_stats",
]

TICK_LOG_DIR = Path("/mnt/e/psxdata/tick_logs")

_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS tick_logs (
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL,
    timestamp   REAL NOT NULL,
    _ts         TEXT NOT NULL,
    price       REAL,
    open        REAL,
    high        REAL,
    low         REAL,
    change      REAL,
    change_pct  REAL,
    volume      INTEGER DEFAULT 0,
    value       REAL DEFAULT 0,
    trades      INTEGER DEFAULT 0,
    bid         REAL DEFAULT 0,
    ask         REAL DEFAULT 0,
    bid_vol     INTEGER DEFAULT 0,
    ask_vol     INTEGER DEFAULT 0,
    prev_close  REAL,
    source_file TEXT,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, timestamp, price)
);
CREATE INDEX IF NOT EXISTS idx_tick_logs_ts_date ON tick_logs(_ts, timestamp);
CREATE INDEX IF NOT EXISTS idx_tick_logs_sym_market ON tick_logs(symbol, market, timestamp);
CREATE INDEX IF NOT EXISTS idx_tick_logs_market_ts ON tick_logs(market, _ts);
CREATE INDEX IF NOT EXISTS idx_tick_logs_source ON tick_logs(source_file);
CREATE INDEX IF NOT EXISTS idx_tick_logs_epoch ON tick_logs(timestamp);
"""

_table_ready = False


def ensure_tick_logs_table(con: sqlite3.Connection) -> None:
    """Create tick_logs table + indices if they don't exist yet."""
    global _table_ready
    if _table_ready:
        return
    con.executescript(_CREATE_TABLE_SQL)
    _table_ready = True


def _available_files() -> list[Path]:
    """List available tick log JSONL files, newest first."""
    return sorted(TICK_LOG_DIR.glob("ticks_*.jsonl"), reverse=True)


def _parse_jsonl(path: Path) -> list[dict]:
    """Parse a JSONL file into a list of dicts."""
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def _tick_to_row(t: dict, source_file: str) -> tuple:
    """Convert a tick dict to a database row tuple."""
    return (
        t.get("symbol", ""),
        t.get("market", "REG"),
        t.get("timestamp", 0),
        t.get("_ts", ""),
        t.get("price", 0),
        t.get("open", 0),
        t.get("high", 0),
        t.get("low", 0),
        t.get("change", 0),
        t.get("changePercent", 0),
        t.get("volume", 0),
        t.get("value", 0),
        t.get("trades", 0),
        t.get("bid", 0),
        t.get("ask", 0),
        t.get("bidVol", 0),
        t.get("askVol", 0),
        t.get("previousClose", 0),
        source_file,
    )


_INSERT_SQL = """
    INSERT OR IGNORE INTO tick_logs
        (symbol, market, timestamp, _ts, price, open, high, low,
         change, change_pct, volume, value, trades,
         bid, ask, bid_vol, ask_vol, prev_close, source_file)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

BATCH_SIZE = 10_000  # Smaller batches = shorter write locks

DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")


def _wal_connection() -> sqlite3.Connection:
    """Open a dedicated WAL-mode connection for writes (doesn't block readers)."""
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def insert_ticks_from_file(con: sqlite3.Connection, path: Path) -> int:
    """Parse a JSONL file and INSERT OR IGNORE all ticks into tick_logs table.

    Opens its own WAL connection so the shared read connection is never blocked.
    Commits every BATCH_SIZE rows so the write lock is held briefly.

    Returns:
        Number of new ticks inserted.
    """
    records = _parse_jsonl(path)
    if not records:
        return 0

    source_file = path.name
    rows = [_tick_to_row(t, source_file) for t in records]

    wcon = _wal_connection()
    ensure_tick_logs_table(wcon)

    before = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        wcon.executemany(_INSERT_SQL, batch)
        wcon.commit()  # Release write lock between batches

    after = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
    wcon.close()
    return after - before


# Backward-compatible alias
upsert_ticks_from_file = insert_ticks_from_file


# ── Background backfill with progress tracking ──────────────────────────────

_backfill_status: dict = {}
_backfill_lock = threading.Lock()


def get_backfill_status() -> dict:
    """Get current backfill progress (thread-safe)."""
    with _backfill_lock:
        return dict(_backfill_status)


def _run_backfill(files: list[Path]) -> None:
    """Worker: insert files one by one, updating shared progress dict."""
    wcon = _wal_connection()
    ensure_tick_logs_table(wcon)

    total = len(files)
    for i, path in enumerate(files):
        with _backfill_lock:
            _backfill_status.update(
                current_file=path.name,
                files_done=i,
                files_total=total,
                running=True,
            )

        records = _parse_jsonl(path)
        if not records:
            continue

        rows = [_tick_to_row(t, path.name) for t in records]
        for j in range(0, len(rows), BATCH_SIZE):
            batch = rows[j : j + BATCH_SIZE]
            wcon.executemany(_INSERT_SQL, batch)
            wcon.commit()

        with _backfill_lock:
            _backfill_status["ticks_inserted"] = (
                _backfill_status.get("ticks_inserted", 0) + len(rows)
            )

    wcon.close()
    with _backfill_lock:
        _backfill_status.update(running=False, files_done=total)


def backfill_background(files: list[Path]) -> None:
    """Launch backfill in a background thread. Poll get_backfill_status()."""
    with _backfill_lock:
        if _backfill_status.get("running"):
            return  # Already running
        _backfill_status.clear()
        _backfill_status.update(
            running=True, files_done=0, files_total=len(files),
            ticks_inserted=0, current_file="",
        )
    t = threading.Thread(target=_run_backfill, args=(files,), daemon=True)
    t.start()


def sync_latest_file(con: sqlite3.Connection) -> dict:
    """Sync the most recent JSONL file into the tick_logs table.

    Returns:
        Dict with keys: file, date, ticks_synced, status
    """
    ensure_tick_logs_table(con)
    files = _available_files()
    if not files:
        return {"file": None, "date": None, "ticks_synced": 0, "status": "no_files"}

    latest = files[0]
    date_str = latest.stem.replace("ticks_", "")
    count = upsert_ticks_from_file(con, latest)

    return {
        "file": latest.name,
        "date": date_str,
        "ticks_synced": count,
        "status": "ok",
    }


def backfill_all_files(con: sqlite3.Connection) -> list[dict]:
    """Backfill all available JSONL files into tick_logs (upsert, skip existing).

    Returns:
        List of dicts with: file, date, ticks_synced
    """
    ensure_tick_logs_table(con)
    files = _available_files()
    results = []

    # Get already synced files
    try:
        existing = set(
            row[0]
            for row in con.execute(
                "SELECT DISTINCT source_file FROM tick_logs"
            ).fetchall()
        )
    except sqlite3.OperationalError:
        existing = set()

    for path in reversed(files):  # oldest first
        date_str = path.stem.replace("ticks_", "")
        if path.name in existing:
            results.append({
                "file": path.name,
                "date": date_str,
                "ticks_synced": 0,
                "status": "skipped",
            })
            continue

        count = upsert_ticks_from_file(con, path)
        results.append({
            "file": path.name,
            "date": date_str,
            "ticks_synced": count,
            "status": "ok",
        })

    return results


def get_tick_logs(
    con: sqlite3.Connection,
    symbol: str | None = None,
    market: str | None = None,
    date: str | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """Query tick_logs with optional filters.

    Args:
        con: Database connection
        symbol: Filter by symbol
        market: Filter by market (REG, FUT, IDX, etc.)
        date: Filter by date (YYYY-MM-DD) — matches _ts prefix
        limit: Max rows
    """
    ensure_tick_logs_table(con)
    clauses = []
    params = []

    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if market:
        clauses.append("market = ?")
        params.append(market)
    if date:
        clauses.append("_ts LIKE ?")
        params.append(f"{date}%")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    return pd.read_sql_query(
        f"SELECT * FROM tick_logs {where} ORDER BY timestamp DESC LIMIT ?",
        con,
        params=params,
    )


def get_synced_files(con: sqlite3.Connection) -> pd.DataFrame:
    """Get summary of synced files in tick_logs."""
    ensure_tick_logs_table(con)
    try:
        return pd.read_sql_query(
            """SELECT source_file,
                      COUNT(*) as tick_count,
                      COUNT(DISTINCT symbol) as symbols,
                      MIN(_ts) as first_tick,
                      MAX(_ts) as last_tick
               FROM tick_logs
               GROUP BY source_file
               ORDER BY source_file DESC""",
            con,
        )
    except Exception:
        return pd.DataFrame()


def get_tick_logs_stats(con: sqlite3.Connection) -> dict:
    """Get overall stats for tick_logs table (uses fast per-file aggregation)."""
    ensure_tick_logs_table(con)
    try:
        # Aggregate per source_file first, then roll up — avoids full table scan
        row = con.execute(
            """SELECT COALESCE(SUM(cnt), 0),
                      COUNT(*),
                      MIN(first_tick),
                      MAX(last_tick)
               FROM (
                   SELECT source_file,
                          COUNT(*) as cnt,
                          MIN(_ts) as first_tick,
                          MAX(_ts) as last_tick
                   FROM tick_logs
                   GROUP BY source_file
               )"""
        ).fetchone()
        # Symbol count — uses idx_tick_logs_sym_market
        sym_count = con.execute(
            "SELECT COUNT(DISTINCT symbol) FROM tick_logs"
        ).fetchone()[0]
        return {
            "total_ticks": row[0],
            "symbols": sym_count,
            "files": row[1],
            "first_tick": row[2],
            "last_tick": row[3],
        }
    except Exception:
        return {"total_ticks": 0, "symbols": 0, "files": 0, "first_tick": None, "last_tick": None}
