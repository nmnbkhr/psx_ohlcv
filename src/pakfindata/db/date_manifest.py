"""
Date manifest — instant date lookup for all large tables without scanning.

File: /mnt/e/psxdata/date_manifest.json
Format:
{
    "intraday_bars": ["2025-01-02", "2025-01-03", ...],
    "ohlcv_5s": ["2025-03-01", ...],
    "eod_ohlcv": ["2020-01-02", ...],
    "tick_jsonl": ["2025-11-15", ...],
    ...
    "_updated": "2026-04-10T16:30:00+05:00"
}

Usage:
    from pakfindata.db.date_manifest import get_dates, get_latest_date, add_date

    # In page — instant, no DB:
    dates = get_dates("intraday_bars")
    latest = get_latest_date("intraday_bars")

    # After data ingestion — keeps manifest current:
    add_date("intraday_bars", "2026-04-10")

Rebuild:
    python -m pakfindata.db.date_manifest rebuild
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PKT = timezone(timedelta(hours=5))
MANIFEST_PATH = DATA_ROOT / "date_manifest.json"

# ─── Tables to track ─────────────────────────────────────────────────────────
# Built from Step 0 audit of page queries that scan for dates.
# source: "sqlite_psx" | "sqlite_tick" | "sqlite_commod" | "parquet" | "jsonl"

TABLE_REGISTRY = {
    # ── Heavy tables scanned by multiple pages ──
    "eod_ohlcv":            {"source": "sqlite_psx",    "date_col": "date"},
    "intraday_bars":        {"source": "sqlite_psx",    "date_col": "ts",         "extract": "substr10"},
    "tick_logs":            {"source": "sqlite_psx",    "date_col": "source_file", "extract": "source_file"},

    # ── tick_bars.db ──
    "ohlcv_5s":             {"source": "sqlite_tick",   "date_col": "ts",         "extract": "substr10"},
    "index_ohlcv_5s":       {"source": "sqlite_tick",   "date_col": "ts",         "extract": "substr10"},

    # ── Fixed income (queried by fixed_income.py + treasury_dashboard.py) ──
    "pkrv_daily":           {"source": "sqlite_psx",    "date_col": "date"},
    "pkisrv_daily":         {"source": "sqlite_psx",    "date_col": "date"},
    "pkfrv_daily":          {"source": "sqlite_psx",    "date_col": "date"},
    "konia_daily":          {"source": "sqlite_psx",    "date_col": "date"},
    "kibor_daily":          {"source": "sqlite_psx",    "date_col": "date"},

    # ── Other pages with date scans ──
    "nccpl_fipi_sector":    {"source": "sqlite_psx",    "date_col": "date"},
    "futures_eod":          {"source": "sqlite_psx",    "date_col": "date"},
    "post_close_turnover":  {"source": "sqlite_psx",    "date_col": "date"},
    "trading_sessions":     {"source": "sqlite_psx",    "date_col": "session_date"},
    "psx_indices":          {"source": "sqlite_psx",    "date_col": "index_date"},
    "mutual_fund_nav":      {"source": "sqlite_psx",    "date_col": "date"},

    # ── Commodity (commod.db) ──
    "pmex_ohlc":            {"source": "sqlite_commod", "date_col": "trading_date"},
    "pmex_margins":         {"source": "sqlite_commod", "date_col": "report_date"},

    # ── JSONL files (filesystem, no DB) ──
    "tick_jsonl":           {"source": "jsonl",         "date_col": None},
}

_DB_PATHS = {
    "sqlite_psx":    str(Path("/home/smnb/psxdata_rescue/psx.sqlite")),
    "sqlite_tick":   str(Path("/home/smnb/psxdata_rescue/tick_bars.db")),
    "sqlite_commod": str(Path("/home/smnb/psxdata_rescue/commod/commod.db")),
}


# ─── Core API ─────────────────────────────────────────────────────────────────

_cache = {}
_cache_mtime = 0.0


def _load() -> dict:
    """Load manifest with file-mtime caching to avoid re-reading unchanged file."""
    global _cache, _cache_mtime
    if not MANIFEST_PATH.exists():
        return {}
    try:
        mtime = MANIFEST_PATH.stat().st_mtime
        if mtime == _cache_mtime and _cache:
            return _cache
        data = json.loads(MANIFEST_PATH.read_text())
        _cache = data
        _cache_mtime = mtime
        return data
    except (json.JSONDecodeError, IOError):
        return {}


def _save(data: dict):
    global _cache, _cache_mtime
    tmp = str(MANIFEST_PATH) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, default=str)
    os.replace(tmp, str(MANIFEST_PATH))
    _cache = data
    _cache_mtime = MANIFEST_PATH.stat().st_mtime


def get_dates(table: str) -> list[str]:
    """Get sorted list of available dates (desc). Reads manifest (<1ms).
    Returns empty list if manifest has no entry — run rebuild to populate."""
    manifest = _load()
    return manifest.get(table, [])


def get_latest_date(table: str) -> str | None:
    """Most recent date with data. Use as default for date pickers."""
    dates = get_dates(table)
    return dates[0] if dates else None


def has_date(table: str, date_str: str) -> bool:
    """Check without querying DB."""
    return date_str in set(get_dates(table))


def add_date(table: str, date_str: str):
    """Add one date after data ingestion. Idempotent, fast."""
    manifest = _load()
    dates = manifest.get(table, [])
    date_str = str(date_str)[:10]
    if date_str not in dates:
        dates.append(date_str)
        dates.sort(reverse=True)
        manifest[table] = dates
        manifest["_updated"] = datetime.now(PKT).isoformat()
        _save(manifest)


def add_dates_bulk(table: str, new_dates: list[str]) -> int:
    """Add multiple dates at once. More efficient than repeated add_date()."""
    manifest = _load()
    existing = set(manifest.get(table, []))
    added = 0
    for d in new_dates:
        d = str(d)[:10]
        if d not in existing:
            existing.add(d)
            added += 1
    if added > 0:
        manifest[table] = sorted(existing, reverse=True)
        manifest["_updated"] = datetime.now(PKT).isoformat()
        _save(manifest)
    return added


def remove_date(table: str, date_str: str):
    """Remove a date (e.g., after data cleanup)."""
    manifest = _load()
    dates = manifest.get(table, [])
    date_str = str(date_str)[:10]
    if date_str in dates:
        dates.remove(date_str)
        manifest[table] = dates
        manifest["_updated"] = datetime.now(PKT).isoformat()
        _save(manifest)


def refresh_tables(tables: list[str]) -> dict:
    """Rescan specific tables and update manifest. Returns {table: count}."""
    manifest = _load()
    result = {}
    for table in tables:
        dates = _scan_table(table)
        manifest[table] = dates
        result[table] = len(dates)
    manifest["_updated"] = datetime.now(PKT).isoformat()
    _save(manifest)
    return result


# ─── Scanning (used by rebuild and fallback) ──────────────────────────────────

def _scan_table(table: str) -> list[str]:
    """Scan actual data source for distinct dates. Slow — only for rebuild."""
    cfg = TABLE_REGISTRY.get(table)
    if not cfg:
        return []

    source = cfg["source"]

    if source == "jsonl":
        return _scan_jsonl()

    if source.startswith("sqlite_"):
        db_path = _DB_PATHS.get(source)
        if not db_path:
            return []
        return _scan_sqlite(db_path, table, cfg["date_col"], cfg.get("extract"))

    return []


def _scan_sqlite(db_path: str, table: str, date_col: str, extract: str | None = None) -> list[str]:
    """Scan SQLite table for distinct dates."""
    import sqlite3
    try:
        con = sqlite3.connect(db_path, timeout=30)
        if extract == "substr10":
            expr = f"SUBSTR({date_col}, 1, 10)"
        elif extract == "source_file":
            # tick_logs: source_file like "ticks_2026-04-10.jsonl"
            expr = f"REPLACE(REPLACE({date_col}, 'ticks_', ''), '.jsonl', '')"
        else:
            expr = date_col
        rows = con.execute(
            f"SELECT DISTINCT {expr} AS d FROM {table} ORDER BY d DESC"
        ).fetchall()
        con.close()
        return [str(r[0])[:10] for r in rows if r[0] is not None]
    except Exception as e:
        print(f"    ! SQLite scan failed for {table}: {e}")
        return []


def _scan_jsonl() -> list[str]:
    """List JSONL tick files on disk — no DB needed."""
    jsonl_dir = DATA_ROOT / "tick_logs_cloud"
    if not jsonl_dir.exists():
        return []
    dates = set()
    for f in jsonl_dir.glob("*.jsonl"):
        # Handle "2026-04-10.jsonl" and "ticks_2026-04-10.jsonl"
        stem = f.stem.replace("ticks_", "").replace("raw_ws_", "")
        if len(stem) == 10:
            dates.add(stem)
    return sorted(dates, reverse=True)


# ─── Rebuild all ──────────────────────────────────────────────────────────────

def rebuild(tables: list[str] | None = None):
    """Full rebuild — scans all sources. Run once, then use add_date() incrementally."""
    if tables is None:
        tables = list(TABLE_REGISTRY.keys())

    manifest = _load()
    print(f"Rebuilding date manifest for {len(tables)} tables...")

    for table in tables:
        cfg = TABLE_REGISTRY.get(table)
        if not cfg:
            print(f"  ! {table}: not in TABLE_REGISTRY, skipping")
            continue

        print(f"  {table:25s} ({cfg['source']})...", end="", flush=True)
        t0 = time.time()
        dates = _scan_table(table)
        elapsed = (time.time() - t0) * 1000
        manifest[table] = dates

        if dates:
            print(f" {len(dates):>5} dates  ({dates[-1]} -> {dates[0]})  [{elapsed:.0f}ms]")
        else:
            print(f" empty  [{elapsed:.0f}ms]")

    manifest["_updated"] = datetime.now(PKT).isoformat()
    _save(manifest)
    print(f"\nSaved to {MANIFEST_PATH} ({MANIFEST_PATH.stat().st_size:,} bytes)")


def show():
    """Print manifest summary."""
    manifest = _load()
    if not manifest:
        print("Manifest is empty. Run: python -m pakfindata.db.date_manifest rebuild")
        return

    updated = manifest.get("_updated", "unknown")
    print(f"Date Manifest (updated: {updated})")
    print(f"File: {MANIFEST_PATH}")
    print(f"Size: {MANIFEST_PATH.stat().st_size:,} bytes\n")

    for table, dates in sorted(manifest.items()):
        if table.startswith("_"):
            continue
        if isinstance(dates, list) and dates:
            print(f"  {table:25s}  {len(dates):>5} dates  {dates[-1]} -> {dates[0]}")
        elif isinstance(dates, list):
            print(f"  {table:25s}  empty")


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "rebuild"

    if cmd == "rebuild":
        rebuild()
    elif cmd == "show":
        show()
    elif cmd == "rebuild-one" and len(sys.argv) > 2:
        rebuild([sys.argv[2]])
    else:
        print("Usage:")
        print("  python -m pakfindata.db.date_manifest rebuild          # full rebuild")
        print("  python -m pakfindata.db.date_manifest rebuild-one TBL  # one table")
        print("  python -m pakfindata.db.date_manifest show             # print summary")
