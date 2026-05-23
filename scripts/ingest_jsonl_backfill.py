"""Backfill tick_logs + intraday_bars from JSONL tick files.

For each unprocessed ticks_YYYY-MM-DD.jsonl:
  1. Insert every tick into tick_logs (ms precision, full bid/ask).
  2. Aggregate to 1-second OHLCV bars and insert into intraday_bars
     with market/interval/trade_count/vwap.

Durability:
  - Batched inserts (INSERT OR IGNORE) — safe to re-run; PK collisions silently dropped
  - Processes one file at a time with checkpoint commits
  - Tracks processed files via source_file in tick_logs (idempotent)
  - Fallback to raw_ws_*.jsonl if ticks_*.jsonl missing for a date

Usage:
    python scripts/ingest_jsonl_backfill.py                  # process all unprocessed
    python scripts/ingest_jsonl_backfill.py --date 2026-04-15
    python scripts/ingest_jsonl_backfill.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pakfindata.settings import get_settings  # noqa: E402

DEFAULT_DB = get_settings().db_path
JSONL_DIR = Path(get_settings().data_root) / "tick_logs_cloud"
PKT = timezone(timedelta(hours=5))
BATCH_SIZE = 50_000

TICK_INSERT_SQL = """
INSERT OR IGNORE INTO tick_logs (
    symbol, market, timestamp, _ts, price, open, high, low,
    change, change_pct, volume, value, trades,
    bid, ask, bid_vol, ask_vol, prev_close, source_file
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

BAR_INSERT_SQL = """
INSERT OR IGNORE INTO intraday_bars (
    symbol, market, date, ts, ts_epoch, interval,
    open, high, low, close, volume, value, trade_count, vwap, source
) VALUES (?, ?, ?, ?, ?, '1s', ?, ?, ?, ?, ?, ?, ?, ?, 'jsonl')
"""


def apply_pragmas(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-524288")
    con.execute("PRAGMA busy_timeout=60000")


def tick_row_from_processed(tick: dict, source_file: str) -> tuple | None:
    """Flatten processed ticks_*.jsonl format to tick_logs columns."""
    sym = tick.get("symbol")
    mkt = tick.get("market")
    ts = tick.get("timestamp")
    _ts = tick.get("_ts")
    if not sym or not mkt or ts is None or not _ts:
        return None
    return (
        sym, mkt, float(ts), _ts,
        tick.get("price"), tick.get("open"), tick.get("high"), tick.get("low"),
        tick.get("change"), tick.get("changePercent"),
        int(tick.get("volume") or 0), float(tick.get("value") or 0),
        int(tick.get("trades") or 0),
        float(tick.get("bid") or 0), float(tick.get("ask") or 0),
        int(tick.get("bidVol") or 0), int(tick.get("askVol") or 0),
        tick.get("previousClose"), source_file,
    )


def tick_row_from_raw(msg: dict, source_file: str) -> tuple | None:
    """Flatten raw_ws_*.jsonl tickUpdate to tick_logs columns."""
    if msg.get("type") != "tickUpdate":
        return None
    sym = msg.get("symbol")
    mkt = msg.get("market")
    ts_ms = msg.get("timestamp")
    t = msg.get("tick") or {}
    if not sym or not mkt or ts_ms is None:
        return None
    ts_float = ts_ms / 1000.0
    _ts = datetime.fromtimestamp(ts_float, PKT).isoformat()
    return (
        sym, mkt, ts_float, _ts,
        t.get("c"), t.get("o"), t.get("h"), t.get("l"),
        t.get("ch"), t.get("pch"),
        int(t.get("v") or 0), float(t.get("val") or 0),
        int(t.get("tr") or 0),
        float(t.get("bp") or 0), float(t.get("ap") or 0),
        int(t.get("bv") or 0), int(t.get("av") or 0),
        t.get("ldcp"), source_file,
    )


def _find_source_file(date: str) -> tuple[Path | None, str]:
    """Return (path, format) for a given date. format = 'processed' or 'raw'."""
    processed = JSONL_DIR / f"ticks_{date}.jsonl"
    if processed.exists() and processed.stat().st_size > 0:
        return processed, "processed"
    raw = JSONL_DIR / f"raw_ws_{date}.jsonl"
    if raw.exists() and raw.stat().st_size > 0:
        return raw, "raw"
    return None, ""


def _iter_ticks(path: Path, fmt: str):
    """Yield tick dicts from JSONL, regardless of format."""
    with open(path) as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line), fmt
            except json.JSONDecodeError:
                continue


def aggregate_bars(ticks: list[tuple]) -> list[tuple]:
    """Aggregate tick_logs rows into 1-second OHLCV bars.

    Input rows are tuples following TICK_INSERT_SQL column order.
    Groups by (symbol, market, floor(timestamp)).
    """
    # Column indices matching TICK_INSERT_SQL:
    # 0 symbol, 1 market, 2 timestamp, 3 _ts, 4 price, 5-7 open/high/low (L1 snapshot),
    # 8 change, 9 change_pct, 10 volume, 11 value, 12 trades,
    # 13 bid, 14 ask, 15 bid_vol, 16 ask_vol, 17 prev_close, 18 source_file

    # bucket: (sym, mkt, sec_epoch) -> dict with price list, value list, trade_count, last_volume
    buckets: dict[tuple, dict] = {}

    for row in ticks:
        sym = row[0]
        mkt = row[1]
        ts = row[2]
        price = row[4]
        vol_cum = row[10]  # cumulative daily volume (PSX convention)
        val_cum = row[11]
        trades_cum = row[12]

        if price is None or price <= 0:
            continue
        sec = int(ts)
        key = (sym, mkt, sec)
        b = buckets.get(key)
        if b is None:
            b = {"o": price, "h": price, "l": price, "c": price,
                 "vol_first": vol_cum, "vol_last": vol_cum,
                 "val_first": val_cum, "val_last": val_cum,
                 "tr_first": trades_cum, "tr_last": trades_cum,
                 "px_vol_sum": 0.0, "vol_delta_sum": 0.0}
            buckets[key] = b
        else:
            b["h"] = max(b["h"], price)
            b["l"] = min(b["l"], price)
            b["c"] = price
            b["vol_last"] = vol_cum
            b["val_last"] = val_cum
            b["tr_last"] = trades_cum

    # Transform buckets to bar rows
    bars = []
    for (sym, mkt, sec), b in buckets.items():
        bar_vol = max(0, (b["vol_last"] or 0) - (b["vol_first"] or 0))
        bar_val = max(0.0, (b["val_last"] or 0) - (b["val_first"] or 0))
        bar_trades = max(0, (b["tr_last"] or 0) - (b["tr_first"] or 0))
        vwap = (bar_val / bar_vol) if bar_vol > 0 else b["c"]
        dt = datetime.fromtimestamp(sec, PKT)
        bars.append((
            sym, mkt, dt.strftime("%Y-%m-%d"),
            dt.strftime("%Y-%m-%d %H:%M:%S"), sec,
            b["o"], b["h"], b["l"], b["c"],
            float(bar_vol), float(bar_val), int(bar_trades), float(vwap),
        ))
    return bars


def process_date(con: sqlite3.Connection, date: str, dry_run: bool = False,
                 source_filter: str = "any") -> dict:
    """Ingest one date. Returns stats dict."""
    path, fmt = _find_source_file(date)
    if not path:
        return {"date": date, "error": "no source file"}

    # Check if already processed
    source_name = path.name
    done = con.execute(
        "SELECT COUNT(*) FROM tick_logs WHERE source_file = ?", [source_name]
    ).fetchone()[0]
    if done > 0 and not dry_run:
        return {"date": date, "skipped": True, "existing_rows": done}

    print(f"[{date}] Processing {path.name} ({path.stat().st_size/1e6:.1f} MB, fmt={fmt})")
    t0 = time.time()

    tick_rows: list[tuple] = []
    parsed = 0
    bad = 0

    for msg, _ in _iter_ticks(path, fmt):
        parsed += 1
        if fmt == "processed":
            row = tick_row_from_processed(msg, source_name)
        else:
            row = tick_row_from_raw(msg, source_name)
        if row is None:
            bad += 1
            continue
        tick_rows.append(row)

    parse_time = time.time() - t0
    print(f"  Parsed {parsed:,} lines ({bad} skipped) in {parse_time:.1f}s")

    if dry_run:
        bars = aggregate_bars(tick_rows)
        print(f"  [DRY-RUN] would insert {len(tick_rows):,} ticks and {len(bars):,} 1s bars")
        return {
            "date": date, "dry_run": True,
            "tick_count": len(tick_rows), "bar_count": len(bars),
        }

    # 1) Insert ticks in batches — count via total_changes delta
    t0 = time.time()
    tc_before = con.total_changes
    for i in range(0, len(tick_rows), BATCH_SIZE):
        batch = tick_rows[i:i+BATCH_SIZE]
        con.executemany(TICK_INSERT_SQL, batch)
    con.commit()
    tick_inserted = con.total_changes - tc_before
    tick_time = time.time() - t0
    print(f"  ticks inserted: {tick_inserted:,} (dupes dropped: {len(tick_rows) - tick_inserted}) "
          f"in {tick_time:.1f}s")

    # 2) Aggregate and insert 1s bars
    t0 = time.time()
    bar_rows = aggregate_bars(tick_rows)
    tc_before = con.total_changes
    for i in range(0, len(bar_rows), BATCH_SIZE):
        batch = bar_rows[i:i+BATCH_SIZE]
        con.executemany(BAR_INSERT_SQL, batch)
    con.commit()
    bar_inserted = con.total_changes - tc_before
    bar_time = time.time() - t0
    print(f"  1s bars inserted: {bar_inserted:,} (dupes dropped: {len(bar_rows) - bar_inserted}) "
          f"in {bar_time:.1f}s")

    return {
        "date": date, "ticks": tick_inserted, "bars": bar_inserted,
        "skipped": False,
    }


def find_missing_dates(con: sqlite3.Connection) -> list[str]:
    """Find all JSONL dates on disk that haven't been fully ingested."""
    disk = sorted({
        p.stem.replace("ticks_", "").replace("raw_ws_", "")
        for p in JSONL_DIR.glob("ticks_*.jsonl")
    } | {
        p.stem.replace("raw_ws_", "")
        for p in JSONL_DIR.glob("raw_ws_*.jsonl")
    })
    processed = {r[0] for r in con.execute(
        "SELECT DISTINCT source_file FROM tick_logs WHERE source_file IS NOT NULL"
    )}
    # Normalize: treat ticks_DATE.jsonl and raw_ws_DATE.jsonl as 'DATE'
    processed_dates = set()
    for f in processed:
        base = f.replace("ticks_", "").replace("raw_ws_", "").replace(".jsonl", "")
        processed_dates.add(base)
    return [d for d in disk if d and d not in processed_dates]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--date", help="Process single date (YYYY-MM-DD)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    con = sqlite3.connect(args.db)
    apply_pragmas(con)

    if args.date:
        targets = [args.date]
    else:
        targets = find_missing_dates(con)
        print(f"[INFO] Found {len(targets)} unprocessed date(s): {targets}")

    if not targets:
        print("[INFO] Nothing to process")
        con.close()
        return 0

    t_all = time.time()
    total_ticks = 0
    total_bars = 0
    for date in targets:
        try:
            stats = process_date(con, date, args.dry_run)
            if stats.get("skipped"):
                print(f"[{date}] already ingested ({stats['existing_rows']:,} rows)")
            elif "error" in stats:
                print(f"[{date}] ERROR: {stats['error']}")
            else:
                total_ticks += stats.get("ticks", stats.get("tick_count", 0))
                total_bars += stats.get("bars", stats.get("bar_count", 0))
        except Exception as e:
            print(f"[{date}] ERROR: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n[DONE] total: {total_ticks:,} ticks, {total_bars:,} bars "
          f"in {time.time()-t_all:.1f}s")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
