#!/usr/bin/env python3
"""
Sharded kline fetcher — split symbols across parallel processes.

Usage:
    # Run 6 shards in 6 terminals:
    python scripts/fetch_klines_sharded.py 1m --shard 1 --shards 6
    python scripts/fetch_klines_sharded.py 1m --shard 2 --shards 6
    python scripts/fetch_klines_sharded.py 1m --shard 3 --shards 6
    python scripts/fetch_klines_sharded.py 1m --shard 4 --shards 6
    python scripts/fetch_klines_sharded.py 1m --shard 5 --shards 6
    python scripts/fetch_klines_sharded.py 1m --shard 6 --shards 6

    # Or launch all 6 at once:
    python scripts/fetch_klines_sharded.py 1m --shards 6 --launch-all

Each shard gets ~90 symbols (540/6), runs sequentially, writes its own CSV,
and inserts into the same SQLite table (deduped by INSERT OR IGNORE).
"""

import argparse, subprocess, sys, time, json, csv, sqlite3, requests
from pathlib import Path
from datetime import datetime, timezone, timedelta

PSXT_BASE = "https://psxterminal.com/api"
PKT = timezone(timedelta(hours=5))
DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")
DATA_DIR = Path.home() / "psxdata" / "intraday" / datetime.now(PKT).strftime("%Y-%m-%d")

RATE_LIMIT = 0.5
INDEX_SYMBOLS = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30"
}

session = requests.Session()
session.headers.update({"User-Agent": "pakfindata/1.0", "Accept": "application/json"})


def api_get(endpoint, params=None, retries=3):
    """GET with retry on 503/timeout."""
    for attempt in range(retries):
        try:
            r = session.get(f"{PSXT_BASE}/{endpoint}", params=params, timeout=15)
            if r.status_code == 200:
                text = r.text.split("<")[0].strip()
                if text:
                    d = json.loads(text)
                    if d.get("success"):
                        return d
            if r.status_code >= 500 and attempt < retries - 1:
                wait = 3 * (attempt + 1)
                time.sleep(wait)
                continue
            return None
        except (requests.Timeout, requests.ConnectionError):
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception:
            return None
    return None


def get_excluded_symbols():
    """Get suspended/winding-up symbols from DB — no point fetching klines for these."""
    excluded = set()
    try:
        con = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = con.execute(
            "SELECT DISTINCT symbol FROM company_listing_status "
            "WHERE is_current=1 AND status IN ('SUSPENDED', 'WINDING-UP')"
        ).fetchall()
        con.close()
        excluded = {r[0] for r in rows}
    except Exception:
        pass
    return excluded


def get_symbols():
    excluded = get_excluded_symbols()
    if excluded:
        print(f"  Excluding {len(excluded)} suspended/winding-up symbols")

    data = api_get("symbols")
    if data:
        syms = sorted([s for s in data["data"] if s not in INDEX_SYMBOLS and s not in excluded])
        return syms
    # Fallback: load from SQLite if API is down
    print("API down, loading symbols from DB...")
    try:
        con = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = con.execute("SELECT DISTINCT symbol FROM psx_eod ORDER BY symbol").fetchall()
        con.close()
        if rows:
            syms = [r[0] for r in rows if r[0] not in INDEX_SYMBOLS and r[0] not in excluded]
            print(f"  Loaded {len(syms)} symbols from psx_eod table")
            return syms
    except Exception:
        pass
    print("ERROR: Cannot fetch symbol list from API or DB")
    sys.exit(1)


def fetch_symbol_klines(sym, timeframe):
    bars = []
    end_ts = None
    pages = 0

    while True:
        params = {"limit": 100}
        if end_ts:
            params["endTimestamp"] = end_ts

        data = api_get(f"klines/{sym}/{timeframe}", params)
        if not data or not data.get("data"):
            break

        batch = data["data"]
        bars.extend(batch)
        pages += 1

        if len(batch) < 100:
            break

        earliest = min(b["timestamp"] for b in batch)
        end_ts = earliest - 1
        time.sleep(RATE_LIMIT + 0.2)

    seen = set()
    unique = []
    for b in bars:
        key = (b["symbol"], b["timeframe"], b["timestamp"])
        if key not in seen:
            seen.add(key)
            unique.append(b)

    return unique, pages


def write_csv(bars, filepath):
    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "timestamp", "datetime", "open", "high", "low", "close", "volume", "timeframe"])
        for b in sorted(bars, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(b["timestamp"] / 1000, PKT).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([b["symbol"], b["timestamp"], dt,
                        b["open"], b["high"], b["low"], b["close"],
                        b["volume"], b.get("timeframe", "")])
    print(f"  CSV: {filepath.name} — {len(bars):,} rows")


def store_db(bars, table):
    columns = ["symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume"]
    con = sqlite3.connect(str(DB_PATH), timeout=60)
    con.execute("PRAGMA journal_mode=WAL")

    col_defs = ", ".join(
        f"{c} {'TEXT' if c in ('symbol','timeframe') else 'INTEGER' if c in ('timestamp','volume') else 'REAL'}"
        for c in columns
    )
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs}, PRIMARY KEY (symbol, timeframe, timestamp))")

    stored = 0
    for b in bars:
        try:
            values = [b.get(c) for c in columns]
            con.execute(f"INSERT OR IGNORE INTO {table} VALUES ({', '.join('?' * len(columns))})", values)
            stored += 1
        except Exception:
            pass

    con.commit()
    con.close()
    return stored


def run_shard(symbols, timeframe, shard_id, total_shards):
    """Run one shard."""
    tag = f"shard {shard_id}/{total_shards}"
    print(f"\n{'='*60}")
    print(f"  KLINES {timeframe} DEEP — {tag} — {len(symbols)} symbols")
    print(f"  Symbols: {symbols[0]} ... {symbols[-1]}")
    print(f"{'='*60}")

    all_bars = []
    failed = []
    done = 0
    start = time.time()

    for sym in symbols:
        try:
            bars, pages = fetch_symbol_klines(sym, timeframe)
            all_bars.extend(bars)
            done += 1
            if bars:
                print(f"  [{done}/{len(symbols)}] {sym}: {len(bars)} bars ({pages} pages)")
            else:
                failed.append(sym)
                print(f"  [{done}/{len(symbols)}] {sym}: no data")
        except Exception as e:
            failed.append(sym)
            done += 1
            print(f"  [{done}/{len(symbols)}] {sym}: ERROR {e}")
        time.sleep(1.0)

    elapsed = time.time() - start
    print(f"\n  {tag} done: {len(all_bars):,} bars from {done - len(failed)}/{len(symbols)} symbols in {elapsed:.0f}s")

    if failed:
        print(f"  Failed ({len(failed)}): {', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}")

    if all_bars:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")
        fname = f"psxt_{date_str}_{timeframe}_shard{shard_id}.csv"

        write_csv(all_bars, DATA_DIR / fname)

        table = f"psxt_klines_{timeframe}"
        stored = store_db(all_bars, table)
        print(f"  DB:  {stored:,} rows → {table}")

    return all_bars


def launch_all(timeframe, total_shards):
    """Launch all shards as background processes."""
    script = Path(__file__).resolve()
    procs = []
    for i in range(1, total_shards + 1):
        log = DATA_DIR / f"shard{i}_{timeframe}.log"
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(script), timeframe, "--shard", str(i), "--shards", str(total_shards)]
        print(f"  Launching shard {i}/{total_shards} → {log.name}")
        f = open(log, "w")
        p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
        procs.append((i, p, f))
        if i < total_shards:
            print(f"  Waiting 10s before next shard...")
            time.sleep(10)

    print(f"\n  {total_shards} shards launched. Logs in: {DATA_DIR}/shard*_{timeframe}.log")
    print(f"  Monitor: tail -f {DATA_DIR}/shard*_{timeframe}.log")

    # Wait for all
    for i, p, f in procs:
        p.wait()
        f.close()
        status = "OK" if p.returncode == 0 else f"FAILED (exit {p.returncode})"
        print(f"  Shard {i}: {status}")

    print("\n  ALL SHARDS COMPLETE")


def main():
    parser = argparse.ArgumentParser(description="Sharded kline fetcher")
    parser.add_argument("timeframe", choices=["1m", "5m", "15m", "1h", "1d", "1w"])
    parser.add_argument("--shard", type=int, help="This shard number (1-based)")
    parser.add_argument("--shards", type=int, default=6, help="Total number of shards (default: 6)")
    parser.add_argument("--launch-all", action="store_true", help="Launch all shards as background processes")

    args = parser.parse_args()

    print(f"Fetching symbols...")
    symbols = get_symbols()
    print(f"{len(symbols)} symbols loaded")

    if args.launch_all:
        launch_all(args.timeframe, args.shards)
        return

    if not args.shard:
        print("ERROR: specify --shard N or --launch-all")
        sys.exit(1)

    # Split symbols into shards
    chunk_size = len(symbols) // args.shards
    remainder = len(symbols) % args.shards
    chunks = []
    start = 0
    for i in range(args.shards):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(symbols[start:end])
        start = end

    my_symbols = chunks[args.shard - 1]
    print(f"Shard {args.shard}/{args.shards}: {len(my_symbols)} symbols")

    run_shard(my_symbols, args.timeframe, args.shard, args.shards)


if __name__ == "__main__":
    main()
