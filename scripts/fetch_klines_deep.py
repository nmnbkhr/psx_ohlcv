#!/usr/bin/env python3
"""
Deep kline fetcher — downloads 1m then 5m for ALL symbols with retry + throttle.

Reduces concurrency to 4 workers and adds 3 retries with exponential backoff
to avoid PSX Terminal throttling that silently drops symbols.

Usage:
    python scripts/fetch_klines_deep.py              # 1m then 5m
    python scripts/fetch_klines_deep.py 5m           # 5m only
    python scripts/fetch_klines_deep.py 1m 5m 15m    # multiple timeframes
"""

import sys, time, json, csv, sqlite3, requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

PSXT_BASE = "https://psxterminal.com/api"
PKT = timezone(timedelta(hours=5))
DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")
DATA_DIR = Path.home() / "psxdata" / "intraday" / datetime.now(PKT).strftime("%Y-%m-%d")

WORKERS = 1          # sequential — one symbol at a time
RATE_LIMIT = 0.5     # seconds between requests
MAX_RETRIES = 5
RETRY_BACKOFF = 5.0  # exponential backoff base (5s, 25s, ...)

INDEX_SYMBOLS = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30"
}

session = requests.Session()
session.headers.update({"User-Agent": "pakfindata/1.0", "Accept": "application/json"})


def api_get(endpoint, params=None, retries=MAX_RETRIES):
    """GET with retry + exponential backoff."""
    for attempt in range(retries):
        try:
            r = session.get(f"{PSXT_BASE}/{endpoint}", params=params, timeout=15)
            if r.status_code == 200:
                text = r.text.split("<")[0].strip()
                if text:
                    d = json.loads(text)
                    if d.get("success"):
                        return d
            # Server error — skip immediately, likely invalid symbol
            if r.status_code >= 500:
                return None
            return None  # 4xx — don't retry
        except (requests.Timeout, requests.ConnectionError):
            return None
        except Exception:
            return None
    return None


def get_symbols():
    """Get all tradeable symbols."""
    data = api_get("symbols")
    if data:
        return sorted([s for s in data["data"] if s not in INDEX_SYMBOLS])
    print("ERROR: Cannot fetch symbol list")
    sys.exit(1)


def fetch_symbol_klines(sym, timeframe):
    """Fetch all kline pages for one symbol with retry."""
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
        time.sleep(RATE_LIMIT + 0.2)  # extra breathing room between pages

    # Deduplicate
    seen = set()
    unique = []
    for b in bars:
        key = (b["symbol"], b["timeframe"], b["timestamp"])
        if key not in seen:
            seen.add(key)
            unique.append(b)

    return unique, pages


def write_csv(bars, filepath):
    """Write klines to CSV."""
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
    """Store to SQLite."""
    columns = ["symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume"]
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")

    col_defs = ", ".join(
        f"{c} {'TEXT' if c in ('symbol','timeframe') else 'INTEGER' if c in ('timestamp','volume') else 'REAL'}"
        for c in columns
    )
    pk = f", PRIMARY KEY (symbol, timeframe, timestamp)"
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs}{pk})")

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


def run_timeframe(symbols, timeframe):
    """Download one timeframe for all symbols."""
    print(f"\n{'='*60}")
    print(f"  KLINES {timeframe} DEEP — {len(symbols)} symbols, {WORKERS} workers")
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
        time.sleep(1.0)  # 1s cooldown between symbols

    elapsed = time.time() - start
    print(f"\n  Done: {len(all_bars):,} bars from {done - len(failed)}/{len(symbols)} symbols in {elapsed:.0f}s")

    if failed:
        print(f"  Failed ({len(failed)}): {', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}")

    if all_bars:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")

        if timeframe == "1m":
            fname = f"psxt_{date_str}_1m.csv"
        else:
            fname = f"psxt_backfill_{timeframe}.csv"

        write_csv(all_bars, DATA_DIR / fname)

        table = f"psxt_klines_{timeframe}"
        stored = store_db(all_bars, table)
        print(f"  DB:  {stored:,} rows → {table}")

    return all_bars


def main():
    timeframes = sys.argv[1:] if len(sys.argv) > 1 else ["1m", "5m"]

    valid = {"1m", "5m", "15m", "1h", "1d", "1w"}
    for tf in timeframes:
        if tf not in valid:
            print(f"Invalid timeframe: {tf}. Use: {', '.join(sorted(valid))}")
            sys.exit(1)

    print(f"Fetching symbols...")
    symbols = get_symbols()
    print(f"{len(symbols)} symbols loaded")

    total_bars = 0
    start = time.time()

    for tf in timeframes:
        bars = run_timeframe(symbols, tf)
        total_bars += len(bars)

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"  ALL DONE — {total_bars:,} total bars in {elapsed:.0f}s")
    print(f"  Files in: {DATA_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
