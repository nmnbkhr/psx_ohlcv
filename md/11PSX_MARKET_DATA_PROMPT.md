# Claude Code Prompt: PSX Market Data Backfill — DPS + PSX Terminal

## Context

Two data sources for Pakistan Stock Exchange market data:

### Source 1: PSX DPS (Official — PRIMARY)
```
Base URL: https://dps.psx.com.pk

GET /timeseries/eod/{SYMBOL}
  → Daily OHLCV, ~1,237 days (~5 years back to 2021)
  → Format: {"status":1,"data":[[timestamp, close, volume, open], ...]}
  → Newest first, all days in one response (no pagination needed)
  → ~487 symbols available

GET /timeseries/int/{SYMBOL}
  → Tick-level trades, today only (~4,000-10,000 trades per active symbol)
  → Format: {"status":1,"data":[[timestamp, price, volume], ...]}
  → Newest first, full day in one response
  → Only current trading day — no historical

GET /market-watch
  → Current market snapshot (all symbols, live prices)
```

### Source 2: PSX Terminal (Third-party — SUPPLEMENTARY)
```
Base URL: https://psxterminal.com/api

GET /klines/{SYMBOL}/{INTERVAL}?limit=100&startTimestamp={ms}&endTimestamp={ms}
  → OHLCV candles at multiple intervals
  → Intervals: 1m, 5m, 15m, 1h, 1d, 1w
  → Max 100 bars per request (HARD LIMIT from server)
  → Supports pagination via startTimestamp/endTimestamp (milliseconds)
  → Format: {"success":true,"data":[{"symbol","timeframe","timestamp","open","high","low","close","volume"}]}
  → 1m data: today only (past dates redirect to today)
  → 1d data: ~100 trading days (~5 months)
  → IMPORTANT: Their API sometimes appends 503 HTML after JSON — strip with .split("<")[0]

GET /symbols → list of all ~487 symbols
GET /fundamentals/{SYMBOL} → P/E, dividend yield, market cap
GET /companies/{SYMBOL} → business description, key people
```

### Storage: ~/psxdata/intraday/
### Project: ~/pakfindata/

## Task: Build unified backfill system

### Step 1: Check existing data and code

```bash
# What intraday scrapers already exist?
grep -rn "timeseries/int\|timeseries/eod\|dps.psx.com.pk/timeseries" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

# What psxterminal scrapers exist?
grep -rn "psxterminal.com/api\|klines" ~/pakfindata/src/ --include="*.py" | grep -v __pacache__

# What's already in the intraday folder?
ls -la ~/psxdata/intraday/ 2>/dev/null || echo "folder doesn't exist yet"

# Check existing OHLCV tables in DB
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in con.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]:
    if any(k in t.lower() for k in ['ohlcv','kline','candle','intraday','timeseries','tick','eod']):
        count = con.execute(f'SELECT COUNT(*) FROM [{t}]').fetchone()[0]
        cols = [c[1] for c in con.execute(f'PRAGMA table_info([{t}])').fetchall()]
        print(f'{t}: {count:,} rows | {cols[:8]}')
con.close()
"
```

Read the output before proceeding. Don't duplicate existing functionality.

### Step 2: Create the unified scraper

Create `src/pakfindata/sources/psx_market_data.py`:

```python
"""
Unified PSX Market Data Backfill — DPS (primary) + PSX Terminal (supplementary).

Sources:
  DPS (official):
    /timeseries/eod/{sym}  → 5-year daily OHLCV (1,237+ days)
    /timeseries/int/{sym}  → Today's tick-level trades (4,000+ per symbol)
  
  PSX Terminal (supplementary):
    /klines/{sym}/{tf}     → Intraday OHLCV (1m/5m/15m/1h, max 100/request, paginated)

Usage:
  python -m pakfindata.sources.psx_market_data eod                  # DPS daily all symbols
  python -m pakfindata.sources.psx_market_data eod HUBC             # DPS daily one symbol
  python -m pakfindata.sources.psx_market_data ticks                # DPS today's ticks all symbols
  python -m pakfindata.sources.psx_market_data ticks HUBC           # DPS today's ticks one symbol
  python -m pakfindata.sources.psx_market_data klines 1h            # PSX Terminal 1h all symbols
  python -m pakfindata.sources.psx_market_data klines 5m            # PSX Terminal 5m all symbols
  python -m pakfindata.sources.psx_market_data klines 1m            # PSX Terminal 1m (today only)
  python -m pakfindata.sources.psx_market_data klines 1h --deep     # Paginate for max bars
  python -m pakfindata.sources.psx_market_data all                  # Everything in one go
  python -m pakfindata.sources.psx_market_data status               # Show coverage
"""

import requests
import sqlite3
import csv
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ═══════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════

DPS_BASE = "https://dps.psx.com.pk"
PSXT_BASE = "https://psxterminal.com/api"

DATA_DIR = Path.home() / "psxdata" / "intraday"
DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")

PKT = timezone(timedelta(hours=5))
RATE_LIMIT = 0.3  # seconds between requests

# Skip index symbols (no trade data)
INDEX_SYMBOLS = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30"
}


# ═══════════════════════════════════════════════════════
# HTTP CLIENT
# ═══════════════════════════════════════════════════════

class HTTPClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "pakfindata/1.0",
            "Accept": "application/json",
        })
    
    def get_dps(self, endpoint: str) -> dict | None:
        """Fetch from DPS — returns full response, no pagination needed."""
        try:
            r = self.session.get(f"{DPS_BASE}/{endpoint}", timeout=30)
            if r.status_code == 200:
                d = r.json()
                if d.get("status") == 1:
                    return d
            return None
        except Exception as e:
            return None
    
    def get_psxt(self, endpoint: str, params: dict = None) -> dict | None:
        """Fetch from PSX Terminal — handles 503 HTML appended to JSON."""
        try:
            r = self.session.get(f"{PSXT_BASE}/{endpoint}", params=params, timeout=15)
            if r.status_code != 200:
                return None
            # PSX Terminal sometimes appends 503 HTML after valid JSON
            text = r.text.split("<")[0].strip()
            if not text:
                return None
            d = json.loads(text)
            return d if d.get("success") else None
        except Exception:
            return None


# ═══════════════════════════════════════════════════════
# SYMBOL LIST
# ═══════════════════════════════════════════════════════

def get_all_symbols(client: HTTPClient) -> list[str]:
    """Get all tradeable symbols from PSX Terminal."""
    data = client.get_psxt("symbols")
    if data:
        return [s for s in data["data"] if s not in INDEX_SYMBOLS]
    # Fallback: use DPS market-watch
    return []


# ═══════════════════════════════════════════════════════
# SOURCE 1: DPS EOD — 5-YEAR DAILY OHLCV
# ═══════════════════════════════════════════════════════

def fetch_dps_eod(client: HTTPClient, symbols: list[str]) -> list[dict]:
    """
    Fetch daily OHLCV from DPS for all symbols.
    
    DPS format: [[timestamp, close, volume, open], ...]
    NOTE: DPS does NOT provide high/low — only open, close, volume.
    We store what we get and supplement with PSX Terminal for full OHLCV.
    """
    all_bars = []
    total = len(symbols)
    
    for i, sym in enumerate(symbols, 1):
        data = client.get_dps(f"timeseries/eod/{sym}")
        if data and data.get("data"):
            for row in data["data"]:
                # DPS EOD: [timestamp_seconds, close, volume, open]
                ts = row[0]
                all_bars.append({
                    "symbol": sym,
                    "source": "dps",
                    "timeframe": "1d",
                    "timestamp": ts * 1000,  # convert to ms for consistency
                    "open": row[3],
                    "high": None,  # DPS doesn't provide high
                    "low": None,   # DPS doesn't provide low
                    "close": row[1],
                    "volume": row[2],
                })
        
        if i % 25 == 0:
            print(f"  [{i}/{total}] {len(all_bars):,} bars")
        time.sleep(RATE_LIMIT)
    
    return all_bars


# ═══════════════════════════════════════════════════════
# SOURCE 1: DPS INTRADAY — TODAY'S TICK TRADES
# ═══════════════════════════════════════════════════════

def fetch_dps_ticks(client: HTTPClient, symbols: list[str]) -> list[dict]:
    """
    Fetch today's tick-level trade data from DPS for all symbols.
    
    DPS format: [[timestamp_seconds, price, volume], ...]
    Each row is a single trade execution — this is the richest data available.
    """
    all_ticks = []
    total = len(symbols)
    
    for i, sym in enumerate(symbols, 1):
        data = client.get_dps(f"timeseries/int/{sym}")
        if data and data.get("data"):
            for row in data["data"]:
                all_ticks.append({
                    "symbol": sym,
                    "timestamp": row[0],
                    "price": row[1],
                    "volume": row[2],
                })
        
        if i % 25 == 0:
            print(f"  [{i}/{total}] {len(all_ticks):,} ticks")
        time.sleep(RATE_LIMIT)
    
    return all_ticks


# ═══════════════════════════════════════════════════════
# SOURCE 2: PSX TERMINAL KLINES — INTRADAY OHLCV
# ═══════════════════════════════════════════════════════

def fetch_psxt_klines(client: HTTPClient, symbols: list[str], 
                      timeframe: str, deep: bool = False) -> list[dict]:
    """
    Fetch OHLCV klines from PSX Terminal.
    
    Max 100 bars per request. With deep=True, paginates BACKWARDS
    using endTimestamp to get ALL available history.
    
    Deep pagination strategy:
      1. First request: latest 100 bars (no startTimestamp)
      2. Get earliest timestamp from batch
      3. Next request: endTimestamp = earliest - 1
      4. Repeat until empty response or < 100 bars returned
      5. Deduplicate by (symbol, timeframe, timestamp)
    """
    all_bars = []
    total = len(symbols)
    
    for i, sym in enumerate(symbols, 1):
        sym_bars = []
        end_ts = None  # start from latest
        
        while True:
            params = {"limit": 100}
            if end_ts:
                params["endTimestamp"] = end_ts
            
            data = client.get_psxt(f"klines/{sym}/{timeframe}", params)
            if not data or not data.get("data"):
                break
            
            batch = data["data"]
            sym_bars.extend(batch)
            
            # If not deep mode, just grab one batch
            if not deep:
                break
            
            # If we got less than 100, we've reached the beginning
            if len(batch) < 100:
                break
            
            # Paginate backwards
            earliest = min(b["timestamp"] for b in batch)
            end_ts = earliest - 1
            
            time.sleep(RATE_LIMIT)
        
        # Deduplicate
        seen = set()
        for bar in sym_bars:
            key = (bar["symbol"], bar["timeframe"], bar["timestamp"])
            if key not in seen:
                seen.add(key)
                all_bars.append(bar)
        
        if i % 25 == 0:
            print(f"  [{i}/{total}] {len(all_bars):,} bars")
        time.sleep(RATE_LIMIT)
    
    return all_bars


# ═══════════════════════════════════════════════════════
# FILE WRITERS
# ═══════════════════════════════════════════════════════

def write_eod_csv(bars: list[dict], filepath: Path):
    """Write DPS EOD data to CSV."""
    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "timestamp", "date", "open", "high", "low", "close", "volume", "source"])
        for b in sorted(bars, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(b["timestamp"] / 1000, PKT).strftime("%Y-%m-%d")
            w.writerow([
                b["symbol"], b["timestamp"], dt,
                b["open"], b.get("high", ""), b.get("low", ""), b["close"],
                b["volume"], b.get("source", "dps")
            ])
    print(f"  ✅ {filepath.name}: {len(bars):,} rows")


def write_ticks_csv(ticks: list[dict], filepath: Path):
    """Write DPS tick trade data to CSV."""
    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "timestamp", "datetime", "price", "volume"])
        for t in sorted(ticks, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(t["timestamp"], PKT).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([t["symbol"], t["timestamp"], dt, t["price"], t["volume"]])
    print(f"  ✅ {filepath.name}: {len(ticks):,} rows")


def write_klines_csv(bars: list[dict], filepath: Path):
    """Write PSX Terminal klines to CSV."""
    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "timestamp", "datetime", "open", "high", "low", "close", "volume", "timeframe"])
        for b in sorted(bars, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(b["timestamp"] / 1000, PKT).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([
                b["symbol"], b["timestamp"], dt,
                b["open"], b["high"], b["low"], b["close"],
                b["volume"], b.get("timeframe", "")
            ])
    print(f"  ✅ {filepath.name}: {len(bars):,} rows")


# ═══════════════════════════════════════════════════════
# DB WRITER
# ═══════════════════════════════════════════════════════

def store_to_db(bars: list[dict], table: str, columns: list[str]):
    """Store data to SQLite with INSERT OR IGNORE."""
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    
    # Create table dynamically based on columns
    col_defs = ", ".join(f"{c} {'TEXT' if c in ('symbol','source','timeframe','date','datetime') else 'INTEGER' if c in ('timestamp','volume') else 'REAL'}" for c in columns)
    pk_cols = [c for c in columns if c in ("symbol", "timeframe", "timestamp")]
    pk = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""
    
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs}{pk})")
    
    placeholders = ", ".join("?" * len(columns))
    stored = 0
    for bar in bars:
        try:
            values = [bar.get(c) for c in columns]
            con.execute(f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", values)
            stored += 1
        except Exception:
            pass
    
    con.commit()
    con.close()
    return stored


# ═══════════════════════════════════════════════════════
# STATUS REPORT
# ═══════════════════════════════════════════════════════

def show_status():
    """Show data coverage across all files and DB tables."""
    print("═══════════════════════════════════════════════")
    print("  PSX MARKET DATA COVERAGE")
    print("═══════════════════════════════════════════════")
    
    # CSV files
    print("\n📁 CSV FILES (~/psxdata/intraday/):")
    if DATA_DIR.exists():
        for f in sorted(DATA_DIR.glob("*.csv")):
            # Count lines (minus header)
            with open(f) as fh:
                lines = sum(1 for _ in fh) - 1
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name:45s} {lines:>10,} rows  {size_kb:>8,.0f} KB")
    else:
        print("  (folder doesn't exist)")
    
    # DB tables
    print("\n💾 DATABASE TABLES:")
    try:
        con = sqlite3.connect(str(DB_PATH), timeout=10)
        for tbl in ["psx_eod", "psx_ticks", "psxt_klines", "psxt_klines_1h",
                     "psxt_klines_5m", "psxt_klines_15m", "psxt_klines_1m",
                     "psxt_klines_1w"]:
            try:
                row = con.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(timestamp), MAX(timestamp)
                    FROM {tbl}
                """).fetchone()
                if row[0] > 0:
                    t1 = datetime.fromtimestamp(row[2] / 1000 if row[2] > 1e12 else row[2], PKT)
                    t2 = datetime.fromtimestamp(row[3] / 1000 if row[3] > 1e12 else row[3], PKT)
                    print(f"  {tbl:25s} {row[0]:>10,} rows | {row[1]:>4} symbols | {t1.date()} → {t2.date()}")
                else:
                    print(f"  {tbl:25s} empty")
            except:
                pass
        con.close()
    except:
        print("  (DB not accessible)")
    
    print()


# ═══════════════════════════════════════════════════════
# MAIN COMMANDS
# ═══════════════════════════════════════════════════════

def cmd_eod(client, symbols, symbol=None):
    """DPS daily OHLCV — 5 years history."""
    syms = [symbol] if symbol else symbols
    print(f"\n═══ DPS EOD — {len(syms)} symbols, ~5 years daily ═══")
    
    bars = fetch_dps_eod(client, syms)
    if bars:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        write_eod_csv(bars, DATA_DIR / "dps_eod_daily.csv")
        
        # Also store in DB
        stored = store_to_db(
            [{"symbol": b["symbol"], "timestamp": b["timestamp"],
              "open": b["open"], "close": b["close"], "volume": b["volume"],
              "source": "dps"} for b in bars],
            "psx_eod",
            ["symbol", "timestamp", "open", "close", "volume", "source"]
        )
        print(f"  💾 DB: {stored:,} rows → psx_eod")
    
    return bars


def cmd_ticks(client, symbols, symbol=None):
    """DPS today's tick trades."""
    syms = [symbol] if symbol else symbols
    date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    print(f"\n═══ DPS TICKS — {len(syms)} symbols, {date_str} trades ═══")
    
    ticks = fetch_dps_ticks(client, syms)
    if ticks:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        write_ticks_csv(ticks, DATA_DIR / f"dps_ticks_{date_str}.csv")
        
        stored = store_to_db(
            ticks, "psx_ticks",
            ["symbol", "timestamp", "price", "volume"]
        )
        print(f"  💾 DB: {stored:,} rows → psx_ticks")
    
    return ticks


def cmd_klines(client, symbols, timeframe, deep=False, symbol=None):
    """PSX Terminal intraday klines."""
    syms = [symbol] if symbol else symbols
    date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    mode = "deep paginated" if deep else "latest 100"
    print(f"\n═══ PSXT KLINES {timeframe} — {len(syms)} symbols ({mode}) ═══")
    
    bars = fetch_psxt_klines(client, syms, timeframe, deep=deep)
    if bars:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        # File naming based on timeframe
        if timeframe == "1m":
            fname = f"psxt_{date_str}_1m.csv"  # today only
        elif deep:
            fname = f"psxt_backfill_{timeframe}.csv"
        else:
            fname = f"psxt_latest_{timeframe}.csv"
        
        write_klines_csv(bars, DATA_DIR / fname)
        
        # DB table per timeframe
        table = f"psxt_klines_{timeframe.replace('m','m').replace('h','h')}"
        stored = store_to_db(
            [{"symbol": b["symbol"], "timeframe": b["timeframe"],
              "timestamp": b["timestamp"], "open": b["open"], "high": b["high"],
              "low": b["low"], "close": b["close"], "volume": b["volume"]}
             for b in bars],
            table,
            ["symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume"]
        )
        print(f"  💾 DB: {stored:,} rows → {table}")
    
    return bars


def cmd_all(client, symbols):
    """Run everything — full backfill."""
    print("═══════════════════════════════════════════════")
    print("  FULL PSX MARKET DATA BACKFILL")
    print("═══════════════════════════════════════════════")
    
    start_time = time.time()
    total_bars = 0
    total_ticks = 0
    
    # 1. DPS EOD — 5 years daily (PRIMARY, biggest value)
    bars = cmd_eod(client, symbols)
    total_bars += len(bars) if bars else 0
    
    # 2. DPS Ticks — today's trades (PRIMARY)
    ticks = cmd_ticks(client, symbols)
    total_ticks += len(ticks) if ticks else 0
    
    # 3. PSX Terminal — weekly (deep, ~2 years)
    bars = cmd_klines(client, symbols, "1w", deep=True)
    total_bars += len(bars) if bars else 0
    
    # 4. PSX Terminal — 1h (deep, ~16+ days)
    bars = cmd_klines(client, symbols, "1h", deep=True)
    total_bars += len(bars) if bars else 0
    
    # 5. PSX Terminal — 15m (deep, ~4+ days)
    bars = cmd_klines(client, symbols, "15m", deep=True)
    total_bars += len(bars) if bars else 0
    
    # 6. PSX Terminal — 5m (deep, ~1-2 days)
    bars = cmd_klines(client, symbols, "5m", deep=True)
    total_bars += len(bars) if bars else 0
    
    # 7. PSX Terminal — 1m (today only, paginated)
    bars = cmd_klines(client, symbols, "1m", deep=True)
    total_bars += len(bars) if bars else 0
    
    elapsed = time.time() - start_time
    
    print(f"\n{'═' * 50}")
    print(f"✅ COMPLETE in {elapsed/60:.1f} minutes")
    print(f"   Bars: {total_bars:,}")
    print(f"   Ticks: {total_ticks:,}")
    print(f"   Total: {total_bars + total_ticks:,} data points")
    print(f"📁 Files: {DATA_DIR}/")
    
    show_status()


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PSX Market Data Backfill — DPS + PSX Terminal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  eod                  DPS 5-year daily OHLCV (all symbols)
  eod HUBC             DPS daily for one symbol
  ticks                DPS today's tick trades (all symbols)
  ticks HUBC           DPS ticks for one symbol
  klines 1h            PSX Terminal 1h klines (latest 100 bars)
  klines 5m --deep     PSX Terminal 5m, paginated for max bars
  klines 1m            PSX Terminal 1m (today only, auto-paginates)
  all                  Full backfill — everything at once
  status               Show data coverage report
        """,
    )
    parser.add_argument("command", choices=["eod", "ticks", "klines", "all", "status"])
    parser.add_argument("arg", nargs="?", help="Symbol (for eod/ticks) or timeframe (for klines)")
    parser.add_argument("--deep", action="store_true", 
                        help="Paginate backwards for maximum bars (klines only)")
    parser.add_argument("--symbol", help="Filter to one symbol (for klines)")
    
    args = parser.parse_args()
    
    if args.command == "status":
        show_status()
        exit()
    
    client = HTTPClient()
    
    # Test API availability
    test = client.get_psxt("symbols")
    if not test:
        print("⚠️  PSX Terminal API is down. DPS commands still work.")
    
    symbols = get_all_symbols(client) if test else []
    if not symbols:
        # Fallback: hardcoded major symbols
        print("⚠️  Using DPS market-watch for symbol list...")
        dps_data = client.get_dps("market-watch")
        # If that fails too, use a small default list
        if not dps_data:
            symbols = ["HUBC", "OGDC", "PPL", "MCB", "UBL", "HBL", "LUCK", 
                       "ENGRO", "FFC", "EFERT", "SYS", "TRG", "MARI"]
            print(f"⚠️  Using {len(symbols)} default symbols")
    
    print(f"📊 {len(symbols)} symbols loaded")
    
    if args.command == "eod":
        sym = args.arg if args.arg and args.arg.upper() in [s.upper() for s in symbols] else None
        cmd_eod(client, symbols, symbol=args.arg.upper() if args.arg else None)
    
    elif args.command == "ticks":
        cmd_ticks(client, symbols, symbol=args.arg.upper() if args.arg else None)
    
    elif args.command == "klines":
        tf = args.arg or "1h"
        if tf not in ("1m", "5m", "15m", "1h", "1d", "1w"):
            print(f"❌ Invalid timeframe: {tf}. Use: 1m, 5m, 15m, 1h, 1d, 1w")
            exit(1)
        # 1m always paginates (today only, ~375 bars/symbol)
        deep = args.deep or (tf == "1m")
        cmd_klines(client, symbols, tf, deep=deep, symbol=args.symbol)
    
    elif args.command == "all":
        cmd_all(client, symbols)
```

### Step 3: Integrate with Streamlit UI

Add a "Market Data Sync" section to the Admin/Sync page:

```
PSX Market Data
───────────────
Source: DPS (Official)
  [📊 Sync 5-Year Daily]    ← eod (all symbols, ~10 min)
  [📈 Fetch Today's Ticks]  ← ticks (all symbols, ~3 min)

Source: PSX Terminal (Supplementary)
  [1h Deep Backfill]  [5m Deep]  [15m Deep]  [1m Today]

[🚀 Full Backfill — Everything]   ← all (~30 min)

Coverage:
  DPS Daily:    487 symbols | 602,000 bars | 2021-03-17 → today
  DPS Ticks:    487 symbols | 1,200,000 trades | today
  PSXT 1h:     487 symbols | 48,700 bars | ~16 days
  PSXT 5m:     487 symbols | 48,700 bars | ~2 days
  PSXT 1m:     487 symbols | 180,000 bars | today
```

### Step 4: Add daily auto-sync

In tick_service.py after EOD flush, or as cron:

```python
# After market close (15:35 PKT):
from pakfindata.sources.psx_market_data import HTTPClient, cmd_ticks, cmd_klines, get_all_symbols

client = HTTPClient()
symbols = get_all_symbols(client)

# Save today's tick trades from DPS (official source)
cmd_ticks(client, symbols)

# Save today's 1m klines from PSX Terminal (supplementary)
cmd_klines(client, symbols, "1m", deep=True)

print("📊 Daily intraday data synced")
```

## File naming convention

```
~/psxdata/intraday/
│
├── DPS Source (Official):
│   ├── dps_eod_daily.csv              # 5-year daily OHLCV, all symbols
│   ├── dps_ticks_2026-03-17.csv       # Today's tick trades, all symbols
│   ├── dps_ticks_2026-03-18.csv       # Tomorrow's ticks (accumulates daily)
│   └── ...
│
├── PSX Terminal Source (Supplementary):
│   ├── psxt_backfill_1w.csv           # Deep backfill weekly (~2 years)
│   ├── psxt_backfill_1h.csv           # Deep backfill hourly (~16 days)  
│   ├── psxt_backfill_15m.csv          # Deep backfill 15min (~4 days)
│   ├── psxt_backfill_5m.csv           # Deep backfill 5min (~2 days)
│   ├── psxt_2026-03-17_1m.csv         # Today's 1-minute bars
│   ├── psxt_latest_1h.csv             # Latest 100 bars (quick refresh)
│   └── ...
│
└── DB Tables (in /mnt/e/psxdata/psx.sqlite):
    ├── psx_eod                         # DPS daily OHLCV
    ├── psx_ticks                       # DPS tick trades
    ├── psxt_klines_1w                  # PSX Terminal weekly
    ├── psxt_klines_1h                  # PSX Terminal hourly
    ├── psxt_klines_15m                 # PSX Terminal 15-minute
    ├── psxt_klines_5m                  # PSX Terminal 5-minute
    └── psxt_klines_1m                  # PSX Terminal 1-minute
```

## VERIFY

```bash
# Test single symbol
python -m pakfindata.sources.psx_market_data eod HUBC
python -m pakfindata.sources.psx_market_data ticks HUBC
python -m pakfindata.sources.psx_market_data klines 1h --symbol HUBC --deep

# Check files
ls -la ~/psxdata/intraday/

# Check DB
python -m pakfindata.sources.psx_market_data status

# Full backfill (do this once, ~30 min)
python -m pakfindata.sources.psx_market_data all

# Daily routine (after market close)
python -m pakfindata.sources.psx_market_data ticks
python -m pakfindata.sources.psx_market_data klines 1m
```

## IMPORTANT NOTES

1. **DPS EOD does NOT provide high/low** — only open, close, volume. 
   PSX Terminal klines DO have OHLCV. For full OHLCV daily, cross-reference both.

2. **PSX Terminal 1m data is today only** — past dates silently redirect to today.
   Must fetch daily before midnight.

3. **PSX Terminal max 100 bars/request** — deep mode paginates backwards 
   using endTimestamp. For 1m (~375 bars/day), needs 4 requests per symbol.

4. **Rate limit at 0.3s** — respectful to both servers. Full backfill 
   takes ~30 min but won't get you blocked.

5. **503 HTML appended** — PSX Terminal sometimes appends nginx 503 HTML 
   after valid JSON. Always strip with `.split("<")[0]`.

6. **DPS ticks accumulate daily** — each day gets its own file 
   (dps_ticks_2026-03-17.csv). Don't overwrite.

7. **Store in BOTH CSV and DB** — CSV for portability/backup, DB for queries.
