# Claude Code Prompt: PSX Terminal Klines Backfill & Sync

## Context

psxterminal.com provides historical OHLCV klines via REST API:
```
GET https://psxterminal.com/api/klines/{symbol}/{interval}?limit=100
```

**Available intervals:** 1m, 5m, 15m, 1h, 1d, 1w
**Max limit per request:** 100
**Supports pagination:** `startTimestamp` and `endTimestamp` (milliseconds)

**Response format:**
```json
{
  "success": true,
  "data": [
    {
      "symbol": "HUBC",
      "timeframe": "1d",
      "timestamp": 1773100800000,
      "open": 204,
      "high": 208.04,
      "low": 198,
      "close": 207.13,
      "volume": 4762988
    }
  ],
  "count": 5,
  "startTimestamp": null,
  "endTimestamp": null,
  "requestedLimit": 5,
  "appliedLimit": 5
}
```

**Also available REST endpoints to scrape:**
```
GET /api/ticks/REG/{symbol}     → live tick (price, bid, ask, volume, trades)
GET /api/fundamentals/{symbol}  → P/E, dividend yield, market cap, free float
GET /api/companies/{symbol}     → business description, key people, shares
GET /api/stats/{market}         → market breadth, top gainers/losers
GET /api/symbols                → all ~487 symbols
GET /api/dividends/{symbol}     → dividend history
```

Project: pakfindata at `~/pakfindata/`
DB: `/mnt/e/psxdata/psx.sqlite`

## Task: Build klines backfill scraper + sync integration

### Step 1: Check existing DB schema

```bash
# What OHLCV tables already exist?
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
tables = [t[0] for t in con.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%ohlcv%' OR name LIKE '%kline%' OR name LIKE '%candle%' OR name LIKE '%intraday%')\").fetchall()]
for t in tables:
    cols = [c[1] for c in con.execute(f'PRAGMA table_info([{t}])').fetchall()]
    count = con.execute(f'SELECT COUNT(*) FROM [{t}]').fetchone()[0]
    print(f'{t}: {count:,} rows | {cols}')
con.close()
"

# Also check what psxterminal-related code exists
grep -rn "psxterminal\|klines\|kline" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20
```

Read the output and adapt the schema below accordingly. If an OHLCV table 
already exists with compatible columns, USE IT. Don't create duplicates.

### Step 2: Create/extend the klines table

If no suitable table exists, create:

```sql
CREATE TABLE IF NOT EXISTS psxt_klines (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,       -- '1m','5m','15m','1h','1d','1w'
    timestamp INTEGER NOT NULL,    -- Unix ms
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_psxt_klines_sym_tf 
ON psxt_klines(symbol, timeframe, timestamp);
```

If a compatible table already exists (e.g. `ohlcv` or `intraday_ohlcv`), 
add any missing columns and use it instead.

### Step 3: Build the scraper

Create `src/pakfindata/sources/psxterminal.py`:

```python
"""
PSX Terminal API — Klines backfill and live data sync.

Endpoints:
  /api/klines/{symbol}/{interval}?limit=100&startTimestamp=&endTimestamp=
  /api/ticks/{market}/{symbol}
  /api/fundamentals/{symbol}
  /api/companies/{symbol}
  /api/stats/{market}
  /api/symbols
  /api/dividends/{symbol}

Usage:
  python -m pakfindata.sources.psxterminal klines HUBC 1d          # latest 100 daily bars
  python -m pakfindata.sources.psxterminal klines HUBC 1h --all    # full hourly history
  python -m pakfindata.sources.psxterminal klines-all 1d           # all 487 symbols daily
  python -m pakfindata.sources.psxterminal klines-all 1h           # all symbols hourly
  python -m pakfindata.sources.psxterminal fundamentals            # all symbols fundamentals
  python -m pakfindata.sources.psxterminal symbols                 # list all symbols
  python -m pakfindata.sources.psxterminal status                  # show DB coverage
"""

import requests
import sqlite3
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

BASE_URL = "https://psxterminal.com/api"
DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")
INTERVALS = ["1m", "5m", "15m", "1h", "1d", "1w"]
MAX_LIMIT = 100
RATE_LIMIT_DELAY = 0.5  # seconds between requests — be nice to their server


class PSXTerminalClient:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "pakfindata/1.0",
            "Accept": "application/json",
        })
    
    def _get(self, endpoint: str, params: dict = None) -> dict | None:
        """Make GET request with rate limiting."""
        url = f"{BASE_URL}/{endpoint}"
        try:
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    return data
            return None
        except Exception as e:
            print(f"  ❌ {endpoint}: {e}")
            return None
    
    # ─── SYMBOLS ──────────────────────────────────────────────
    
    def get_symbols(self) -> list[str]:
        """Get all available symbols."""
        data = self._get("symbols")
        return data["data"] if data else []
    
    # ─── KLINES ───────────────────────────────────────────────
    
    def fetch_klines(self, symbol: str, timeframe: str, 
                     limit: int = 100,
                     start_ts: int = None, 
                     end_ts: int = None) -> list[dict]:
        """Fetch klines for one symbol/timeframe."""
        params = {"limit": min(limit, MAX_LIMIT)}
        if start_ts:
            params["startTimestamp"] = start_ts
        if end_ts:
            params["endTimestamp"] = end_ts
        
        data = self._get(f"klines/{symbol}/{timeframe}", params)
        return data["data"] if data else []
    
    def fetch_all_klines(self, symbol: str, timeframe: str) -> list[dict]:
        """Paginate backwards to get ALL available klines for a symbol."""
        all_bars = []
        end_ts = None  # start from latest
        
        while True:
            params = {"limit": MAX_LIMIT}
            if end_ts:
                params["endTimestamp"] = end_ts
            
            batch = self.fetch_klines(symbol, timeframe, **params)
            if not batch:
                break
            
            all_bars.extend(batch)
            
            # Get earliest timestamp for next pagination
            earliest = min(b["timestamp"] for b in batch)
            
            # If we got less than limit, we've hit the beginning
            if len(batch) < MAX_LIMIT:
                break
            
            # Move end_ts to just before the earliest bar
            end_ts = earliest - 1
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Sort chronologically and deduplicate
        seen = set()
        unique = []
        for bar in sorted(all_bars, key=lambda x: x["timestamp"]):
            key = (bar["symbol"], bar["timeframe"], bar["timestamp"])
            if key not in seen:
                seen.add(key)
                unique.append(bar)
        
        return unique
    
    # ─── FUNDAMENTALS ─────────────────────────────────────────
    
    def fetch_fundamentals(self, symbol: str) -> dict | None:
        """Fetch fundamentals for one symbol."""
        data = self._get(f"fundamentals/{symbol}")
        return data["data"] if data else None
    
    # ─── COMPANY ──────────────────────────────────────────────
    
    def fetch_company(self, symbol: str) -> dict | None:
        """Fetch company info for one symbol."""
        data = self._get(f"companies/{symbol}")
        return data["data"] if data else None
    
    # ─── LIVE TICK ────────────────────────────────────────────
    
    def fetch_tick(self, symbol: str, market: str = "REG") -> dict | None:
        """Fetch current tick for one symbol."""
        data = self._get(f"ticks/{market}/{symbol}")
        return data["data"] if data else None
    
    # ─── DIVIDENDS ────────────────────────────────────────────
    
    def fetch_dividends(self, symbol: str) -> list | None:
        """Fetch dividend history for one symbol."""
        data = self._get(f"dividends/{symbol}")
        return data["data"] if data else None
    
    # ─── MARKET STATS ─────────────────────────────────────────
    
    def fetch_stats(self, market: str = "REG") -> dict | None:
        """Fetch market statistics."""
        data = self._get(f"stats/{market}")
        return data["data"] if data else None
    
    # ─── DB STORAGE ───────────────────────────────────────────
    
    def _get_con(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        return con
    
    def _ensure_tables(self, con: sqlite3.Connection):
        con.execute("""
            CREATE TABLE IF NOT EXISTS psxt_klines (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                fetched_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (symbol, timeframe, timestamp)
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_psxt_klines_sym_tf 
            ON psxt_klines(symbol, timeframe, timestamp)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS psxt_fundamentals (
                symbol TEXT PRIMARY KEY,
                market_cap TEXT,
                pe_ratio REAL,
                dividend_yield REAL,
                free_float TEXT,
                volume_30_avg REAL,
                year_change REAL,
                listed_in TEXT,
                is_non_compliant INTEGER,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS psxt_companies (
                symbol TEXT PRIMARY KEY,
                business_description TEXT,
                key_people TEXT,
                shares INTEGER,
                free_float INTEGER,
                market_cap REAL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.commit()
    
    def store_klines(self, bars: list[dict]):
        """Store klines to DB with INSERT OR IGNORE."""
        if not bars:
            return 0
        con = self._get_con()
        self._ensure_tables(con)
        stored = 0
        for b in bars:
            try:
                con.execute(
                    "INSERT OR IGNORE INTO psxt_klines (symbol, timeframe, timestamp, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?,?)",
                    (b["symbol"], b["timeframe"], b["timestamp"],
                     b["open"], b["high"], b["low"], b["close"], b["volume"])
                )
                stored += 1
            except Exception:
                pass
        con.commit()
        con.close()
        return stored
    
    def store_fundamentals(self, symbol: str, data: dict):
        """Store fundamentals to DB."""
        con = self._get_con()
        self._ensure_tables(con)
        con.execute("""
            INSERT OR REPLACE INTO psxt_fundamentals 
            (symbol, market_cap, pe_ratio, dividend_yield, free_float, 
             volume_30_avg, year_change, listed_in, is_non_compliant)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            symbol,
            data.get("marketCap"),
            data.get("peRatio"),
            data.get("dividendYield"),
            data.get("freeFloat"),
            data.get("volume30Avg"),
            data.get("yearChange"),
            data.get("listedIn"),
            1 if data.get("isNonCompliant") else 0,
        ))
        con.commit()
        con.close()
    
    def store_company(self, symbol: str, data: dict):
        """Store company info to DB."""
        import json
        con = self._get_con()
        self._ensure_tables(con)
        stats = data.get("financialStats", {})
        con.execute("""
            INSERT OR REPLACE INTO psxt_companies 
            (symbol, business_description, key_people, shares, free_float, market_cap)
            VALUES (?,?,?,?,?,?)
        """, (
            symbol,
            data.get("businessDescription"),
            json.dumps(data.get("keyPeople", [])),
            stats.get("shares", {}).get("numeric"),
            stats.get("freeFloat", {}).get("numeric"),
            stats.get("marketCap", {}).get("numeric"),
        ))
        con.commit()
        con.close()
    
    # ─── HIGH-LEVEL SYNC COMMANDS ─────────────────────────────
    
    def sync_klines_symbol(self, symbol: str, timeframe: str, full: bool = False):
        """Sync klines for one symbol."""
        if full:
            print(f"  📊 {symbol}/{timeframe} — full backfill...")
            bars = self.fetch_all_klines(symbol, timeframe)
        else:
            bars = self.fetch_klines(symbol, timeframe, limit=100)
        
        if bars:
            stored = self.store_klines(bars)
            ts_min = datetime.fromtimestamp(min(b["timestamp"] for b in bars) / 1000)
            ts_max = datetime.fromtimestamp(max(b["timestamp"] for b in bars) / 1000)
            print(f"  ✅ {symbol}/{timeframe}: {len(bars)} bars ({ts_min.date()} → {ts_max.date()}), {stored} new")
        else:
            print(f"  ⚠️  {symbol}/{timeframe}: no data")
    
    def sync_klines_all(self, timeframe: str, full: bool = False):
        """Sync klines for ALL symbols."""
        symbols = self.get_symbols()
        # Filter to only REG market symbols (skip index symbols like ALLSHR, KSE100)
        # Index symbols don't have klines
        index_symbols = {"ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", 
                         "KMIALLSHR", "BKTI", "OGTI", "PSXDIV20", "UPP9",
                         "NITPGI", "NBPPGI", "MZNPI", "JSMFI", "ACI", 
                         "JSGBKTI", "HBLTTI", "MII30"}
        symbols = [s for s in symbols if s not in index_symbols]
        
        total = len(symbols)
        print(f"📊 Syncing {timeframe} klines for {total} symbols...")
        
        success = 0
        failed = 0
        for i, sym in enumerate(symbols, 1):
            try:
                if full:
                    bars = self.fetch_all_klines(sym, timeframe)
                else:
                    bars = self.fetch_klines(sym, timeframe, limit=100)
                
                if bars:
                    self.store_klines(bars)
                    success += 1
                    if i % 50 == 0 or i == total:
                        print(f"  [{i}/{total}] {sym}: {len(bars)} bars ✅")
                else:
                    failed += 1
                
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                print(f"  [{i}/{total}] {sym}: ❌ {e}")
                failed += 1
        
        print(f"\n✅ Done: {success} symbols synced, {failed} failed")
    
    def sync_fundamentals_all(self):
        """Fetch fundamentals for all symbols."""
        symbols = self.get_symbols()
        print(f"📊 Fetching fundamentals for {len(symbols)} symbols...")
        
        success = 0
        for i, sym in enumerate(symbols, 1):
            data = self.fetch_fundamentals(sym)
            if data:
                self.store_fundamentals(sym, data)
                success += 1
            
            if i % 50 == 0:
                print(f"  [{i}/{len(symbols)}] {success} done")
            
            time.sleep(RATE_LIMIT_DELAY)
        
        print(f"✅ Fundamentals: {success}/{len(symbols)} symbols")
    
    def sync_companies_all(self):
        """Fetch company info for all symbols."""
        symbols = self.get_symbols()
        print(f"📊 Fetching company info for {len(symbols)} symbols...")
        
        success = 0
        for i, sym in enumerate(symbols, 1):
            data = self.fetch_company(sym)
            if data:
                self.store_company(sym, data)
                success += 1
            
            if i % 50 == 0:
                print(f"  [{i}/{len(symbols)}] {success} done")
            
            time.sleep(RATE_LIMIT_DELAY)
        
        print(f"✅ Companies: {success}/{len(symbols)} symbols")
    
    def status(self):
        """Show DB coverage."""
        con = self._get_con()
        self._ensure_tables(con)
        
        print("═══════════════════════════════════════")
        print("  PSX TERMINAL DATA COVERAGE")
        print("═══════════════════════════════════════")
        
        # Klines
        print("\n📊 KLINES:")
        for tf in INTERVALS:
            row = con.execute("""
                SELECT COUNT(DISTINCT symbol), COUNT(*),
                       MIN(timestamp), MAX(timestamp)
                FROM psxt_klines WHERE timeframe = ?
            """, (tf,)).fetchone()
            if row[0] > 0:
                t1 = datetime.fromtimestamp(row[2] / 1000).strftime("%Y-%m-%d")
                t2 = datetime.fromtimestamp(row[3] / 1000).strftime("%Y-%m-%d")
                print(f"  {tf:4s} | {row[0]:4d} symbols | {row[1]:>10,} bars | {t1} → {t2}")
            else:
                print(f"  {tf:4s} | empty")
        
        # Fundamentals
        n = con.execute("SELECT COUNT(*) FROM psxt_fundamentals").fetchone()[0]
        print(f"\n📈 FUNDAMENTALS: {n} symbols")
        
        # Companies
        n = con.execute("SELECT COUNT(*) FROM psxt_companies").fetchone()[0]
        print(f"🏢 COMPANIES: {n} symbols")
        
        # Total DB size
        print(f"\n💾 DB: {self.db_path} ({self.db_path.stat().st_size / 1e6:.1f} MB)")
        
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PSX Terminal API Sync")
    sub = parser.add_subparsers(dest="command")
    
    # klines <symbol> <timeframe>
    kp = sub.add_parser("klines", help="Fetch klines for one symbol")
    kp.add_argument("symbol", help="Symbol (e.g. HUBC)")
    kp.add_argument("timeframe", help="Interval: 1m, 5m, 15m, 1h, 1d, 1w")
    kp.add_argument("--all", action="store_true", help="Full backfill (paginate all history)")
    
    # klines-all <timeframe>
    ka = sub.add_parser("klines-all", help="Fetch klines for ALL symbols")
    ka.add_argument("timeframe", help="Interval: 1m, 5m, 15m, 1h, 1d, 1w")
    ka.add_argument("--all", action="store_true", help="Full backfill per symbol")
    
    # fundamentals
    sub.add_parser("fundamentals", help="Fetch fundamentals for all symbols")
    
    # companies
    sub.add_parser("companies", help="Fetch company info for all symbols")
    
    # symbols
    sub.add_parser("symbols", help="List all symbols")
    
    # status
    sub.add_parser("status", help="Show DB coverage")
    
    args = parser.parse_args()
    client = PSXTerminalClient()
    
    if args.command == "klines":
        client.sync_klines_symbol(args.symbol, args.timeframe, full=args.all)
    elif args.command == "klines-all":
        client.sync_klines_all(args.timeframe, full=args.all)
    elif args.command == "fundamentals":
        client.sync_fundamentals_all()
    elif args.command == "companies":
        client.sync_companies_all()
    elif args.command == "symbols":
        syms = client.get_symbols()
        print(f"{len(syms)} symbols: {', '.join(syms[:20])}...")
    elif args.command == "status":
        client.status()
    else:
        parser.print_help()
```

### Step 4: Add to Streamlit Sync UI

In the pakfindata Streamlit app, find the Sync/Admin page and add a section:

```
PSX Terminal Sync
─────────────────
[Sync Daily Klines (all symbols)]    ← calls sync_klines_all("1d")
[Sync Hourly Klines (all symbols)]   ← calls sync_klines_all("1h")
[Sync Fundamentals]                   ← calls sync_fundamentals_all()
[Sync Companies]                      ← calls sync_companies_all()
[Full Backfill Daily]                 ← calls sync_klines_all("1d", full=True)

Status:
  1d | 487 symbols | 48,700 bars | 2024-01-01 → 2026-03-16
  1h | 487 symbols | 292,200 bars | 2025-06-01 → 2026-03-16
  Fundamentals | 487 symbols
  Companies | 487 symbols
```

### Step 5: Add daily auto-sync

After market close (in tick_service.py EOD section or as separate cron),
auto-sync the latest klines:

```python
# After EOD flush in tick_service.py:
from pakfindata.sources.psxterminal import PSXTerminalClient
client = PSXTerminalClient()
client.sync_klines_all("1d")    # ~4 min for 487 symbols
client.sync_klines_all("1h")    # ~4 min
print("📊 PSX Terminal daily klines synced")
```

## VERIFY

```bash
# Test single symbol
python -m pakfindata.sources.psxterminal klines HUBC 1d
python -m pakfindata.sources.psxterminal klines HUBC 1h

# Full backfill for one symbol
python -m pakfindata.sources.psxterminal klines HUBC 1d --all

# Check DB
python -m pakfindata.sources.psxterminal status

# Check data
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for tf in ['1d','1h','5m']:
    r = con.execute('SELECT COUNT(DISTINCT symbol), COUNT(*) FROM psxt_klines WHERE timeframe=?',(tf,)).fetchone()
    print(f'{tf}: {r[0]} symbols, {r[1]:,} bars')
con.close()
"

# Sync all symbols daily (test with 5 first)
python3 -c "
from pakfindata.sources.psxterminal import PSXTerminalClient
c = PSXTerminalClient()
for sym in ['HUBC','OGDC','PPL','MCB','UBL']:
    c.sync_klines_symbol(sym, '1d', full=True)
c.status()
"
```
