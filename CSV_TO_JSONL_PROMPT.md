# Claude Code Prompt: Convert 1m Klines CSV → JSONL Tick Format

## Context

We have:
1. `~/psxdata/intraday/redo_psxt_2026-03-16_1m.csv` — 1-minute OHLCV bars (4,289 rows, 222 symbols)
2. `/mnt/e/tick_logs/` — JSONL tick files from previous days (March 13, 2026)
3. `/mnt/e/psxdata/psx.sqlite` — DPS EOD data with previousClose values

The CSV has 1-minute aggregated bars. We need to convert each bar into a JSONL record 
matching the raw tick format as closely as possible.

## Step 1: Understand the target JSONL format

Each line in the JSONL is one JSON object:
```json
{
  "symbol": "786",
  "market": "REG",
  "price": 38.66,
  "open": 38.66,
  "change": -4.29,
  "changePercent": -0.09988,
  "volume": 1137,
  "value": 43956.42,
  "trades": 5,
  "high": 38.66,
  "low": 38.66,
  "bid": 0.0,
  "ask": 38.66,
  "bidVol": 0,
  "askVol": 88679,
  "previousClose": 42.95,
  "timestamp": 1773721020.364,
  "_ts": "2026-03-17T09:17:00.679+05:00"
}
```

## Step 2: Read the March 13 JSONL to extract previousClose values

```bash
# Find the JSONL files
ls -la /mnt/e/tick_logs/ | head -20

# Look for March 13 or closest date
ls /mnt/e/tick_logs/*2026-03-13* 2>/dev/null || ls /mnt/e/tick_logs/*2026-03* 2>/dev/null
```

Read the JSONL files and for each symbol, extract the LAST `price` value — 
that becomes the `previousClose` for March 16 (March 14-15 were weekend).

Also check DPS EOD as backup source:
```python
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
# DPS EOD format: [timestamp, close, volume, open]
# Get March 13 closing prices
rows = con.execute("""
    SELECT symbol, close FROM daily_ohlcv 
    WHERE date = '2026-03-13'
""").fetchall()
```

Adapt the table/column names based on what actually exists.

## Step 3: Determine market segment per symbol

```python
# Known FUT symbols end with specific suffixes or have known patterns
# Known ETF symbols
ETF_SYMBOLS = {"ACIETF", "MIIETF", "MZNPETF", "NBPGETF", "NITGETF", 
               "UBLPETF", "HBLETF", "JSMFETF"}

# ODL symbols (odd lot)
# BNB symbols (bargain/negotiated)
# Default everything else to REG

def get_market(symbol: str) -> str:
    if symbol in ETF_SYMBOLS:
        return "REG"  # ETFs trade on REG
    # Check PSX Terminal API for actual mapping if needed
    return "REG"
```

## Step 4: Build the converter

Create `~/pakfindata/scripts/csv_to_jsonl.py`:

```python
"""
Convert 1-minute klines CSV to JSONL tick-like format.

Fields we CAN populate:
  ✅ symbol          — direct from CSV
  ✅ price           — use bar's close price
  ✅ open            — direct from CSV (day open = first bar's open)
  ✅ high            — running max across all bars for that symbol
  ✅ low             — running min across all bars for that symbol
  ✅ timestamp       — bar close time (bar_start + 59 seconds), in seconds
  ✅ _ts             — ISO 8601 PKT format
  
Fields we CAN COMPUTE:
  ⚠️ market          — default "REG", map known FUT/ODL symbols
  ⚠️ change          — close - previousClose (need prev day data)
  ⚠️ changePercent   — change / previousClose (decimal, not x100)
  ⚠️ volume          — CUMULATIVE running sum of bar volumes per symbol
  ⚠️ previousClose   — from March 13 JSONL or DPS EOD data
  ⚠️ value           — ESTIMATED: cumulative volume × average bar price

Fields we CANNOT recover (set to 0/null):
  ❌ trades          — not in klines API (set to 0)
  ❌ bid             — order book data (set to 0.0)
  ❌ ask             — order book data (set to 0.0)  
  ❌ bidVol          — order book data (set to 0)
  ❌ askVol          — order book data (set to 0)

Output: ~/psxdata/intraday/redo_psxt_2026-03-16_ticks.jsonl
"""

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

PKT = timezone(timedelta(hours=5))

# ─── CONFIG ───────────────────────────────────────────
CSV_INPUT = Path.home() / "psxdata" / "intraday" / "redo_psxt_2026-03-16_1m.csv"
JSONL_OUTPUT = Path.home() / "psxdata" / "intraday" / "redo_psxt_2026-03-16_ticks.jsonl"
TICK_LOGS_DIR = Path("/mnt/e/tick_logs")
DB_PATH = Path("/mnt/e/psxdata/psx.sqlite")


# ─── STEP 1: Get previousClose from March 13 ─────────
def load_previous_close() -> dict:
    """Load previous closing prices. Try JSONL tick_logs first, then DPS EOD."""
    prev_close = {}
    
    # Method 1: Read March 13 JSONL from tick_logs
    jsonl_files = sorted(TICK_LOGS_DIR.glob("*2026-03-13*")) if TICK_LOGS_DIR.exists() else []
    # Also try: files might be named differently
    if not jsonl_files and TICK_LOGS_DIR.exists():
        jsonl_files = sorted(TICK_LOGS_DIR.glob("*.jsonl"))
        # Filter to March 13 by checking content
    
    for jf in jsonl_files:
        print(f"Reading JSONL: {jf.name}")
        with open(jf, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    tick = json.loads(line)
                    sym = tick.get("symbol", "")
                    price = tick.get("price", 0)
                    if sym and price:
                        prev_close[sym] = price  # Last occurrence wins
                except:
                    continue
    
    if prev_close:
        print(f"  Got previousClose for {len(prev_close)} symbols from JSONL")
        return prev_close
    
    # Method 2: DPS EOD from SQLite
    if DB_PATH.exists():
        import sqlite3
        con = sqlite3.connect(str(DB_PATH))
        
        # Try common table names
        for table in ["daily_ohlcv", "ohlcv_daily", "stock_data", "eod"]:
            try:
                # DPS EOD: close is the closing price
                # Find March 13 data (or closest trading day before March 16)
                rows = con.execute(f"""
                    SELECT symbol, close FROM {table}
                    WHERE date IN ('2026-03-13', '2026-03-12', '2026-03-11')
                    ORDER BY date DESC
                """).fetchall()
                for sym, close in rows:
                    if sym not in prev_close:
                        prev_close[sym] = close
                if prev_close:
                    print(f"  Got previousClose for {len(prev_close)} symbols from DB table '{table}'")
                    break
            except:
                continue
        
        # Also try: the DPS timeseries format
        if not prev_close:
            try:
                # Check what tables exist
                tables = [r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()]
                print(f"  Available tables: {tables[:10]}")
            except:
                pass
        
        con.close()
    
    # Method 3: Compute from first bar's open (rough approximation)
    # This is the fallback — first bar's open IS roughly yesterday's close
    # (in continuous markets, today's open ≈ yesterday's close)
    
    if not prev_close:
        print("  ⚠️ No previous close data found. Will use first bar's open as approximation.")
    
    return prev_close


# ─── STEP 2: Read CSV ────────────────────────────────
def read_csv(filepath: Path) -> list[dict]:
    """Read 1m klines CSV."""
    rows = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "symbol": r["symbol"].strip(),
                "timestamp_ms": int(r["timestamp"].strip()),
                "datetime": r["datetime"].strip(),
                "open": float(r["open"].strip()),
                "high": float(r["high"].strip()),
                "low": float(r["low"].strip()),
                "close": float(r["close"].strip()),
                "volume": int(r["volume"].strip()),
            })
    return rows


# ─── STEP 3: Convert ─────────────────────────────────
def convert(rows: list[dict], prev_close: dict) -> list[dict]:
    """Convert 1m bars to tick-like JSONL records."""
    
    # Group by symbol, sorted by time
    by_symbol = defaultdict(list)
    for r in rows:
        by_symbol[r["symbol"]].append(r)
    
    for sym in by_symbol:
        by_symbol[sym].sort(key=lambda x: x["timestamp_ms"])
    
    # Known market mappings
    ETF_SYMS = {"ACIETF", "MIIETF", "MZNPETF", "NBPGETF", "NITGETF", 
                "UBLPETF", "HBLETF", "JSMFETF"}
    
    output = []
    
    for sym, bars in sorted(by_symbol.items()):
        # Get previousClose
        pc = prev_close.get(sym, bars[0]["open"])  # fallback: first bar's open
        
        # Running state
        day_open = bars[0]["open"]
        day_high = bars[0]["high"]
        day_low = bars[0]["low"]
        cum_volume = 0
        cum_value = 0.0
        
        # Determine market
        market = "REG"
        if sym in ETF_SYMS:
            market = "REG"
        # FUT symbols typically end with a month code like HUBCF, OGDCF etc.
        # ODL symbols are on the odd-lot market
        # For now default all to REG — refine later with actual PSX Terminal data
        
        for bar in bars:
            # Update running state
            day_high = max(day_high, bar["high"])
            day_low = min(day_low, bar["low"])
            cum_volume += bar["volume"]
            
            # Estimate value (turnover) — volume × VWAP approximation
            bar_vwap = (bar["open"] + bar["high"] + bar["low"] + bar["close"]) / 4
            cum_value += bar["volume"] * bar_vwap
            
            # Price = bar close (last traded price of that minute)
            price = bar["close"]
            
            # Change from previous close
            change = round(price - pc, 4)
            change_pct = round(change / pc, 6) if pc else 0
            
            # Timestamp: bar END time (start + 59s), in seconds with fractional
            ts_seconds = bar["timestamp_ms"] / 1000 + 59  # end of minute
            
            # ISO timestamp
            dt = datetime.fromtimestamp(ts_seconds, PKT)
            iso_ts = dt.strftime("%Y-%m-%dT%H:%M:%S.000+05:00")
            
            record = {
                "symbol": sym,
                "market": market,
                "price": price,
                "open": day_open,
                "change": round(change, 2),
                "changePercent": round(change_pct, 5),
                "volume": cum_volume,
                "value": round(cum_value, 2),
                "trades": 0,          # ❌ NOT AVAILABLE from klines
                "high": day_high,
                "low": day_low,
                "bid": 0.0,           # ❌ NOT AVAILABLE from klines
                "ask": 0.0,           # ❌ NOT AVAILABLE from klines
                "bidVol": 0,          # ❌ NOT AVAILABLE from klines
                "askVol": 0,          # ❌ NOT AVAILABLE from klines
                "previousClose": pc,
                "timestamp": round(ts_seconds, 3),
                "_ts": iso_ts,
                "_source": "klines_1m",  # flag this as synthetic
                "_bar_open": bar["open"],   # preserve original bar OHLC
                "_bar_high": bar["high"],
                "_bar_low": bar["low"],
                "_bar_close": bar["close"],
                "_bar_volume": bar["volume"],
            }
            
            output.append(record)
    
    # Sort by timestamp (chronological across all symbols)
    output.sort(key=lambda x: x["timestamp"])
    
    return output


# ─── MAIN ─────────────────────────────────────────────
if __name__ == "__main__":
    print("═══════════════════════════════════════════════")
    print("  CSV → JSONL Converter (1m klines → tick format)")
    print("═══════════════════════════════════════════════")
    
    # Load previous close
    print("\n1. Loading previousClose data...")
    prev_close = load_previous_close()
    
    # Read CSV
    print(f"\n2. Reading CSV: {CSV_INPUT}")
    rows = read_csv(CSV_INPUT)
    print(f"   {len(rows):,} bars, {len(set(r['symbol'] for r in rows))} symbols")
    
    # Convert
    print("\n3. Converting to JSONL...")
    output = convert(rows, prev_close)
    
    # Write JSONL
    print(f"\n4. Writing: {JSONL_OUTPUT}")
    with open(JSONL_OUTPUT, 'w') as f:
        for record in output:
            f.write(json.dumps(record) + "\n")
    
    print(f"   ✅ {len(output):,} records written")
    
    # Summary
    print("\n═══ SUMMARY ═══")
    symbols_with_pc = sum(1 for r in set(rec["symbol"] for rec in output) 
                         if r in prev_close)
    total_syms = len(set(rec["symbol"] for rec in output))
    print(f"   Symbols:         {total_syms}")
    print(f"   Records:         {len(output):,}")
    print(f"   With prevClose:  {symbols_with_pc} / {total_syms}")
    print(f"   File size:       {JSONL_OUTPUT.stat().st_size / 1024:.0f} KB")
    
    # Field quality report
    print("\n═══ FIELD QUALITY ═══")
    print("   ✅ EXACT:      symbol, open, high, low, price (=close)")
    print("   ✅ COMPUTED:   change, changePercent, volume (cumulative)")
    print("   ⚠️ ESTIMATED:  value (vol × approx VWAP)")
    print("   ⚠️ APPROX:    previousClose (from prev day if available)")
    print("   ❌ ZERO:       bid, ask, bidVol, askVol, trades")
    print("   📌 EXTRA:      _source, _bar_open/high/low/close/volume (original bar data)")
    
    # Sample output
    print("\n═══ SAMPLE RECORD ═══")
    if output:
        print(json.dumps(output[0], indent=2))
```

## Step 5: Run it

```bash
cd ~/pakfindata
python scripts/csv_to_jsonl.py
```

## Step 6: Verify

```bash
# Check output
wc -l ~/psxdata/intraday/redo_psxt_2026-03-16_ticks.jsonl

# Pretty print first 3 records
head -3 ~/psxdata/intraday/redo_psxt_2026-03-16_ticks.jsonl | python3 -m json.tool

# Compare with real JSONL (March 13)
echo "=== REAL TICK ==="
head -1 /mnt/e/tick_logs/*2026-03-13* | python3 -m json.tool

echo "=== SYNTHETIC TICK ==="
head -1 ~/psxdata/intraday/redo_psxt_2026-03-16_ticks.jsonl | python3 -m json.tool
```

## IMPORTANT LIMITATIONS

This file is **NOT the same as real tick data**. It's a synthetic approximation:

| Aspect | Real ticks | This file |
|--------|-----------|-----------|
| Resolution | Every trade (~4,000/day/symbol) | 1 per minute (~67/day/symbol) |
| Bid/Ask | Real order book | Zeros |
| Trades count | Exact | Zero |
| Volume | Cumulative from exchange | Cumulative from bar sums |
| Value/turnover | Exact PKR | Estimated (vol × avg price) |
| Symbols | ~487 | 222 (API returned partial) |
| Time coverage | Full day 09:15-15:30 | Partial 09:17-13:49 |
| Source flag | N/A | `_source: "klines_1m"` |

The `_source: "klines_1m"` flag ensures you can always distinguish synthetic 
records from real tick data in your database.
