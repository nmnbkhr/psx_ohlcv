# Claude Code Prompt: PSX Live Market — Memory-Only + EOD Flush

## What this is

Add a live market feature to PSX OHLCV. Two new files only. Completely independent — no changes to any existing code.

**Key design: EVERYTHING stays in memory during market hours. Single flush to SQLite after market close (EOD). Zero DB writes during trading.**

## Rules — READ THESE FIRST

1. **CREATE only 2 files:** `services/tick_collector.py` and `ui/pages/live_market.py`
2. **DO NOT modify ANY existing files** — zero changes to app.py, main.py, routers, db.py, anything
3. **DO NOT import from any existing PSX OHLCV modules** — both files are 100% self-contained
4. **ALL data stays in memory during market hours** — no SQLite writes until EOD
5. **Snapshot JSON is the ONLY bridge** between collector and Streamlit page (written every 2s, ~100KB)
6. **Streamlit page is dead simple** — reads JSON, renders table. No tabs, no complex widgets
7. **pip install websockets streamlit-autorefresh** before running

## Architecture

```
tick_collector.py                live_snapshot.json              live_market.py
(separate process)    ──2s──▶    (~100KB JSON file)   ──2s──▶   (Streamlit page)
                                                                 reads JSON only
All ticks + bars in RAM (~200 MB)
       │
       └──── EOD flush ONCE at 15:35 PKT ──▶  data/tick_bars.db
```

Memory: ~200 MB for full day = 0.6% of 32GB. Invisible.

---

## File 1: `services/tick_collector.py`

Standalone background process. Run with: `python services/tick_collector.py`

### What it does

1. Connects to `wss://psxterminal.com/`
2. Subscribes to markets: REG, FUT, ODL, BNB, CSF
3. For every tick: updates in-memory dict of latest price per symbol
4. Builds 5-second OHLCV bars from ticks — **all kept in memory (list)**
5. All raw ticks also kept in memory (list)
6. Every 2 seconds → write `data/live_snapshot.json` with all current prices + market stats
7. After market close (15:35 PKT) → flush ALL bars + raw ticks to SQLite → build indexes → dedup → clear memory → sleep
8. On disconnect → auto-reconnect after 5 seconds

### In-memory data structures

```python
class TickCollector:
    def __init__(self):
        self.live = {}              # "REG:HUBC" → latest tick dict (all symbols)
        self.raw_ticks = []         # ALL raw ticks for the day
        self.completed_bars = []    # ALL completed 5s bars for the day
        self.builder = BarBuilder(interval_seconds=5)  # currently open bars
        self.tick_count = 0
        self.connected = False
```

**No SQLite connection during market hours. No DB writes. Only the snapshot JSON every 2 seconds.**

### BarBuilder logic

```
Tick arrives → bucket = round timestamp down to nearest 5-second boundary
If tick belongs to a NEW bucket for that symbol:
    → close the OLD bucket → append to self.completed_bars (in memory)
    → start new bucket with this tick as open
If tick belongs to CURRENT bucket:
    → update high/low/close/volume/trades
```

Stale flush: every snapshot write cycle, force-close any open bar older than 10 seconds (handles illiquid stocks). Append those to `self.completed_bars` too.

### Snapshot JSON — written every 2 seconds

Atomic write: write to `.tmp` file then `os.rename()` so Streamlit never reads a partial file.

```json
{
  "timestamp": "2026-02-20T12:34:56+05:00",
  "connected": true,
  "tick_count": 12340,
  "bars_in_memory": 5102,
  "raw_ticks_in_memory": 12340,
  "ram_mb": 125.4,
  "symbol_count": 489,
  "breadth": {"gainers": 293, "losers": 145, "unchanged": 51},
  "top_gainers": [
    {"symbol": "GUSM", "price": 45.2, "changePercent": 0.1003, "volume": 320000}
  ],
  "top_losers": [
    {"symbol": "FNEL", "price": 12.3, "changePercent": -0.1001, "volume": 180000}
  ],
  "most_active": [
    {"symbol": "HUBC", "price": 234.98, "volume": 5766627, "changePercent": -0.0085}
  ],
  "symbols": [
    {
      "symbol": "HUBC", "market": "REG", "price": 234.98,
      "change": -2.02, "changePercent": -0.0085,
      "volume": 5766627, "high": 240.0, "low": 234.5,
      "bid": 235.0, "ask": 235.1, "trades": 7926
    }
  ]
}
```

Include `ram_mb` — use `import os; os.getpid()` + `psutil.Process(pid).memory_info().rss / 1024 / 1024` (or fallback to `sys.getsizeof` estimate if psutil not available).

### WebSocket connection

```python
WSS_URL = "wss://psxterminal.com/"
MARKETS = ["REG", "FUT", "ODL", "BNB", "CSF"]

async with websockets.connect(WSS_URL, ping_interval=30, ping_timeout=10) as ws:
    for mkt in MARKETS:
        await ws.send(json.dumps({
            "type": "subscribe",
            "subscriptionType": "marketData",
            "params": {"marketType": mkt}
        }))
```

### WebSocket message format (assumed — needs verification)

Based on REST API at `/api/ticks/REG/{symbol}`, messages likely look like:

```json
{
  "symbol": "HUBC", "market": "REG", "price": 234.98,
  "change": -2.02, "changePercent": -0.0085,
  "volume": 5766627, "trades": 7926,
  "high": 240.0, "low": 234.5,
  "bid": 235.0, "ask": 235.1,
  "timestamp": 1769597361
}
```

**Might be wrapped** like `{"type": "tick", "data": {...}}`. So:

- Add a `--debug` CLI flag
- When `--debug`: connect, subscribe, print first 10 raw messages exactly as received, then exit
- User verifies format before real run

### EOD flush — SINGLE write after market close

This is the ONLY time SQLite is touched all day:

```python
def eod_flush(self):
    """Called once after market close. Writes everything to disk."""
    # Force-close any remaining open bars
    stale = self.builder.flush_all()  # close ALL open bars regardless of age
    self.completed_bars.extend(stale)
    
    print(f"📊 EOD flush: {len(self.completed_bars):,} bars, {len(self.raw_ticks):,} ticks")
    
    DB_PATH = DATA_DIR / "tick_bars.db"
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-128000")
    conn.execute("PRAGMA temp_store=MEMORY")
    
    # Create tables if first run
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_5s (
            symbol TEXT, market TEXT, ts TEXT,
            o REAL, h REAL, l REAL, c REAL,
            v INTEGER, t INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_ticks (
            symbol TEXT, market TEXT, ts REAL,
            price REAL, volume INTEGER,
            bid REAL, ask REAL,
            bid_vol INTEGER, ask_vol INTEGER
        )
    """)
    
    # Batch insert all bars
    conn.executemany(
        "INSERT INTO ohlcv_5s VALUES (?,?,?,?,?,?,?,?,?)",
        [(b["symbol"], b["market"], b["timestamp"],
          b["open"], b["high"], b["low"], b["close"],
          b["volume"], b["trades"]) for b in self.completed_bars]
    )
    
    # Batch insert all raw ticks
    conn.executemany(
        "INSERT INTO raw_ticks VALUES (?,?,?,?,?,?,?,?,?)",
        [(t["symbol"], t.get("market","REG"), t.get("timestamp",0),
          t["price"], t.get("volume",0),
          t.get("bid",0), t.get("ask",0),
          t.get("bidVol",0), t.get("askVol",0)) for t in self.raw_ticks]
    )
    
    conn.commit()
    
    # Build indexes + dedup (only after all data is in)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_sym_ts ON ohlcv_5s(symbol, ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tick_sym_ts ON raw_ticks(symbol, ts)")
    conn.execute("""
        DELETE FROM ohlcv_5s WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM ohlcv_5s GROUP BY symbol, market, ts
        )
    """)
    conn.execute("PRAGMA optimize")
    conn.commit()
    conn.close()
    
    print(f"✅ EOD complete: {len(self.completed_bars):,} bars, {len(self.raw_ticks):,} ticks saved")
    
    # Clear memory for next day
    self.completed_bars = []
    self.raw_ticks = []
    self.live = {}
    self.builder = BarBuilder(interval_seconds=5)
    self.tick_count = 0
```

### Market close detection

After 15-second WebSocket timeout (no data received), check if current time >= 15:35 PKT. If yes:
1. Run `eod_flush()`
2. Write final snapshot with `connected: false`
3. Print "💤 Sleeping"
4. Sleep in a loop until next trading day 9:15 AM PKT, then reconnect

### Console output

```
🚀 PSX Tick Collector (memory mode)
   Snapshot: data/live_snapshot.json
   EOD target: data/tick_bars.db
🔗 Connected
✅ REG ✅ FUT ✅ ODL ✅ BNB ✅ CSF
⚡ Ticks: 1,247 | Bars: 500 | Symbols: 312 | RAM: 15 MB
⚡ Ticks: 4,891 | Bars: 1,800 | Symbols: 478 | RAM: 52 MB
⚡ Ticks: 125,430 | Bars: 48,200 | Symbols: 489 | RAM: 143 MB
🔔 Market closed
📊 EOD flush: 48,200 bars, 125,430 ticks
✅ EOD complete
💤 Sleeping until next session
```

Print status every 30 seconds (console only — not related to snapshot timing).

---

## File 2: `ui/pages/live_market.py`

Streamlit page. Reads `data/live_snapshot.json` every 2 seconds. That's all it does.

### Full page structure

```python
import streamlit as st
import json, pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="PSX Live", layout="wide")
st_autorefresh(interval=2000, limit=None, key="live")

PKT = timezone(timedelta(hours=5))
SNAPSHOT = Path(__file__).parent.parent.parent / "data" / "live_snapshot.json"

# --- Check collector ---
if not SNAPSHOT.exists():
    st.error("⚠️ Collector not running. Start: `python services/tick_collector.py`")
    st.stop()

data = json.loads(SNAPSHOT.read_text())
age = (datetime.now(PKT) - datetime.fromisoformat(data["timestamp"])).total_seconds()

if age > 30:
    st.warning(f"⚠️ Data is {int(age)}s stale — collector may have stopped")

# --- Status bar ---
status = "🟢 LIVE" if data["connected"] and age < 10 else "🟡 STALE" if age < 30 else "🔴 DOWN"
ts_str = datetime.fromisoformat(data["timestamp"]).strftime("%H:%M:%S")
ram = data.get("ram_mb", 0)
st.markdown(f"### {status} — {data['symbol_count']} symbols | {data['tick_count']:,} ticks | {ts_str} PKT | {ram:.0f} MB RAM")

# --- Breadth ---
b = data["breadth"]
total = b["gainers"] + b["losers"] + b["unchanged"]
c1, c2, c3 = st.columns(3)
c1.metric("🟢 Gainers", b["gainers"])
c2.metric("🔴 Losers", b["losers"])
c3.metric("⚪ Unchanged", b["unchanged"])

if total > 0:
    gp = b["gainers"]/total*100
    up = b["unchanged"]/total*100
    lp = b["losers"]/total*100
    st.markdown(
        f'<div style="display:flex;height:20px;border-radius:4px;overflow:hidden">'
        f'<div style="width:{gp}%;background:#22c55e"></div>'
        f'<div style="width:{up}%;background:#6b7280"></div>'
        f'<div style="width:{lp}%;background:#ef4444"></div>'
        f'</div>', unsafe_allow_html=True
    )

# --- Top movers ---
col_g, col_l, col_a = st.columns(3)
with col_g:
    st.markdown("**🚀 Top Gainers**")
    for s in data.get("top_gainers", [])[:5]:
        pct = s["changePercent"] * 100
        st.markdown(f"**{s['symbol']}** &nbsp; :green[+{pct:.2f}%]")
with col_l:
    st.markdown("**📉 Top Losers**")
    for s in data.get("top_losers", [])[:5]:
        pct = s["changePercent"] * 100
        st.markdown(f"**{s['symbol']}** &nbsp; :red[{pct:.2f}%]")
with col_a:
    st.markdown("**📊 Most Active**")
    for s in data.get("most_active", [])[:5]:
        st.markdown(f"**{s['symbol']}** &nbsp; {s['volume']:,}")

# --- Filters ---
st.divider()
fc1, fc2 = st.columns([1, 3])
market_filter = fc1.selectbox("Market", ["ALL", "REG", "FUT", "ODL", "BNB", "CSF"])
search = fc2.text_input("Search symbol", "")

# --- Main table ---
df = pd.DataFrame(data["symbols"])
if df.empty:
    st.info("No data yet — waiting for ticks...")
    st.stop()

if market_filter != "ALL":
    df = df[df["market"] == market_filter]
if search:
    df = df[df["symbol"].str.contains(search.upper())]

df["changePercent"] = df["changePercent"] * 100
df = df.sort_values("volume", ascending=False)

display_cols = ["symbol", "market", "price", "change", "changePercent", "volume", "high", "low", "bid", "ask", "trades"]
display_cols = [c for c in display_cols if c in df.columns]

st.dataframe(
    df[display_cols].style.applymap(
        lambda v: "color: #22c55e" if v > 0 else "color: #ef4444" if v < 0 else "",
        subset=[c for c in ["change", "changePercent"] if c in display_cols]
    ),
    use_container_width=True,
    height=600,
    column_config={
        "changePercent": st.column_config.NumberColumn("Chg%", format="%.2f%%"),
        "volume": st.column_config.NumberColumn("Volume", format="%d"),
        "price": st.column_config.NumberColumn("Price", format="%.2f"),
    }
)
```

That's the entire page. Read JSON → render. Nothing else.

---

## Test

```bash
# Install
pip install websockets streamlit-autorefresh

# Debug first — see raw WebSocket messages
python services/tick_collector.py --debug

# Run collector (market hours: Mon-Fri 9:30-15:30 PKT)
python services/tick_collector.py

# Check snapshot
cat data/live_snapshot.json | python -m json.tool | head -20

# After market close — check DB
sqlite3 data/tick_bars.db "SELECT COUNT(*) FROM ohlcv_5s"
sqlite3 data/tick_bars.db "SELECT COUNT(*) FROM raw_ticks"

# Streamlit — Live Market page appears in sidebar automatically
streamlit run ui/app.py
```

## Files summary

| Action | File |
|--------|------|
| CREATE | `services/tick_collector.py` |
| CREATE | `ui/pages/live_market.py` |

**Nothing else. Zero changes to existing code. Zero DB writes during market hours.**
