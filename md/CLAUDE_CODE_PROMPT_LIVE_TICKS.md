# Claude Code Prompt: PSX Live Tick Collector + Live Dashboard Page

## Context

PSX OHLCV is my Pakistan stock market app (FastAPI + Streamlit + SQLite). I want to add:
1. A background tick collector that connects to `wss://psxterminal.com/` WebSocket
2. A Streamlit live dashboard page that shows all symbols updating in real-time

The collector runs as a **separate background process**. Streamlit reads from shared state. They communicate via a shared SQLite DB + a JSON snapshot file for fast reads.

## Architecture

```
tick_collector.py (standalone process)
  ├── Connects to wss://psxterminal.com/
  ├── Subscribes to: REG, FUT, ODL, BNB, CSF
  ├── Builds 5-second OHLCV bars from raw ticks
  ├── Keeps ALL current prices in memory (dict)
  ├── Writes snapshot JSON every 2 seconds → /data/live_snapshot.json
  ├── Flushes completed bars to SQLite every 30 seconds → /data/tick_bars.db
  └── Runs post-market optimization at 15:35 PKT (index + dedup)

Streamlit live page (ui/pages/live_market.py)
  ├── Reads /data/live_snapshot.json every 1-2 seconds (st.autorefresh)
  ├── Shows sortable table: all symbols with price, change, volume, bid/ask
  ├── Color coded: green=up, red=down
  ├── Filter by market (REG/FUT/ODL), sector, search
  ├── Top gainers/losers cards at top
  ├── Market breadth bar (advance/decline/unchanged)
  └── Optional: mini sparkline from recent 5s bars (last 30 min)
```

## Step 1: Create the tick collector background service

**File:** `services/tick_collector.py`

This runs standalone: `python services/tick_collector.py`

```python
"""
PSX Live Tick Collector
Connects to psxterminal.com WebSocket, builds 5s OHLCV bars,
writes live snapshot for Streamlit consumption.

Usage: python services/tick_collector.py
"""

import asyncio, websockets, json, sqlite3, os, signal, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

PKT = timezone(timedelta(hours=5))
MARKETS = ["REG", "FUT", "ODL", "BNB", "CSF"]
INTERVAL = 5          # seconds per bar
SNAPSHOT_INTERVAL = 2  # seconds between snapshot writes
FLUSH_INTERVAL = 30    # seconds between DB flushes
WSS_URL = "wss://psxterminal.com/"

# Paths — adjust to match your PSX OHLCV data directory
DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "tick_bars.db"
SNAPSHOT_PATH = DATA_DIR / "live_snapshot.json"


class BarBuilder:
    """Aggregates ticks into 5-second OHLCV bars."""
    
    def __init__(self, interval_seconds=5):
        self.interval = interval_seconds
        self.bars = {}  # (symbol, market, bucket_ts) → bar
    
    def _bucket(self, ts):
        dt = datetime.fromtimestamp(ts, tz=PKT)
        total_secs = dt.hour * 3600 + dt.minute * 60 + dt.second
        bucket_secs = (total_secs // self.interval) * self.interval
        h, rem = divmod(bucket_secs, 3600)
        m, s = divmod(rem, 60)
        return dt.replace(hour=h, minute=m, second=s, microsecond=0)
    
    def process_tick(self, tick):
        symbol = tick["symbol"]
        market = tick.get("market", "REG")
        price = tick["price"]
        volume = tick.get("volume", 0)
        ts = tick.get("timestamp", datetime.now(PKT).timestamp())
        
        bucket = self._bucket(ts)
        key = (symbol, market, bucket)
        
        # Close previous buckets
        completed = []
        for k in list(self.bars):
            if k[0] == symbol and k[1] == market and k[2] < bucket:
                completed.append(self.bars.pop(k))
        
        # Update current bucket
        if key not in self.bars:
            self.bars[key] = {
                "symbol": symbol, "market": market,
                "timestamp": bucket.isoformat(),
                "open": price, "high": price, "low": price, "close": price,
                "volume": volume, "trades": 1,
            }
        else:
            bar = self.bars[key]
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
            bar["volume"] = volume
            bar["trades"] += 1
        
        return completed
    
    def flush_stale(self, cutoff_seconds=10):
        now = datetime.now(PKT)
        stale = []
        for k in list(self.bars):
            if (now - k[2]).total_seconds() > cutoff_seconds:
                stale.append(self.bars.pop(k))
        return stale


class TickCollector:
    """Main collector: WebSocket → memory → snapshot file → SQLite."""
    
    def __init__(self):
        self.builder = BarBuilder(INTERVAL)
        self.live = {}          # symbol → latest tick data (ALL symbols)
        self.bar_buffer = []    # completed bars waiting for DB flush
        self.tick_count = 0
        self.bars_saved = 0
        self.connected = False
        self.last_tick_time = None
        
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-128000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_5s (
                symbol TEXT, market TEXT, ts TEXT,
                o REAL, h REAL, l REAL, c REAL,
                v INTEGER, t INTEGER
            )
        """)
        conn.commit()
        conn.close()
    
    def process(self, tick):
        """Process a single tick: update live state + build bars."""
        if "price" not in tick or tick["price"] is None:
            return
        
        self.tick_count += 1
        symbol = tick.get("symbol", "?")
        market = tick.get("market", "REG")
        self.last_tick_time = datetime.now(PKT).isoformat()
        
        # Update live snapshot (latest price for every symbol)
        key = f"{market}:{symbol}"
        prev = self.live.get(key, {})
        prev_price = prev.get("price", tick["price"])
        
        self.live[key] = {
            "symbol": symbol,
            "market": market,
            "price": tick["price"],
            "change": tick.get("change", 0),
            "changePercent": tick.get("changePercent", 0),
            "volume": tick.get("volume", 0),
            "value": tick.get("value", 0),
            "trades": tick.get("trades", 0),
            "high": tick.get("high", tick["price"]),
            "low": tick.get("low", tick["price"]),
            "bid": tick.get("bid", 0),
            "ask": tick.get("ask", 0),
            "bidVol": tick.get("bidVol", 0),
            "askVol": tick.get("askVol", 0),
            "timestamp": tick.get("timestamp", 0),
            "updated": datetime.now(PKT).isoformat(),
        }
        
        # Build bars
        completed = self.builder.process_tick(tick)
        if completed:
            self.bar_buffer.extend(completed)
    
    def write_snapshot(self):
        """Write live state to JSON file for Streamlit to read."""
        now = datetime.now(PKT)
        
        # Compute market summary
        symbols = list(self.live.values())
        reg_symbols = [s for s in symbols if s["market"] == "REG"]
        gainers = sum(1 for s in reg_symbols if s.get("changePercent", 0) > 0)
        losers = sum(1 for s in reg_symbols if s.get("changePercent", 0) < 0)
        unchanged = len(reg_symbols) - gainers - losers
        
        # Top movers
        by_change = sorted(reg_symbols, key=lambda x: x.get("changePercent", 0))
        top_gainers = by_change[-10:][::-1]
        top_losers = by_change[:10]
        
        # Most active by volume
        most_active = sorted(reg_symbols, key=lambda x: x.get("volume", 0), reverse=True)[:10]
        
        snapshot = {
            "timestamp": now.isoformat(),
            "connected": self.connected,
            "tick_count": self.tick_count,
            "bars_saved": self.bars_saved,
            "last_tick": self.last_tick_time,
            "symbol_count": len(self.live),
            "markets": {
                mkt: len([s for s in symbols if s["market"] == mkt])
                for mkt in MARKETS
            },
            "breadth": {
                "gainers": gainers,
                "losers": losers,
                "unchanged": unchanged,
            },
            "top_gainers": top_gainers,
            "top_losers": top_losers,
            "most_active": most_active,
            "symbols": symbols,  # ALL symbols
        }
        
        # Atomic write: write to temp, then rename
        tmp = SNAPSHOT_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, default=str))
        tmp.rename(SNAPSHOT_PATH)
    
    def flush_bars_to_db(self):
        """Batch write completed bars to SQLite."""
        stale = self.builder.flush_stale()
        self.bar_buffer.extend(stale)
        
        if not self.bar_buffer:
            return
        
        conn = sqlite3.connect(str(DB_PATH))
        conn.executemany(
            "INSERT INTO ohlcv_5s VALUES (?,?,?,?,?,?,?,?,?)",
            [(b["symbol"], b["market"], b["timestamp"],
              b["open"], b["high"], b["low"], b["close"],
              b["volume"], b["trades"]) for b in self.bar_buffer]
        )
        conn.commit()
        conn.close()
        
        self.bars_saved += len(self.bar_buffer)
        self.bar_buffer = []
    
    def post_market(self):
        """Run after market close: index + dedup + optimize."""
        self.flush_bars_to_db()
        
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_sym_ts ON ohlcv_5s(symbol, ts)")
        conn.execute("""
            DELETE FROM ohlcv_5s WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ohlcv_5s GROUP BY symbol, market, ts
            )
        """)
        conn.execute("PRAGMA optimize")
        conn.commit()
        conn.close()
        print("✅ Post-market optimization complete")


async def main():
    collector = TickCollector()
    
    while True:  # reconnect loop
        try:
            async with websockets.connect(
                WSS_URL, ping_interval=30, ping_timeout=10, max_size=2**20,
            ) as ws:
                collector.connected = True
                print(f"🔗 Connected to {WSS_URL}")
                
                for mkt in MARKETS:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "subscriptionType": "marketData",
                        "params": {"marketType": mkt}
                    }))
                    print(f"✅ Subscribed: {mkt}")
                
                last_snapshot = datetime.now(PKT)
                last_flush = datetime.now(PKT)
                
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except asyncio.TimeoutError:
                        # No data — maybe market closed
                        now = datetime.now(PKT)
                        if now.hour >= 15 and now.minute >= 35:
                            print("🔔 Market closed")
                            collector.post_market()
                            collector.write_snapshot()
                            # Sleep until 9:15 AM next day
                            print("💤 Sleeping until next session...")
                            await asyncio.sleep(600)
                        collector.flush_bars_to_db()
                        collector.write_snapshot()
                        continue
                    
                    tick = json.loads(raw)
                    collector.process(tick)
                    
                    now = datetime.now(PKT)
                    
                    # Snapshot every 2 seconds
                    if (now - last_snapshot).total_seconds() >= SNAPSHOT_INTERVAL:
                        collector.write_snapshot()
                        last_snapshot = now
                    
                    # DB flush every 30 seconds
                    if (now - last_flush).total_seconds() >= FLUSH_INTERVAL:
                        collector.flush_bars_to_db()
                        active = len(collector.builder.bars)
                        print(f"⚡ Ticks: {collector.tick_count:,} | "
                              f"Bars: {collector.bars_saved:,} | "
                              f"Symbols: {len(collector.live)} | "
                              f"Active: {active}")
                        last_flush = now
        
        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            collector.connected = False
            print(f"❌ Disconnected: {e}. Reconnecting in 5s...")
            collector.write_snapshot()
            await asyncio.sleep(5)


if __name__ == "__main__":
    print("🚀 PSX Live Tick Collector starting...")
    print(f"   DB: {DB_PATH}")
    print(f"   Snapshot: {SNAPSHOT_PATH}")
    print(f"   Interval: {INTERVAL}s bars")
    print(f"   Markets: {', '.join(MARKETS)}")
    asyncio.run(main())
```

**Important notes for implementation:**
- The exact WebSocket message format from psxterminal.com is not 100% confirmed yet. The code assumes messages come as JSON objects with fields like `symbol`, `market`, `price`, `change`, `volume`, `bid`, `ask`, `timestamp` — based on the REST API response format at `/api/ticks/REG/{symbol}`. When you first run this, print the first 10 raw messages and adjust field names if needed.
- The WebSocket might send messages in a different structure, e.g. wrapped in `{"type": "update", "data": {...}}`. If so, unwrap before passing to `collector.process()`.
- Test this during PSX market hours (9:30 AM - 3:30 PM PKT, Mon-Fri). Outside hours the WebSocket may connect but send no data.

## Step 2: Create the Streamlit live market page

**File:** `ui/pages/live_market.py`

This reads the snapshot JSON that the collector writes every 2 seconds. Uses `st_autorefresh` to poll every 2 seconds.

Key sections:
1. **Status bar** — connection status, tick count, last update time
2. **Market breadth** — advance/decline/unchanged bar with counts
3. **Top movers row** — 5 top gainers + 5 top losers as metric cards
4. **Most active** — top 10 by volume
5. **Full symbol table** — sortable, filterable by market/search, color-coded change %
6. **Auto-refresh** — every 2 seconds using `streamlit-autorefresh` package

```python
# Install: pip install streamlit-autorefresh
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, limit=None, key="live_refresh")

# Read snapshot
snapshot_path = Path("data/live_snapshot.json")
if snapshot_path.exists():
    data = json.loads(snapshot_path.read_text())
    # ... render dashboard
else:
    st.warning("Tick collector not running. Start it: python services/tick_collector.py")
```

For the full table, use:
```python
import pandas as pd

df = pd.DataFrame(data["symbols"])

# Color the change column
def color_change(val):
    color = "green" if val > 0 else "red" if val < 0 else "gray"
    return f"color: {color}"

# Display
st.dataframe(
    df[["symbol", "market", "price", "change", "changePercent", "volume", "high", "low", "bid", "ask"]]
    .sort_values("volume", ascending=False)
    .style.applymap(color_change, subset=["change", "changePercent"]),
    use_container_width=True,
    height=600,
)
```

For the breadth bar, use a simple stacked horizontal bar:
```python
gainers = data["breadth"]["gainers"]
losers = data["breadth"]["losers"]
unchanged = data["breadth"]["unchanged"]
total = gainers + losers + unchanged

col1, col2, col3 = st.columns(3)
col1.metric("🟢 Gainers", gainers)
col2.metric("🔴 Losers", losers)
col3.metric("⚪ Unchanged", unchanged)

# Visual bar
g_pct = gainers / total * 100 if total else 0
l_pct = losers / total * 100 if total else 0
u_pct = unchanged / total * 100 if total else 0
st.markdown(
    f'<div style="display:flex;height:24px;border-radius:4px;overflow:hidden">'
    f'<div style="width:{g_pct}%;background:#22c55e"></div>'
    f'<div style="width:{u_pct}%;background:#6b7280"></div>'
    f'<div style="width:{l_pct}%;background:#ef4444"></div>'
    f'</div>',
    unsafe_allow_html=True
)
```

## Step 3: Add collector management to the API

**File:** Add to existing `api/main.py` or create `api/routers/live.py`

Endpoints for managing the collector from the UI:

```python
@router.get("/live/status")
async def live_status():
    """Read snapshot file to get collector status."""
    snapshot_path = Path("data/live_snapshot.json")
    if not snapshot_path.exists():
        return {"running": False}
    data = json.loads(snapshot_path.read_text())
    age = (datetime.now() - datetime.fromisoformat(data["timestamp"])).total_seconds()
    return {
        "running": age < 10,  # if snapshot is older than 10s, collector is probably dead
        "connected": data.get("connected", False),
        "tick_count": data.get("tick_count", 0),
        "symbol_count": data.get("symbol_count", 0),
        "bars_saved": data.get("bars_saved", 0),
        "last_update": data.get("timestamp"),
        "age_seconds": round(age, 1),
    }

@router.get("/live/snapshot")
async def live_snapshot():
    """Full snapshot for API consumers."""
    snapshot_path = Path("data/live_snapshot.json")
    if not snapshot_path.exists():
        raise HTTPException(503, "Tick collector not running")
    return json.loads(snapshot_path.read_text())

@router.get("/live/bars/{symbol}")
async def live_bars(symbol: str, minutes: int = 30):
    """Get recent 5s bars for a symbol from tick_bars.db."""
    db_path = Path("data/tick_bars.db")
    if not db_path.exists():
        raise HTTPException(503, "No tick data available")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone(timedelta(hours=5))) - timedelta(minutes=minutes)).isoformat()
    rows = conn.execute(
        "SELECT * FROM ohlcv_5s WHERE symbol=? AND ts>=? ORDER BY ts",
        (symbol.upper(), cutoff)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
```

## Step 4: Add startup script

**File:** `start_live.sh` — convenience script to start everything

```bash
#!/bin/bash
echo "🚀 Starting PSX Live System..."

# Start tick collector in background
echo "Starting tick collector..."
python services/tick_collector.py &
COLLECTOR_PID=$!
echo "Collector PID: $COLLECTOR_PID"

# Wait for first snapshot
sleep 5

# Start Streamlit (if not already running)
echo "Starting Streamlit..."
streamlit run ui/app.py --server.port 8501 &

echo ""
echo "✅ Live system running:"
echo "   Collector: PID $COLLECTOR_PID"
echo "   Dashboard: http://localhost:8501"
echo ""
echo "To stop: kill $COLLECTOR_PID"
```

## Step 5: Add dependency

Add `streamlit-autorefresh` and `websockets` to requirements.txt:
```
websockets>=12.0
streamlit-autorefresh>=1.0.0
```

## Critical rules

1. **tick_collector.py runs as a SEPARATE PROCESS** — never inside Streamlit
2. **Communication is via file** — `data/live_snapshot.json` (atomic write with rename)
3. **Streamlit only READS** — never writes to snapshot or tick_bars.db
4. **Collector is crash-resilient** — auto-reconnects on WebSocket disconnect
5. **Snapshot JSON must be small enough for fast reads** — ~500 symbols × ~200 bytes = ~100KB, fine
6. **The WebSocket message format is assumed** based on REST API — print first 10 raw messages and adjust `collector.process()` field mapping if different
7. **Do NOT use st.session_state for live data** — it doesn't share across users/tabs. The JSON file is the shared state.
8. **Add `data/live_snapshot.json` and `data/tick_bars.db` to .gitignore**
9. **pip install websockets streamlit-autorefresh** before running

## Files to create/modify summary

| Action | File | What |
|--------|------|------|
| CREATE | `services/tick_collector.py` | Background WSS collector + bar builder |
| CREATE | `ui/pages/live_market.py` | Live dashboard page with auto-refresh |
| CREATE | `api/routers/live.py` | Status + snapshot + bars API endpoints |
| MODIFY | `api/main.py` | Register live router |
| CREATE | `start_live.sh` | Convenience startup script |
| MODIFY | `requirements.txt` | Add websockets, streamlit-autorefresh |

## Test

```bash
# 1. Install deps
pip install websockets streamlit-autorefresh

# 2. Start collector (run during market hours 9:30-15:30 PKT)
python services/tick_collector.py

# 3. Check snapshot is being written
cat data/live_snapshot.json | python -m json.tool | head -30

# 4. Check bars DB
sqlite3 data/tick_bars.db "SELECT COUNT(*) FROM ohlcv_5s"
sqlite3 data/tick_bars.db "SELECT DISTINCT symbol FROM ohlcv_5s LIMIT 20"

# 5. Start Streamlit and navigate to Live Market page
streamlit run ui/app.py

# 6. API check
curl http://localhost:8000/live/status
curl http://localhost:8000/live/bars/HUBC?minutes=5
```

## Expected Output

Collector terminal:
```
🚀 PSX Live Tick Collector starting...
   DB: data/tick_bars.db
   Snapshot: data/live_snapshot.json
   Interval: 5s bars
   Markets: REG, FUT, ODL, BNB, CSF
🔗 Connected to wss://psxterminal.com/
✅ Subscribed: REG
✅ Subscribed: FUT
✅ Subscribed: ODL
✅ Subscribed: BNB
✅ Subscribed: CSF
⚡ Ticks: 1,247 | Bars: 489 | Symbols: 312 | Active: 312
⚡ Ticks: 4,891 | Bars: 1,823 | Symbols: 478 | Active: 478
⚡ Ticks: 12,340 | Bars: 5,102 | Symbols: 489 | Active: 489
```

Streamlit live page:
```
🟢 LIVE — Connected | 489 symbols | 12,340 ticks | Updated: 12:34:56

🟢 Gainers: 293  🔴 Losers: 145  ⚪ Unchanged: 51
[████████████████████░░░░░░░████████]

Top Gainers              Top Losers
GUSM  +10.03%            FNEL  -10.01%
BFMOD +10.02%            TSBL  -10.00%
SEL   +10.02%            KOIL  -10.00%

Symbol  Market  Price   Change   Change%   Volume      High    Low
HUBC    REG     234.98  -2.02    -0.85%    5,766,627   240.00  234.50
OGDC    REG     128.45  +1.23    +0.97%    3,221,100   129.00  127.80
HBL     REG     298.12  +3.45    +1.17%    1,845,200   299.50  296.00
...
```
