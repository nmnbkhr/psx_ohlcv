# Claude Code Prompt: PSX Live Indices — Add-on to Live Tick Collector

## What this is

Add live index tracking to PSX OHLCV. Extends the tick collector to subscribe to the IDX market and creates a dedicated indices Streamlit page. The indices page shows KSE-100 as the hero, all 12 PSX indices in a live table, intraday sparklines built from tick history, and a heatmap showing which indices are up/down.

**Key design: Same architecture as live ticks — memory only, JSON bridge, EOD flush. Indices are tracked SEPARATELY from stock symbols in both memory and snapshot.**

## Rules — READ THESE FIRST

1. **MODIFY 1 file:** `services/tick_collector.py` — add IDX subscription + index tracking
2. **CREATE 1 file:** `ui/pages/live_indices.py` — dedicated indices dashboard
3. **DO NOT modify ANY other existing files** — zero changes to app.py, main.py, db.py, live_market.py, anything else
4. **DO NOT import from any existing PSX OHLCV modules** — the new page is self-contained
5. **Index data goes in a SEPARATE `indices` section** of the snapshot JSON — NOT mixed into the `symbols` array
6. **Intraday history for sparklines** — keep last 60 data points per index in memory (5-minute intervals = 5 hours of sparkline)
7. **No additional pip installs needed** — uses same websockets + streamlit-autorefresh from live ticks setup

## Architecture

```
tick_collector.py                live_snapshot.json              live_indices.py
(MODIFIED — adds IDX)  ──2s──▶  (EXTENDED with indices)  ──2s──▶  (NEW page)
                                                                    reads JSON only
Indices in RAM: ~1 MB (12 indices × 60 sparkline points)
       │
       └──── EOD flush adds index_ohlcv_5s table to data/tick_bars.db
```

Memory overhead: ~1 MB. Negligible.

---

## PSX Indices — All 12

| Symbol | Full Name | Type |
|--------|-----------|------|
| KSE100 | KSE-100 Index | Equity (main benchmark) |
| KSE30 | KSE-30 Index | Equity (blue chip) |
| KSE100PR | KSE-100 Price Return | Equity (price only, no dividends) |
| ALLSHR | All Share Index | Equity (full market) |
| KMI30 | KMI-30 Index | Islamic |
| KMIALLSHR | KMI All Shares Index | Islamic |
| MII30 | Mahaana Islamic Index 30 | Islamic |
| MZNPI | Meezan Pakistan Index | Islamic |
| BKTI | Banks Tradable Index | Sectoral |
| JSGBKTI | JS Global Banks Tradable Index | Sectoral |
| JSMFI | JS Momentum Factor Index | Sectoral |
| ACI | Alfalah Consumer Index | Sectoral/ETF |

---

## Changes to File 1: `services/tick_collector.py`

### 1. Add IDX to MARKETS list

```python
MARKETS = ["REG", "FUT", "ODL", "BNB", "CSF", "IDX"]  # was missing IDX
```

### 2. Add index-specific data structures to `__init__`

```python
class TickCollector:
    def __init__(self):
        # ... existing fields unchanged ...
        self.live = {}              # "REG:HUBC" → latest tick (existing)
        self.raw_ticks = []         # existing
        self.completed_bars = []    # existing
        self.builder = BarBuilder(interval_seconds=5)  # existing
        self.tick_count = 0         # existing
        self.connected = False      # existing
        
        # NEW — Index-specific tracking
        self.indices = {}           # "KSE100" → latest index tick dict
        self.index_history = {}     # "KSE100" → deque(maxlen=360) of {ts, value}
                                    #   360 points × 5s = 30 min at tick resolution
                                    #   but we downsample to 5-min intervals for sparkline
        self.index_sparklines = {}  # "KSE100" → list of {ts, value} at 5-min intervals (max 60 points)
        self.index_ticks = []       # ALL raw index ticks for the day (for EOD flush)
```

### 3. Route IDX ticks separately in the message handler

When a tick arrives, check if `market == "IDX"`. If so, route to index handler instead of stock handler:

```python
async def handle_message(self, msg):
    # ... existing parsing logic ...
    
    market = tick.get("market", "REG")
    symbol = tick.get("symbol", "")
    
    if market == "IDX":
        self._handle_index_tick(tick)
    else:
        self._handle_stock_tick(tick)  # existing logic, just wrapped in a method
```

### 4. Index tick handler

```python
def _handle_index_tick(self, tick):
    """Handle an index tick — separate from stock ticks."""
    symbol = tick["symbol"]
    
    # Update latest
    self.indices[symbol] = {
        "symbol": symbol,
        "value": tick.get("price", tick.get("value", 0)),
        "change": tick.get("change", 0),
        "changePercent": tick.get("changePercent", 0),
        "volume": tick.get("volume", 0),        # constituent volume
        "turnover": tick.get("turnover", 0),     # constituent turnover
        "high": tick.get("high", 0),
        "low": tick.get("low", 0),
        "open": tick.get("open", 0),
        "previousClose": tick.get("previousClose", 0),
        "timestamp": tick.get("timestamp", 0),
    }
    
    # Append to raw index ticks (for EOD flush)
    self.index_ticks.append(tick)
    
    # Track for sparkline — append to history deque
    ts_now = time.time()
    if symbol not in self.index_history:
        from collections import deque
        self.index_history[symbol] = deque(maxlen=360)
    self.index_history[symbol].append({
        "ts": ts_now,
        "value": self.indices[symbol]["value"]
    })
    
    # Also feed to BarBuilder for 5s OHLCV bars (reuse existing builder)
    # Index bars go to completed_bars with market="IDX"
    tick_for_bar = dict(tick)
    tick_for_bar["market"] = "IDX"
    tick_for_bar["price"] = self.indices[symbol]["value"]
    # ... feed to self.builder same as stock ticks ...
```

### 5. Build sparklines during snapshot write

Every snapshot cycle (2 seconds), downsample each index's history deque into 5-minute interval sparkline points:

```python
def _build_sparklines(self):
    """Downsample index history into 5-minute sparkline points."""
    for symbol, history in self.index_history.items():
        if not history:
            continue
        
        points = []
        # Group by 5-minute buckets
        for entry in history:
            bucket = int(entry["ts"] // 300) * 300  # 300s = 5 min
            if not points or points[-1]["ts"] != bucket:
                points.append({"ts": bucket, "value": entry["value"]})
            else:
                points[-1]["value"] = entry["value"]  # last value in bucket wins
        
        # Keep last 60 points (= 5 hours of sparkline data)
        self.index_sparklines[symbol] = points[-60:]
```

### 6. Extend snapshot JSON with `indices` section

Add this to the snapshot alongside existing fields. **DO NOT put index data in the `symbols` array.**

```json
{
  "timestamp": "2026-02-20T12:34:56+05:00",
  "connected": true,
  "tick_count": 12340,
  "symbol_count": 489,
  "breadth": { ... },
  "top_gainers": [ ... ],
  "top_losers": [ ... ],
  "most_active": [ ... ],
  "symbols": [ ... ],
  
  "index_count": 12,
  "indices": [
    {
      "symbol": "KSE100",
      "value": 187761.69,
      "change": 2684.32,
      "changePercent": 0.0145,
      "volume": 450870000,
      "turnover": 47510000000,
      "high": 188200.50,
      "low": 185100.20,
      "open": 185077.37,
      "previousClose": 185077.37,
      "sparkline": [185100, 185400, 185900, 186200, 186800, 187100, 187500, 187761]
    }
  ],
  "index_sparklines": {
    "KSE100": [
      {"ts": 1769580000, "value": 185100.20},
      {"ts": 1769580300, "value": 185450.80},
      {"ts": 1769580600, "value": 186200.15}
    ]
  }
}
```

The `sparkline` field in each index entry is a simple list of values (no timestamps) for quick rendering. The `index_sparklines` section has full timestamp+value pairs for the chart.

### 7. Extend EOD flush

Add `index_ohlcv_5s` and `index_raw_ticks` tables to the `eod_flush()` method:

```python
# In eod_flush(), after existing bar/tick inserts:

# Index bars
conn.execute("""
    CREATE TABLE IF NOT EXISTS index_ohlcv_5s (
        symbol TEXT, ts TEXT,
        o REAL, h REAL, l REAL, c REAL,
        v INTEGER, turnover REAL
    )
""")

# Index raw ticks
conn.execute("""
    CREATE TABLE IF NOT EXISTS index_raw_ticks (
        symbol TEXT, ts REAL,
        value REAL, change REAL,
        change_pct REAL,
        volume INTEGER, turnover REAL
    )
""")

# Insert index bars (filter completed_bars where market == "IDX")
index_bars = [b for b in self.completed_bars if b.get("market") == "IDX"]
conn.executemany(
    "INSERT INTO index_ohlcv_5s VALUES (?,?,?,?,?,?,?,?)",
    [(b["symbol"], b["timestamp"],
      b["open"], b["high"], b["low"], b["close"],
      b.get("volume", 0), b.get("turnover", 0)) for b in index_bars]
)

# Insert index raw ticks
conn.executemany(
    "INSERT INTO index_raw_ticks VALUES (?,?,?,?,?,?,?)",
    [(t["symbol"], t.get("timestamp", 0),
      t.get("price", t.get("value", 0)), t.get("change", 0),
      t.get("changePercent", 0),
      t.get("volume", 0), t.get("turnover", 0)) for t in self.index_ticks]
)

# Add indexes
conn.execute("CREATE INDEX IF NOT EXISTS idx_idxbar_sym_ts ON index_ohlcv_5s(symbol, ts)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_idxtick_sym_ts ON index_raw_ticks(symbol, ts)")

# Dedup index bars
conn.execute("""
    DELETE FROM index_ohlcv_5s WHERE rowid NOT IN (
        SELECT MIN(rowid) FROM index_ohlcv_5s GROUP BY symbol, ts
    )
""")
```

### 8. Clear index memory after EOD flush

```python
# At the end of eod_flush(), add:
self.indices = {}
self.index_history = {}
self.index_sparklines = {}
self.index_ticks = []
```

### 9. Update console output

```
🚀 PSX Tick Collector (memory mode)
   Snapshot: data/live_snapshot.json
   EOD target: data/tick_bars.db
🔗 Connected
✅ REG ✅ FUT ✅ ODL ✅ BNB ✅ CSF ✅ IDX
⚡ Ticks: 1,247 | Bars: 500 | Symbols: 312 | Indices: 12 | RAM: 15 MB
⚡ Ticks: 4,891 | Bars: 1,800 | Symbols: 478 | Indices: 12 | RAM: 52 MB
```

---

## File 2: `ui/pages/live_indices.py` (CREATE NEW)

Streamlit page. Reads `data/live_snapshot.json` every 2 seconds — same as live_market.py. Shows ONLY index data.

### Full page structure

```python
import streamlit as st
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="PSX Indices", layout="wide")
st_autorefresh(interval=2000, limit=None, key="indices_refresh")

PKT = timezone(timedelta(hours=5))
SNAPSHOT = Path(__file__).parent.parent.parent / "data" / "live_snapshot.json"

# --- Categorize indices ---
EQUITY_INDICES = ["KSE100", "KSE30", "KSE100PR", "ALLSHR"]
ISLAMIC_INDICES = ["KMI30", "KMIALLSHR", "MII30", "MZNPI"]
SECTORAL_INDICES = ["BKTI", "JSGBKTI", "JSMFI", "ACI"]

INDEX_NAMES = {
    "KSE100": "KSE-100",
    "KSE30": "KSE-30",
    "KSE100PR": "KSE-100 Price Return",
    "ALLSHR": "All Share",
    "KMI30": "KMI-30",
    "KMIALLSHR": "KMI All Shares",
    "MII30": "Mahaana Islamic 30",
    "MZNPI": "Meezan Pakistan",
    "BKTI": "Banks Tradable",
    "JSGBKTI": "JS Global Banks",
    "JSMFI": "JS Momentum Factor",
    "ACI": "Alfalah Consumer",
}

# --- Check collector ---
if not SNAPSHOT.exists():
    st.error("⚠️ Collector not running. Start: `python services/tick_collector.py`")
    st.stop()

data = json.loads(SNAPSHOT.read_text())
age = (datetime.now(PKT) - datetime.fromisoformat(data["timestamp"])).total_seconds()

if age > 30:
    st.warning(f"⚠️ Data is {int(age)}s stale — collector may have stopped")

indices = data.get("indices", [])
if not indices:
    st.info("No index data yet — waiting for IDX ticks...")
    st.stop()

# Build lookup dict
idx_map = {i["symbol"]: i for i in indices}

# --- Status bar ---
status = "🟢 LIVE" if data["connected"] and age < 10 else "🟡 STALE" if age < 30 else "🔴 DOWN"
ts_str = datetime.fromisoformat(data["timestamp"]).strftime("%H:%M:%S")
idx_count = data.get("index_count", len(indices))
st.markdown(f"### {status} — PSX Indices ({idx_count}) | {ts_str} PKT")

# =========================================
# HERO: KSE-100 card
# =========================================
kse100 = idx_map.get("KSE100")
if kse100:
    val = kse100["value"]
    chg = kse100["change"]
    pct = kse100["changePercent"] * 100
    hi = kse100.get("high", 0)
    lo = kse100.get("low", 0)
    opn = kse100.get("open", 0)
    
    color = "#22c55e" if chg >= 0 else "#ef4444"
    arrow = "▲" if chg >= 0 else "▼"
    sign = "+" if chg >= 0 else ""
    
    st.markdown(f"""
    <div style="background:linear-gradient(135deg, #1e293b, #0f172a); padding:24px; border-radius:12px; 
                border-left:4px solid {color}; margin-bottom:20px;">
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
            <div>
                <div style="color:#94a3b8; font-size:14px; margin-bottom:4px;">KSE-100 INDEX</div>
                <div style="color:white; font-size:36px; font-weight:bold;">{val:,.2f}</div>
                <div style="color:{color}; font-size:20px; margin-top:4px;">
                    {arrow} {sign}{chg:,.2f} ({sign}{pct:.2f}%)
                </div>
            </div>
            <div style="text-align:right;">
                <div style="color:#64748b; font-size:13px;">Open: <span style="color:#e2e8f0;">{opn:,.2f}</span></div>
                <div style="color:#64748b; font-size:13px;">High: <span style="color:#22c55e;">{hi:,.2f}</span></div>
                <div style="color:#64748b; font-size:13px;">Low: <span style="color:#ef4444;">{lo:,.2f}</span></div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # KSE-100 sparkline (if available)
    sparkline_data = data.get("index_sparklines", {}).get("KSE100", [])
    if len(sparkline_data) >= 2:
        spark_df = pd.DataFrame(sparkline_data)
        spark_df["time"] = pd.to_datetime(spark_df["ts"], unit="s").dt.tz_localize("UTC").dt.tz_convert("Asia/Karachi")
        st.line_chart(spark_df.set_index("time")["value"], use_container_width=True, height=200)

# =========================================
# Secondary indices: KSE-30 + KMI-30 + All Share
# =========================================
sec_indices = ["KSE30", "KMI30", "ALLSHR"]
cols = st.columns(len(sec_indices))
for i, sym in enumerate(sec_indices):
    idx = idx_map.get(sym)
    if idx:
        pct = idx["changePercent"] * 100
        delta_str = f"{pct:+.2f}%"
        cols[i].metric(
            label=INDEX_NAMES.get(sym, sym),
            value=f"{idx['value']:,.2f}",
            delta=delta_str,
        )

st.divider()

# =========================================
# Index category cards
# =========================================
def render_index_group(title, symbols, emoji):
    """Render a group of indices as a table."""
    st.markdown(f"#### {emoji} {title}")
    rows = []
    for sym in symbols:
        idx = idx_map.get(sym)
        if not idx:
            continue
        pct = idx["changePercent"] * 100
        rows.append({
            "Index": INDEX_NAMES.get(sym, sym),
            "Symbol": sym,
            "Value": idx["value"],
            "Change": idx["change"],
            "Chg%": pct,
            "High": idx.get("high", 0),
            "Low": idx.get("low", 0),
            "Volume": idx.get("volume", 0),
        })
    
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(
            df.style.applymap(
                lambda v: "color: #22c55e" if v > 0 else "color: #ef4444" if v < 0 else "",
                subset=["Change", "Chg%"]
            ),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Value": st.column_config.NumberColumn(format="%.2f"),
                "Change": st.column_config.NumberColumn(format="%+.2f"),
                "Chg%": st.column_config.NumberColumn(format="%+.2f%%"),
                "High": st.column_config.NumberColumn(format="%.2f"),
                "Low": st.column_config.NumberColumn(format="%.2f"),
                "Volume": st.column_config.NumberColumn(format="%d"),
            }
        )

render_index_group("Equity Indices", EQUITY_INDICES, "📊")
render_index_group("Islamic Indices", ISLAMIC_INDICES, "☪️")
render_index_group("Sectoral Indices", SECTORAL_INDICES, "🏭")

st.divider()

# =========================================
# Heatmap — all indices change%
# =========================================
st.markdown("#### 🗺️ Index Heatmap")

heatmap_html = '<div style="display:flex; flex-wrap:wrap; gap:8px; margin-bottom:20px;">'
# Sort by change% descending
sorted_indices = sorted(indices, key=lambda x: x.get("changePercent", 0), reverse=True)
for idx in sorted_indices:
    sym = idx["symbol"]
    name = INDEX_NAMES.get(sym, sym)
    pct = idx["changePercent"] * 100
    val = idx["value"]
    
    # Color intensity based on magnitude
    if pct > 0:
        intensity = min(pct / 3.0, 1.0)  # 3% = full green
        bg = f"rgba(34,197,94,{0.15 + intensity * 0.6})"
        text_color = "#22c55e"
    elif pct < 0:
        intensity = min(abs(pct) / 3.0, 1.0)
        bg = f"rgba(239,68,68,{0.15 + intensity * 0.6})"
        text_color = "#ef4444"
    else:
        bg = "rgba(107,114,128,0.2)"
        text_color = "#9ca3af"
    
    sign = "+" if pct >= 0 else ""
    heatmap_html += f'''
    <div style="background:{bg}; padding:12px 16px; border-radius:8px; min-width:140px; flex:1;">
        <div style="color:#e2e8f0; font-size:12px; font-weight:600;">{name}</div>
        <div style="color:white; font-size:18px; font-weight:bold;">{val:,.0f}</div>
        <div style="color:{text_color}; font-size:14px;">{sign}{pct:.2f}%</div>
    </div>'''
heatmap_html += '</div>'
st.markdown(heatmap_html, unsafe_allow_html=True)

# =========================================
# Intraday sparklines — all indices side by side
# =========================================
all_sparklines = data.get("index_sparklines", {})
if any(len(v) >= 2 for v in all_sparklines.values()):
    st.markdown("#### 📈 Intraday Movement")
    
    # Show 4 indices per row
    display_order = EQUITY_INDICES + ISLAMIC_INDICES + SECTORAL_INDICES
    for row_start in range(0, len(display_order), 4):
        row_symbols = display_order[row_start:row_start + 4]
        cols = st.columns(len(row_symbols))
        for i, sym in enumerate(row_symbols):
            spark = all_sparklines.get(sym, [])
            if len(spark) >= 2:
                with cols[i]:
                    idx = idx_map.get(sym, {})
                    pct = idx.get("changePercent", 0) * 100
                    sign = "+" if pct >= 0 else ""
                    st.caption(f"**{INDEX_NAMES.get(sym, sym)}** {sign}{pct:.2f}%")
                    spark_df = pd.DataFrame(spark)
                    spark_df["time"] = pd.to_datetime(spark_df["ts"], unit="s")
                    st.line_chart(spark_df.set_index("time")["value"], height=120)
```

That's the entire page. Read JSON → render indices. Nothing else.

---

## Debug & Test

```bash
# Step 1: Debug — verify IDX messages come through WebSocket
# (if tick_collector already has --debug, just run it and look for IDX messages)
python services/tick_collector.py --debug

# Step 2: Run collector (now subscribes to IDX too)
python services/tick_collector.py

# Step 3: Check indices in snapshot
cat data/live_snapshot.json | python -m json.tool | python -c "
import json, sys
data = json.load(sys.stdin)
print(f'Index count: {data.get(\"index_count\", 0)}')
for idx in data.get('indices', []):
    print(f'  {idx[\"symbol\"]:12s}  {idx[\"value\"]:>12,.2f}  {idx[\"changePercent\"]*100:>+7.2f}%')
"

# Step 4: After market close — check index tables in DB
sqlite3 data/tick_bars.db "SELECT COUNT(*) as bars FROM index_ohlcv_5s"
sqlite3 data/tick_bars.db "SELECT COUNT(*) as ticks FROM index_raw_ticks"
sqlite3 data/tick_bars.db "SELECT symbol, COUNT(*) as bars FROM index_ohlcv_5s GROUP BY symbol"

# Step 5: Streamlit — Live Indices page appears in sidebar
streamlit run ui/app.py
```

---

## Important: WebSocket message format for IDX

IDX ticks might have different field names than stock ticks. The `--debug` output will clarify. Likely differences:

| Field | Stock tick | Index tick (possible) |
|-------|-----------|----------------------|
| Price | `price` | `price` or `value` or `indexValue` |
| Volume | `volume` | `volume` (constituent) or `0` |
| Bid/Ask | Present | Absent (indices have no bid/ask) |
| Turnover | May not exist | `turnover` or `value` (PKR traded) |

**Handle gracefully:** Use `.get()` with defaults for all optional fields. If IDX ticks don't have bid/ask, that's fine — the index handler doesn't use them.

---

## Files summary

| Action | File | What changes |
|--------|------|-------------|
| MODIFY | `services/tick_collector.py` | Add "IDX" to MARKETS, add `self.indices` + `self.index_history` + `self.index_sparklines` + `self.index_ticks`, route IDX ticks to index handler, extend snapshot JSON with `indices` + `index_sparklines` sections, extend `eod_flush()` with `index_ohlcv_5s` + `index_raw_ticks` tables, clear index memory after flush |
| CREATE | `ui/pages/live_indices.py` | New Streamlit page — KSE-100 hero card, secondary metrics (KSE-30, KMI-30, All Share), three category tables (Equity, Islamic, Sectoral), heatmap, intraday sparkline charts |

**Nothing else. The live_market.py page continues showing stock symbols only. The indices page is completely separate.**
