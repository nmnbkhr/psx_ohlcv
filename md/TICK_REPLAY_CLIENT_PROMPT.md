# Claude Code Prompt: Tick Replay — Client-Side HTML Component

## Problem

The current tick replay uses a Python `while` loop with `st.rerun()` or `st.empty()` 
containers. This is jerky because Streamlit re-renders the entire page on every update.

## Solution

Replace with a **client-side HTML component** using `st.components.v1.html()`.
All animation happens in browser JavaScript at 60fps. Streamlit only sends data once.

## Step 1: Read current tick_replay.py

```bash
cat ~/pakfindata/src/pakfindata/ui/page_views/tick_replay.py
```

**Understand the current data loading logic — we keep that. Only replace the rendering.**

## Step 2: Architecture

```
Streamlit (Python):
  1. Date picker + Symbol picker (Streamlit widgets)
  2. Load tick JSONL data for selected date/symbol
  3. Convert to JSON string
  4. Pass into st.components.v1.html() — ONE TIME

Browser (JavaScript):
  1. Receives tick data as JSON array
  2. Renders TradingView Lightweight Chart
  3. Play/Pause/Speed controls in pure JS
  4. Stats panel updates in JS as ticks advance
  5. Timeline scrubber (range input) for seeking
  6. ALL at 60fps, ZERO server round-trips
```

## Step 3: Rebuild tick_replay.py

Keep the existing data loading functions. Replace ONLY the rendering section.

```python
import streamlit as st
import streamlit.components.v1 as components
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ... keep existing imports and data loading functions ...

def render_page():
    st.title("🔄 Tick Replay")
    
    # ── Controls (Streamlit widgets — these are fine) ──
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Date picker — scan available JSONL files
        jsonl_dir = Path("/mnt/e/psxdata/tick_logs_cloud")
        available_dates = sorted([f.stem for f in jsonl_dir.glob("*.jsonl")], reverse=True)
        if not available_dates:
            st.warning("No tick data found in tick_logs_cloud/")
            return
        date_str = st.selectbox("Date", available_dates)
    
    with col2:
        # Symbol picker — scan the selected JSONL for available symbols
        symbols = get_symbols_for_date(date_str)  # existing function
        symbol = st.selectbox("Symbol", symbols if symbols else ["HUBC"])
    
    if st.button("Load Replay", type="primary"):
        with st.spinner(f"Loading {symbol} ticks for {date_str}..."):
            ticks = load_ticks_for_replay(date_str, symbol)
        
        if not ticks:
            st.error("No ticks found")
            return
        
        st.success(f"Loaded {len(ticks):,} ticks")
        
        # ── Render client-side replay component ──
        ticks_json = json.dumps(ticks)
        
        html = build_replay_html(ticks_json, symbol, date_str)
        components.html(html, height=750, scrolling=False)


def load_ticks_for_replay(date_str: str, symbol: str) -> list:
    """Load ticks from JSONL, return list of dicts for JS."""
    # Use DuckDB for speed if available, else line-by-line
    try:
        import duckdb
        path = Path(f"/mnt/e/psxdata/tick_logs_cloud/{date_str}.jsonl")
        if not path.exists():
            path = Path(f"/mnt/e/psxdata/tick_logs/{date_str}.jsonl")
        if not path.exists():
            return []
        
        con = duckdb.connect()
        df = con.execute(f"""
            SELECT timestamp, price, volume, bid, ask,
                   "bidVol" as bidVol, "askVol" as askVol,
                   open, high, low, change, trades
            FROM read_json_auto('{path}',
                 format='newline_delimited',
                 maximum_object_size=10485760)
            WHERE symbol = '{symbol}'
            ORDER BY timestamp
        """).df()
        con.close()
        
        return df.to_dict('records')
    except Exception as e:
        st.warning(f"DuckDB failed, using fallback: {e}")
        # Fallback to line-by-line
        return _load_ticks_fallback(date_str, symbol)


def _load_ticks_fallback(date_str: str, symbol: str) -> list:
    """Fallback tick loader without DuckDB."""
    import json as _json
    path = Path(f"/mnt/e/psxdata/tick_logs_cloud/{date_str}.jsonl")
    if not path.exists():
        path = Path(f"/mnt/e/psxdata/tick_logs/{date_str}.jsonl")
    if not path.exists():
        return []
    
    ticks = []
    with open(path) as f:
        for line in f:
            try:
                rec = _json.loads(line.strip())
                if rec.get("symbol") != symbol:
                    continue
                ticks.append({
                    "timestamp": rec.get("timestamp", 0),
                    "price": rec.get("price", 0),
                    "volume": rec.get("volume", 0),
                    "bid": rec.get("bid", 0),
                    "ask": rec.get("ask", 0),
                    "bidVol": rec.get("bidVol", 0),
                    "askVol": rec.get("askVol", 0),
                    "open": rec.get("open", 0),
                    "high": rec.get("high", 0),
                    "low": rec.get("low", 0),
                    "change": rec.get("change", 0),
                    "trades": rec.get("trades", 0),
                })
            except:
                continue
    return ticks


def get_symbols_for_date(date_str: str) -> list:
    """Get available symbols for a date from JSONL."""
    try:
        import duckdb
        path = Path(f"/mnt/e/psxdata/tick_logs_cloud/{date_str}.jsonl")
        if not path.exists():
            return []
        con = duckdb.connect()
        df = con.execute(f"""
            SELECT DISTINCT symbol 
            FROM read_json_auto('{path}',
                 format='newline_delimited',
                 maximum_object_size=10485760)
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """).df()
        con.close()
        return df["symbol"].tolist()
    except:
        return ["HUBC", "OGDC", "PPL", "HBL", "UBL", "MCB", "LUCK", "ENGRO"]


def build_replay_html(ticks_json: str, symbol: str, date_str: str) -> str:
    """Build the complete HTML/JS replay component."""
    
    return f"""
<!DOCTYPE html>
<html>
<head>
<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ 
    background: #0B0E11; 
    color: #E0E0E0; 
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    overflow: hidden;
  }}
  
  /* Controls bar */
  .controls {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 16px;
    background: #141821;
    border-bottom: 1px solid #1E2530;
  }}
  .controls button {{
    background: #1E2530;
    color: #C8A96E;
    border: 1px solid #2A3445;
    padding: 6px 14px;
    border-radius: 4px;
    cursor: pointer;
    font-family: inherit;
    font-size: 13px;
  }}
  .controls button:hover {{ background: #2A3445; }}
  .controls button.active {{ background: #C8A96E; color: #0B0E11; }}
  .speed-btn.active {{ background: #C8A96E !important; color: #0B0E11 !important; }}
  
  .controls label {{
    color: #888;
    font-size: 12px;
  }}
  
  /* Stats panel */
  .stats {{
    display: flex;
    gap: 24px;
    padding: 8px 16px;
    background: #0F1318;
    border-bottom: 1px solid #1E2530;
    font-size: 13px;
  }}
  .stat {{ display: flex; flex-direction: column; }}
  .stat-label {{ color: #666; font-size: 10px; text-transform: uppercase; }}
  .stat-value {{ color: #E0E0E0; font-size: 14px; font-weight: bold; }}
  .stat-value.up {{ color: #22c55e; }}
  .stat-value.down {{ color: #ef4444; }}
  .stat-value.gold {{ color: #C8A96E; }}
  
  /* Chart container */
  #chart {{ width: 100%; height: 420px; }}
  
  /* Timeline scrubber */
  .timeline {{
    padding: 8px 16px;
    background: #0F1318;
    border-top: 1px solid #1E2530;
  }}
  .timeline input[type="range"] {{
    width: 100%;
    height: 6px;
    -webkit-appearance: none;
    background: #1E2530;
    border-radius: 3px;
    outline: none;
  }}
  .timeline input[type="range"]::-webkit-slider-thumb {{
    -webkit-appearance: none;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: #C8A96E;
    cursor: pointer;
  }}
  .time-labels {{
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: #555;
    margin-top: 4px;
  }}
  
  /* Order book */
  .orderbook {{
    display: flex;
    gap: 24px;
    padding: 6px 16px;
    background: #0F1318;
    font-size: 12px;
  }}
  .ob-bid {{ color: #22c55e; }}
  .ob-ask {{ color: #ef4444; }}
  .ob-spread {{ color: #C8A96E; }}
  
  /* Tick log */
  .ticklog {{
    padding: 4px 16px;
    background: #0B0E11;
    font-size: 11px;
    max-height: 80px;
    overflow-y: auto;
  }}
  .ticklog-row {{
    display: flex;
    gap: 16px;
    padding: 1px 0;
    border-bottom: 1px solid #0F1318;
  }}
  .ticklog-row.buy {{ color: #22c55e; }}
  .ticklog-row.sell {{ color: #ef4444; }}
</style>
</head>
<body>

<!-- Controls -->
<div class="controls">
  <button id="playBtn" onclick="togglePlay()">▶ Play</button>
  <button onclick="stepBack()">◀ -1m</button>
  <button onclick="stepForward()">+1m ▶</button>
  <span style="color:#444">|</span>
  <label>Speed:</label>
  <button class="speed-btn" data-speed="1" onclick="setSpeed(1)">1x</button>
  <button class="speed-btn" data-speed="5" onclick="setSpeed(5)">5x</button>
  <button class="speed-btn active" data-speed="25" onclick="setSpeed(25)">25x</button>
  <button class="speed-btn" data-speed="100" onclick="setSpeed(100)">100x</button>
  <button class="speed-btn" data-speed="500" onclick="setSpeed(500)">500x</button>
  <span style="color:#444">|</span>
  <span id="tickCounter" style="color:#666;font-size:12px">0 / 0 ticks</span>
  <span style="flex:1"></span>
  <span style="color:#C8A96E;font-size:14px;font-weight:bold">{symbol}</span>
  <span style="color:#555;font-size:12px">{date_str}</span>
</div>

<!-- Stats -->
<div class="stats">
  <div class="stat"><span class="stat-label">Price</span><span id="s_price" class="stat-value gold">—</span></div>
  <div class="stat"><span class="stat-label">Change</span><span id="s_change" class="stat-value">—</span></div>
  <div class="stat"><span class="stat-label">High</span><span id="s_high" class="stat-value">—</span></div>
  <div class="stat"><span class="stat-label">Low</span><span id="s_low" class="stat-value">—</span></div>
  <div class="stat"><span class="stat-label">Volume</span><span id="s_vol" class="stat-value">—</span></div>
  <div class="stat"><span class="stat-label">Trades</span><span id="s_trades" class="stat-value">—</span></div>
  <div class="stat"><span class="stat-label">VWAP</span><span id="s_vwap" class="stat-value gold">—</span></div>
  <div class="stat"><span class="stat-label">Time</span><span id="s_time" class="stat-value">—</span></div>
</div>

<!-- Chart -->
<div id="chart"></div>

<!-- Order Book -->
<div class="orderbook">
  <span>Bid: <span id="ob_bid" class="ob-bid">—</span> (<span id="ob_bidvol">—</span>)</span>
  <span>Ask: <span id="ob_ask" class="ob-ask">—</span> (<span id="ob_askvol">—</span>)</span>
  <span>Spread: <span id="ob_spread" class="ob-spread">—</span></span>
  <span style="flex:1"></span>
  <span>Imbalance: <span id="ob_imbalance">—</span></span>
</div>

<!-- Timeline Scrubber -->
<div class="timeline">
  <input type="range" id="scrubber" min="0" max="100" value="0" oninput="seek(this.value)">
  <div class="time-labels">
    <span id="tl_start">09:15</span>
    <span id="tl_current">—</span>
    <span id="tl_end">15:30</span>
  </div>
</div>

<!-- Last 5 trades -->
<div id="ticklog" class="ticklog"></div>

<script>
// ═══════════════════════════════════════
// DATA
// ═══════════════════════════════════════
const allTicks = {ticks_json};
const totalTicks = allTicks.length;

// ═══════════════════════════════════════
// STATE
// ═══════════════════════════════════════
let currentIdx = 0;
let playing = false;
let speed = 25;
let animFrameId = null;
let lastFrameTime = 0;

// VWAP tracking
let cumPV = 0;   // cumulative price * volume
let cumVol = 0;  // cumulative volume
let prevVol = 0; // previous cumulative volume (for per-tick vol)

// Recent trades for tick log
let recentTrades = [];

// ═══════════════════════════════════════
// CHART SETUP
// ═══════════════════════════════════════
const chartEl = document.getElementById('chart');
const chart = LightweightCharts.createChart(chartEl, {{
  width: chartEl.clientWidth,
  height: 420,
  layout: {{
    background: {{ type: 'solid', color: '#0B0E11' }},
    textColor: '#888',
  }},
  grid: {{
    vertLines: {{ color: '#1E2530' }},
    horzLines: {{ color: '#1E2530' }},
  }},
  crosshair: {{
    mode: LightweightCharts.CrosshairMode.Normal,
  }},
  timeScale: {{
    timeVisible: true,
    secondsVisible: true,
    borderColor: '#1E2530',
  }},
  rightPriceScale: {{
    borderColor: '#1E2530',
  }},
}});

const lineSeries = chart.addLineSeries({{
  color: '#C8A96E',
  lineWidth: 2,
  priceLineVisible: true,
  lastValueVisible: true,
}});

// Volume series
const volumeSeries = chart.addHistogramSeries({{
  priceFormat: {{ type: 'volume' }},
  priceScaleId: 'vol',
  color: '#2A3445',
}});

chart.priceScale('vol').applyOptions({{
  scaleMargins: {{ top: 0.85, bottom: 0 }},
}});

// Resize handler
window.addEventListener('resize', () => {{
  chart.applyOptions({{ width: chartEl.clientWidth }});
}});

// ═══════════════════════════════════════
// REPLAY ENGINE
// ═══════════════════════════════════════
function advanceTo(targetIdx) {{
  if (targetIdx > totalTicks) targetIdx = totalTicks;
  
  const priceBatch = [];
  const volBatch = [];
  
  while (currentIdx < targetIdx) {{
    const tick = allTicks[currentIdx];
    const ts = Math.floor(tick.timestamp);
    
    priceBatch.push({{ time: ts, value: tick.price }});
    
    // Per-tick volume (diff cumulative)
    const tickVol = tick.volume - prevVol;
    prevVol = tick.volume;
    if (tickVol > 0) {{
      volBatch.push({{ 
        time: ts, 
        value: tickVol,
        color: tick.price >= (allTicks[Math.max(0, currentIdx-1)].price || 0) ? '#22c55e55' : '#ef444455'
      }});
      
      // VWAP
      cumPV += tick.price * tickVol;
      cumVol += tickVol;
    }}
    
    currentIdx++;
  }}
  
  // Batch update chart (much faster than per-tick)
  if (priceBatch.length > 0) {{
    // For line series, we need to update one by one (API limitation)
    // But we only update the last N for performance
    const lastTicks = priceBatch.slice(-50);
    for (const p of lastTicks) {{
      lineSeries.update(p);
    }}
    
    const lastVols = volBatch.slice(-50);
    for (const v of lastVols) {{
      volumeSeries.update(v);
    }}
  }}
  
  // Update UI with latest tick
  if (currentIdx > 0) {{
    updateStats(allTicks[currentIdx - 1]);
  }}
}}

function updateStats(tick) {{
  const price = tick.price;
  const change = tick.change || 0;
  const changePct = ((change / (price - change)) * 100) || 0;
  const isUp = change >= 0;
  
  document.getElementById('s_price').textContent = price.toFixed(2);
  
  const changeEl = document.getElementById('s_change');
  changeEl.textContent = (isUp ? '+' : '') + change.toFixed(2) + ' (' + changePct.toFixed(2) + '%)';
  changeEl.className = 'stat-value ' + (isUp ? 'up' : 'down');
  
  document.getElementById('s_high').textContent = tick.high ? tick.high.toFixed(2) : '—';
  document.getElementById('s_low').textContent = tick.low ? tick.low.toFixed(2) : '—';
  document.getElementById('s_vol').textContent = tick.volume ? (tick.volume / 1e6).toFixed(2) + 'M' : '—';
  document.getElementById('s_trades').textContent = tick.trades || '—';
  
  // VWAP
  const vwap = cumVol > 0 ? (cumPV / cumVol) : 0;
  document.getElementById('s_vwap').textContent = vwap > 0 ? vwap.toFixed(2) : '—';
  
  // Time
  const dt = new Date(tick.timestamp * 1000);
  const timeStr = dt.toLocaleTimeString('en-GB', {{ hour12: false, timeZone: 'Asia/Karachi' }});
  document.getElementById('s_time').textContent = timeStr;
  document.getElementById('tl_current').textContent = timeStr;
  
  // Order book
  document.getElementById('ob_bid').textContent = tick.bid ? tick.bid.toFixed(2) : '—';
  document.getElementById('ob_ask').textContent = tick.ask ? tick.ask.toFixed(2) : '—';
  document.getElementById('ob_bidvol').textContent = tick.bidVol ? tick.bidVol.toLocaleString() : '—';
  document.getElementById('ob_askvol').textContent = tick.askVol ? tick.askVol.toLocaleString() : '—';
  
  if (tick.bid > 0 && tick.ask > 0) {{
    document.getElementById('ob_spread').textContent = (tick.ask - tick.bid).toFixed(2);
    const imb = ((tick.bidVol - tick.askVol) / (tick.bidVol + tick.askVol) * 100);
    const imbEl = document.getElementById('ob_imbalance');
    imbEl.textContent = (imb > 0 ? '+' : '') + imb.toFixed(0) + '%';
    imbEl.style.color = imb > 0 ? '#22c55e' : '#ef4444';
  }}
  
  // Tick counter
  document.getElementById('tickCounter').textContent = currentIdx.toLocaleString() + ' / ' + totalTicks.toLocaleString() + ' ticks';
  
  // Scrubber
  document.getElementById('scrubber').value = (currentIdx / totalTicks * 100);
  
  // Tick log (last 5 trades)
  const side = currentIdx > 1 && tick.price >= allTicks[currentIdx - 2].price ? 'buy' : 'sell';
  recentTrades.unshift({{
    time: timeStr,
    price: price.toFixed(2),
    vol: tick.volume > prevVol ? (tick.volume - prevVol).toLocaleString() : '—',
    bid: tick.bid ? tick.bid.toFixed(2) : '—',
    ask: tick.ask ? tick.ask.toFixed(2) : '—',
    side: side
  }});
  if (recentTrades.length > 5) recentTrades.pop();
  
  const logHtml = recentTrades.map(t => 
    `<div class="ticklog-row ${{t.side}}">
      <span style="width:70px">${{t.time}}</span>
      <span style="width:70px">${{t.price}}</span>
      <span style="width:80px">${{t.vol}}</span>
      <span style="width:60px">${{t.bid}}</span>
      <span style="width:60px">${{t.ask}}</span>
      <span style="width:50px">${{t.side.toUpperCase()}}</span>
    </div>`
  ).join('');
  document.getElementById('ticklog').innerHTML = logHtml;
}}

// ═══════════════════════════════════════
// ANIMATION LOOP
// ═══════════════════════════════════════
function animationLoop(timestamp) {{
  if (!playing) return;
  
  // Advance by 'speed' ticks per frame (~60fps)
  const targetIdx = Math.min(currentIdx + speed, totalTicks);
  advanceTo(targetIdx);
  
  if (currentIdx >= totalTicks) {{
    togglePlay(); // Stop at end
    return;
  }}
  
  animFrameId = requestAnimationFrame(animationLoop);
}}

// ═══════════════════════════════════════
// CONTROLS
// ═══════════════════════════════════════
function togglePlay() {{
  playing = !playing;
  const btn = document.getElementById('playBtn');
  
  if (playing) {{
    btn.textContent = '⏸ Pause';
    btn.classList.add('active');
    if (currentIdx >= totalTicks) {{
      // Reset if at end
      resetReplay();
    }}
    animFrameId = requestAnimationFrame(animationLoop);
  }} else {{
    btn.textContent = '▶ Play';
    btn.classList.remove('active');
    if (animFrameId) cancelAnimationFrame(animFrameId);
  }}
}}

function setSpeed(s) {{
  speed = s;
  document.querySelectorAll('.speed-btn').forEach(b => {{
    b.classList.toggle('active', parseInt(b.dataset.speed) === s);
  }});
}}

function seek(pct) {{
  const wasPlaying = playing;
  if (playing) togglePlay();
  
  const targetIdx = Math.floor(pct / 100 * totalTicks);
  
  // Reset and replay to target position
  resetReplay();
  
  // Build chart data up to target in one batch
  const priceData = [];
  const volData = [];
  let pv = 0;
  
  for (let i = 0; i < targetIdx; i++) {{
    const tick = allTicks[i];
    priceData.push({{ time: Math.floor(tick.timestamp), value: tick.price }});
    
    const tv = i > 0 ? tick.volume - allTicks[i-1].volume : 0;
    if (tv > 0) {{
      volData.push({{ 
        time: Math.floor(tick.timestamp), 
        value: tv,
        color: tick.price >= (i > 0 ? allTicks[i-1].price : tick.price) ? '#22c55e55' : '#ef444455'
      }});
      cumPV += tick.price * tv;
      cumVol += tv;
    }}
    prevVol = tick.volume;
  }}
  
  lineSeries.setData(priceData);
  volumeSeries.setData(volData);
  currentIdx = targetIdx;
  
  if (targetIdx > 0) updateStats(allTicks[targetIdx - 1]);
  
  if (wasPlaying) togglePlay();
}}

function stepForward() {{
  // Advance 1 minute worth of ticks
  const curTs = currentIdx > 0 ? allTicks[currentIdx - 1].timestamp : allTicks[0].timestamp;
  const targetTs = curTs + 60;
  let target = currentIdx;
  while (target < totalTicks && allTicks[target].timestamp < targetTs) target++;
  advanceTo(target);
}}

function stepBack() {{
  // Go back 1 minute
  if (currentIdx <= 0) return;
  const curTs = allTicks[currentIdx - 1].timestamp;
  const targetTs = curTs - 60;
  const targetPct = 0;
  
  // Find target index
  let target = currentIdx - 1;
  while (target > 0 && allTicks[target].timestamp > targetTs) target--;
  
  seek(target / totalTicks * 100);
}}

function resetReplay() {{
  currentIdx = 0;
  cumPV = 0;
  cumVol = 0;
  prevVol = 0;
  recentTrades = [];
  lineSeries.setData([]);
  volumeSeries.setData([]);
}}

// ═══════════════════════════════════════
// INIT
// ═══════════════════════════════════════
if (totalTicks > 0) {{
  const firstTs = allTicks[0].timestamp;
  const lastTs = allTicks[totalTicks - 1].timestamp;
  const fmt = (ts) => new Date(ts * 1000).toLocaleTimeString('en-GB', {{ hour12: false, timeZone: 'Asia/Karachi' }});
  document.getElementById('tl_start').textContent = fmt(firstTs);
  document.getElementById('tl_end').textContent = fmt(lastTs);
  document.getElementById('scrubber').max = 100;
  document.getElementById('tickCounter').textContent = '0 / ' + totalTicks.toLocaleString() + ' ticks — Ready';
  
  // Show first tick as starting state
  updateStats(allTicks[0]);
}}

</script>
</body>
</html>
"""
```

## Step 4: Key features of this component

1. **60fps animation** — uses `requestAnimationFrame`, not `setTimeout`
2. **Batch chart updates** — advances `speed` ticks per frame, not one-by-one
3. **Speed control** — 1x, 5x, 25x, 100x, 500x (ticks per frame)
4. **Seek/scrub** — drag timeline slider to any point, chart rebuilds instantly
5. **Step ±1 minute** — buttons to jump forward/backward
6. **Live stats** — price, change, high, low, volume, trades, VWAP
7. **Order book** — bid, ask, spread, imbalance from tick data
8. **Tick log** — last 5 trades with buy/sell coloring
9. **TradingView chart** — professional look with volume histogram
10. **Theme** — matches pakfindata (#0B0E11, #C8A96E gold, JetBrains Mono)
11. **ZERO Streamlit rerenders** — all runs in browser

## Step 5: Handle large tick data

JSONL files are 200MB+. Symbol-filtered data might be 5-15K ticks.
If ticks > 50,000, downsample for the chart (show every Nth tick):

```python
MAX_TICKS_FOR_REPLAY = 50000

if len(ticks) > MAX_TICKS_FOR_REPLAY:
    # Downsample: keep every Nth tick
    step = len(ticks) // MAX_TICKS_FOR_REPLAY
    ticks = ticks[::step]
    st.info(f"Downsampled to {len(ticks):,} ticks for performance")
```

## Step 6: CDN requirement

The component uses TradingView Lightweight Charts from CDN:
```
https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js
```

This requires internet access from the user's browser (not server).
If offline, fallback to the static slider approach.

## IMPORTANT

1. **DO NOT use st.rerun(), time.sleep(), or while loops for animation**
2. **DO NOT use st.empty() containers that update in Python**
3. **ALL animation happens in JavaScript via requestAnimationFrame**
4. **Streamlit sends data ONCE via components.html()**
5. **Keep existing date/symbol picker as Streamlit widgets**
6. **Keep existing JSONL/DuckDB loading functions**
7. **Only replace the rendering/animation with client-side HTML**
