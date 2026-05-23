# Claude Code Prompt: Phase 3 — Deep Integration

## Context

pakfindata (~/pakfindata/) Streamlit app. Bloomberg Terminal dark theme.
DB: `/mnt/e/psxdata/psx.sqlite`.

- Phase 1 done: circuit limits, index weights, VAR margins in existing pages
- Phase 2 done: Microstructure page, Derivatives page

Phase 3 adds deep features that connect multiple data sources together.

**Rules:**
- Additive only
- Read existing code FIRST
- Raw numpy/pandas only
- Cache aggressively

## Step 1: Audit current state

```bash
# All pages
ls ~/pakfindata/src/pakfindata/ui/page_views/

# All engine modules
ls ~/pakfindata/src/pakfindata/engine/

# All source modules
ls ~/pakfindata/src/pakfindata/sources/

# Check tick data availability
ls -lh /mnt/e/psxdata/tick_logs_cloud/ | wc -l
du -sh /mnt/e/psxdata/tick_logs_cloud/

# Check off-market data
ls /mnt/e/psxdata/downloads/daily/*/off_market/ 2>/dev/null | head -10

# Check what Phase 2 built
cat ~/pakfindata/src/pakfindata/ui/page_views/microstructure.py | head -30
cat ~/pakfindata/src/pakfindata/ui/page_views/derivatives.py | head -30

# Existing signal scanner
grep -rn "signal_score\|composite\|batch_scan" ~/pakfindata/src/pakfindata/engine/ | head -10
```

**STOP — read ALL output before proceeding.**

---

## Feature 1: Tick Replay Engine

### Location
`src/pakfindata/ui/page_views/tick_replay.py`

### What
Play back any historical day's tick data like a video — watch the market unfold 
in real-time or at accelerated speed. Like a TradingView replay but with 
PSX tick-level data nobody else has.

### Data Source
Cloud tick JSONL: `/mnt/e/psxdata/tick_logs_cloud/{date}.jsonl`

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  TICK REPLAY                    [Date: 2026-03-18]          │
│  Symbol: [HUBC ▼]              [▶ Play] [⏸ Pause] [⏹ Stop] │
│  Speed: [1x] [5x] [10x] [50x]  Time: 09:15:00 → 15:30:00  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Price Chart (builds tick by tick)                   │   │
│  │                                                      │   │
│  │  ──────────╲                                        │   │
│  │             ╲──────╱╲                               │   │
│  │                     ╲──────                         │   │
│  │  ▎ cursor at current replay time                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Volume Bars (builds as replay progresses)           │   │
│  │  ▓▓▓ ▓▓▓▓▓ ▓▓ ▓▓▓▓ ▓▓▓▓▓▓▓                        │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────┐  ┌────────────────────────────┐   │
│  │  Live Stats          │  │  Order Book Snapshot       │   │
│  │  Price:  188.38      │  │  Bid: 188.35 (5,000)      │   │
│  │  Change: -6.72       │  │  Ask: 188.40 (3,200)      │   │
│  │  Volume: 5,978,764   │  │  Spread: 0.05             │   │
│  │  Trades: 2,341       │  │  Imbalance: +23%          │   │
│  │  VWAP:   190.23      │  │                            │   │
│  │  High:   197.90      │  │  Last 5 trades:            │   │
│  │  Low:    187.10      │  │  188.38 x 500  SELL        │   │
│  │                       │  │  188.38 x 50   SELL        │   │
│  │  Elapsed: 4h 34m     │  │  188.40 x 250  BUY         │   │
│  │  Ticks:  2,341/4,185 │  │  188.35 x 100  SELL        │   │
│  └─────────────────────┘  │  188.40 x 1000 BUY         │   │
│                            └────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Timeline Scrubber                                   │   │
│  │  ◀ ═══════════════●══════════════════════════════ ▶  │   │
│  │  09:15        11:00    ▲13:49      15:00    15:30   │   │
│  │                     current                          │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Implementation

**Replay Engine (backend):**

```python
class TickReplayEngine:
    """Replays tick data from JSONL files."""
    
    def __init__(self, date_str: str, symbol: str):
        self.ticks = self._load_ticks(date_str, symbol)
        self.current_idx = 0
        self.state = {
            "price": 0, "open": 0, "high": 0, "low": float("inf"),
            "volume": 0, "trades": 0, "vwap_num": 0, "vwap_den": 0,
            "bid": 0, "ask": 0, "bidVol": 0, "askVol": 0,
            "prices": [],  # for chart
            "volumes": [],  # for chart
        }
    
    def advance_to(self, target_time: float) -> dict:
        """Advance replay to target timestamp. Return current state."""
        while self.current_idx < len(self.ticks):
            tick = self.ticks[self.current_idx]
            if tick["timestamp"] > target_time:
                break
            self._process_tick(tick)
            self.current_idx += 1
        return self.state
    
    def _process_tick(self, tick: dict):
        """Update state with new tick."""
        price = tick.get("price", 0)
        vol = tick.get("volume", 0)
        
        if self.state["open"] == 0:
            self.state["open"] = price
        self.state["price"] = price
        self.state["high"] = max(self.state["high"], price)
        self.state["low"] = min(self.state["low"], price)
        self.state["volume"] = vol  # cumulative from exchange
        self.state["trades"] = tick.get("trades", self.state["trades"])
        self.state["bid"] = tick.get("bid", 0)
        self.state["ask"] = tick.get("ask", 0)
        self.state["bidVol"] = tick.get("bidVol", 0)
        self.state["askVol"] = tick.get("askVol", 0)
        
        # VWAP
        # Note: need per-tick volume, not cumulative
        # Diff from previous cumulative volume
        
        # Append to chart data
        self.state["prices"].append({"time": tick["timestamp"], "price": price})
        self.state["volumes"].append({"time": tick["timestamp"], "vol": vol})
```

**Streamlit Replay UI:**

Use `st.empty()` containers that update in a loop with `time.sleep()` 
for the playback effect. The speed multiplier controls sleep duration:

```python
# Replay loop
speed = st.select_slider("Speed", [1, 5, 10, 50, 100])
play_btn = st.button("▶ Play")

if play_btn:
    chart_placeholder = st.empty()
    stats_placeholder = st.empty()
    
    engine = TickReplayEngine(date_str, symbol)
    ticks = engine.ticks
    
    start_ts = ticks[0]["timestamp"]
    end_ts = ticks[-1]["timestamp"]
    
    # Step through time
    current_ts = start_ts
    step = 1.0  # 1 second of market time per step
    
    while current_ts <= end_ts:
        state = engine.advance_to(current_ts)
        
        # Update chart
        with chart_placeholder.container():
            # Render price chart up to current_ts
            ...
        
        # Update stats
        with stats_placeholder.container():
            # Render stats panel
            ...
        
        current_ts += step * speed
        time.sleep(0.05)  # 50ms real time per step
```

**Timeline Scrubber:**

Use `st.slider` with min/max as market open/close timestamps.
User can drag to jump to any point in the day.

```python
time_slider = st.slider(
    "Timeline",
    min_value=market_open_ts,
    max_value=market_close_ts,
    value=current_ts,
    format="HH:mm:ss"
)
```

### Key Considerations

1. **Streamlit reruns on every interaction** — the replay loop needs 
   `st.session_state` to maintain position between reruns
2. **Chart updates** — use `st.line_chart` or plotly with `st.plotly_chart` 
   that rebuilds with growing data
3. **Performance** — don't render all ticks, sample to 1-second intervals 
   for the chart, show raw ticks in the table
4. **Available dates** — scan tick_logs_cloud folder for `.jsonl` files, 
   show only available dates in picker

---

## Feature 2: Off-Market Block Trade Alerts

### Location
Add to existing **Signal Dashboard** or **Live Market** page as a new panel.
Also create: `src/pakfindata/engine/block_trades.py`

### Data Source
- `/mnt/e/psxdata/downloads/daily/{date}/off_market/off_market_summary.csv`

### What
Off-market transactions (also called "cross trades" or "block deals") are 
large negotiated trades executed outside the regular order book. They signal 
institutional activity — a large buyer/seller moving significant volume.

### Analytics

**1. Block Trade Scanner**

```python
def load_off_market(date_str: str) -> pd.DataFrame:
    """Load off-market transaction summary."""
    # Parse the CSV from downloads
    # Expected columns (verify from actual file):
    # Symbol, Buyer Broker, Seller Broker, Volume, Rate, Value, Trade Time
    ...

def analyze_blocks(date_str: str) -> pd.DataFrame:
    """Analyze off-market trades for signals."""
    df = load_off_market(date_str)
    if df.empty:
        return df
    
    # Metrics per symbol:
    # - Total block volume vs regular volume (from DPS EOD)
    # - Block volume as % of total volume
    # - Number of block trades
    # - Average block size
    # - Net direction (if buyer/seller info available)
    
    # Load regular volume for comparison
    # block_pct = block_volume / total_volume * 100
    # High block_pct (>20%) = significant institutional interest
    
    return df
```

**2. Block Trade Alerts Panel**

```
┌──────────────────────────────────────────────────┐
│  🏦 OFF-MARKET / BLOCK TRADES — 2026-03-18       │
│                                                    │
│  Symbol  Block Vol  Regular Vol  Block%  # Trades  │
│  HUBC    500,000    5,978,764    8.4%    3         │
│  LUCK    1,200,000  3,456,789    34.7%   5  ⚠️    │
│  ENGRO   300,000    2,100,000    14.3%   2         │
│                                                    │
│  ⚠️ = Block volume > 20% of regular volume        │
│                                                    │
│  Total block value: PKR 2.3 Billion                │
│  Most active broker (buyer): XYZ Securities        │
│  Most active broker (seller): ABC Capital           │
└──────────────────────────────────────────────────┘
```

**3. Historical Block Trade Trends**

```python
def block_trade_history(symbol: str, days: int = 30) -> pd.DataFrame:
    """Load block trade history for a symbol across multiple days."""
    # Iterate over /mnt/e/psxdata/downloads/daily/{date}/off_market/
    # Build time series of block volumes
    # Compare with price action — do block buys precede price increases?
    ...
```

**4. Integration with Signal Scanner**

Add block trade as a signal factor:

```python
def block_trade_score(symbol: str, date_str: str) -> float:
    """
    Score 0-100 based on off-market activity.
    High block activity = institutional interest = higher score.
    
    0-30:   No block trades
    30-60:  Small blocks (<10% of volume)
    60-80:  Significant blocks (10-30%)
    80-100: Very heavy blocks (>30%)
    """
    ...
```

### Important Notes

1. **Off-market CSV format is unknown** — Step 1 must inspect the actual file
2. **Not all days have off-market trades** — handle empty files gracefully
3. **Broker names may be codes** — may need a broker code → name mapping
4. **Block trades happen AFTER market** — the CSV is typically available post-close
5. **Backfill needed** for historical trend analysis

---

## Feature 3: Intraday Volume Profile Engine

### Location
`src/pakfindata/engine/volume_profile.py`
Used by: Microstructure page, Live Market page, Signal Scanner

### What
Build institutional-grade volume profiles from tick data. This powers 
multiple pages — it's an engine module, not a page.

### Analytics

**1. Price-Volume Profile (Session Profile)**

```python
def compute_volume_profile(
    ticks: pd.DataFrame,
    tick_size: float = 0.01,
    value_area_pct: float = 0.70
) -> dict:
    """
    Compute volume profile from tick data.
    
    Returns:
        {
            "levels": [{"price": 188.50, "volume": 50000, "buy_vol": 30000, "sell_vol": 20000}, ...],
            "poc": 188.40,         # Point of Control (highest volume price)
            "vah": 189.20,         # Value Area High
            "val": 187.80,         # Value Area Low
            "total_volume": 5978764,
            "buy_volume": 3200000,
            "sell_volume": 2778764,
        }
    
    Algorithm:
    1. Round each tick price to nearest tick_size
    2. Aggregate volume at each price level
    3. Classify buy/sell using tick rule (price > prev = buy)
    4. POC = price level with maximum volume
    5. Value Area = smallest range containing value_area_pct of total volume
       - Start from POC, expand up/down adding the side with more volume
    """
    # Round prices to tick size
    ticks["price_level"] = (ticks["price"] / tick_size).round() * tick_size
    
    # Aggregate volume per price level
    # Need per-tick volume (diff cumulative)
    # Classify buy/sell
    # Find POC
    # Compute Value Area (expand from POC)
    ...
```

**2. Time-Price-Volume Profile (TPO)**

```python
def compute_tpo_profile(
    ticks: pd.DataFrame,
    period_minutes: int = 30,
    tick_size: float = 0.01
) -> dict:
    """
    Market Profile / TPO (Time Price Opportunity).
    
    Divides the day into periods (e.g., 30-min blocks labeled A-N).
    At each price level, records which periods traded there.
    
    Returns:
        {
            "periods": ["A", "B", "C", ...],
            "profile": {
                188.50: ["A", "B", "C", "D"],      # traded in 4 periods
                188.40: ["A", "B", "C", "D", "E"],  # traded in 5 periods (POC)
                188.30: ["C", "D", "E"],
                ...
            },
            "poc": 188.40,
            "initial_balance_high": 189.50,  # high of first 2 periods
            "initial_balance_low": 187.80,   # low of first 2 periods
        }
    """
    ...
```

**3. VWAP Bands**

```python
def compute_vwap_bands(ticks: pd.DataFrame, num_std: list = [1, 2]) -> dict:
    """
    Compute VWAP and standard deviation bands.
    
    VWAP = Σ(price × volume) / Σ(volume)
    Running VWAP recalculated at each tick.
    
    Bands at ±1σ and ±2σ from VWAP.
    
    Returns:
        {
            "vwap": [{"time": ts, "vwap": 190.23}, ...],
            "upper_1": [...],
            "lower_1": [...],
            "upper_2": [...],
            "lower_2": [...],
            "final_vwap": 190.23,
        }
    """
    # Running VWAP calculation
    # Need per-tick volume (diff cumulative)
    # cumulative_pv = Σ(price_i × vol_i)
    # cumulative_v = Σ(vol_i)
    # vwap_i = cumulative_pv / cumulative_v
    # 
    # For bands: compute running variance of price around VWAP
    # σ = sqrt(Σ(vol_i × (price_i - vwap_i)²) / cumulative_v)
    ...
```

**4. Delta Volume (Cumulative Delta)**

```python
def compute_cumulative_delta(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Cumulative delta = running sum of (buy_volume - sell_volume).
    
    Rising delta + rising price = strong trend
    Falling delta + rising price = divergence (reversal warning)
    
    Trade classification: Lee-Ready algorithm
        if price >= ask → buy
        if price <= bid → sell
        if between → tick rule (compare with previous price)
    """
    ...
```

### Integration Points

These engine functions are used by multiple pages:

| Function | Used in |
|----------|---------|
| `compute_volume_profile()` | Microstructure page (existing), Tick Replay |
| `compute_tpo_profile()` | New section in Microstructure page |
| `compute_vwap_bands()` | Microstructure page, Live Market overlay |
| `compute_cumulative_delta()` | Microstructure page, Signal Scanner factor |

### Signal Scanner Integration

Add volume profile metrics as signal factors:

```python
def volume_profile_score(symbol: str, date_str: str) -> float:
    """
    Score 0-100 based on volume profile characteristics.
    
    Factors:
    - Is current price above/below POC? (above = bullish)
    - Is price inside or outside Value Area?
    - Delta trend (rising delta = bullish)
    - Price relative to VWAP (above = bullish)
    
    Combine into single score.
    """
    ...
```

Add to composite scorer with weight:
```python
weights = {
    # ... existing ...
    "volume_profile": 0.10,  # 10% weight
    "block_trades": 0.05,    # 5% weight (from Feature 2)
}
```

---

## Feature 4: Cross-Data Intelligence Panel

### Location
Add as a panel in the existing **Signal Dashboard** page.

### What
Combine ALL data sources into one actionable view per symbol.
This is where everything comes together.

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  INTELLIGENCE BRIEF — HUBC            [Symbol picker]       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Composite Score: 73/100 ████████████████████░░░░░ BULLISH  │
│                                                             │
│  ┌─ Price Action ────────────────────────────────────────┐  │
│  │ Close: 188.38 (-3.44%)  Vol: 5.9M (1.2x avg)         │  │
│  │ Near circuit: NO  │ VWAP: 190.23  │ Above POC: NO     │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Microstructure ──────────────────────────────────────┐  │
│  │ Spread: 0.05 (tight)  │ VPIN: 0.45 (normal)          │  │
│  │ Order flow: +23% buy  │ Avg trade: 2,548 shs         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Derivatives ─────────────────────────────────────────┐  │
│  │ Futures OI: 1.2M (+5%)  │ Basis: +0.59% (premium)    │  │
│  │ OI signal: Long buildup  │ Rollover: 27%              │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Institutional ───────────────────────────────────────┐  │
│  │ Block trades: 3 (500K shares, 8.4% of volume)         │  │
│  │ VAR margin: 18.5% (stable, 0% change)                 │  │
│  │ Index weight: 4.2% of KSE100                          │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─ Score Breakdown ─────────────────────────────────────┐  │
│  │ Momentum:  72  ████████████████░░░░                    │  │
│  │ Volume:    68  ██████████████░░░░░░                    │  │
│  │ VPIN:      55  ████████████░░░░░░░░                    │  │
│  │ VProfile:  80  ██████████████████░░                    │  │
│  │ VAR:       50  ██████████░░░░░░░░░░                    │  │
│  │ Futures:   85  ███████████████████░                    │  │
│  │ Blocks:    65  ██████████████░░░░░░                    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Implementation

This panel calls all the loaders and engines built in Phase 1-3:

```python
def build_intelligence_brief(symbol: str, date_str: str) -> dict:
    """Assemble all data sources into one brief."""
    
    brief = {}
    
    # Price action (existing)
    brief["price"] = load_eod_data(symbol, date_str)
    
    # Circuit limits (Phase 1)
    brief["circuit"] = load_circuit_limits(date_str)
    
    # Microstructure (Phase 2) 
    ticks = load_tick_data(date_str, symbol)
    if not ticks.empty:
        brief["spread"] = compute_spread_stats(ticks)
        brief["vpin"] = compute_vpin(ticks)
        brief["order_flow"] = compute_order_flow(ticks)
        brief["volume_profile"] = compute_volume_profile(ticks)
        brief["vwap"] = compute_vwap_bands(ticks)
        brief["delta"] = compute_cumulative_delta(ticks)
    
    # Derivatives (Phase 2)
    brief["futures_oi"] = load_futures_oi(date_str)
    brief["basis"] = compute_basis(symbol, date_str)
    
    # Block trades (Phase 3)
    brief["blocks"] = analyze_blocks(date_str)
    
    # VAR margins (Phase 1)
    brief["var"] = load_var_margins(date_str)
    
    # Index weight (Phase 1)
    brief["weight"] = load_index_weights()
    
    # Composite score
    brief["score"] = compute_composite_score(symbol, date_str)
    
    return brief
```

---

## Page Registration

```bash
# Check how existing pages are registered
grep -B2 -A2 "page_views\|navigation" ~/pakfindata/src/pakfindata/ui/app.py | head -30
```

Add new pages:
```python
# Under "Advanced" or "Analytics" section:
"Tick Replay": page_views.tick_replay,

# Intelligence Brief goes INTO the existing Signal Dashboard page as a new panel
# Not a separate page
```

## VERIFY

```bash
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py

# Test:
# 1. Tick Replay → pick date → pick HUBC → hit Play → watch chart build
# 2. Signal Dashboard → click any symbol → see Intelligence Brief panel
# 3. Signal Scanner → run batch → see volume_profile and block_trade columns
```

## PREREQUISITES

Before running Phase 3, ensure:

```bash
# 1. Cloud tick logs exist
ls /mnt/e/psxdata/tick_logs_cloud/*.jsonl | wc -l

# 2. Off-market downloads exist (backfill 30 days)
ls /mnt/e/psxdata/downloads/daily/*/off_market/ | wc -l

# 3. Phase 1 and Phase 2 are working
# Run the app, check Microstructure + Derivatives pages load

# 4. Install any missing deps
cd ~/pakfindata && source .venv/bin/activate
pip install openpyxl xlrd
```
