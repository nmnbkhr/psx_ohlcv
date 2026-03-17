# Claude Code Prompt: Top-Down Microstructure Analysis Engine

## Context — pakfindata App

This is a new Streamlit page for the **pakfindata** platform (formerly psx_ohlcv). It adds a dedicated "Microstructure Analysis" page that performs a 3-layer quantitative analysis on any PSX symbol — from macro regime detection down to tick-level order flow.

**Architecture:** Streamlit multi-page app at `ui/app.py`, pages in `ui/pages/`.
**Brand:** Bloomberg Terminal dark theme — `#0B0E11` background, gold `#C8A96E` accent, `JetBrains Mono` font.
**Databases:**
- Primary: SQLite at `/mnt/e/psxdata/psx.sqlite` (EOD + intraday data)
- Tick DB: SQLite at `/mnt/e/psxdata/tick_bars.db` (raw ticks + 5s bars from live collector)

---

## Rules — READ THESE FIRST

1. **CREATE 1 file:** `ui/pages/microstructure.py` — the full analysis page
2. **CREATE 1 file:** `src/pakfindata/analytics/microstructure.py` — the computation engine (pure pandas/numpy, no UI code)
3. **DO NOT modify ANY existing files** — zero changes to app.py, db.py, live_market.py, or anything else
4. **DO NOT import from existing pakfindata modules except `config.py`** (for DB paths). The analytics engine and page are self-contained.
5. **Use `sqlite3` directly** — no SQLAlchemy, no ORM. Raw SQL queries with pandas `read_sql`.
6. **All math in pandas/numpy** — no external TA libraries (no `ta-lib`, no `pandas-ta`). Implement from scratch.
7. **Handle missing data gracefully** — if tick_bars.db doesn't exist or has no data for the symbol, Layer 3 shows "No tick data available" instead of crashing.
8. **PSX market hours:** 09:15–15:30 PKT (UTC+5). All timestamps must respect this.
9. **Follow the existing Streamlit page pattern** — use `st.set_page_config`, sidebar symbol selector, dark theme CSS matching the existing pages.

---

## Database Schema Reference

### Table 1: `eod_data` (in psx.sqlite) — Years of Daily Data
```sql
CREATE TABLE eod_data (
    symbol   TEXT NOT NULL,
    date     TEXT NOT NULL,       -- 'YYYY-MM-DD'
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   INTEGER,
    UNIQUE(symbol, date)
);
```
**KNOWN ISSUE:** Some older records have fake high/low (derived as max/min of open,close). The market_summary table has real OHLC from .Z files. For symbols where `high == max(open,close)` AND `low == min(open,close)` for ALL rows, log a warning: "⚠️ High/Low may be derived — ATR/volatility calculations affected."

### Table 2: `intraday_data` (in psx.sqlite) — Intraday Bars
```sql
CREATE TABLE intraday_data (
    symbol    TEXT NOT NULL,
    timestamp TEXT NOT NULL,      -- 'YYYY-MM-DD HH:MM:SS' in PKT
    price     REAL,               -- This is the LAST price of the bar
    volume    INTEGER,
    UNIQUE(symbol, timestamp)
);
```
**NOTE:** These are fetched from `dps.psx.com.pk/timeseries/int/{SYMBOL}`. They are NOT uniform 5-min bars — they are price snapshots at varying intervals. Treat `price` as close of each snapshot. To build proper OHLCV bars, you must resample.

### Table 3: `raw_ticks` (in tick_bars.db) — Raw Tick Data from Live Collector
```sql
CREATE TABLE raw_ticks (
    ts       REAL NOT NULL,       -- Unix epoch (float, millisecond precision)
    symbol   TEXT NOT NULL,
    market   TEXT NOT NULL,       -- 'REG', 'FUT', 'ODL', 'BNB'
    price    REAL,
    volume   INTEGER,
    change   REAL,                -- Price change from previous close
    high     REAL,                -- Session high at time of tick
    low      REAL,                -- Session low at time of tick
    open     REAL,                -- Session open
    prev     REAL,                -- Previous day close
    turnover REAL                 -- Cumulative turnover
);
```
**CRITICAL:** `ts` is Unix epoch (seconds.milliseconds). Convert with:
```python
df['datetime'] = pd.to_datetime(df['ts'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Karachi')
```

### Table 4: `ohlcv_5s` (in tick_bars.db) — 5-Second Aggregated Bars
```sql
CREATE TABLE ohlcv_5s (
    ts       REAL NOT NULL,       -- Unix epoch of bar start
    symbol   TEXT NOT NULL,
    market   TEXT NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   INTEGER,
    ticks    INTEGER,             -- Number of ticks in this bar
    vwap     REAL,                -- Volume-weighted average price for this bar
    turnover REAL
);
```

---

## Analysis Specification

### Layer 1: The Macro Regime (from `eod_data`)

Query: Last 2 years of daily OHLCV for `[SYMBOL]`.

**1A. Annualized Volatility**
```python
# Log returns on close prices
log_returns = np.log(df['close'] / df['close'].shift(1)).dropna()
# Annualize: PSX has ~245 trading days/year
ann_vol = log_returns.std() * np.sqrt(245) * 100  # as percentage
```

**1B. Hurst Exponent (R/S Method)**
```python
def hurst_exponent(series, max_lag=100):
    """
    Rescaled Range (R/S) analysis.
    H > 0.5 → Trending (persistent)
    H = 0.5 → Random walk
    H < 0.5 → Mean-reverting (anti-persistent)
    """
    lags = range(2, min(max_lag, len(series) // 2))
    rs_values = []
    for lag in lags:
        chunks = [series[i:i+lag] for i in range(0, len(series) - lag, lag)]
        rs_per_chunk = []
        for chunk in chunks:
            if len(chunk) < lag:
                continue
            mean = np.mean(chunk)
            devs = np.cumsum(chunk - mean)
            R = np.max(devs) - np.min(devs)
            S = np.std(chunk, ddof=1)
            if S > 0:
                rs_per_chunk.append(R / S)
        if rs_per_chunk:
            rs_values.append((lag, np.mean(rs_per_chunk)))
    if len(rs_values) < 5:
        return 0.5  # insufficient data
    log_lags = np.log([v[0] for v in rs_values])
    log_rs = np.log([v[1] for v in rs_values])
    H = np.polyfit(log_lags, log_rs, 1)[0]
    return np.clip(H, 0.0, 1.0)
```

**1C. 200-Day SMA & Distance**
```python
sma_200 = df['close'].rolling(200).mean().iloc[-1]
current_price = df['close'].iloc[-1]
distance_pct = ((current_price - sma_200) / sma_200) * 100
```

**1D. PSX-Specific Enhancements (ADD THESE — not in original prompt)**
- **Circuit Breaker Detection:** Flag if the stock hit upper/lower lock in the last 5 sessions. PSX locks at ±7.5% for most stocks. Check: `if abs(daily_return) >= 7.0` → flag as potential circuit breaker.
- **Relative Strength vs KSE-100:** Calculate 20-day rolling beta and alpha against KSE-100 index (from `eod_data` where symbol = 'KSE100' or query the index table if available, otherwise skip gracefully).
- **Sector Context:** Pull the symbol's sector from the `symbols` table and display it.

**Layer 1 Output:**
| Metric | Value | Interpretation |
|--------|-------|----------------|
| Ann. Volatility | 34.2% | High |
| Hurst Exponent | 0.62 | Trending |
| Regime | TRENDING | H > 0.5 |
| 200-SMA | 245.30 | — |
| Price vs SMA | +8.7% | Above |
| Circuit Breaker (5d) | None | Clean |
| Macro Score (0-33) | 28 | — |

---

### Layer 2: The Intraday Anchor (from `intraday_data` + `ohlcv_5s`)

**Data Strategy:** Use `ohlcv_5s` from tick_bars.db if available (higher quality, true OHLCV bars). Fall back to `intraday_data` from psx.sqlite if tick_bars.db is missing or has no data for this symbol.

**2A. Rolling VWAP with Bands**
```python
def rolling_vwap(df, window=None):
    """
    Anchored VWAP — reset daily at session open (09:15 PKT).
    If window=None, anchor from start of current session.
    """
    df = df.copy()
    df['session_date'] = df['datetime'].dt.date
    
    # For each session, calculate cumulative VWAP
    result = []
    for date, group in df.groupby('session_date'):
        typical_price = (group['high'] + group['low'] + group['close']) / 3
        cum_tp_vol = (typical_price * group['volume']).cumsum()
        cum_vol = group['volume'].cumsum()
        vwap = cum_tp_vol / cum_vol.replace(0, np.nan)
        
        # Standard deviation bands
        squared_diff = ((typical_price - vwap) ** 2 * group['volume']).cumsum()
        vwap_std = np.sqrt(squared_diff / cum_vol.replace(0, np.nan))
        
        group = group.copy()
        group['vwap'] = vwap
        group['vwap_upper_1'] = vwap + vwap_std
        group['vwap_lower_1'] = vwap - vwap_std
        group['vwap_upper_2'] = vwap + 2 * vwap_std
        group['vwap_lower_2'] = vwap - 2 * vwap_std
        result.append(group)
    
    return pd.concat(result)
```

**2B. Volume Profile — Point of Control (POC)**
```python
def volume_profile(df, bins=50, lookback_days=20):
    """
    Volume at Price histogram.
    POC = price level with the highest traded volume.
    """
    recent = df[df['datetime'] >= df['datetime'].max() - pd.Timedelta(days=lookback_days)]
    
    price_min, price_max = recent['close'].min(), recent['close'].max()
    price_bins = np.linspace(price_min, price_max, bins + 1)
    
    vol_at_price = np.zeros(bins)
    for i in range(bins):
        mask = (recent['close'] >= price_bins[i]) & (recent['close'] < price_bins[i+1])
        vol_at_price[i] = recent.loc[mask, 'volume'].sum()
    
    poc_idx = np.argmax(vol_at_price)
    poc_price = (price_bins[poc_idx] + price_bins[poc_idx + 1]) / 2
    
    # Value Area (70% of volume)
    sorted_indices = np.argsort(vol_at_price)[::-1]
    total_vol = vol_at_price.sum()
    cumulative = 0
    value_area_indices = []
    for idx in sorted_indices:
        cumulative += vol_at_price[idx]
        value_area_indices.append(idx)
        if cumulative >= total_vol * 0.70:
            break
    
    va_low = price_bins[min(value_area_indices)]
    va_high = price_bins[max(value_area_indices) + 1]
    
    return {
        'poc': poc_price,
        'va_low': va_low,
        'va_high': va_high,
        'profile': vol_at_price,
        'price_levels': (price_bins[:-1] + price_bins[1:]) / 2
    }
```

**2C. Efficiency Ratio Spike Detection**
```python
def efficiency_ratio(df):
    """
    ER = (High - Low) / Volume
    Spike = ER > 2 * rolling_mean(ER, 20)
    High ER = large price move on low volume → institutional sweep
    """
    df = df.copy()
    df['er'] = (df['high'] - df['low']) / df['volume'].replace(0, np.nan)
    df['er_ma'] = df['er'].rolling(20, min_periods=5).mean()
    df['er_spike'] = df['er'] > (2 * df['er_ma'])
    return df
```

**2D. PSX-Specific Enhancements (ADD THESE)**
- **Auction Awareness:** PSX has opening auction (09:15–09:30) and closing auction (15:28–15:30). Mark these periods differently in VWAP — exclude or flag auction bars since they distort volume calculations.
- **Ready Market vs Futures:** If the symbol appears in both REG and FUT markets in tick_bars.db, show the basis (futures premium/discount vs spot).

**Layer 2 Output:** Interactive Plotly chart showing:
- Candlestick/line chart with VWAP + bands overlaid
- Volume Profile as horizontal histogram on the right y-axis
- POC line, Value Area shading
- ER spike markers
- Score contribution (0-33)

---

### Layer 3: The Execution DNA (from `raw_ticks` in tick_bars.db)

**IMPORTANT:** This layer requires `tick_bars.db` to exist and have data. If unavailable, show a clear message: "Layer 3 requires live tick data. Start the tick collector during market hours: `python services/tick_service.py`"

**3A. Lee-Ready Trade Classification (Adapted for PSX)**
```python
def lee_ready_classify(df):
    """
    Classify each tick as buyer-initiated (+1) or seller-initiated (-1).
    
    PSX tick data does NOT have separate bid/ask quotes.
    We use the Tick Rule (simplified Lee-Ready):
    - If price > previous price → BUY (+1)
    - If price < previous price → SELL (-1)
    - If price == previous price → use last non-zero classification
    
    NOTE: The full Lee-Ready uses midquote = (bid+ask)/2.
    Since PSX ticks from psxterminal.com don't carry bid/ask,
    we fall back to the pure tick rule. If bid/ask becomes available
    in the future, upgrade to: sign = +1 if price > midquote else -1.
    """
    df = df.copy()
    df['price_diff'] = df['price'].diff()
    df['trade_sign'] = 0
    df.loc[df['price_diff'] > 0, 'trade_sign'] = 1   # Buy
    df.loc[df['price_diff'] < 0, 'trade_sign'] = -1  # Sell
    
    # Forward-fill zeros (tick rule continuation)
    df['trade_sign'] = df['trade_sign'].replace(0, np.nan).ffill().fillna(0).astype(int)
    
    return df
```

**3B. Cumulative Volume Delta (CVD)**
```python
def cumulative_volume_delta(df):
    """
    CVD = cumulative sum of (signed_volume).
    signed_volume = trade_sign * volume
    
    Rising CVD = aggressive buying dominance
    Falling CVD = aggressive selling dominance
    Divergence: Price up + CVD down = bearish warning
    """
    df = df.copy()
    df['signed_volume'] = df['trade_sign'] * df['volume']
    df['cvd'] = df['signed_volume'].cumsum()
    return df
```

**3C. Order Flow Imbalance (OFI) per Minute**
```python
def order_flow_imbalance(df):
    """
    OFI = (buy_volume - sell_volume) / total_volume per minute.
    Range: -1.0 (all sells) to +1.0 (all buys).
    """
    df = df.copy()
    df['minute'] = df['datetime'].dt.floor('1min')
    
    buys = df[df['trade_sign'] == 1].groupby('minute')['volume'].sum().rename('buy_vol')
    sells = df[df['trade_sign'] == -1].groupby('minute')['volume'].sum().rename('sell_vol')
    
    ofi = pd.DataFrame({'buy_vol': buys, 'sell_vol': sells}).fillna(0)
    ofi['total'] = ofi['buy_vol'] + ofi['sell_vol']
    ofi['ofi'] = (ofi['buy_vol'] - ofi['sell_vol']) / ofi['total'].replace(0, np.nan)
    
    return ofi
```

**3D. PSX-Specific Enhancements (ADD THESE)**
- **Cross-Market Flow:** If the symbol trades in both REG and FUT markets, compute CVD for each separately and show divergence. Futures CVD leading REG CVD = smart money signal.
- **Block Trade Detection:** Flag any single tick where `volume > 5 * median_tick_volume` as a potential block/institutional trade. Highlight on the CVD chart.
- **Session Segmentation:** Break the trading day into: Pre-Open (09:15-09:30), Morning (09:30-12:00), Afternoon (12:00-15:00), Close Auction (15:00-15:30). Show OFI heatmap by segment.

---

## Signal Score Calculation (1-100)

```python
def compute_signal_score(macro, intraday, execution):
    """
    Composite score. Each layer contributes 0-33 points.
    Total = macro_score + intraday_score + execution_score + 1 (base).
    """
    # === MACRO SCORE (0-33) ===
    macro_score = 0
    
    # Trend direction: +15 if trending UP, +8 if trending DOWN, +3 if random walk
    if macro['hurst'] > 0.55 and macro['sma_distance_pct'] > 0:
        macro_score += 15  # Trending UP
    elif macro['hurst'] > 0.55 and macro['sma_distance_pct'] < 0:
        macro_score += 5   # Trending DOWN (penalize for short signal)
    elif macro['hurst'] > 0.45:
        macro_score += 3   # Random walk
    else:
        macro_score += 10  # Mean reverting — could be opportunity
    
    # Price above SMA: +10 if above, +3 if within 2%, 0 if far below
    if macro['sma_distance_pct'] > 2:
        macro_score += 10
    elif macro['sma_distance_pct'] > -2:
        macro_score += 5
    else:
        macro_score += 0
    
    # Volatility regime: moderate vol is best (not too calm, not too wild)
    if 15 < macro['ann_vol'] < 40:
        macro_score += 8  # Goldilocks volatility
    elif macro['ann_vol'] <= 15:
        macro_score += 3  # Too quiet
    else:
        macro_score += 2  # Too volatile
    
    # === INTRADAY SCORE (0-33) ===
    intraday_score = 0
    
    # Price near VWAP mean: +15 if within 0.5 std, +8 if within 1 std, +3 if within 2 std
    if intraday.get('vwap_distance_std') is not None:
        d = abs(intraday['vwap_distance_std'])
        if d < 0.5:
            intraday_score += 15  # At fair value
        elif d < 1.0:
            intraday_score += 10
        elif d < 2.0:
            intraday_score += 5
        else:
            intraday_score += 0  # Far from VWAP — extended
    
    # Price near POC: +10 if within 1%, +5 if within 2%
    if intraday.get('poc_distance_pct') is not None:
        d = abs(intraday['poc_distance_pct'])
        if d < 1:
            intraday_score += 10
        elif d < 2:
            intraday_score += 5
    
    # No ER spike (clean price action): +8 if clean, +2 if spike
    if not intraday.get('er_spike_active', False):
        intraday_score += 8
    else:
        intraday_score += 2  # Spike = abnormal activity, caution
    
    # === EXECUTION SCORE (0-33) ===
    execution_score = 0
    
    if execution.get('has_tick_data', False):
        # CVD trend: +15 if positive and rising, +5 if flat, 0 if falling
        cvd_slope = execution.get('cvd_slope', 0)
        if cvd_slope > 0:
            execution_score += 15  # Aggressive buying
        elif cvd_slope > -0.1:
            execution_score += 5   # Neutral
        else:
            execution_score += 0   # Aggressive selling
        
        # OFI: +10 if last 15 min OFI > +0.3 (buying imbalance)
        recent_ofi = execution.get('recent_ofi', 0)
        if recent_ofi > 0.3:
            execution_score += 10  # Strong buy imbalance
        elif recent_ofi > 0:
            execution_score += 5   # Mild buy
        else:
            execution_score += 0   # Sell imbalance
        
        # Block trade alignment: +8 if recent blocks are buys
        if execution.get('block_bias', 0) > 0:
            execution_score += 8
        elif execution.get('block_bias', 0) == 0:
            execution_score += 4  # No blocks
        else:
            execution_score += 0  # Blocks are sells
    else:
        execution_score = 16  # Neutral when no tick data (don't penalize)
    
    total = 1 + macro_score + intraday_score + execution_score  # 1-100
    return min(total, 100)
```

---

## Streamlit Page Layout: `ui/pages/microstructure.py`

```
┌─────────────────────────────────────────────────────────────────┐
│  SIDEBAR                                                         │
│  ┌──────────────┐                                                │
│  │ Symbol [▼]   │  ← selectbox from symbols table               │
│  │ HUBC         │                                                │
│  └──────────────┘                                                │
│  [🔍 Run Analysis]  ← button                                    │
│  ─────────────────                                               │
│  Data Availability:                                              │
│  ✅ EOD: 1,247 days                                             │
│  ✅ Intraday: 892 bars                                          │
│  ⚠️ Ticks: No data                                              │
│  ─────────────────                                               │
│  Last Updated: 2026-03-13                                        │
├─────────────────────────────────────────────────────────────────┤
│  MAIN AREA                                                       │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │  🎯 SIGNAL SCORE       [  78  ]    ← big gauge/number       ││
│  │  Macro: 28/33 | Intraday: 25/33 | Execution: 24/33         ││
│  │  Interpretation: "Strong Buy Setup"                          ││
│  └──────────────────────────────────────────────────────────────┘│
│                                                                   │
│  ═══ LAYER 1: MACRO REGIME ═══                                   │
│  ┌───────────┬───────────┬───────────┬───────────┐              │
│  │ Hurst     │ Ann. Vol  │ 200-SMA   │ vs SMA    │              │
│  │ 0.62      │ 34.2%     │ PKR 245   │ +8.7%     │              │
│  │ TRENDING  │ HIGH      │           │ ABOVE     │              │
│  └───────────┴───────────┴───────────┴───────────┘              │
│  [2-Year Price Chart with SMA-200 overlaid]                      │
│  [Hurst Exponent rolling window chart]                           │
│                                                                   │
│  ═══ LAYER 2: INTRADAY ANCHOR ═══                                │
│  [Candlestick + VWAP Bands chart — full width]                   │
│  [Volume Profile histogram — right side overlay]                 │
│  POC: PKR 267.50 | VA: 262-273 | ER Spike: None                │
│                                                                   │
│  ═══ LAYER 3: EXECUTION DNA ═══                                  │
│  [CVD chart — 3 day cumulative]                                  │
│  [OFI heatmap — per minute, color-coded]                         │
│  [Trade classification pie: 52% Buy / 48% Sell]                 │
│  Block Trades: 3 detected (2 Buy, 1 Sell)                       │
│                                                                   │
│  ═══ METHODOLOGY ═══                                             │
│  [Expandable section explaining each metric]                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Requirements

### File 1: `src/pakfindata/analytics/microstructure.py`

This is the **pure computation engine**. No Streamlit imports. Must be importable and testable independently.

```python
"""
pakfindata.analytics.microstructure
Top-Down Microstructure Analysis Engine for PSX

Three-layer quantitative analysis:
  Layer 1: Macro Regime (daily data)
  Layer 2: Intraday Anchor (intraday bars)
  Layer 3: Execution DNA (tick-level order flow)
"""
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

# DB Paths — import from config if available, else hardcode
PSX_DB = Path("/mnt/e/psxdata/psx.sqlite")
TICK_DB = Path("/mnt/e/psxdata/tick_bars.db")
PSX_TRADING_DAYS_PER_YEAR = 245

@dataclass
class MacroResult:
    ann_volatility: float
    hurst_exponent: float
    regime: str  # 'TRENDING', 'MEAN_REVERTING', 'RANDOM_WALK'
    sma_200: float
    current_price: float
    sma_distance_pct: float
    circuit_breaker_flags: list  # dates where ±7% daily move
    sector: Optional[str] = None
    fake_hl_warning: bool = False
    score: int = 0
    daily_df: Optional[pd.DataFrame] = None  # for charting

@dataclass
class IntradayResult:
    vwap_df: Optional[pd.DataFrame] = None  # bars with vwap columns
    volume_profile: Optional[Dict] = None    # poc, va_low, va_high, profile, levels
    er_spike_active: bool = False
    vwap_distance_std: Optional[float] = None
    poc_distance_pct: Optional[float] = None
    data_source: str = 'none'  # 'tick_bars', 'intraday_data', 'none'
    score: int = 0

@dataclass
class ExecutionResult:
    has_tick_data: bool = False
    classified_df: Optional[pd.DataFrame] = None  # ticks with trade_sign
    cvd_df: Optional[pd.DataFrame] = None
    ofi_df: Optional[pd.DataFrame] = None
    cvd_slope: float = 0.0
    recent_ofi: float = 0.0
    block_trades: Optional[pd.DataFrame] = None
    block_bias: int = 0  # +1 buy-heavy, -1 sell-heavy, 0 neutral
    buy_pct: float = 50.0
    sell_pct: float = 50.0
    score: int = 0

@dataclass
class MicrostructureReport:
    symbol: str
    macro: MacroResult
    intraday: IntradayResult
    execution: ExecutionResult
    signal_score: int = 0
    interpretation: str = ""

# Implement all functions here:
# - query_eod_data(symbol, lookback_years=2) -> pd.DataFrame
# - query_intraday_data(symbol) -> pd.DataFrame  
# - query_tick_data(symbol, lookback_days=3) -> pd.DataFrame
# - hurst_exponent(series, max_lag=100) -> float
# - rolling_vwap(df) -> pd.DataFrame
# - volume_profile(df, bins=50, lookback_days=20) -> dict
# - efficiency_ratio(df) -> pd.DataFrame
# - lee_ready_classify(df) -> pd.DataFrame
# - cumulative_volume_delta(df) -> pd.DataFrame
# - order_flow_imbalance(df) -> pd.DataFrame
# - detect_block_trades(df, threshold_multiplier=5) -> pd.DataFrame
# - detect_circuit_breakers(df, threshold=7.0) -> list
# - compute_macro(symbol) -> MacroResult
# - compute_intraday(symbol) -> IntradayResult
# - compute_execution(symbol) -> ExecutionResult
# - compute_signal_score(macro, intraday, execution) -> int
# - run_analysis(symbol) -> MicrostructureReport
```

### File 2: `ui/pages/microstructure.py`

The Streamlit page. Import from the analytics engine. All charts use `plotly.graph_objects` (not matplotlib — matches existing page patterns).

**Charting Requirements:**
- Dark theme: `template='plotly_dark'`, paper_bgcolor='#0B0E11', plot_bgcolor='#0B0E11'
- Gold accent: `#C8A96E` for primary lines/highlights
- Grid color: `#1A1D23`
- Font: Use system default (JetBrains Mono if installed, else monospace)
- All prices formatted with PKR prefix and commas: `PKR 1,245.50`

**Signal Score Display:**
- Score 0-30: 🔴 RED — "Weak / Avoid"
- Score 31-50: 🟡 YELLOW — "Neutral / Wait"
- Score 51-70: 🟢 GREEN — "Moderate Buy Setup"
- Score 71-85: 💚 BRIGHT GREEN — "Strong Buy Setup"
- Score 86-100: ⭐ GOLD — "Exceptional Confluence"

**Custom CSS (inject at top of page):**
```python
st.markdown("""
<style>
    .stApp { background-color: #0B0E11; }
    .metric-card {
        background: linear-gradient(135deg, #12151A 0%, #1A1D23 100%);
        border: 1px solid #2A2D35;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: bold; color: #C8A96E; }
    .metric-label { font-size: 0.85rem; color: #8B8D93; }
    .score-gauge {
        font-size: 4rem;
        font-weight: bold;
        text-align: center;
        padding: 20px;
    }
    .layer-header {
        font-size: 1.1rem;
        font-weight: bold;
        color: #C8A96E;
        border-bottom: 1px solid #2A2D35;
        padding-bottom: 8px;
        margin-top: 24px;
    }
</style>
""", unsafe_allow_html=True)
```

---

## Data Query Patterns

### Querying across two databases:
```python
def get_eod(symbol: str, days: int = 500) -> pd.DataFrame:
    """Query daily OHLCV from psx.sqlite."""
    with sqlite3.connect(str(PSX_DB)) as conn:
        df = pd.read_sql("""
            SELECT date, open, high, low, close, volume
            FROM eod_data
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
        """, conn, params=(symbol, days))
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values('date').reset_index(drop=True)

def get_ticks(symbol: str, days: int = 3) -> pd.DataFrame:
    """Query raw ticks from tick_bars.db."""
    if not TICK_DB.exists():
        return pd.DataFrame()
    
    cutoff_ts = (pd.Timestamp.now(tz='Asia/Karachi') - pd.Timedelta(days=days)).timestamp()
    
    with sqlite3.connect(str(TICK_DB)) as conn:
        df = pd.read_sql("""
            SELECT ts, symbol, market, price, volume, change,
                   high, low, open, prev, turnover
            FROM raw_ticks
            WHERE symbol = ? AND ts >= ?
            ORDER BY ts ASC
        """, conn, params=(symbol, cutoff_ts))
    
    if df.empty:
        return df
    
    # CRITICAL: Convert epoch to PKT datetime
    df['datetime'] = pd.to_datetime(df['ts'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Karachi')
    return df
```

---

## Edge Cases to Handle

1. **Symbol not found in eod_data** → Show error: "Symbol not found. Check the Symbols page for valid symbols."
2. **Less than 200 days of EOD data** → Calculate SMA with available data, note "SMA-{N} (insufficient for 200)".
3. **Less than 30 days of EOD data** → Skip Hurst Exponent, show "Insufficient data for Hurst calculation".
4. **No intraday data at all** → Layer 2 shows "No intraday data available for {SYMBOL}". Score defaults to 16/33.
5. **tick_bars.db doesn't exist** → Layer 3 shows info box with instructions to start collector.
6. **tick_bars.db exists but no data for symbol** → "No tick data for {SYMBOL}. Data is only available for symbols traded during live collection."
7. **Market is currently open** → Show a "🔴 LIVE" indicator next to the score. Add a refresh button.
8. **Weekend/holiday** → Show last available data with "Last session: {date}" label.
9. **Fake H/L detection** → If all rows have `high == max(open,close)` and `low == min(open,close)`, show ⚠️ warning.

---

## Testing / Verification

After implementation, verify:

```bash
# 1. Analytics engine imports cleanly
python -c "from pakfindata.analytics.microstructure import run_analysis; print('OK')"

# 2. Check symbol data availability
python -c "
import sqlite3
conn = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
cursor = conn.execute('SELECT COUNT(*) FROM eod_data WHERE symbol = \"HUBC\"')
print(f'HUBC EOD rows: {cursor.fetchone()[0]}')
cursor = conn.execute('SELECT COUNT(*) FROM intraday_data WHERE symbol = \"HUBC\"')
print(f'HUBC intraday rows: {cursor.fetchone()[0]}')
"

# 3. Run analysis for a liquid stock
python -c "
from pakfindata.analytics.microstructure import run_analysis
report = run_analysis('HUBC')
print(f'Score: {report.signal_score}')
print(f'Hurst: {report.macro.hurst_exponent:.3f}')
print(f'Regime: {report.macro.regime}')
print(f'Interpretation: {report.interpretation}')
"

# 4. Start Streamlit and navigate to page
streamlit run ui/app.py
# → Sidebar should show "Microstructure Analysis" page
# → Select HUBC → Click "Run Analysis"
# → All three layers should render without errors
```

---

## Dependencies

These should already be installed in the pakfindata environment:
- `pandas` (core data manipulation)
- `numpy` (math)
- `plotly` (charting — already used by existing Streamlit pages)
- `streamlit` (UI)
- `sqlite3` (stdlib — no install needed)

**DO NOT install:** `ta-lib`, `pandas-ta`, `scipy`, `statsmodels`, or any other external TA/stats library. All calculations are implemented from scratch with numpy/pandas for full control and zero dependency bloat.

---

## Summary

This prompt creates a **new Streamlit page** and a **standalone analytics engine** that performs institutional-grade quantitative analysis on any PSX symbol. It uses the actual pakfindata database schema, handles the dual-DB architecture (psx.sqlite + tick_bars.db), respects PSX market specifics (trading hours, circuit breakers, auction periods), and follows the existing Bloomberg Terminal dark theme.

The 3-layer architecture ensures the analysis degrades gracefully — Layer 1 (macro) always works with daily data, Layer 2 (intraday) works if any bar data exists, and Layer 3 (execution) only activates when live tick data has been collected.
