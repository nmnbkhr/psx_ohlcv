# Claude Code Prompt: Strategy 5 — VWAP Execution Optimizer

## Context

pakfindata already computes VWAP, POC (Point of Control), and Value Area on the Signal 
Analysis page (Layer 2). This strategy turns those into an execution algorithm — slicing 
large orders proportionally to the historical volume curve to minimize market impact.

**The problem it solves:** A fund manager wants to buy 500,000 shares of OGDC. Executing 
at market in one order would move the price 1-2%. A VWAP slicer distributes the order 
across the day, matching the stock's natural volume pattern, reducing slippage to ~0.1%.

**What VWAP execution means:**
- VWAP = Σ(Price × Volume) / Σ(Volume) for the day
- Goal: execute at or below VWAP (for buys) / at or above VWAP (for sells)
- Method: slice the order into N chunks, size each proportional to that time period's historical volume

## What already exists

```bash
# Find existing VWAP/volume profile code
grep -rn "vwap\|VWAP\|volume_profile\|poc\|value_area\|POC\|Value.Area" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check intraday bar data available
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# ohlcv_5s bars — the core intraday data
print('ohlcv_5s:')
print(con.execute('SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM ohlcv_5s').fetchone())

# Sample volume distribution for OGDC across a day
print('\nVolume by hour (OGDC latest date):')
df = con.execute('''
    SELECT 
        EXTRACT(HOUR FROM CAST(timestamp AS TIMESTAMP)) as hour,
        SUM(volume) as vol,
        COUNT(*) as bars
    FROM ohlcv_5s 
    WHERE symbol = \"OGDC\" 
    AND date = (SELECT MAX(date) FROM ohlcv_5s WHERE symbol = \"OGDC\")
    GROUP BY 1 ORDER BY 1
''').df()
print(df.to_string())
con.close()
"
```

**READ ALL OUTPUT before proceeding.**

## Step 1: Create the VWAP Execution Engine

Create `src/pakfindata/engine/vwap_execution.py`:

```python
"""
VWAP Execution Optimizer.

Builds historical volume profiles per symbol, then generates optimal 
order slicing schedules to minimize market impact.

Three execution modes:
  1. VWAP: slice proportional to historical volume curve
  2. TWAP: equal slices across time (simple baseline)
  3. Aggressive: front-load when spread is tight, back-load when wide

Performance measured by:
  - Implementation Shortfall = (Execution VWAP - Arrival Price) / Arrival Price
  - VWAP Slippage = (Execution VWAP - Market VWAP) / Market VWAP
  - Market Impact = price move caused by our execution

PSX-Specific:
  - Market hours: 09:30-15:30 (Mon-Thu), 09:30-16:30 (Fri)
  - 5-second bars from DPS timeseries (ohlcv_5s in DuckDB)
  - Auction periods: 09:15-09:30 (opening), 15:28-15:30 (closing on Mon-Thu)
  - Circuit breakers: ±7.5% — cannot execute beyond limits
  - Lot sizes vary by symbol (typically 500 shares)
  - Trading days: 245/year
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")

# PSX market hours
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 30
MARKET_CLOSE_HOUR_REGULAR = 15   # Mon-Thu
MARKET_CLOSE_MIN_REGULAR = 30
MARKET_CLOSE_HOUR_FRIDAY = 16    # Fri
MARKET_CLOSE_MIN_FRIDAY = 30

# Execution intervals
SLICE_INTERVAL_MINUTES = 15  # 15-minute execution buckets


@dataclass
class VolumeProfile:
    """Historical volume profile for a symbol."""
    symbol: str
    dates_used: int
    total_bars: int
    intervals: list[dict]    # [{time_start, time_end, avg_volume, pct_of_day, avg_spread_bps}]
    daily_avg_volume: float
    vwap_typical: float      # typical daily VWAP
    poc_price: float         # most common price (Point of Control)
    value_area_low: float
    value_area_high: float


@dataclass
class ExecutionSlice:
    """One slice of a VWAP execution order."""
    slice_num: int
    time_start: str
    time_end: str
    target_shares: int
    target_pct: float          # % of total order
    historical_volume: float   # average volume in this interval
    participation_rate: float  # our volume / market volume
    limit_price: float         # suggested limit price
    urgency: str              # "LOW", "MEDIUM", "HIGH"


@dataclass
class ExecutionPlan:
    """Complete VWAP execution plan for an order."""
    symbol: str
    side: str                  # "BUY" or "SELL"
    total_shares: int
    strategy: str              # "VWAP", "TWAP", "AGGRESSIVE"
    max_participation: float   # max % of market volume per interval
    slices: list[ExecutionSlice]
    estimated_vwap: float
    estimated_slippage_bps: float
    estimated_duration_min: int
    arrival_price: float
    warnings: list[str]


def build_volume_profile(
    symbol: str,
    lookback_days: int = 20,
    interval_minutes: int = SLICE_INTERVAL_MINUTES,
) -> VolumeProfile:
    """
    Build average intraday volume profile from historical 5-second bars.
    
    Groups volume into N-minute intervals across the day, averages 
    across lookback_days to get the typical volume shape.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Get recent 5-second bars
    df = con.execute("""
        SELECT timestamp, open, high, low, close, volume, date
        FROM ohlcv_5s
        WHERE symbol = ?
        AND date >= CURRENT_DATE - INTERVAL ? DAY
        ORDER BY timestamp
    """, [symbol, lookback_days]).df()
    
    con.close()
    
    if df.empty or len(df) < 100:
        return None
    
    # Convert timestamp
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Karachi")
    
    # Extract time components
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute
    df["time_bucket"] = df["hour"] * 60 + (df["minute"] // interval_minutes) * interval_minutes
    
    # Compute VWAP per bar
    df["vwap_contrib"] = df["close"] * df["volume"]
    
    # Group by time bucket, average across days
    profile = df.groupby("time_bucket").agg(
        avg_volume=("volume", "mean"),
        total_volume=("volume", "sum"),
        avg_close=("close", "mean"),
        bar_count=("volume", "count"),
        avg_high=("high", "mean"),
        avg_low=("low", "mean"),
    ).reset_index()
    
    # Only keep market hours (09:30 - 15:30)
    market_open = MARKET_OPEN_HOUR * 60 + MARKET_OPEN_MIN
    market_close = MARKET_CLOSE_HOUR_REGULAR * 60 + MARKET_CLOSE_MIN_REGULAR
    profile = profile[(profile["time_bucket"] >= market_open) & 
                      (profile["time_bucket"] < market_close)]
    
    if profile.empty:
        return None
    
    # Compute percentage of daily volume per interval
    total_avg_vol = profile["avg_volume"].sum()
    profile["pct_of_day"] = profile["avg_volume"] / total_avg_vol if total_avg_vol > 0 else 0
    
    # Average spread (approximate from high-low)
    profile["avg_spread_bps"] = ((profile["avg_high"] - profile["avg_low"]) / 
                                  profile["avg_close"] * 10000)
    
    # Build intervals list
    intervals = []
    for _, row in profile.iterrows():
        bucket = int(row["time_bucket"])
        hour = bucket // 60
        minute = bucket % 60
        end_minute = minute + interval_minutes
        end_hour = hour + end_minute // 60
        end_minute = end_minute % 60
        
        intervals.append({
            "time_start": f"{hour:02d}:{minute:02d}",
            "time_end": f"{end_hour:02d}:{end_minute:02d}",
            "avg_volume": row["avg_volume"],
            "pct_of_day": row["pct_of_day"],
            "avg_spread_bps": row["avg_spread_bps"],
            "avg_price": row["avg_close"],
        })
    
    # POC and Value Area
    # Use tick data or 5s bars to build price-volume profile
    price_vol = df.groupby(df["close"].round(2))["volume"].sum().sort_values(ascending=False)
    poc = price_vol.index[0] if len(price_vol) > 0 else df["close"].median()
    
    # Value Area: prices containing 70% of volume
    cum_vol = price_vol.cumsum()
    total = cum_vol.iloc[-1] if len(cum_vol) > 0 else 1
    va_prices = cum_vol[cum_vol <= total * 0.7].index
    va_low = va_prices.min() if len(va_prices) > 0 else df["close"].min()
    va_high = va_prices.max() if len(va_prices) > 0 else df["close"].max()
    
    # Typical VWAP
    total_vwap_num = (df["close"] * df["volume"]).sum()
    total_vwap_den = df["volume"].sum()
    typical_vwap = total_vwap_num / total_vwap_den if total_vwap_den > 0 else df["close"].mean()
    
    n_dates = df["date"].nunique()
    daily_avg = total_avg_vol * (60 / interval_minutes)  # scale to full bars per interval
    
    return VolumeProfile(
        symbol=symbol,
        dates_used=n_dates,
        total_bars=len(df),
        intervals=intervals,
        daily_avg_volume=daily_avg,
        vwap_typical=typical_vwap,
        poc_price=poc,
        value_area_low=va_low,
        value_area_high=va_high,
    )


def generate_execution_plan(
    symbol: str,
    side: str,
    total_shares: int,
    strategy: str = "VWAP",
    max_participation: float = 0.15,   # max 15% of interval volume
    start_time: str = None,            # "HH:MM" or None for market open
    end_time: str = None,              # "HH:MM" or None for market close
    arrival_price: float = None,
    lookback_days: int = 20,
) -> ExecutionPlan:
    """
    Generate a VWAP execution plan for a large order.
    
    Args:
        symbol: stock symbol
        side: "BUY" or "SELL"
        total_shares: total order size
        strategy: "VWAP", "TWAP", or "AGGRESSIVE"
        max_participation: max fraction of market volume per interval
        start_time: execution start (default: market open)
        end_time: execution end (default: market close)
        arrival_price: current price (for slippage estimation)
        lookback_days: days for volume profile
    """
    profile = build_volume_profile(symbol, lookback_days)
    
    if profile is None:
        return ExecutionPlan(
            symbol=symbol, side=side, total_shares=total_shares,
            strategy=strategy, max_participation=max_participation,
            slices=[], estimated_vwap=0, estimated_slippage_bps=0,
            estimated_duration_min=0, arrival_price=arrival_price or 0,
            warnings=["No volume profile available — insufficient data"]
        )
    
    if arrival_price is None:
        # Use latest close
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        result = con.execute("""
            SELECT close FROM eod_ohlcv WHERE symbol = ? ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchone()
        con.close()
        arrival_price = result[0] if result else 0
    
    # Filter intervals by start/end time
    intervals = profile.intervals
    if start_time:
        intervals = [i for i in intervals if i["time_start"] >= start_time]
    if end_time:
        intervals = [i for i in intervals if i["time_end"] <= end_time]
    
    if not intervals:
        intervals = profile.intervals  # fallback to full day
    
    warnings = []
    
    # ── VWAP strategy: proportional to volume ──
    if strategy == "VWAP":
        total_pct = sum(i["pct_of_day"] for i in intervals)
        
        slices = []
        remaining_shares = total_shares
        
        for idx, interval in enumerate(intervals):
            # Proportion of order for this interval
            if total_pct > 0:
                slice_pct = interval["pct_of_day"] / total_pct
            else:
                slice_pct = 1.0 / len(intervals)
            
            target_shares = int(total_shares * slice_pct)
            
            # Cap by participation rate
            max_shares = int(interval["avg_volume"] * max_participation)
            if target_shares > max_shares and max_shares > 0:
                target_shares = max_shares
                warnings.append(
                    f"{interval['time_start']}: capped at {max_participation:.0%} participation "
                    f"({max_shares:,} shares vs {int(total_shares * slice_pct):,} target)"
                )
            
            # Ensure we don't exceed remaining
            target_shares = min(target_shares, remaining_shares)
            remaining_shares -= target_shares
            
            # Participation rate
            participation = target_shares / interval["avg_volume"] if interval["avg_volume"] > 0 else 0
            
            # Limit price suggestion
            if side == "BUY":
                limit_price = interval["avg_price"] * 1.001  # slight premium for fills
            else:
                limit_price = interval["avg_price"] * 0.999  # slight discount
            
            # Urgency based on volume
            if interval["pct_of_day"] > 0.1:
                urgency = "HIGH"  # high volume period — get it done
            elif interval["pct_of_day"] > 0.05:
                urgency = "MEDIUM"
            else:
                urgency = "LOW"  # low volume — be patient
            
            slices.append(ExecutionSlice(
                slice_num=idx + 1,
                time_start=interval["time_start"],
                time_end=interval["time_end"],
                target_shares=target_shares,
                target_pct=slice_pct,
                historical_volume=interval["avg_volume"],
                participation_rate=participation,
                limit_price=round(limit_price, 2),
                urgency=urgency,
            ))
        
        # Distribute any remaining shares to highest-volume intervals
        if remaining_shares > 0:
            sorted_slices = sorted(slices, key=lambda s: s.historical_volume, reverse=True)
            for s in sorted_slices:
                add = min(remaining_shares, int(s.historical_volume * 0.05))
                s.target_shares += add
                remaining_shares -= add
                if remaining_shares <= 0:
                    break
    
    # ── TWAP strategy: equal slices ──
    elif strategy == "TWAP":
        shares_per_slice = total_shares // len(intervals)
        remainder = total_shares % len(intervals)
        
        slices = []
        for idx, interval in enumerate(intervals):
            target = shares_per_slice + (1 if idx < remainder else 0)
            participation = target / interval["avg_volume"] if interval["avg_volume"] > 0 else 0
            
            if participation > max_participation:
                warnings.append(f"{interval['time_start']}: TWAP exceeds {max_participation:.0%} participation")
            
            slices.append(ExecutionSlice(
                slice_num=idx + 1,
                time_start=interval["time_start"],
                time_end=interval["time_end"],
                target_shares=target,
                target_pct=1.0 / len(intervals),
                historical_volume=interval["avg_volume"],
                participation_rate=participation,
                limit_price=round(interval["avg_price"], 2),
                urgency="MEDIUM",
            ))
    
    # ── AGGRESSIVE strategy: front-load when spread is tight ──
    elif strategy == "AGGRESSIVE":
        # Weight by inverse spread (tighter spread → more shares)
        spreads = [i["avg_spread_bps"] for i in intervals]
        max_spread = max(spreads) if spreads else 1
        weights = [(max_spread - s + 1) for s in spreads]  # inverse: tight spread = high weight
        total_weight = sum(weights)
        
        slices = []
        remaining = total_shares
        for idx, (interval, weight) in enumerate(zip(intervals, weights)):
            slice_pct = weight / total_weight if total_weight > 0 else 1.0 / len(intervals)
            target = int(total_shares * slice_pct)
            target = min(target, remaining, int(interval["avg_volume"] * max_participation * 1.5))
            remaining -= target
            
            participation = target / interval["avg_volume"] if interval["avg_volume"] > 0 else 0
            
            slices.append(ExecutionSlice(
                slice_num=idx + 1,
                time_start=interval["time_start"],
                time_end=interval["time_end"],
                target_shares=target,
                target_pct=slice_pct,
                historical_volume=interval["avg_volume"],
                participation_rate=participation,
                limit_price=round(interval["avg_price"] * (1.002 if side == "BUY" else 0.998), 2),
                urgency="HIGH" if interval["avg_spread_bps"] < np.median(spreads) else "LOW",
            ))
    
    # Estimate execution VWAP
    total_cost = sum(s.target_shares * s.limit_price for s in slices)
    total_executed = sum(s.target_shares for s in slices)
    est_vwap = total_cost / total_executed if total_executed > 0 else arrival_price
    
    # Estimate slippage
    slippage_bps = abs(est_vwap - arrival_price) / arrival_price * 10000 if arrival_price > 0 else 0
    
    # Estimate duration
    active_slices = [s for s in slices if s.target_shares > 0]
    if active_slices:
        duration = len(active_slices) * SLICE_INTERVAL_MINUTES
    else:
        duration = 0
    
    # Order size warnings
    if total_shares > profile.daily_avg_volume * 0.25:
        warnings.append(f"Order is {total_shares/profile.daily_avg_volume:.0%} of daily volume — high impact risk")
    if total_shares > profile.daily_avg_volume:
        warnings.append(f"Order EXCEEDS daily volume — consider multi-day execution")
    
    return ExecutionPlan(
        symbol=symbol,
        side=side,
        total_shares=total_shares,
        strategy=strategy,
        max_participation=max_participation,
        slices=slices,
        estimated_vwap=round(est_vwap, 2),
        estimated_slippage_bps=round(slippage_bps, 1),
        estimated_duration_min=duration,
        arrival_price=arrival_price,
        warnings=warnings,
    )


def evaluate_execution(
    symbol: str,
    date_str: str,
    side: str,
    executed_shares: list[dict],  # [{time, shares, price}]
) -> dict:
    """
    Evaluate how well an execution performed vs VWAP benchmark.
    
    executed_shares: list of fills [{time: "10:15", shares: 5000, price: 276.50}]
    
    Returns performance metrics.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Get market VWAP for the day
    bars = con.execute("""
        SELECT timestamp, close, volume FROM ohlcv_5s
        WHERE symbol = ? AND date = ?
        ORDER BY timestamp
    """, [symbol, date_str]).df()
    
    con.close()
    
    if bars.empty:
        return {"error": "No intraday data for evaluation"}
    
    # Market VWAP
    market_vwap = (bars["close"] * bars["volume"]).sum() / bars["volume"].sum()
    
    # Execution VWAP
    exec_df = pd.DataFrame(executed_shares)
    exec_vwap = (exec_df["price"] * exec_df["shares"]).sum() / exec_df["shares"].sum()
    
    # Arrival price (first bar of the day)
    arrival = bars.iloc[0]["close"]
    
    # Metrics
    vwap_slippage_bps = (exec_vwap - market_vwap) / market_vwap * 10000
    impl_shortfall_bps = (exec_vwap - arrival) / arrival * 10000
    
    if side == "SELL":
        vwap_slippage_bps = -vwap_slippage_bps
        impl_shortfall_bps = -impl_shortfall_bps
    
    return {
        "market_vwap": round(market_vwap, 2),
        "execution_vwap": round(exec_vwap, 2),
        "arrival_price": round(arrival, 2),
        "vwap_slippage_bps": round(vwap_slippage_bps, 1),
        "implementation_shortfall_bps": round(impl_shortfall_bps, 1),
        "total_shares": exec_df["shares"].sum(),
        "total_cost": (exec_df["price"] * exec_df["shares"]).sum(),
        "num_fills": len(exec_df),
        "beat_vwap": vwap_slippage_bps < 0,
    }


def compare_strategies(symbol: str, total_shares: int, side: str = "BUY") -> pd.DataFrame:
    """
    Compare VWAP vs TWAP vs AGGRESSIVE execution plans side by side.
    """
    results = []
    for strategy in ["VWAP", "TWAP", "AGGRESSIVE"]:
        plan = generate_execution_plan(
            symbol=symbol, side=side, total_shares=total_shares,
            strategy=strategy
        )
        
        # Compute concentration metrics
        shares_list = [s.target_shares for s in plan.slices]
        max_slice_pct = max(s.target_pct for s in plan.slices) if plan.slices else 0
        participation_rates = [s.participation_rate for s in plan.slices if s.target_shares > 0]
        max_participation = max(participation_rates) if participation_rates else 0
        
        results.append({
            "strategy": strategy,
            "est_vwap": plan.estimated_vwap,
            "est_slippage_bps": plan.estimated_slippage_bps,
            "duration_min": plan.estimated_duration_min,
            "active_slices": len([s for s in plan.slices if s.target_shares > 0]),
            "max_slice_pct": max_slice_pct,
            "max_participation": max_participation,
            "warnings": len(plan.warnings),
        })
    
    return pd.DataFrame(results)
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_vwap.py`:

### Tab 1: Volume Profile
```
For selected symbol:
├── Intraday volume profile bar chart (vertical bars, X=time, Y=avg volume)
│   ├── Color intensity by volume (darker = more volume)
│   ├── Overlay: spread (line) on secondary axis
│   └── Highlight: open/close auction periods
├── Summary cards: Daily Avg Volume, POC Price, Value Area (Low-High), Typical VWAP
├── Volume distribution: % of daily volume per 15-min interval
├── Heatmap: volume by day-of-week × time-of-day (Mon is different from Fri)
└── Profile lookback selector: 5 days / 10 / 20 / 60
```

### Tab 2: Execution Planner
```
├── Order input:
│   ├── Symbol selector
│   ├── Side: BUY / SELL
│   ├── Total shares: number input
│   ├── Strategy: VWAP / TWAP / AGGRESSIVE
│   ├── Max participation: slider 5%-30% (default 15%)
│   ├── Start time: time picker (default 09:30)
│   ├── End time: time picker (default 15:30)
│   └── [Generate Plan] button
├── Execution schedule table:
│   Slice | Time | Target Shares | % of Order | Hist Volume | Participation | Limit Price | Urgency
├── Visual: stacked area chart — order slices vs market volume
├── Warnings panel (red if order > 25% daily volume)
├── Metrics: Est VWAP, Slippage, Duration, Arrival Price
└── Strategy comparison table (VWAP vs TWAP vs AGGRESSIVE side by side)
```

### Tab 3: Execution Simulator
```
├── Select a historical date to simulate execution
├── Feed the plan into actual intraday data
├── Show: what would have happened if we executed this plan on date X
├── Results: Execution VWAP vs Market VWAP vs Arrival
├── Fill-by-fill log with timestamps
├── Price chart with execution markers (triangles at each fill)
└── Performance: slippage bps, implementation shortfall, beat VWAP?
```

### Tab 4: Benchmark & Research
```
├── Volume profile stability: how consistent is the volume shape across days?
├── VWAP vs TWAP historical comparison across symbols
├── Optimal participation rate analysis
├── Market impact model: slippage vs order size regression
├── PSX-specific patterns:
│   ├── Opening auction volume spike (09:15-09:35)
│   ├── Lunch dip (12:00-13:00)  
│   ├── Closing rush (15:00-15:30)
│   └── Friday extended hours effect
└── Methodology + TCA (Transaction Cost Analysis) explanation
```

### Key chart — Execution plan vs historical volume:
```python
import plotly.graph_objects as go
from plotly.subplots import make_subplots

fig = make_subplots(specs=[[{"secondary_y": True}]])

# Historical volume bars (background)
fig.add_trace(go.Bar(
    x=[s.time_start for s in plan.slices],
    y=[s.historical_volume for s in plan.slices],
    name="Market Volume",
    marker_color="rgba(200,169,110,0.3)",
    width=0.4,
), secondary_y=False)

# Order slices (foreground)
colors = ["#22C55E" if s.urgency == "HIGH" else "#3B82F6" if s.urgency == "MEDIUM" else "#6B7280"
          for s in plan.slices]
fig.add_trace(go.Bar(
    x=[s.time_start for s in plan.slices],
    y=[s.target_shares for s in plan.slices],
    name="Order Slices",
    marker_color=colors,
    width=0.3,
), secondary_y=False)

# Participation rate line
fig.add_trace(go.Scatter(
    x=[s.time_start for s in plan.slices],
    y=[s.participation_rate * 100 for s in plan.slices],
    name="Participation %",
    line=dict(color="#EF4444", width=2, dash="dot"),
), secondary_y=True)

fig.add_hline(y=15, line_dash="dash", line_color="#EF4444", opacity=0.5,
              annotation_text="Max 15%", secondary_y=True)

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=400, barmode="overlay")
fig.update_yaxes(title_text="Volume (shares)", secondary_y=False)
fig.update_yaxes(title_text="Participation %", secondary_y=True)
```

## Step 3: Add to sidebar

```python
st.page_link("page_views/strategy_vwap.py", label="VWAP Execution", icon="🎯")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test volume profile
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.vwap_execution import build_volume_profile

profile = build_volume_profile('OGDC', lookback_days=20)
if profile:
    print(f'Symbol: {profile.symbol}')
    print(f'Days used: {profile.dates_used}')
    print(f'Daily avg volume: {profile.daily_avg_volume:,.0f}')
    print(f'POC: {profile.poc_price:.2f}')
    print(f'Value Area: {profile.value_area_low:.2f} - {profile.value_area_high:.2f}')
    print(f'Typical VWAP: {profile.vwap_typical:.2f}')
    print(f'\nVolume profile ({len(profile.intervals)} intervals):')
    for i in profile.intervals[:5]:
        print(f'  {i[\"time_start\"]}-{i[\"time_end\"]}: {i[\"avg_volume\"]:,.0f} ({i[\"pct_of_day\"]:.1%}) spread: {i[\"avg_spread_bps\"]:.1f}bps')
    print('  ...')
else:
    print('No profile available')
"

# Test execution plan
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.vwap_execution import generate_execution_plan, compare_strategies

# Generate VWAP plan for 500K shares of OGDC
plan = generate_execution_plan('OGDC', side='BUY', total_shares=500000, strategy='VWAP')
print(f'Strategy: {plan.strategy}')
print(f'Total: {plan.total_shares:,} shares')
print(f'Est VWAP: {plan.estimated_vwap}')
print(f'Est Slippage: {plan.estimated_slippage_bps} bps')
print(f'Duration: {plan.estimated_duration_min} min')
print(f'Slices: {len(plan.slices)}')
print(f'Warnings: {plan.warnings}')
print(f'\nTop 5 slices:')
for s in plan.slices[:5]:
    print(f'  {s.time_start}-{s.time_end}: {s.target_shares:,} shares ({s.target_pct:.1%}) '
          f'participation: {s.participation_rate:.1%} urgency: {s.urgency}')

# Compare strategies
print(f'\n=== Strategy Comparison ===')
comp = compare_strategies('OGDC', 500000, 'BUY')
print(comp.to_string())
"

# Test execution evaluation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.vwap_execution import evaluate_execution
import duckdb

con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
latest = str(con.execute('SELECT MAX(date) FROM ohlcv_5s WHERE symbol=\"OGDC\"').fetchone()[0])
con.close()

# Simulate naive execution (all at open)
result = evaluate_execution('OGDC', latest, 'BUY', [
    {'time': '09:35', 'shares': 500000, 'price': 280.0}
])
print(f'Naive execution eval:')
for k, v in result.items():
    print(f'  {k}: {v}')
"
```

## IMPORTANT NOTES

1. **ohlcv_5s is the core data source** — 5-second bars from DPS, synced to DuckDB
2. **15-minute execution intervals** — granular enough for VWAP matching, practical for manual execution
3. **Max 15% participation** — exceeding this moves the market (on PSX especially)
4. **Three strategies:** VWAP (match volume curve), TWAP (equal slices), AGGRESSIVE (exploit tight spreads)
5. **PSX auction periods:** 09:15-09:30 opening, 15:28-15:30 closing — volume spikes here
6. **Order size warning at 25% daily volume** — above this, multi-day execution recommended
7. **Value Area from 5s bars** — same methodology as Signal Analysis Layer 2
8. **Execution simulator uses real historical bars** — not synthetic data
9. **Implementation Shortfall** = execution cost vs. arrival price — the true cost of trading
10. **No TA libraries** — all in numpy/pandas
11. **Add under STRATEGIES** in sidebar as the final strategy
12. **This is an EXECUTION tool, not a signal** — it tells you HOW to execute, not WHAT to trade
13. **Combine with other strategies:** Strategy 1-4 generate signals, Strategy 5 executes them optimally
