# Claude Code Prompt: Strategy 1 — VPIN Regime-Switching Signal Engine

## Context

pakfindata already computes VPIN (Volume-Synchronized Probability of Informed Trading) 
on the Microstructure page. This strategy turns VPIN into a tradeable signal by combining 
it with Hurst exponent regime detection.

**The edge:** On NYSE, VPIN signals last microseconds. On PSX, they persist for hours 
because algo penetration is near zero. A VPIN spike at 11 AM can still be actionable at 2 PM.

## What already exists

```bash
# Find existing VPIN code
grep -rn "vpin\|VPIN\|toxicity" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Find existing Hurst code
grep -rn "hurst\|Hurst" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -10

# Check what data is available
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
print('tick_logs:', con.execute('SELECT COUNT(*), MIN(date), MAX(date) FROM tick_logs').fetchone())
print('eod_ohlcv:', con.execute('SELECT COUNT(*), MIN(date), MAX(date) FROM eod_ohlcv').fetchone())
con.close()
"
```

**READ ALL OUTPUT before proceeding.**

## Step 1: Create the VPIN Signal Engine

Create `src/pakfindata/engine/vpin_strategy.py`:

```python
"""
VPIN Regime-Switching Strategy Engine.

Combines:
  1. VPIN toxicity (from tick data) — measures informed trading probability
  2. Hurst exponent (from EOD data) — determines trending vs mean-reverting regime
  3. Signal generation — when to enter/exit based on VPIN state transitions

Signal Logic:
  - VPIN > 0.7 → TOXIC: reduce exposure, tighten stops (informed traders active)
  - VPIN drops from >0.7 to <0.4 → CLEARING: toxic flow absorbed, enter in Hurst direction
  - VPIN < 0.3 → SAFE: normal trading, use Hurst regime for strategy selection
  - Hurst > 0.55 → TRENDING: use momentum (follow the move)
  - Hurst < 0.45 → MEAN-REVERTING: fade extremes (buy dips, sell rips)
  - Hurst 0.45-0.55 → RANDOM WALK: no edge, reduce size

PSX-Specific Constants:
  - Trading days: 245/year
  - Circuit breakers: ±7.5%
  - Market hours: 09:30-15:30 (Mon-Thu), 09:30-16:30 (Fri)
  - Tick epoch unit: 's' (seconds), timezone: Asia/Karachi (PKT = UTC+5)
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245


class VPINState(Enum):
    SAFE = "SAFE"           # VPIN < 0.3 — normal market
    ELEVATED = "ELEVATED"   # VPIN 0.3-0.5 — caution
    WARNING = "WARNING"     # VPIN 0.5-0.7 — reduce size
    TOXIC = "TOXIC"         # VPIN > 0.7 — exit/hedge
    CLEARING = "CLEARING"   # VPIN dropping from TOXIC — entry signal


class HurstRegime(Enum):
    TRENDING = "TRENDING"           # H > 0.55
    RANDOM_WALK = "RANDOM_WALK"     # H 0.45-0.55
    MEAN_REVERTING = "MEAN_REVERTING"  # H < 0.45


@dataclass
class VPINSignal:
    symbol: str
    date: str
    timestamp: str
    vpin: float
    vpin_state: VPINState
    hurst: float
    hurst_regime: HurstRegime
    signal: str          # "BUY", "SELL", "HOLD", "EXIT", "REDUCE"
    confidence: float    # 0-1
    reason: str
    position_size: float  # 0-1 (fraction of max position)


def compute_vpin(ticks_df: pd.DataFrame, n_buckets: int = 50) -> pd.DataFrame:
    """
    Compute VPIN from tick data using bulk volume classification.
    
    Args:
        ticks_df: DataFrame with columns [price, volume, timestamp]
        n_buckets: number of volume buckets
    
    Returns:
        DataFrame with VPIN per bucket
    """
    if ticks_df.empty or len(ticks_df) < 20:
        return pd.DataFrame()
    
    # Total volume
    total_vol = ticks_df["volume"].sum()
    if total_vol <= 0:
        return pd.DataFrame()
    
    bucket_size = total_vol / n_buckets
    if bucket_size <= 0:
        return pd.DataFrame()
    
    # Bulk Volume Classification (BVC)
    # Assign buy/sell volume based on price change direction within each bucket
    results = []
    cum_vol = 0
    bucket_start_idx = 0
    bucket_num = 0
    
    for i in range(len(ticks_df)):
        cum_vol += ticks_df.iloc[i]["volume"]
        
        if cum_vol >= bucket_size or i == len(ticks_df) - 1:
            bucket = ticks_df.iloc[bucket_start_idx:i+1]
            
            if len(bucket) > 0:
                price_start = bucket["price"].iloc[0]
                price_end = bucket["price"].iloc[-1]
                bucket_vol = bucket["volume"].sum()
                
                # BVC: use normalized price change to estimate buy/sell split
                if price_start > 0:
                    z = (price_end - price_start) / price_start
                else:
                    z = 0
                
                # CDF approximation for buy fraction
                from scipy.stats import norm
                buy_frac = norm.cdf(z * 100)  # scale for sensitivity
                
                buy_vol = bucket_vol * buy_frac
                sell_vol = bucket_vol * (1 - buy_frac)
                
                results.append({
                    "bucket": bucket_num,
                    "buy_vol": buy_vol,
                    "sell_vol": sell_vol,
                    "total_vol": bucket_vol,
                    "imbalance": abs(buy_vol - sell_vol),
                    "timestamp": bucket["timestamp"].iloc[-1],
                    "price": price_end,
                })
                
                bucket_num += 1
            
            cum_vol = 0
            bucket_start_idx = i + 1
    
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    
    # VPIN = rolling average of |buy - sell| / total over last N buckets
    window = min(50, len(df))
    df["vpin"] = df["imbalance"].rolling(window, min_periods=1).sum() / \
                 df["total_vol"].rolling(window, min_periods=1).sum()
    
    return df


def compute_hurst(prices: pd.Series, window: int = 100) -> float:
    """
    Compute Hurst exponent using R/S analysis.
    
    Returns:
        Hurst exponent (0.5 = random walk, >0.55 trending, <0.45 mean-reverting)
    """
    if len(prices) < window:
        return 0.5  # default to random walk
    
    returns = np.log(prices / prices.shift(1)).dropna().values[-window:]
    
    if len(returns) < 20:
        return 0.5
    
    # R/S analysis across multiple sub-periods
    rs_values = []
    ns = []
    
    for n in [10, 20, 30, 50, 75, 100]:
        if n > len(returns):
            continue
        
        # Split into sub-periods of length n
        num_periods = len(returns) // n
        if num_periods < 1:
            continue
        
        rs_list = []
        for j in range(num_periods):
            subset = returns[j*n:(j+1)*n]
            mean = np.mean(subset)
            deviate = np.cumsum(subset - mean)
            R = np.max(deviate) - np.min(deviate)
            S = np.std(subset, ddof=1)
            if S > 0:
                rs_list.append(R / S)
        
        if rs_list:
            rs_values.append(np.log(np.mean(rs_list)))
            ns.append(np.log(n))
    
    if len(rs_values) < 3:
        return 0.5
    
    # Linear regression: log(R/S) = H * log(n) + c
    coeffs = np.polyfit(ns, rs_values, 1)
    hurst = coeffs[0]
    
    return np.clip(hurst, 0.0, 1.0)


def classify_vpin_state(vpin: float, prev_state: VPINState = None) -> VPINState:
    """Classify current VPIN into a state."""
    if vpin >= 0.7:
        return VPINState.TOXIC
    elif vpin >= 0.5:
        return VPINState.WARNING
    elif vpin >= 0.3:
        # Check if we're clearing from toxic
        if prev_state in (VPINState.TOXIC, VPINState.WARNING):
            return VPINState.CLEARING
        return VPINState.ELEVATED
    else:
        # Check if we just cleared from toxic
        if prev_state in (VPINState.TOXIC, VPINState.WARNING, VPINState.CLEARING):
            return VPINState.CLEARING
        return VPINState.SAFE


def classify_hurst_regime(hurst: float) -> HurstRegime:
    """Classify Hurst exponent into a regime."""
    if hurst > 0.55:
        return HurstRegime.TRENDING
    elif hurst < 0.45:
        return HurstRegime.MEAN_REVERTING
    else:
        return HurstRegime.RANDOM_WALK


def generate_signal(
    symbol: str,
    vpin: float,
    vpin_state: VPINState,
    hurst: float,
    hurst_regime: HurstRegime,
    price_vs_sma: float,     # price relative to SMA-200 (e.g., +0.05 = 5% above)
    recent_return_5d: float,  # 5-day return
    ofi: float = 0.0,        # Order flow imbalance from tick data
) -> VPINSignal:
    """
    Generate trading signal combining VPIN + Hurst + price context.
    
    The core logic:
    1. VPIN determines RISK LEVEL (position sizing)
    2. Hurst determines STRATEGY TYPE (momentum vs mean-reversion)
    3. Price context determines DIRECTION (long vs short vs flat)
    """
    now = datetime.now(PKT)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    
    signal = "HOLD"
    confidence = 0.0
    reason = ""
    position_size = 0.0
    
    # ── Rule 1: VPIN risk gating ──
    if vpin_state == VPINState.TOXIC:
        signal = "EXIT"
        confidence = 0.9
        reason = f"VPIN {vpin:.3f} > 0.7 — toxic flow, informed traders active"
        position_size = 0.0
        
    elif vpin_state == VPINState.WARNING:
        signal = "REDUCE"
        confidence = 0.7
        reason = f"VPIN {vpin:.3f} elevated — reduce exposure, tighten stops"
        position_size = 0.25
        
    elif vpin_state == VPINState.CLEARING:
        # This is the key signal — toxic flow clearing = entry opportunity
        if hurst_regime == HurstRegime.TRENDING:
            # In trending regime, enter in the direction of the trend
            if price_vs_sma > 0 and recent_return_5d > 0:
                signal = "BUY"
                confidence = 0.8
                reason = f"VPIN clearing ({vpin:.3f}), trending regime (H={hurst:.3f}), price above SMA — momentum long"
                position_size = 0.7
            elif price_vs_sma < 0 and recent_return_5d < 0:
                signal = "SELL"
                confidence = 0.8
                reason = f"VPIN clearing ({vpin:.3f}), trending regime (H={hurst:.3f}), price below SMA — momentum short"
                position_size = 0.7
            else:
                signal = "HOLD"
                confidence = 0.5
                reason = f"VPIN clearing but mixed signals — wait for clarity"
                position_size = 0.3
                
        elif hurst_regime == HurstRegime.MEAN_REVERTING:
            # In mean-reverting regime, fade the recent move
            if recent_return_5d < -0.03:  # dropped 3%+
                signal = "BUY"
                confidence = 0.75
                reason = f"VPIN clearing ({vpin:.3f}), mean-reverting (H={hurst:.3f}), oversold — fade the dip"
                position_size = 0.6
            elif recent_return_5d > 0.03:  # rallied 3%+
                signal = "SELL"
                confidence = 0.75
                reason = f"VPIN clearing ({vpin:.3f}), mean-reverting (H={hurst:.3f}), overbought — fade the rally"
                position_size = 0.6
            else:
                signal = "HOLD"
                confidence = 0.4
                reason = f"VPIN clearing, mean-reverting but no extreme — wait"
                position_size = 0.2
        else:
            signal = "HOLD"
            confidence = 0.3
            reason = f"VPIN clearing but random walk regime — no edge"
            position_size = 0.15
            
    elif vpin_state == VPINState.SAFE:
        # Normal conditions — use Hurst for strategy selection
        if hurst_regime == HurstRegime.TRENDING:
            if price_vs_sma > 0.02 and ofi > 0.2:
                signal = "BUY"
                confidence = 0.6
                reason = f"Safe VPIN ({vpin:.3f}), trending (H={hurst:.3f}), above SMA, positive OFI"
                position_size = 0.5
            elif price_vs_sma < -0.02 and ofi < -0.2:
                signal = "SELL"
                confidence = 0.6
                reason = f"Safe VPIN ({vpin:.3f}), trending (H={hurst:.3f}), below SMA, negative OFI"
                position_size = 0.5
            else:
                signal = "HOLD"
                confidence = 0.3
                reason = f"Safe conditions, trending but no clear direction"
                position_size = 0.3
        else:
            signal = "HOLD"
            confidence = 0.2
            reason = f"Normal conditions, no strong signal"
            position_size = 0.2
    
    else:  # ELEVATED
        signal = "HOLD"
        confidence = 0.4
        reason = f"VPIN {vpin:.3f} elevated — watching for spike or clearing"
        position_size = 0.3
    
    return VPINSignal(
        symbol=symbol,
        date=date_str,
        timestamp=time_str,
        vpin=vpin,
        vpin_state=vpin_state,
        hurst=hurst,
        hurst_regime=hurst_regime,
        signal=signal,
        confidence=confidence,
        reason=reason,
        position_size=position_size,
    )


def backtest_vpin_strategy(
    symbol: str,
    lookback_days: int = 250,
    initial_capital: float = 1_000_000,
) -> dict:
    """
    Backtest VPIN regime-switching strategy on historical data.
    
    For each day:
      1. Compute VPIN from tick data (if available)
      2. Compute Hurst from EOD price history
      3. Generate signal
      4. Track P&L
    
    Returns dict with equity curve, trades, metrics.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Get EOD data
    eod = con.execute("""
        SELECT date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
    """, [symbol, lookback_days]).df()
    
    if eod.empty or len(eod) < 100:
        con.close()
        return {"error": f"Not enough EOD data for {symbol}"}
    
    eod = eod.sort_values("date").reset_index(drop=True)
    
    # Get available tick dates
    tick_dates = [r[0] for r in con.execute("""
        SELECT DISTINCT date FROM tick_logs
        WHERE symbol = ?
        ORDER BY date
    """, [symbol]).fetchall()]
    
    con.close()
    
    # Run backtest
    capital = initial_capital
    position = 0  # shares held
    position_price = 0  # entry price
    equity_curve = []
    trades = []
    prev_vpin_state = VPINState.SAFE
    
    for i in range(100, len(eod)):
        row = eod.iloc[i]
        date_str = str(row["date"])
        close = row["close"]
        
        # Compute Hurst from last 100 days
        prices = eod.iloc[max(0, i-100):i+1]["close"]
        hurst = compute_hurst(prices, window=100)
        hurst_regime = classify_hurst_regime(hurst)
        
        # Get VPIN if tick data available for this date
        vpin = 0.2  # default safe
        if date_str in tick_dates:
            try:
                con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
                ticks = con.execute("""
                    SELECT price, volume, timestamp FROM tick_logs
                    WHERE symbol = ? AND date = ?
                    ORDER BY timestamp
                """, [symbol, date_str]).df()
                con.close()
                
                if len(ticks) > 50:
                    vpin_df = compute_vpin(ticks, n_buckets=50)
                    if not vpin_df.empty:
                        vpin = vpin_df["vpin"].iloc[-1]
            except:
                pass
        
        vpin_state = classify_vpin_state(vpin, prev_vpin_state)
        
        # Price context
        sma_200 = eod.iloc[max(0, i-200):i+1]["close"].mean()
        price_vs_sma = (close - sma_200) / sma_200 if sma_200 > 0 else 0
        recent_return = (close / eod.iloc[max(0, i-5)]["close"]) - 1
        
        # Generate signal
        sig = generate_signal(
            symbol=symbol,
            vpin=vpin,
            vpin_state=vpin_state,
            hurst=hurst,
            hurst_regime=hurst_regime,
            price_vs_sma=price_vs_sma,
            recent_return_5d=recent_return,
        )
        
        # Execute signal
        if sig.signal == "BUY" and position <= 0:
            # Close short if any
            if position < 0:
                pnl = (position_price - close) * abs(position)
                capital += pnl
                trades.append({"date": date_str, "action": "COVER", "price": close, "pnl": pnl})
            
            # Open long
            shares = int((capital * sig.position_size) / close)
            if shares > 0:
                position = shares
                position_price = close
                trades.append({"date": date_str, "action": "BUY", "price": close, "shares": shares, "reason": sig.reason})
        
        elif sig.signal == "SELL" and position >= 0:
            # Close long if any
            if position > 0:
                pnl = (close - position_price) * position
                capital += pnl
                trades.append({"date": date_str, "action": "SELL", "price": close, "pnl": pnl})
            
            # Open short (PSX allows short selling for some symbols)
            shares = int((capital * sig.position_size) / close)
            if shares > 0:
                position = -shares
                position_price = close
                trades.append({"date": date_str, "action": "SHORT", "price": close, "shares": shares, "reason": sig.reason})
        
        elif sig.signal == "EXIT" and position != 0:
            if position > 0:
                pnl = (close - position_price) * position
            else:
                pnl = (position_price - close) * abs(position)
            capital += pnl
            trades.append({"date": date_str, "action": "EXIT", "price": close, "pnl": pnl, "reason": sig.reason})
            position = 0
        
        elif sig.signal == "REDUCE" and abs(position) > 0:
            # Reduce position by half
            reduce_shares = abs(position) // 2
            if reduce_shares > 0:
                if position > 0:
                    pnl = (close - position_price) * reduce_shares
                    position -= reduce_shares
                else:
                    pnl = (position_price - close) * reduce_shares
                    position += reduce_shares
                capital += pnl
                trades.append({"date": date_str, "action": "REDUCE", "price": close, "pnl": pnl, "reason": sig.reason})
        
        # Mark to market
        unrealized = 0
        if position > 0:
            unrealized = (close - position_price) * position
        elif position < 0:
            unrealized = (position_price - close) * abs(position)
        
        equity = capital + unrealized
        equity_curve.append({
            "date": date_str,
            "equity": equity,
            "capital": capital,
            "position": position,
            "vpin": vpin,
            "vpin_state": vpin_state.value,
            "hurst": hurst,
            "hurst_regime": hurst_regime.value,
            "signal": sig.signal,
            "close": close,
        })
        
        prev_vpin_state = vpin_state
    
    # Close any remaining position
    if position != 0:
        final_close = eod.iloc[-1]["close"]
        if position > 0:
            pnl = (final_close - position_price) * position
        else:
            pnl = (position_price - final_close) * abs(position)
        capital += pnl
    
    # Compute metrics
    eq_df = pd.DataFrame(equity_curve)
    eq_returns = eq_df["equity"].pct_change().dropna()
    
    total_return = (capital - initial_capital) / initial_capital
    ann_return = (1 + total_return) ** (TRADING_DAYS / len(eq_df)) - 1
    ann_vol = eq_returns.std() * np.sqrt(TRADING_DAYS) if len(eq_returns) > 0 else 0
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0
    max_dd = (eq_df["equity"] / eq_df["equity"].cummax() - 1).min()
    
    # Buy and hold comparison
    bh_return = (eod.iloc[-1]["close"] / eod.iloc[100]["close"]) - 1
    
    winning_trades = [t for t in trades if t.get("pnl", 0) > 0]
    losing_trades = [t for t in trades if t.get("pnl", 0) < 0]
    
    return {
        "equity_curve": eq_df,
        "trades": pd.DataFrame(trades),
        "metrics": {
            "total_return": total_return,
            "annualized_return": ann_return,
            "annualized_volatility": ann_vol,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_dd,
            "total_trades": len(trades),
            "winning_trades": len(winning_trades),
            "losing_trades": len(losing_trades),
            "win_rate": len(winning_trades) / max(1, len(winning_trades) + len(losing_trades)),
            "buy_hold_return": bh_return,
            "alpha": total_return - bh_return,
        }
    }
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_vpin.py`:

This page has 4 tabs:

### Tab 1: Live Signal
```
Current VPIN state for selected symbol:
├── VPIN gauge (0-1, color-coded: green/yellow/orange/red)
├── Hurst regime badge (TRENDING / RANDOM / MEAN-REVERTING)
├── Current signal: BUY / SELL / HOLD / EXIT / REDUCE
├── Confidence bar (0-100%)
├── Position size recommendation (0-100%)
├── Reason text
└── Signal history (last 10 signals as a table)
```

### Tab 2: Backtest
```
├── Symbol selector
├── Lookback period (1Y / 2Y / All)
├── [Run Backtest] button
├── Metrics cards: Return, Sharpe, MaxDD, Win Rate, Alpha vs B&H
├── Equity curve chart (strategy vs buy-and-hold)
├── Drawdown chart
├── VPIN state overlay on price chart
└── Trade log table
```

### Tab 3: Scanner
```
├── Scan top 50 symbols for current VPIN state
├── Table: Symbol | VPIN | State | Hurst | Regime | Signal | Confidence
├── Sort by confidence (highest first)
├── Color-coded: red=TOXIC, orange=WARNING, green=CLEARING, gray=SAFE
└── Click any row → jumps to Live Signal tab for that symbol
```

### Tab 4: Methodology
```
├── VPIN explanation with formula
├── Hurst exponent explanation
├── Signal matrix table (VPIN state × Hurst regime → action)
├── Position sizing rules
└── Risk management rules
```

### Key Streamlit implementation:

```python
import streamlit as st

def render_page():
    st.title("⚡ VPIN Regime-Switching Strategy")
    st.caption("Order flow toxicity + trend regime detection")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Live Signal", "Backtest", "Scanner", "Methodology"
    ])
    
    # ... implement each tab using the engine functions above
```

### For the VPIN gauge, use Plotly:
```python
import plotly.graph_objects as go

fig = go.Figure(go.Indicator(
    mode="gauge+number",
    value=vpin,
    title={"text": "VPIN Toxicity"},
    gauge={
        "axis": {"range": [0, 1]},
        "bar": {"color": "white"},
        "steps": [
            {"range": [0, 0.3], "color": "#22C55E"},     # Safe
            {"range": [0.3, 0.5], "color": "#EAB308"},    # Elevated
            {"range": [0.5, 0.7], "color": "#F97316"},    # Warning
            {"range": [0.7, 1.0], "color": "#EF4444"},    # Toxic
        ],
    }
))
fig.update_layout(
    paper_bgcolor="#0B0E11",
    font_color="#E0E0E0",
    height=250,
)
st.plotly_chart(fig, use_container_width=True)
```

## Step 3: Add page to sidebar

In `app.py`, add under a new **STRATEGIES** section in the sidebar:

```python
# After RESEARCH section
st.sidebar.markdown("**STRATEGIES**", unsafe_allow_html=True)
st.page_link("page_views/strategy_vpin.py", label="VPIN Regime Switch", icon="⚡")
```

## Step 4: Install scipy if not already

```bash
conda activate psx
pip install scipy --break-system-packages 2>/dev/null || pip install scipy
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test VPIN computation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.vpin_strategy import compute_vpin, compute_hurst, generate_signal
from pakfindata.engine.vpin_strategy import classify_vpin_state, classify_hurst_regime
from pakfindata.engine.vpin_strategy import VPINState, HurstRegime
import duckdb, pandas as pd

# Load tick data for HUBC
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
ticks = con.execute('''
    SELECT price, volume, timestamp FROM tick_logs
    WHERE symbol = \"HUBC\" AND date = (SELECT MAX(date) FROM tick_logs WHERE symbol = \"HUBC\")
    ORDER BY timestamp
''').df()
print(f'Ticks: {len(ticks)}')

# Compute VPIN
vpin_df = compute_vpin(ticks, n_buckets=50)
if not vpin_df.empty:
    latest_vpin = vpin_df['vpin'].iloc[-1]
    print(f'VPIN: {latest_vpin:.4f}')
    print(f'State: {classify_vpin_state(latest_vpin).value}')

# Compute Hurst
eod = con.execute('''
    SELECT close FROM eod_ohlcv WHERE symbol = \"HUBC\" ORDER BY date DESC LIMIT 200
''').df()
con.close()
hurst = compute_hurst(eod['close'], window=100)
print(f'Hurst: {hurst:.4f}')
print(f'Regime: {classify_hurst_regime(hurst).value}')

# Generate signal
sig = generate_signal(
    symbol='HUBC', vpin=latest_vpin,
    vpin_state=classify_vpin_state(latest_vpin),
    hurst=hurst, hurst_regime=classify_hurst_regime(hurst),
    price_vs_sma=0.004, recent_return_5d=0.013
)
print(f'Signal: {sig.signal} | Confidence: {sig.confidence:.0%} | Size: {sig.position_size:.0%}')
print(f'Reason: {sig.reason}')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.vpin_strategy import backtest_vpin_strategy
result = backtest_vpin_strategy('HUBC', lookback_days=300)
if 'error' not in result:
    m = result['metrics']
    print(f'Return: {m[\"total_return\"]:.2%}')
    print(f'Sharpe: {m[\"sharpe_ratio\"]:.2f}')
    print(f'MaxDD: {m[\"max_drawdown\"]:.2%}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Alpha vs B&H: {m[\"alpha\"]:.2%}')
else:
    print(result['error'])
"
```

## IMPORTANT NOTES

1. **scipy** is required for `norm.cdf()` in VPIN BVC — install it first
2. **No TA libraries** — all math in raw numpy/pandas/scipy
3. **PSX-specific:** TRADING_DAYS=245, circuit breakers ±7.5%
4. **VPIN window:** 50 buckets default — tune based on PSX tick volume
5. **Hurst window:** 100 trading days — long enough for regime detection
6. **The CLEARING signal is the key edge** — when VPIN drops from TOXIC back to SAFE, 
   the informed traders have finished. This is the entry point.
7. **Position sizing is signal-dependent** — toxic = 0%, clearing = 70%, safe = 50%
8. **Backtest uses walk-forward** — Hurst computed on rolling window, no future leak
9. **Add under STRATEGIES section** in sidebar, not RESEARCH
10. **DuckDB read_only=True** for all queries — never write
