# Claude Code Prompt: Strategy 2 — Order Flow Imbalance (OFI) Alpha

## Context

pakfindata already computes OFI in Layer 3 of Signal Analysis (showing as "Recent OFI (15m)"). 
Your tick data has bid/ask/bidVol/askVol on every tick — this is the raw material for OFI.

**The strategy:** When 15-minute OFI exceeds a threshold, the next bar tends to move in the 
same direction. On PSX, OFI signals persist because there are no HFT firms arbitraging them away.

**What OFI measures:** Net buying vs selling pressure at the best bid/ask. When bidVol increases 
(buyers stacking) and askVol decreases (sellers pulling), OFI goes positive → price likely to rise.

## What already exists

```bash
# Find existing OFI code
grep -rn "ofi\|OFI\|order.*flow\|imbalance\|bidVol\|askVol" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check tick_logs columns — confirm bidVol/askVol exist
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
print('tick_logs columns:')
for c in con.execute('DESCRIBE tick_logs').fetchall():
    print(f'  {c[0]}: {c[1]}')
print()

# Sample 5 rows to see actual data
df = con.execute('''
    SELECT symbol, price, volume, bid, ask, \"bidVol\", \"askVol\", timestamp, date
    FROM tick_logs WHERE symbol = \"HUBC\" 
    ORDER BY timestamp DESC LIMIT 5
''').df()
print(df.to_string())
con.close()
"
```

**READ ALL OUTPUT — especially column names. They may be quoted or different.**

## Step 1: Create the OFI Strategy Engine

Create `src/pakfindata/engine/ofi_strategy.py`:

```python
"""
Order Flow Imbalance (OFI) Alpha Strategy.

OFI = Σ(ΔbidVol × I(bid unchanged or up)) - Σ(ΔaskVol × I(ask unchanged or down))

Simplified for PSX tick data:
  OFI_t = bidVol_t - askVol_t (when best bid/ask are at same level)
  Normalized OFI = OFI / (bidVol + askVol)

Signal: When 15-min normalized OFI exceeds threshold → predict next-bar direction.

Academic basis: Cont, Kukanov & Stoikov (2014) "The Price Impact of Order Book Events"
showed OFI explains ~65% of short-term price changes.

PSX-Specific:
  - Trading days: 245/year
  - Tick data has: price, volume, bid, ask, bidVol, askVol, timestamp
  - Circuit breakers: ±7.5%
  - No HFT → OFI signals persist 15-60 minutes
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245


@dataclass
class OFISignal:
    symbol: str
    date: str
    bar_end: str           # end of the 15-min bar
    ofi_raw: float         # raw OFI (bidVol - askVol sum)
    ofi_normalized: float  # normalized OFI (-1 to +1)
    signal: str            # "LONG", "SHORT", "FLAT"
    strength: float        # abs(ofi_normalized), 0-1
    predicted_direction: float  # expected next-bar return direction
    tick_count: int        # number of ticks in this bar


def load_ticks_for_ofi(symbol: str, date_str: str) -> pd.DataFrame:
    """Load tick data with bid/ask volumes for OFI computation."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Try DuckDB tick_logs first
    df = con.execute("""
        SELECT price, volume, bid, ask, 
               "bidVol", "askVol", 
               timestamp, date
        FROM tick_logs
        WHERE symbol = ? AND date = ?
        ORDER BY timestamp
    """, [symbol, date_str]).df()
    
    con.close()
    return df


def compute_ofi_bars(ticks: pd.DataFrame, bar_minutes: int = 15) -> pd.DataFrame:
    """
    Compute OFI for each time bar.
    
    OFI per tick:
      If bid_t >= bid_{t-1}: buy_pressure = bidVol_t - bidVol_{t-1} (positive = buyers adding)
      If ask_t <= ask_{t-1}: sell_pressure = askVol_t - askVol_{t-1} (positive = sellers adding)
      OFI_tick = buy_pressure - sell_pressure
    
    Simplified (when we don't have reliable level changes):
      OFI_tick = bidVol_t - askVol_t (instantaneous imbalance)
    
    Aggregated per bar:
      OFI_bar = mean(OFI_tick) over bar period
      Normalized = OFI_bar / mean(bidVol + askVol)
    """
    if ticks.empty or len(ticks) < 10:
        return pd.DataFrame()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(ticks["timestamp"]):
        ticks["timestamp"] = pd.to_datetime(ticks["timestamp"], unit="s", utc=True)
        ticks["timestamp"] = ticks["timestamp"].dt.tz_convert("Asia/Karachi")
    
    # Compute per-tick OFI
    bid_vol = ticks["bidVol"].fillna(0)
    ask_vol = ticks["askVol"].fillna(0)
    
    # Method 1: Simple instantaneous imbalance
    ticks = ticks.copy()
    ticks["ofi_instant"] = bid_vol - ask_vol
    ticks["total_depth"] = bid_vol + ask_vol
    
    # Method 2: Delta-based (changes in bid/ask volume)
    ticks["bid_delta"] = bid_vol.diff().fillna(0)
    ticks["ask_delta"] = ask_vol.diff().fillna(0)
    ticks["bid_change"] = ticks["bid"].diff().fillna(0)
    ticks["ask_change"] = ticks["ask"].diff().fillna(0)
    
    # OFI per Cont et al: 
    # If bid goes up or stays: add bid_delta to OFI
    # If ask goes down or stays: subtract ask_delta from OFI
    ticks["ofi_delta"] = 0.0
    mask_bid_up = ticks["bid_change"] >= 0
    mask_ask_dn = ticks["ask_change"] <= 0
    ticks.loc[mask_bid_up, "ofi_delta"] += ticks.loc[mask_bid_up, "bid_delta"]
    ticks.loc[mask_ask_dn, "ofi_delta"] -= ticks.loc[mask_ask_dn, "ask_delta"]
    
    # Resample to bars
    ticks = ticks.set_index("timestamp")
    
    bars = ticks.resample(f"{bar_minutes}min").agg({
        "price": ["first", "last", "max", "min"],
        "volume": "sum",
        "ofi_instant": "mean",
        "ofi_delta": "sum",
        "total_depth": "mean",
        "bid": "last",
        "ask": "last",
        "bidVol": "last",
        "askVol": "last",
    }).dropna()
    
    # Flatten columns
    bars.columns = [
        "open", "close", "high", "low",
        "volume",
        "ofi_instant_mean",
        "ofi_delta_sum",
        "avg_depth",
        "bid", "ask", "bidVol", "askVol",
    ]
    
    # Normalize OFI
    bars["ofi_normalized"] = bars["ofi_instant_mean"] / bars["avg_depth"].replace(0, np.nan)
    bars["ofi_normalized"] = bars["ofi_normalized"].fillna(0).clip(-1, 1)
    
    # Also compute from delta method
    bars["ofi_delta_norm"] = bars["ofi_delta_sum"] / bars["volume"].replace(0, np.nan)
    bars["ofi_delta_norm"] = bars["ofi_delta_norm"].fillna(0).clip(-1, 1)
    
    # Bar return
    bars["bar_return"] = bars["close"] / bars["open"] - 1
    
    # Next bar return (for signal evaluation)
    bars["next_return"] = bars["bar_return"].shift(-1)
    
    # Spread
    bars["spread"] = bars["ask"] - bars["bid"]
    bars["spread_bps"] = (bars["spread"] / bars["close"] * 10000)
    
    # Tick count per bar
    bars["tick_count"] = ticks.resample(f"{bar_minutes}min")["price"].count()
    
    bars = bars.reset_index()
    bars = bars.rename(columns={"timestamp": "bar_time"})
    
    return bars


def generate_ofi_signals(
    bars: pd.DataFrame,
    long_threshold: float = 0.3,
    short_threshold: float = -0.3,
    min_ticks: int = 20,
) -> list[OFISignal]:
    """
    Generate trading signals from OFI bars.
    
    Rules:
      - OFI_normalized > long_threshold → LONG (buyers dominant)
      - OFI_normalized < short_threshold → SHORT (sellers dominant)
      - Otherwise → FLAT
      - Require minimum tick count for signal validity
    """
    signals = []
    
    for _, bar in bars.iterrows():
        ofi = bar["ofi_normalized"]
        ticks = bar.get("tick_count", 0)
        
        if ticks < min_ticks:
            sig = "FLAT"
            strength = 0
        elif ofi > long_threshold:
            sig = "LONG"
            strength = min(1.0, (ofi - long_threshold) / (1.0 - long_threshold))
        elif ofi < short_threshold:
            sig = "SHORT"
            strength = min(1.0, (short_threshold - ofi) / (1.0 + short_threshold))
        else:
            sig = "FLAT"
            strength = 0
        
        signals.append(OFISignal(
            symbol="",
            date=str(bar.get("bar_time", ""))[:10],
            bar_end=str(bar.get("bar_time", "")),
            ofi_raw=bar.get("ofi_instant_mean", 0),
            ofi_normalized=ofi,
            signal=sig,
            strength=strength,
            predicted_direction=np.sign(ofi) if abs(ofi) > 0.1 else 0,
            tick_count=int(ticks),
        ))
    
    return signals


def backtest_ofi_strategy(
    symbol: str,
    bar_minutes: int = 15,
    long_threshold: float = 0.3,
    short_threshold: float = -0.3,
    min_ticks: int = 20,
    stop_loss_pct: float = 0.02,    # 2% stop loss
    take_profit_pct: float = 0.03,  # 3% take profit
    max_hold_bars: int = 4,         # max bars to hold (1 hour at 15min)
) -> dict:
    """
    Backtest OFI strategy across all available tick dates.
    
    For each bar:
      1. Compute OFI
      2. If OFI > threshold → go long at next bar open
      3. Hold until: take profit, stop loss, or max hold bars
      4. Track P&L
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Get all dates with tick data for this symbol
    dates = [r[0] for r in con.execute("""
        SELECT DISTINCT date FROM tick_logs
        WHERE symbol = ?
        ORDER BY date
    """, [symbol]).fetchall()]
    
    con.close()
    
    if not dates:
        return {"error": f"No tick data for {symbol}"}
    
    all_trades = []
    all_bars = []
    
    for date_str in dates:
        ticks = load_ticks_for_ofi(symbol, str(date_str))
        if ticks.empty or len(ticks) < 50:
            continue
        
        bars = compute_ofi_bars(ticks, bar_minutes=bar_minutes)
        if bars.empty:
            continue
        
        bars["symbol"] = symbol
        all_bars.append(bars)
        
        # Generate signals and simulate trades
        position = None  # {"entry_price", "direction", "entry_bar", "entry_time"}
        
        for i in range(len(bars) - 1):
            bar = bars.iloc[i]
            next_bar = bars.iloc[i + 1]
            ofi = bar["ofi_normalized"]
            ticks_in_bar = bar.get("tick_count", 0)
            
            # Check exit conditions for open position
            if position is not None:
                bars_held = i - position["entry_bar"]
                
                if position["direction"] == "LONG":
                    pnl_pct = (next_bar["open"] / position["entry_price"]) - 1
                else:
                    pnl_pct = 1 - (next_bar["open"] / position["entry_price"])
                
                # Exit conditions
                exit_reason = None
                if pnl_pct >= take_profit_pct:
                    exit_reason = "TAKE_PROFIT"
                elif pnl_pct <= -stop_loss_pct:
                    exit_reason = "STOP_LOSS"
                elif bars_held >= max_hold_bars:
                    exit_reason = "MAX_HOLD"
                elif (position["direction"] == "LONG" and ofi < -0.1):
                    exit_reason = "OFI_REVERSAL"
                elif (position["direction"] == "SHORT" and ofi > 0.1):
                    exit_reason = "OFI_REVERSAL"
                
                if exit_reason:
                    all_trades.append({
                        "date": str(date_str),
                        "symbol": symbol,
                        "direction": position["direction"],
                        "entry_time": position["entry_time"],
                        "entry_price": position["entry_price"],
                        "exit_time": str(next_bar["bar_time"]),
                        "exit_price": next_bar["open"],
                        "pnl_pct": pnl_pct,
                        "bars_held": bars_held,
                        "exit_reason": exit_reason,
                        "entry_ofi": position["entry_ofi"],
                    })
                    position = None
            
            # Check entry conditions (only if flat)
            if position is None and ticks_in_bar >= min_ticks:
                if ofi > long_threshold:
                    position = {
                        "entry_price": next_bar["open"],
                        "direction": "LONG",
                        "entry_bar": i + 1,
                        "entry_time": str(next_bar["bar_time"]),
                        "entry_ofi": ofi,
                    }
                elif ofi < short_threshold:
                    position = {
                        "entry_price": next_bar["open"],
                        "direction": "SHORT",
                        "entry_bar": i + 1,
                        "entry_time": str(next_bar["bar_time"]),
                        "entry_ofi": ofi,
                    }
    
    if not all_trades:
        return {"error": "No trades generated", "bars": pd.concat(all_bars) if all_bars else pd.DataFrame()}
    
    trades_df = pd.DataFrame(all_trades)
    bars_df = pd.concat(all_bars, ignore_index=True) if all_bars else pd.DataFrame()
    
    # Compute metrics
    total_trades = len(trades_df)
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]
    
    avg_win = winning["pnl_pct"].mean() if len(winning) > 0 else 0
    avg_loss = losing["pnl_pct"].mean() if len(losing) > 0 else 0
    win_rate = len(winning) / total_trades if total_trades > 0 else 0
    
    # Profit factor
    gross_profit = winning["pnl_pct"].sum() if len(winning) > 0 else 0
    gross_loss = abs(losing["pnl_pct"].sum()) if len(losing) > 0 else 1
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
    
    # Cumulative returns
    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    total_return = trades_df["cum_return"].iloc[-1] - 1 if len(trades_df) > 0 else 0
    
    # Max drawdown
    cum_max = trades_df["cum_return"].cummax()
    drawdowns = trades_df["cum_return"] / cum_max - 1
    max_dd = drawdowns.min()
    
    # Sharpe (approximate — using per-trade returns)
    if trades_df["pnl_pct"].std() > 0:
        # Annualize: assume ~4 trades per day, 245 days
        trades_per_year = total_trades / max(1, len(dates)) * TRADING_DAYS
        sharpe = (trades_df["pnl_pct"].mean() / trades_df["pnl_pct"].std()) * np.sqrt(trades_per_year)
    else:
        sharpe = 0
    
    # Exit reason breakdown
    exit_reasons = trades_df["exit_reason"].value_counts().to_dict()
    
    # OFI predictive accuracy
    correct_direction = ((trades_df["entry_ofi"] > 0) & (trades_df["pnl_pct"] > 0)) | \
                        ((trades_df["entry_ofi"] < 0) & (trades_df["pnl_pct"] > 0))
    directional_accuracy = correct_direction.mean()
    
    # Long vs Short breakdown
    longs = trades_df[trades_df["direction"] == "LONG"]
    shorts = trades_df[trades_df["direction"] == "SHORT"]
    
    return {
        "trades": trades_df,
        "bars": bars_df,
        "metrics": {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "total_return": total_return,
            "max_drawdown": max_dd,
            "sharpe_ratio": sharpe,
            "directional_accuracy": directional_accuracy,
            "avg_bars_held": trades_df["bars_held"].mean(),
            "exit_reasons": exit_reasons,
            "long_trades": len(longs),
            "short_trades": len(shorts),
            "long_win_rate": (longs["pnl_pct"] > 0).mean() if len(longs) > 0 else 0,
            "short_win_rate": (shorts["pnl_pct"] > 0).mean() if len(shorts) > 0 else 0,
            "dates_tested": len(dates),
        }
    }


def scan_current_ofi(symbols: list[str] = None, bar_minutes: int = 15) -> pd.DataFrame:
    """
    Scan current OFI for multiple symbols using latest available tick data.
    Returns table sorted by absolute OFI strength.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    if symbols is None:
        # Get top 50 symbols by tick count on most recent date
        symbols = [r[0] for r in con.execute("""
            SELECT symbol FROM tick_logs
            WHERE date = (SELECT MAX(date) FROM tick_logs)
            GROUP BY symbol
            ORDER BY COUNT(*) DESC
            LIMIT 50
        """).fetchall()]
    
    latest_date = con.execute("SELECT MAX(date) FROM tick_logs").fetchone()[0]
    con.close()
    
    results = []
    
    for sym in symbols:
        ticks = load_ticks_for_ofi(sym, str(latest_date))
        if ticks.empty or len(ticks) < 30:
            continue
        
        bars = compute_ofi_bars(ticks, bar_minutes=bar_minutes)
        if bars.empty:
            continue
        
        last_bar = bars.iloc[-1]
        
        ofi = last_bar["ofi_normalized"]
        if abs(ofi) > 0.15:  # only show meaningful OFI
            signal = "LONG" if ofi > 0.3 else "SHORT" if ofi < -0.3 else "WEAK"
            results.append({
                "symbol": sym,
                "ofi": ofi,
                "ofi_abs": abs(ofi),
                "signal": signal,
                "price": last_bar["close"],
                "spread_bps": last_bar.get("spread_bps", 0),
                "tick_count": last_bar.get("tick_count", 0),
                "bar_return": last_bar["bar_return"],
                "date": str(latest_date),
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values("ofi_abs", ascending=False).reset_index(drop=True)
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_ofi.py`:

### Tab 1: Live OFI Monitor
```
For selected symbol, show:
├── 15-minute OFI bar chart (green bars = positive OFI, red = negative)
├── Current OFI gauge (-1 to +1)
├── Current signal: LONG / SHORT / FLAT
├── OFI vs Price overlay chart (dual axis: price + OFI line)
├── bid/ask depth bars (visual of bidVol vs askVol)
├── Last 10 bars table with OFI, return, tick count
└── Auto-fallback to most recent date if today not available
```

### Tab 2: Backtest
```
├── Symbol selector
├── Parameter tuning:
│   ├── Bar size: [5min | 15min | 30min | 60min]
│   ├── Long threshold: slider 0.1 to 0.8 (default 0.3)
│   ├── Short threshold: slider -0.8 to -0.1 (default -0.3)
│   ├── Stop loss: slider 0.5% to 5% (default 2%)
│   ├── Take profit: slider 1% to 10% (default 3%)
│   └── Max hold bars: slider 1-10 (default 4)
├── [Run Backtest] button
├── Metric cards: Trades, Win Rate, Profit Factor, Sharpe, MaxDD, Return
├── Equity curve (cumulative return per trade)
├── Trade distribution: histogram of P&L per trade
├── Long vs Short breakdown
├── Exit reason pie chart (take profit / stop loss / max hold / reversal)
└── Trade log table
```

### Tab 3: OFI Scanner
```
├── Scan top 50 symbols on latest date
├── Table: Symbol | OFI | Signal | Price | Spread(bps) | Ticks | Bar Return
├── Color: green rows = LONG, red = SHORT
├── Sort by |OFI| descending (strongest imbalance first)
└── Click symbol → jump to Live tab
```

### Tab 4: OFI Research
```
├── OFI vs Next-Bar Return scatter plot
│   (does high OFI predict positive return? show R²)
├── OFI autocorrelation chart (does OFI persist across bars?)
├── OFI by time of day (is OFI more predictive at open? close?)
├── OFI distribution histogram
├── Threshold optimization: test multiple thresholds, show win rate curve
└── Methodology explanation
```

### Key Plotly chart for OFI vs Price:
```python
import plotly.graph_objects as go
from plotly.subplots import make_subplots

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                    row_heights=[0.7, 0.3], vertical_spacing=0.05)

# Price
fig.add_trace(go.Scatter(x=bars["bar_time"], y=bars["close"],
    name="Price", line=dict(color="#E0E0E0", width=1)), row=1, col=1)

# OFI bars
colors = ["#22C55E" if x > 0 else "#EF4444" for x in bars["ofi_normalized"]]
fig.add_trace(go.Bar(x=bars["bar_time"], y=bars["ofi_normalized"],
    name="OFI", marker_color=colors), row=2, col=1)

# Threshold lines
fig.add_hline(y=0.3, line_dash="dash", line_color="#22C55E", opacity=0.5, row=2, col=1)
fig.add_hline(y=-0.3, line_dash="dash", line_color="#EF4444", opacity=0.5, row=2, col=1)

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#0B0E11",
    plot_bgcolor="#0B0E11",
    height=500,
    showlegend=False,
)
```

## Step 3: Add page to sidebar

In `app.py`, add under STRATEGIES section (after VPIN):

```python
st.page_link("page_views/strategy_ofi.py", label="OFI Alpha", icon="📊")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test OFI computation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.ofi_strategy import load_ticks_for_ofi, compute_ofi_bars, scan_current_ofi

# Load ticks
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
latest = con.execute('SELECT MAX(date) FROM tick_logs').fetchone()[0]
con.close()

ticks = load_ticks_for_ofi('HUBC', str(latest))
print(f'Ticks: {len(ticks)}')
print(f'Columns: {list(ticks.columns)}')
print(f'bidVol range: {ticks[\"bidVol\"].min()}-{ticks[\"bidVol\"].max()}')

# Compute bars
bars = compute_ofi_bars(ticks, bar_minutes=15)
print(f'\nBars: {len(bars)}')
print(bars[['bar_time','ofi_normalized','bar_return','tick_count']].tail(5).to_string())

# Check predictive power
if 'next_return' in bars.columns:
    corr = bars['ofi_normalized'].corr(bars['next_return'])
    print(f'\nOFI-NextReturn correlation: {corr:.4f}')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.ofi_strategy import backtest_ofi_strategy

result = backtest_ofi_strategy('HUBC', bar_minutes=15, long_threshold=0.3, short_threshold=-0.3)
if 'error' not in result:
    m = result['metrics']
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Profit Factor: {m[\"profit_factor\"]:.2f}')
    print(f'Total Return: {m[\"total_return\"]:.2%}')
    print(f'Sharpe: {m[\"sharpe_ratio\"]:.2f}')
    print(f'MaxDD: {m[\"max_drawdown\"]:.2%}')
    print(f'Directional Accuracy: {m[\"directional_accuracy\"]:.0%}')
    print(f'Avg Bars Held: {m[\"avg_bars_held\"]:.1f}')
    print(f'Exit Reasons: {m[\"exit_reasons\"]}')
    print(f'Long WR: {m[\"long_win_rate\"]:.0%}, Short WR: {m[\"short_win_rate\"]:.0%}')
else:
    print(result['error'])
"

# Test scanner
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.ofi_strategy import scan_current_ofi

df = scan_current_ofi()
print(f'Symbols with OFI signal: {len(df)}')
if not df.empty:
    print(df[['symbol','ofi','signal','price','tick_count']].head(10).to_string())
"
```

## IMPORTANT NOTES

1. **Column names may be quoted** — tick_logs uses "bidVol" and "askVol" (camelCase, quoted in SQL)
2. **Two OFI methods included:** instantaneous (bidVol - askVol) and delta-based (Cont et al.)
3. **15-minute bars are default** — PSX has enough ticks for 15min bars on liquid names
4. **Threshold 0.3** means OFI must be 30%+ imbalanced toward buyers/sellers for a signal
5. **Stop loss 2%, take profit 3%** — asymmetric to ensure winners > losers
6. **Max hold 4 bars (1 hour)** — OFI is a short-term signal, don't overstay
7. **OFI reversal exit** — if OFI flips against position, exit early
8. **Scanner only shows |OFI| > 0.15** — filter out noise
9. **No TA libraries** — all in numpy/pandas
10. **Add under STRATEGIES section** in sidebar after VPIN
11. **Backtest uses only available tick dates** — not every EOD day has ticks
12. **R² research tab** is crucial — if OFI doesn't predict next-bar on PSX, the strategy doesn't work
