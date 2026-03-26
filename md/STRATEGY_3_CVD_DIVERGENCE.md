# Claude Code Prompt: Strategy 3 — CVD Divergence Trading

## Context

pakfindata already computes CVD (Cumulative Volume Delta) in Layer 3 of Signal Analysis. 
The CVD chart shows price vs cumulative buy-sell volume. This strategy automates 
divergence detection — when price and CVD disagree, a reversal is likely.

**The concept (Wyckoff in quant form):**
- Price makes new HIGH but CVD makes LOWER high → Distribution → SELL signal
- Price makes new LOW but CVD makes HIGHER low → Accumulation → BUY signal

**Why it works on PSX:** Institutional accumulation/distribution takes days on PSX because 
of low liquidity. CVD captures this invisible hand before price reflects it.

## What already exists

```bash
# Find existing CVD code
grep -rn "cvd\|CVD\|cumulative.*volume.*delta\|cum_delta\|buy_vol\|sell_vol" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check tick_logs structure
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# Sample ticks with price changes to understand buy/sell classification
df = con.execute('''
    SELECT price, volume, bid, ask, \"bidVol\", \"askVol\", change, timestamp
    FROM tick_logs WHERE symbol = \"HUBC\" 
    AND date = (SELECT MAX(date) FROM tick_logs WHERE symbol = \"HUBC\")
    ORDER BY timestamp LIMIT 20
''').df()
print(df.to_string())
print(f'\nTotal ticks: {con.execute(\"SELECT COUNT(*) FROM tick_logs WHERE symbol=\\\"HUBC\\\"\").fetchone()[0]}')
con.close()
"
```

**READ ALL OUTPUT before proceeding.**

## Step 1: Create the CVD Divergence Engine

Create `src/pakfindata/engine/cvd_strategy.py`:

```python
"""
CVD Divergence Trading Strategy.

Detects divergences between price action and Cumulative Volume Delta (CVD).

CVD = running sum of (buy_volume - sell_volume) per tick.
Buy volume: trade at ask or above (aggressive buyer).
Sell volume: trade at bid or below (aggressive seller).

Divergence types:
  BEARISH: Price new high + CVD lower high → distribution → SELL
  BULLISH: Price new low + CVD higher low → accumulation → BUY
  HIDDEN BEARISH: Price lower high + CVD higher high → continuation down
  HIDDEN BULLISH: Price higher low + CVD lower low → continuation up

PSX-Specific:
  - Circuit breakers ±7.5% — divergences near limits are unreliable
  - Low liquidity = CVD divergences persist for hours/days
  - Trading days: 245/year
  - Tick classification: use bid/ask proximity when trade direction unknown
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245


class DivergenceType(Enum):
    BULLISH = "BULLISH"               # Price lower low, CVD higher low → BUY
    BEARISH = "BEARISH"               # Price higher high, CVD lower high → SELL
    HIDDEN_BULLISH = "HIDDEN_BULLISH" # Price higher low, CVD lower low → continuation up
    HIDDEN_BEARISH = "HIDDEN_BEARISH" # Price lower high, CVD higher high → continuation down
    NONE = "NONE"


@dataclass
class CVDDivergence:
    type: DivergenceType
    symbol: str
    date: str
    detected_at: str         # timestamp of detection
    price_pivot_1: float     # first pivot price
    price_pivot_2: float     # second pivot (more recent)
    cvd_pivot_1: float       # CVD at first pivot
    cvd_pivot_2: float       # CVD at second pivot
    signal: str              # "BUY", "SELL", "HOLD"
    confidence: float        # 0-1
    reason: str


@dataclass 
class CVDAnalysis:
    """Full CVD analysis for a symbol on a date."""
    symbol: str
    date: str
    ticks: int
    cvd_final: float          # final CVD value
    cvd_slope: float          # linear slope of CVD (positive = net buying)
    buy_volume: float         # total buy volume
    sell_volume: float        # total sell volume
    buy_sell_ratio: float     # buy / (buy + sell)
    divergences: list         # list of CVDDivergence
    price_highs: list         # detected price swing highs
    price_lows: list          # detected price swing lows
    cvd_at_highs: list        # CVD values at price highs
    cvd_at_lows: list         # CVD values at price lows


def classify_tick_direction(row: pd.Series) -> str:
    """
    Classify a tick as BUY or SELL based on trade price vs bid/ask.
    
    Rules:
      1. If price >= ask → BUY (aggressive buyer lifting the ask)
      2. If price <= bid → SELL (aggressive seller hitting the bid)
      3. If price between bid and ask → use tick rule (compare to previous)
      4. Fallback: use bid/ask midpoint
    """
    price = row["price"]
    bid = row.get("bid", 0)
    ask = row.get("ask", 0)
    
    if ask > 0 and price >= ask:
        return "BUY"
    elif bid > 0 and price <= bid:
        return "SELL"
    elif bid > 0 and ask > 0:
        mid = (bid + ask) / 2
        return "BUY" if price >= mid else "SELL"
    else:
        # Fallback: use price change direction
        change = row.get("change", 0)
        return "BUY" if change >= 0 else "SELL"


def compute_cvd(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Cumulative Volume Delta from tick data.
    
    Returns DataFrame with columns: timestamp, price, volume, direction, 
    delta (buy_vol - sell_vol per tick), cvd (cumulative)
    """
    if ticks.empty:
        return pd.DataFrame()
    
    df = ticks.copy()
    
    # Classify each tick
    df["direction"] = df.apply(classify_tick_direction, axis=1)
    
    # Volume delta per tick
    df["delta"] = np.where(df["direction"] == "BUY", df["volume"], -df["volume"])
    
    # Cumulative volume delta
    df["cvd"] = df["delta"].cumsum()
    
    # Buy and sell volume running totals
    df["buy_vol_cum"] = np.where(df["direction"] == "BUY", df["volume"], 0).cumsum()
    df["sell_vol_cum"] = np.where(df["direction"] == "SELL", df["volume"], 0).cumsum()
    
    return df


def detect_swing_points(series: pd.Series, window: int = 20) -> tuple[list, list]:
    """
    Detect swing highs and swing lows in a series.
    
    A swing high: point where value is highest within ±window bars.
    A swing low: point where value is lowest within ±window bars.
    
    Returns (highs_indices, lows_indices)
    """
    highs = []
    lows = []
    
    values = series.values
    n = len(values)
    
    for i in range(window, n - window):
        left = values[max(0, i-window):i]
        right = values[i+1:min(n, i+window+1)]
        
        if len(left) == 0 or len(right) == 0:
            continue
        
        # Swing high: current > all neighbors in window
        if values[i] >= np.max(left) and values[i] >= np.max(right):
            highs.append(i)
        
        # Swing low: current < all neighbors in window
        if values[i] <= np.min(left) and values[i] <= np.min(right):
            lows.append(i)
    
    return highs, lows


def detect_divergences(
    cvd_df: pd.DataFrame,
    swing_window: int = 20,
    min_price_move: float = 0.005,   # min 0.5% between pivots
    min_cvd_divergence: float = 0.1, # min 10% CVD divergence ratio
) -> list[CVDDivergence]:
    """
    Detect CVD divergences from tick-level CVD data.
    
    Resamples to 5-minute bars first (too noisy at tick level).
    """
    if cvd_df.empty or len(cvd_df) < 50:
        return []
    
    # Resample to 5-minute bars for swing detection
    df = cvd_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Karachi")
    
    bars = df.set_index("timestamp").resample("5min").agg({
        "price": "last",
        "cvd": "last",
        "volume": "sum",
    }).dropna().reset_index()
    
    if len(bars) < swing_window * 3:
        return []
    
    # Detect swing points in price
    price_highs, price_lows = detect_swing_points(bars["price"], window=swing_window)
    
    # Detect swing points in CVD
    cvd_highs, cvd_lows = detect_swing_points(bars["cvd"], window=swing_window)
    
    divergences = []
    symbol = cvd_df.get("symbol", pd.Series([""])).iloc[0] if "symbol" in cvd_df.columns else ""
    date_str = str(cvd_df.get("date", pd.Series([""])).iloc[0]) if "date" in cvd_df.columns else ""
    
    # ── Check for BEARISH divergence (price higher high, CVD lower high) ──
    if len(price_highs) >= 2:
        for i in range(1, len(price_highs)):
            idx1 = price_highs[i-1]
            idx2 = price_highs[i]
            
            p1 = bars.iloc[idx1]["price"]
            p2 = bars.iloc[idx2]["price"]
            c1 = bars.iloc[idx1]["cvd"]
            c2 = bars.iloc[idx2]["cvd"]
            
            # Price higher high
            price_higher = (p2 - p1) / p1 > min_price_move
            # CVD lower high
            cvd_lower = c2 < c1
            
            if price_higher and cvd_lower:
                cvd_ratio = abs(c2 - c1) / max(abs(c1), 1)
                if cvd_ratio > min_cvd_divergence:
                    divergences.append(CVDDivergence(
                        type=DivergenceType.BEARISH,
                        symbol=symbol,
                        date=date_str,
                        detected_at=str(bars.iloc[idx2]["timestamp"]),
                        price_pivot_1=p1,
                        price_pivot_2=p2,
                        cvd_pivot_1=c1,
                        cvd_pivot_2=c2,
                        signal="SELL",
                        confidence=min(1.0, cvd_ratio),
                        reason=f"Price new high ({p2:.2f} > {p1:.2f}) but CVD declining ({c2:.0f} < {c1:.0f}) — distribution"
                    ))
    
    # ── Check for BULLISH divergence (price lower low, CVD higher low) ──
    if len(price_lows) >= 2:
        for i in range(1, len(price_lows)):
            idx1 = price_lows[i-1]
            idx2 = price_lows[i]
            
            p1 = bars.iloc[idx1]["price"]
            p2 = bars.iloc[idx2]["price"]
            c1 = bars.iloc[idx1]["cvd"]
            c2 = bars.iloc[idx2]["cvd"]
            
            # Price lower low
            price_lower = (p1 - p2) / p1 > min_price_move
            # CVD higher low
            cvd_higher = c2 > c1
            
            if price_lower and cvd_higher:
                cvd_ratio = abs(c2 - c1) / max(abs(c1), 1)
                if cvd_ratio > min_cvd_divergence:
                    divergences.append(CVDDivergence(
                        type=DivergenceType.BULLISH,
                        symbol=symbol,
                        date=date_str,
                        detected_at=str(bars.iloc[idx2]["timestamp"]),
                        price_pivot_1=p1,
                        price_pivot_2=p2,
                        cvd_pivot_1=c1,
                        cvd_pivot_2=c2,
                        signal="BUY",
                        confidence=min(1.0, cvd_ratio),
                        reason=f"Price new low ({p2:.2f} < {p1:.2f}) but CVD rising ({c2:.0f} > {c1:.0f}) — accumulation"
                    ))
    
    # ── Check for HIDDEN BEARISH (price lower high, CVD higher high) ──
    if len(price_highs) >= 2:
        for i in range(1, len(price_highs)):
            idx1 = price_highs[i-1]
            idx2 = price_highs[i]
            
            p1 = bars.iloc[idx1]["price"]
            p2 = bars.iloc[idx2]["price"]
            c1 = bars.iloc[idx1]["cvd"]
            c2 = bars.iloc[idx2]["cvd"]
            
            price_lower_high = (p1 - p2) / p1 > min_price_move
            cvd_higher_high = c2 > c1
            
            if price_lower_high and cvd_higher_high:
                cvd_ratio = abs(c2 - c1) / max(abs(c1), 1)
                if cvd_ratio > min_cvd_divergence:
                    divergences.append(CVDDivergence(
                        type=DivergenceType.HIDDEN_BEARISH,
                        symbol=symbol,
                        date=date_str,
                        detected_at=str(bars.iloc[idx2]["timestamp"]),
                        price_pivot_1=p1,
                        price_pivot_2=p2,
                        cvd_pivot_1=c1,
                        cvd_pivot_2=c2,
                        signal="SELL",
                        confidence=min(0.8, cvd_ratio * 0.7),
                        reason=f"Hidden bearish: price failing to make new high but CVD rising — sellers absorbing"
                    ))
    
    # ── Check for HIDDEN BULLISH (price higher low, CVD lower low) ──
    if len(price_lows) >= 2:
        for i in range(1, len(price_lows)):
            idx1 = price_lows[i-1]
            idx2 = price_lows[i]
            
            p1 = bars.iloc[idx1]["price"]
            p2 = bars.iloc[idx2]["price"]
            c1 = bars.iloc[idx1]["cvd"]
            c2 = bars.iloc[idx2]["cvd"]
            
            price_higher_low = (p2 - p1) / p1 > min_price_move
            cvd_lower_low = c2 < c1
            
            if price_higher_low and cvd_lower_low:
                cvd_ratio = abs(c2 - c1) / max(abs(c1), 1)
                if cvd_ratio > min_cvd_divergence:
                    divergences.append(CVDDivergence(
                        type=DivergenceType.HIDDEN_BULLISH,
                        symbol=symbol,
                        date=date_str,
                        detected_at=str(bars.iloc[idx2]["timestamp"]),
                        price_pivot_1=p1,
                        price_pivot_2=p2,
                        cvd_pivot_1=c1,
                        cvd_pivot_2=c2,
                        signal="BUY",
                        confidence=min(0.8, cvd_ratio * 0.7),
                        reason=f"Hidden bullish: price making higher lows but CVD falling — buyers absorbing supply"
                    ))
    
    return divergences


def analyze_cvd(symbol: str, date_str: str = None) -> CVDAnalysis:
    """
    Full CVD analysis for a symbol on a given date.
    Loads ticks, computes CVD, detects divergences.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    if date_str is None:
        # Use most recent date with data
        result = con.execute("""
            SELECT MAX(date) FROM tick_logs WHERE symbol = ?
        """, [symbol]).fetchone()
        date_str = str(result[0]) if result[0] else None
    
    if not date_str:
        con.close()
        return None
    
    ticks = con.execute("""
        SELECT price, volume, bid, ask, "bidVol", "askVol", 
               change, timestamp, date, symbol
        FROM tick_logs
        WHERE symbol = ? AND date = ?
        ORDER BY timestamp
    """, [symbol, date_str]).df()
    
    con.close()
    
    if ticks.empty or len(ticks) < 30:
        return None
    
    # Compute CVD
    cvd_df = compute_cvd(ticks)
    
    # Summary stats
    buy_vol = cvd_df[cvd_df["direction"] == "BUY"]["volume"].sum()
    sell_vol = cvd_df[cvd_df["direction"] == "SELL"]["volume"].sum()
    total = buy_vol + sell_vol
    
    # CVD slope (linear regression)
    x = np.arange(len(cvd_df))
    if len(x) > 1:
        slope = np.polyfit(x, cvd_df["cvd"].values, 1)[0]
    else:
        slope = 0
    
    # Detect divergences
    divergences = detect_divergences(cvd_df, swing_window=15)
    
    # Swing points for visualization
    bars_5m = cvd_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(bars_5m["timestamp"]):
        bars_5m["timestamp"] = pd.to_datetime(bars_5m["timestamp"], unit="s", utc=True)
        bars_5m["timestamp"] = bars_5m["timestamp"].dt.tz_convert("Asia/Karachi")
    
    bars_5m = bars_5m.set_index("timestamp").resample("5min").agg({
        "price": "last", "cvd": "last"
    }).dropna().reset_index()
    
    price_highs_idx, price_lows_idx = detect_swing_points(bars_5m["price"], window=15)
    
    return CVDAnalysis(
        symbol=symbol,
        date=date_str,
        ticks=len(ticks),
        cvd_final=cvd_df["cvd"].iloc[-1],
        cvd_slope=slope,
        buy_volume=buy_vol,
        sell_volume=sell_vol,
        buy_sell_ratio=buy_vol / total if total > 0 else 0.5,
        divergences=divergences,
        price_highs=[bars_5m.iloc[i]["price"] for i in price_highs_idx],
        price_lows=[bars_5m.iloc[i]["price"] for i in price_lows_idx],
        cvd_at_highs=[bars_5m.iloc[i]["cvd"] for i in price_highs_idx],
        cvd_at_lows=[bars_5m.iloc[i]["cvd"] for i in price_lows_idx],
    )


def scan_divergences(symbols: list[str] = None, date_str: str = None) -> pd.DataFrame:
    """
    Scan multiple symbols for CVD divergences on a given date.
    Returns table of all detected divergences sorted by confidence.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    if date_str is None:
        date_str = str(con.execute("SELECT MAX(date) FROM tick_logs").fetchone()[0])
    
    if symbols is None:
        symbols = [r[0] for r in con.execute("""
            SELECT symbol FROM tick_logs
            WHERE date = ?
            GROUP BY symbol
            HAVING COUNT(*) > 100
            ORDER BY COUNT(*) DESC
            LIMIT 50
        """, [date_str]).fetchall()]
    
    con.close()
    
    results = []
    for sym in symbols:
        analysis = analyze_cvd(sym, date_str)
        if analysis is None:
            continue
        
        for div in analysis.divergences:
            results.append({
                "symbol": sym,
                "type": div.type.value,
                "signal": div.signal,
                "confidence": div.confidence,
                "price_1": div.price_pivot_1,
                "price_2": div.price_pivot_2,
                "cvd_1": div.cvd_pivot_1,
                "cvd_2": div.cvd_pivot_2,
                "detected_at": div.detected_at,
                "reason": div.reason,
                "ticks": analysis.ticks,
                "cvd_slope": analysis.cvd_slope,
                "buy_sell_ratio": analysis.buy_sell_ratio,
            })
        
        # Also add non-divergence summary if CVD is extreme
        if abs(analysis.buy_sell_ratio - 0.5) > 0.1:
            bias = "ACCUMULATION" if analysis.buy_sell_ratio > 0.6 else "DISTRIBUTION" if analysis.buy_sell_ratio < 0.4 else "NEUTRAL"
            results.append({
                "symbol": sym,
                "type": f"CVD_BIAS_{bias}",
                "signal": "BUY" if bias == "ACCUMULATION" else "SELL" if bias == "DISTRIBUTION" else "HOLD",
                "confidence": abs(analysis.buy_sell_ratio - 0.5) * 2,
                "price_1": 0, "price_2": 0,
                "cvd_1": 0, "cvd_2": analysis.cvd_final,
                "detected_at": "",
                "reason": f"Buy/Sell ratio: {analysis.buy_sell_ratio:.1%} — {bias.lower()}",
                "ticks": analysis.ticks,
                "cvd_slope": analysis.cvd_slope,
                "buy_sell_ratio": analysis.buy_sell_ratio,
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values("confidence", ascending=False).reset_index(drop=True)


def backtest_cvd_divergence(
    symbol: str,
    hold_bars: int = 20,       # hold for 20 5-min bars (100 min) after signal
    stop_loss_pct: float = 0.025,
    take_profit_pct: float = 0.04,
) -> dict:
    """
    Backtest CVD divergence strategy across all available tick dates.
    
    For each divergence detected:
      - Enter at next bar after detection
      - Hold for max hold_bars (5-min bars)
      - Exit on TP, SL, or time
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    dates = [str(r[0]) for r in con.execute("""
        SELECT DISTINCT date FROM tick_logs
        WHERE symbol = ?
        ORDER BY date
    """, [symbol]).fetchall()]
    
    con.close()
    
    if not dates:
        return {"error": f"No tick data for {symbol}"}
    
    all_trades = []
    all_divergences = []
    
    for date_str in dates:
        analysis = analyze_cvd(symbol, date_str)
        if analysis is None or not analysis.divergences:
            continue
        
        # Load 5-min bars for trade execution
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        ticks = con.execute("""
            SELECT price, volume, timestamp FROM tick_logs
            WHERE symbol = ? AND date = ?
            ORDER BY timestamp
        """, [symbol, date_str]).df()
        con.close()
        
        if ticks.empty:
            continue
        
        if not pd.api.types.is_datetime64_any_dtype(ticks["timestamp"]):
            ticks["timestamp"] = pd.to_datetime(ticks["timestamp"], unit="s", utc=True)
            ticks["timestamp"] = ticks["timestamp"].dt.tz_convert("Asia/Karachi")
        
        bars = ticks.set_index("timestamp").resample("5min").agg({
            "price": ["first", "last", "max", "min"],
            "volume": "sum",
        }).dropna()
        bars.columns = ["open", "close", "high", "low", "volume"]
        bars = bars.reset_index()
        
        for div in analysis.divergences:
            all_divergences.append(div)
            
            # Find bar index after divergence detection
            det_time = pd.to_datetime(div.detected_at)
            entry_idx = None
            for j in range(len(bars)):
                if bars.iloc[j]["timestamp"] >= det_time:
                    entry_idx = j + 1  # enter on NEXT bar
                    break
            
            if entry_idx is None or entry_idx >= len(bars) - 1:
                continue
            
            entry_price = bars.iloc[entry_idx]["open"]
            direction = 1 if div.signal == "BUY" else -1
            
            # Simulate hold
            exit_price = None
            exit_reason = "MAX_HOLD"
            bars_held = 0
            
            for k in range(entry_idx, min(entry_idx + hold_bars, len(bars))):
                bar = bars.iloc[k]
                bars_held = k - entry_idx
                
                if direction == 1:  # Long
                    if (bar["high"] - entry_price) / entry_price >= take_profit_pct:
                        exit_price = entry_price * (1 + take_profit_pct)
                        exit_reason = "TAKE_PROFIT"
                        break
                    if (entry_price - bar["low"]) / entry_price >= stop_loss_pct:
                        exit_price = entry_price * (1 - stop_loss_pct)
                        exit_reason = "STOP_LOSS"
                        break
                else:  # Short
                    if (entry_price - bar["low"]) / entry_price >= take_profit_pct:
                        exit_price = entry_price * (1 - take_profit_pct)
                        exit_reason = "TAKE_PROFIT"
                        break
                    if (bar["high"] - entry_price) / entry_price >= stop_loss_pct:
                        exit_price = entry_price * (1 + stop_loss_pct)
                        exit_reason = "STOP_LOSS"
                        break
            
            if exit_price is None:
                exit_idx = min(entry_idx + hold_bars, len(bars) - 1)
                exit_price = bars.iloc[exit_idx]["close"]
            
            pnl_pct = direction * (exit_price / entry_price - 1)
            
            all_trades.append({
                "date": date_str,
                "symbol": symbol,
                "div_type": div.type.value,
                "direction": "LONG" if direction == 1 else "SHORT",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": pnl_pct,
                "bars_held": bars_held,
                "exit_reason": exit_reason,
                "confidence": div.confidence,
            })
    
    if not all_trades:
        return {"error": "No trades generated", "divergences_found": len(all_divergences)}
    
    trades_df = pd.DataFrame(all_trades)
    
    # Metrics
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]
    
    total_return = (1 + trades_df["pnl_pct"]).prod() - 1
    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    max_dd = (trades_df["cum_return"] / trades_df["cum_return"].cummax() - 1).min()
    
    # By divergence type
    type_stats = trades_df.groupby("div_type").agg(
        trades=("pnl_pct", "count"),
        win_rate=("pnl_pct", lambda x: (x > 0).mean()),
        avg_pnl=("pnl_pct", "mean"),
    ).to_dict("index")
    
    return {
        "trades": trades_df,
        "metrics": {
            "total_trades": len(trades_df),
            "win_rate": len(winning) / len(trades_df),
            "avg_win": winning["pnl_pct"].mean() if len(winning) > 0 else 0,
            "avg_loss": losing["pnl_pct"].mean() if len(losing) > 0 else 0,
            "total_return": total_return,
            "max_drawdown": max_dd,
            "profit_factor": abs(winning["pnl_pct"].sum() / losing["pnl_pct"].sum()) if len(losing) > 0 and losing["pnl_pct"].sum() != 0 else 0,
            "dates_tested": len(dates),
            "divergences_found": len(all_divergences),
            "by_type": type_stats,
        }
    }
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_cvd.py`:

### Tab 1: Live CVD Analysis
```
For selected symbol (with date fallback):
├── Price + CVD dual-axis chart (Plotly)
│   ├── Top subplot: price line with swing high/low markers
│   ├── Bottom subplot: CVD line with swing markers
│   ├── Divergence lines connecting pivots (dashed red for bearish, green for bullish)
│   └── Shaded regions where divergence detected
├── Summary cards: CVD Final, CVD Slope, Buy/Sell Ratio, Ticks
├── Divergences table: Type, Signal, Confidence, Pivots, Reason
├── Buy vs Sell volume bar (horizontal stacked)
└── CVD slope indicator (arrow up/down/flat)
```

### Tab 2: Backtest
```
├── Symbol selector
├── Parameters:
│   ├── Hold bars: slider 5-40 (default 20)
│   ├── Stop loss: 1%-5% (default 2.5%)
│   ├── Take profit: 2%-10% (default 4%)
│   └── Swing detection window: 10-30 (default 15)
├── [Run Backtest]
├── Metrics: Trades, Win Rate, PF, Return, MaxDD
├── By divergence type breakdown table
├── Equity curve
├── Trade log
└── P&L distribution histogram
```

### Tab 3: Scanner
```
├── Scan top 50 symbols for divergences on latest date
├── Table: Symbol | Type | Signal | Confidence | Buy/Sell Ratio | CVD Slope | Ticks
├── Color: green=BULLISH, red=BEARISH, blue=HIDDEN
├── Sort by confidence
└── CVD bias column (accumulation vs distribution)
```

### Tab 4: Methodology
```
├── Divergence type diagrams (4 types illustrated)
├── Wyckoff theory connection
├── Swing point detection explanation
├── Why CVD divergences work on PSX (low HFT, institutional persistence)
└── Risk parameters explanation
```

### Key chart — Price vs CVD with divergence lines:
```python
from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                    row_heights=[0.6, 0.4], vertical_spacing=0.05,
                    subplot_titles=["Price", "CVD"])

# Price
fig.add_trace(go.Scatter(x=bars.index, y=bars["price"],
    line=dict(color="#E0E0E0", width=1), name="Price"), row=1, col=1)

# Swing highs/lows on price
fig.add_trace(go.Scatter(x=[bars.index[i] for i in highs], 
    y=[bars.iloc[i]["price"] for i in highs],
    mode="markers", marker=dict(symbol="triangle-down", size=10, color="#EF4444"),
    name="Swing High"), row=1, col=1)

fig.add_trace(go.Scatter(x=[bars.index[i] for i in lows],
    y=[bars.iloc[i]["price"] for i in lows],
    mode="markers", marker=dict(symbol="triangle-up", size=10, color="#22C55E"),
    name="Swing Low"), row=1, col=1)

# CVD
fig.add_trace(go.Scatter(x=bars.index, y=bars["cvd"],
    line=dict(color="#C8A96E", width=1.5), name="CVD", fill="tozeroy",
    fillcolor="rgba(200,169,110,0.1)"), row=2, col=1)

# Divergence lines (connect pivot pairs)
for div in divergences:
    color = "#EF4444" if "BEARISH" in div.type.value else "#22C55E"
    # Draw line on price subplot
    # Draw line on CVD subplot
    # These connect the two pivots showing the divergence visually

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=600)
```

## Step 3: Add to sidebar

```python
st.page_link("page_views/strategy_cvd.py", label="CVD Divergence", icon="🔀")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test CVD computation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.cvd_strategy import analyze_cvd, scan_divergences

# Single symbol analysis
analysis = analyze_cvd('HUBC')
if analysis:
    print(f'Symbol: {analysis.symbol}')
    print(f'Date: {analysis.date}')
    print(f'Ticks: {analysis.ticks}')
    print(f'CVD Final: {analysis.cvd_final:,.0f}')
    print(f'CVD Slope: {analysis.cvd_slope:,.2f}')
    print(f'Buy/Sell: {analysis.buy_sell_ratio:.1%}')
    print(f'Divergences: {len(analysis.divergences)}')
    for d in analysis.divergences:
        print(f'  {d.type.value}: {d.signal} ({d.confidence:.0%}) — {d.reason}')
else:
    print('No analysis available')
"

# Scan multiple symbols
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.cvd_strategy import scan_divergences

df = scan_divergences()
print(f'Results: {len(df)}')
if not df.empty:
    print(df[['symbol','type','signal','confidence','buy_sell_ratio']].head(15).to_string())
"

# Backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.cvd_strategy import backtest_cvd_divergence

result = backtest_cvd_divergence('HUBC')
if 'error' not in result:
    m = result['metrics']
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Profit Factor: {m[\"profit_factor\"]:.2f}')
    print(f'Return: {m[\"total_return\"]:.2%}')
    print(f'MaxDD: {m[\"max_drawdown\"]:.2%}')
    print(f'By type: {m[\"by_type\"]}')
else:
    print(result)
"
```

## IMPORTANT NOTES

1. **4 divergence types:** Regular Bullish, Regular Bearish, Hidden Bullish, Hidden Bearish
2. **Swing detection uses ±15 bar window** on 5-min bars (75 min lookback each side)
3. **Tick classification:** bid/ask proximity method (price >= ask = BUY, price <= bid = SELL)
4. **CVD resampled to 5-min** for swing detection — tick level is too noisy
5. **Stop loss 2.5%, take profit 4%** — asymmetric for positive expectancy
6. **Max hold 20 bars (100 min)** — divergences resolve within a session
7. **Scanner shows CVD_BIAS** entries too — even without divergence, extreme buy/sell ratio is informative
8. **No TA libraries** — all in numpy/pandas
9. **Add under STRATEGIES** in sidebar after OFI Alpha
10. **Existing CVD in Layer 3** computes CVD slope already — this engine adds pivot detection + divergence logic on top
11. **Hidden divergences have lower confidence** (capped at 0.8 × 0.7) — they're continuation signals, less reliable
12. **PSX edge:** Institutional accumulation on PSX takes DAYS. A CVD divergence detected intraday often leads to a multi-day move.
