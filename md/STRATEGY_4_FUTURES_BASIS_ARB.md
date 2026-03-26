# Claude Code Prompt: Strategy 4 — Futures Basis Mean-Reversion

## Context

pakfindata's Derivatives page already shows futures basis (premium/discount) and real OI 
from PSX DFC XLS files. This strategy trades the basis spread when it deviates beyond 
normal range — sell futures + buy spot when basis is too wide, reverse when it narrows.

**The edge:** PSX futures are thinly traded. Basis often overshoots due to low liquidity 
and retail speculation. Mean-reversion is reliable because settlement forces convergence — 
futures MUST converge to spot at expiry. This is a structural, not statistical, edge.

**What basis means:**
- Basis = (Futures Price - Spot Price) / Spot Price × 100
- Positive basis (premium) = market expects price to rise
- Negative basis (discount) = market expects price to fall
- At expiry, basis → 0 (forced convergence)

## What already exists

```bash
# Find existing basis/futures code
grep -rn "basis\|premium\|discount\|futures.*spot\|DFC\|open_interest\|rollover" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check futures data in DuckDB
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# Check what futures tables exist
for t in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main'\").fetchall():
    if 'fut' in t[0].lower() or 'contract' in t[0].lower() or 'deriv' in t[0].lower():
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        print(f'{t[0]}: {count:,}')

con.close()
"

# Check psx.sqlite for futures data
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
tables = [r[0] for r in con.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
for t in tables:
    if 'fut' in t.lower() or 'contract' in t.lower() or 'dfc' in t.lower() or 'deriv' in t.lower():
        count = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in con.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'{t}: {count:,} rows — cols: {cols[:10]}')
con.close()
"

# Sample futures data
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
# Try common table names
for t in ['futures_contracts','dfc_data','contracts','market_summary_fut']:
    try:
        df = con.execute(f'SELECT * FROM {t} LIMIT 3').fetchall()
        cols = [r[1] for r in con.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'\n{t}: {cols}')
        for row in df: print(f'  {row}')
    except: pass
con.close()
"
```

**READ ALL OUTPUT — understand table names, column names, and data format before proceeding.**

## Step 1: Create the Futures Basis Engine

Create `src/pakfindata/engine/basis_strategy.py`:

```python
"""
Futures Basis Mean-Reversion Strategy.

Trades the spread between PSX futures and spot when it deviates 
beyond normal range. Settlement forces convergence — structural edge.

Key concepts:
  Basis = (Futures - Spot) / Spot × 100 (in %)
  Fair basis ≈ risk-free rate × (days to expiry / 365)
  Excess basis = Actual basis - Fair basis
  Signal: when |Excess basis| > 2σ → mean-revert

PSX DFC contracts:
  - Monthly expiry (last Thursday of month)
  - 3 contract months available (current, next, far)
  - Settlement: physical delivery
  - Lot sizes vary by symbol
  - OI data from PSX DFC XLS files (real, not estimated)

PSX-Specific:
  - KIBOR as risk-free rate proxy (from DuckDB/SBP data)
  - Circuit breakers ±7.5% on spot → futures can gap
  - Low liquidity = wide basis spreads = more opportunities
  - Rollover happens ~3 days before expiry
"""

import numpy as np
import pandas as pd
import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
TRADING_DAYS = 245


@dataclass
class BasisSignal:
    symbol: str
    date: str
    spot_price: float
    futures_price: float
    contract_month: str
    days_to_expiry: int
    basis_pct: float          # raw basis in %
    fair_basis_pct: float     # theoretical fair basis
    excess_basis_pct: float   # basis - fair basis
    basis_zscore: float       # z-score of basis vs recent history
    oi_contracts: int         # open interest (real from XLS)
    oi_change_pct: float      # OI change vs previous day
    signal: str               # "SELL_BASIS", "BUY_BASIS", "HOLD"
    confidence: float
    reason: str


def get_kibor_rate() -> float:
    """Get current KIBOR 3M rate as risk-free proxy."""
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        # Try to find KIBOR in any available table
        for query in [
            "SELECT value FROM kibor_daily ORDER BY date DESC LIMIT 1",
            "SELECT offer FROM kibor_rates WHERE tenor='3M' ORDER BY date DESC LIMIT 1",
        ]:
            try:
                result = con.execute(query).fetchone()
                if result and result[0]:
                    con.close()
                    return float(result[0]) / 100  # convert from % to decimal
            except:
                continue
        con.close()
    except:
        pass
    
    # Fallback: current SBP rate environment
    return 0.105  # 10.5% as of March 2026


def get_expiry_date(contract_month: str, year: int = None) -> datetime:
    """
    Get last Thursday of the contract month (PSX futures expiry).
    contract_month: "MAR", "APR", "MAY", etc.
    """
    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    
    if year is None:
        year = datetime.now(PKT).year
    
    month_num = months.get(contract_month.upper(), 0)
    if month_num == 0:
        return None
    
    # Last Thursday of the month
    import calendar
    last_day = calendar.monthrange(year, month_num)[1]
    date = datetime(year, month_num, last_day)
    
    # Walk back to Thursday (3 = Thursday)
    while date.weekday() != 3:
        date -= timedelta(days=1)
    
    return date


def load_futures_data(symbol: str = None, days: int = 90) -> pd.DataFrame:
    """
    Load futures contract data from psx.sqlite.
    Finds the correct table and columns dynamically.
    """
    con = sqlite3.connect(str(PSX_SQLITE))
    
    # Discover futures tables
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    
    futures_table = None
    for t in tables:
        tl = t.lower()
        if any(k in tl for k in ['futures', 'contract', 'dfc', 'market_summary']):
            cols = [r[1] for r in con.execute(f"PRAGMA table_info({t})").fetchall()]
            if any('close' in c.lower() or 'price' in c.lower() for c in cols):
                futures_table = t
                break
    
    if not futures_table:
        con.close()
        return pd.DataFrame()
    
    # Get column names
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({futures_table})").fetchall()]
    
    # Build query based on available columns
    where_clauses = []
    if symbol:
        # Find the symbol/base_symbol column
        sym_col = next((c for c in cols if c.lower() in ['symbol','base_symbol','underlying']), None)
        if sym_col:
            where_clauses.append(f"{sym_col} LIKE '%{symbol}%'")
    
    date_col = next((c for c in cols if c.lower() in ['date','trade_date','trading_date']), None)
    if date_col:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        where_clauses.append(f"{date_col} >= '{cutoff}'")
    
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    df = pd.read_sql(f"SELECT * FROM {futures_table} {where} ORDER BY {date_col or 'ROWID'}", con)
    con.close()
    
    return df


def load_spot_data(symbol: str, days: int = 90) -> pd.DataFrame:
    """Load EOD spot data from DuckDB."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    df = con.execute("""
        SELECT date, close as spot_close, volume as spot_volume
        FROM eod_ohlcv
        WHERE symbol = ? AND date >= ?
        ORDER BY date
    """, [symbol, cutoff]).df()
    
    con.close()
    return df


def compute_basis_history(
    symbol: str,
    days: int = 180,
) -> pd.DataFrame:
    """
    Compute historical basis for a symbol across all contract months.
    Merges futures prices with spot prices by date.
    
    Returns DataFrame with: date, spot, futures, contract_month, 
    days_to_expiry, basis_pct, fair_basis_pct, excess_basis_pct
    """
    futures = load_futures_data(symbol, days)
    spot = load_spot_data(symbol, days)
    
    if futures.empty or spot.empty:
        return pd.DataFrame()
    
    # Normalize column names (futures table structure varies)
    futures_cols = {c.lower(): c for c in futures.columns}
    
    # Find key columns
    date_col = next((futures_cols[k] for k in ['date','trade_date','trading_date'] if k in futures_cols), None)
    close_col = next((futures_cols[k] for k in ['close','last_price','settlement'] if k in futures_cols), None)
    month_col = next((futures_cols[k] for k in ['contract_month','expiry_month','month','contract'] if k in futures_cols), None)
    volume_col = next((futures_cols[k] for k in ['volume','traded_volume','qty'] if k in futures_cols), None)
    
    if not date_col or not close_col:
        return pd.DataFrame()
    
    # Prepare futures data
    fut_df = futures[[date_col, close_col]].copy()
    fut_df.columns = ["date", "futures_close"]
    if month_col:
        fut_df["contract_month"] = futures[month_col]
    else:
        fut_df["contract_month"] = "FRONT"
    if volume_col:
        fut_df["fut_volume"] = futures[volume_col]
    
    fut_df["date"] = pd.to_datetime(fut_df["date"]).dt.strftime("%Y-%m-%d")
    spot["date"] = pd.to_datetime(spot["date"]).dt.strftime("%Y-%m-%d")
    
    # Merge on date
    merged = fut_df.merge(spot, on="date", how="inner")
    
    if merged.empty:
        return pd.DataFrame()
    
    # Compute basis
    merged["futures_close"] = pd.to_numeric(merged["futures_close"], errors="coerce")
    merged["spot_close"] = pd.to_numeric(merged["spot_close"], errors="coerce")
    merged = merged.dropna(subset=["futures_close", "spot_close"])
    merged = merged[merged["spot_close"] > 0]
    
    merged["basis_pct"] = (merged["futures_close"] - merged["spot_close"]) / merged["spot_close"] * 100
    
    # Days to expiry (approximate — use contract month)
    kibor = get_kibor_rate()
    
    def calc_days_to_expiry(row):
        month_str = str(row.get("contract_month", "")).upper()[:3]
        expiry = get_expiry_date(month_str)
        if expiry:
            trade_date = pd.to_datetime(row["date"])
            dte = (expiry - trade_date).days
            return max(0, dte)
        return 30  # default
    
    merged["days_to_expiry"] = merged.apply(calc_days_to_expiry, axis=1)
    
    # Fair basis = cost of carry
    merged["fair_basis_pct"] = kibor * (merged["days_to_expiry"] / 365) * 100
    
    # Excess basis
    merged["excess_basis_pct"] = merged["basis_pct"] - merged["fair_basis_pct"]
    
    # Rolling z-score of excess basis
    window = 20
    merged["excess_mean"] = merged["excess_basis_pct"].rolling(window, min_periods=10).mean()
    merged["excess_std"] = merged["excess_basis_pct"].rolling(window, min_periods=10).std()
    merged["basis_zscore"] = (merged["excess_basis_pct"] - merged["excess_mean"]) / merged["excess_std"].replace(0, np.nan)
    
    return merged.sort_values("date").reset_index(drop=True)


def generate_basis_signals(
    basis_df: pd.DataFrame,
    entry_zscore: float = 2.0,
    exit_zscore: float = 0.5,
) -> list[BasisSignal]:
    """
    Generate basis trading signals.
    
    SELL_BASIS: basis z-score > +entry_zscore (premium too high → sell futures, buy spot)
    BUY_BASIS: basis z-score < -entry_zscore (discount too deep → buy futures, sell spot)
    EXIT: z-score returns within ±exit_zscore
    """
    signals = []
    
    for _, row in basis_df.iterrows():
        z = row.get("basis_zscore", 0)
        
        if pd.isna(z):
            continue
        
        if z > entry_zscore:
            signal = "SELL_BASIS"
            confidence = min(1.0, abs(z) / 4)
            reason = (f"Basis z-score {z:.2f} > {entry_zscore} — premium too high at "
                     f"{row['basis_pct']:.2f}% (fair: {row['fair_basis_pct']:.2f}%). "
                     f"Sell futures, buy spot.")
        elif z < -entry_zscore:
            signal = "BUY_BASIS"
            confidence = min(1.0, abs(z) / 4)
            reason = (f"Basis z-score {z:.2f} < -{entry_zscore} — discount too deep at "
                     f"{row['basis_pct']:.2f}% (fair: {row['fair_basis_pct']:.2f}%). "
                     f"Buy futures, sell spot.")
        elif abs(z) < exit_zscore:
            signal = "EXIT"
            confidence = 0.5
            reason = f"Basis normalized (z={z:.2f}) — close position"
        else:
            signal = "HOLD"
            confidence = 0.2
            reason = f"Basis z-score {z:.2f} within range"
        
        signals.append(BasisSignal(
            symbol=row.get("symbol", ""),
            date=str(row["date"]),
            spot_price=row["spot_close"],
            futures_price=row["futures_close"],
            contract_month=str(row.get("contract_month", "")),
            days_to_expiry=int(row.get("days_to_expiry", 0)),
            basis_pct=row["basis_pct"],
            fair_basis_pct=row.get("fair_basis_pct", 0),
            excess_basis_pct=row.get("excess_basis_pct", 0),
            basis_zscore=z,
            oi_contracts=0,
            oi_change_pct=0,
            signal=signal,
            confidence=confidence,
            reason=reason,
        ))
    
    return signals


def backtest_basis_strategy(
    symbol: str,
    entry_zscore: float = 2.0,
    exit_zscore: float = 0.5,
    max_hold_days: int = 15,
    stop_loss_bps: float = 200,    # 200 bps = 2% stop
    days: int = 365,
) -> dict:
    """
    Backtest basis mean-reversion strategy.
    
    Trade logic:
      Entry: |z-score| > entry_zscore
        → z > +entry: short futures, long spot (sell basis)
        → z < -entry: long futures, short spot (buy basis)
      Exit: |z-score| < exit_zscore OR max_hold OR stop_loss
      P&L: change in basis spread between entry and exit
    """
    basis_df = compute_basis_history(symbol, days=days)
    
    if basis_df.empty or len(basis_df) < 30:
        return {"error": f"Not enough basis data for {symbol}"}
    
    trades = []
    position = None  # {"entry_date", "entry_basis", "direction", "entry_zscore"}
    
    for i, row in basis_df.iterrows():
        z = row.get("basis_zscore", 0)
        if pd.isna(z):
            continue
        
        basis = row["basis_pct"]
        date = str(row["date"])
        
        # Check exits
        if position is not None:
            days_held = (pd.to_datetime(date) - pd.to_datetime(position["entry_date"])).days
            basis_change = basis - position["entry_basis"]
            
            # P&L depends on direction
            if position["direction"] == "SELL_BASIS":
                pnl_bps = -basis_change * 100  # profit when basis narrows
            else:
                pnl_bps = basis_change * 100   # profit when basis widens back
            
            exit_reason = None
            if abs(z) < exit_zscore:
                exit_reason = "MEAN_REVERT"
            elif days_held >= max_hold_days:
                exit_reason = "MAX_HOLD"
            elif pnl_bps < -stop_loss_bps:
                exit_reason = "STOP_LOSS"
            elif row.get("days_to_expiry", 30) <= 3:
                exit_reason = "NEAR_EXPIRY"
            
            if exit_reason:
                trades.append({
                    "symbol": symbol,
                    "entry_date": position["entry_date"],
                    "exit_date": date,
                    "direction": position["direction"],
                    "entry_basis": position["entry_basis"],
                    "exit_basis": basis,
                    "entry_zscore": position["entry_zscore"],
                    "exit_zscore": z,
                    "pnl_bps": pnl_bps,
                    "days_held": days_held,
                    "exit_reason": exit_reason,
                })
                position = None
        
        # Check entries (only if flat)
        if position is None:
            if z > entry_zscore:
                position = {
                    "entry_date": date,
                    "entry_basis": basis,
                    "direction": "SELL_BASIS",
                    "entry_zscore": z,
                }
            elif z < -entry_zscore:
                position = {
                    "entry_date": date,
                    "entry_basis": basis,
                    "direction": "BUY_BASIS",
                    "entry_zscore": z,
                }
    
    # Close any open position at end
    if position is not None and len(basis_df) > 0:
        last = basis_df.iloc[-1]
        basis_change = last["basis_pct"] - position["entry_basis"]
        pnl_bps = (-basis_change if position["direction"] == "SELL_BASIS" else basis_change) * 100
        trades.append({
            "symbol": symbol, "entry_date": position["entry_date"],
            "exit_date": str(last["date"]), "direction": position["direction"],
            "entry_basis": position["entry_basis"], "exit_basis": last["basis_pct"],
            "entry_zscore": position["entry_zscore"], "exit_zscore": last.get("basis_zscore", 0),
            "pnl_bps": pnl_bps, "days_held": 0, "exit_reason": "END_OF_DATA",
        })
    
    if not trades:
        return {"error": "No trades generated", "basis_data": basis_df}
    
    trades_df = pd.DataFrame(trades)
    
    # Metrics
    winning = trades_df[trades_df["pnl_bps"] > 0]
    losing = trades_df[trades_df["pnl_bps"] <= 0]
    
    return {
        "trades": trades_df,
        "basis_history": basis_df,
        "metrics": {
            "total_trades": len(trades_df),
            "win_rate": len(winning) / len(trades_df),
            "avg_win_bps": winning["pnl_bps"].mean() if len(winning) > 0 else 0,
            "avg_loss_bps": losing["pnl_bps"].mean() if len(losing) > 0 else 0,
            "total_pnl_bps": trades_df["pnl_bps"].sum(),
            "avg_days_held": trades_df["days_held"].mean(),
            "profit_factor": abs(winning["pnl_bps"].sum() / losing["pnl_bps"].sum()) if len(losing) > 0 and losing["pnl_bps"].sum() != 0 else 0,
            "max_drawdown_bps": trades_df["pnl_bps"].cumsum().cummax().sub(trades_df["pnl_bps"].cumsum()).max(),
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "sell_basis_trades": len(trades_df[trades_df["direction"] == "SELL_BASIS"]),
            "buy_basis_trades": len(trades_df[trades_df["direction"] == "BUY_BASIS"]),
        }
    }


def scan_basis_opportunities(symbols: list[str] = None) -> pd.DataFrame:
    """
    Scan multiple symbols for basis trading opportunities.
    Returns symbols where |z-score| > 1.5 (approaching entry).
    """
    if symbols is None:
        # Get symbols that have futures data
        futures = load_futures_data(days=7)
        if futures.empty:
            return pd.DataFrame()
        
        # Find the base symbol column
        cols_lower = {c.lower(): c for c in futures.columns}
        sym_col = next((cols_lower[k] for k in ['base_symbol','symbol','underlying'] if k in cols_lower), None)
        if sym_col:
            symbols = futures[sym_col].unique().tolist()[:30]
        else:
            return pd.DataFrame()
    
    results = []
    for sym in symbols:
        basis_df = compute_basis_history(sym, days=60)
        if basis_df.empty:
            continue
        
        latest = basis_df.iloc[-1]
        z = latest.get("basis_zscore", 0)
        if pd.isna(z):
            continue
        
        if abs(z) > 1.0:
            signal = "SELL_BASIS" if z > 2 else "BUY_BASIS" if z < -2 else "WATCH"
            results.append({
                "symbol": sym,
                "spot": latest["spot_close"],
                "futures": latest["futures_close"],
                "basis_pct": latest["basis_pct"],
                "fair_basis_pct": latest.get("fair_basis_pct", 0),
                "excess_pct": latest.get("excess_basis_pct", 0),
                "zscore": z,
                "days_to_expiry": latest.get("days_to_expiry", 0),
                "contract": latest.get("contract_month", ""),
                "signal": signal,
                "date": str(latest["date"]),
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values("zscore", key=abs, ascending=False).reset_index(drop=True)
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_basis.py`:

### Tab 1: Basis Monitor
```
For selected symbol:
├── Spot vs Futures price chart (dual line, same axis)
├── Basis % chart below (bar chart, green=premium, red=discount)
├── Basis z-score chart with ±2σ bands (entry zones shaded)
├── Fair basis line overlay (KIBOR-implied)
├── Current metrics cards: Spot, Futures, Basis%, Fair%, Excess%, Z-score, DTE
├── Signal badge: SELL_BASIS / BUY_BASIS / HOLD
├── OI card (if available)
└── Contract month selector (front/next/far)
```

### Tab 2: Backtest
```
├── Symbol selector (only symbols with futures)
├── Parameters:
│   ├── Entry z-score: slider 1.5-3.0 (default 2.0)
│   ├── Exit z-score: slider 0.3-1.0 (default 0.5)
│   ├── Max hold days: slider 5-30 (default 15)
│   ├── Stop loss: slider 50-500 bps (default 200)
│   └── Lookback: 6M / 1Y / 2Y
├── [Run Backtest]
├── Metrics: Trades, Win Rate, PF, Total P&L (bps), Avg Hold, MaxDD
├── Equity curve (cumulative bps)
├── Basis z-score chart with entry/exit markers
├── Exit reason breakdown
└── Trade log
```

### Tab 3: Opportunity Scanner
```
├── Scan all symbols with active futures
├── Table: Symbol | Spot | Futures | Basis% | Fair% | Excess% | Z-score | DTE | Signal
├── Color: red=SELL_BASIS, green=BUY_BASIS, yellow=WATCH
├── Sort by |z-score|
└── Calendar: expiry dates for current month contracts
```

### Tab 4: Basis Research
```
├── Basis term structure: front vs next vs far month basis
├── Basis vs OI scatter (does high OI = tighter basis?)
├── Basis seasonality: does basis widen before dividends? earnings?
├── Historical basis distribution per symbol
├── Convergence speed: how fast does basis normalize?
├── Carry trade analysis: earn premium by selling overpriced futures
└── Methodology + academic references
```

### Key chart — Basis z-score with entry zones:
```python
import plotly.graph_objects as go

fig = go.Figure()

# Z-score line
fig.add_trace(go.Scatter(x=df["date"], y=df["basis_zscore"],
    line=dict(color="#C8A96E", width=1.5), name="Basis Z-score"))

# Entry zones
fig.add_hrect(y0=2, y1=4, fillcolor="#EF4444", opacity=0.1,
              annotation_text="SELL BASIS ZONE", line_width=0)
fig.add_hrect(y0=-4, y1=-2, fillcolor="#22C55E", opacity=0.1,
              annotation_text="BUY BASIS ZONE", line_width=0)

# Zero line and thresholds
fig.add_hline(y=0, line_color="#555", line_dash="dot")
fig.add_hline(y=2, line_color="#EF4444", line_dash="dash", opacity=0.5)
fig.add_hline(y=-2, line_color="#22C55E", line_dash="dash", opacity=0.5)

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=350,
                  yaxis_title="Basis Z-score")
```

## Step 3: Add to sidebar

```python
st.page_link("page_views/strategy_basis.py", label="Futures Basis Arb", icon="⚖️")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test basis computation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.basis_strategy import compute_basis_history, scan_basis_opportunities, get_kibor_rate

print(f'KIBOR rate: {get_kibor_rate():.2%}')

# Compute basis for HUBC
df = compute_basis_history('HUBC', days=180)
print(f'\nBasis history: {len(df)} rows')
if not df.empty:
    print(df[['date','spot_close','futures_close','basis_pct','fair_basis_pct','excess_basis_pct','basis_zscore']].tail(10).to_string())

# Scan opportunities
print('\n=== OPPORTUNITIES ===')
opp = scan_basis_opportunities()
if not opp.empty:
    print(opp[['symbol','basis_pct','fair_basis_pct','zscore','signal','days_to_expiry']].to_string())
else:
    print('No opportunities found')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.basis_strategy import backtest_basis_strategy

result = backtest_basis_strategy('HUBC', entry_zscore=2.0, days=365)
if 'error' not in result:
    m = result['metrics']
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Profit Factor: {m[\"profit_factor\"]:.2f}')
    print(f'Total P&L: {m[\"total_pnl_bps\"]:.0f} bps')
    print(f'Avg Hold: {m[\"avg_days_held\"]:.1f} days')
    print(f'Max DD: {m[\"max_drawdown_bps\"]:.0f} bps')
    print(f'Exit reasons: {m[\"exit_reasons\"]}')
else:
    print(result)
"
```

## IMPORTANT NOTES

1. **Futures table structure varies** — code discovers columns dynamically. READ the discovery output first.
2. **KIBOR as risk-free rate** — used to compute fair basis (cost of carry). Falls back to 10.5% if not found.
3. **PSX expiry = last Thursday of month** — the get_expiry_date function computes this.
4. **Z-score uses 20-day rolling window** — long enough for stable estimate, short enough for responsiveness.
5. **Entry at ±2σ, exit at ±0.5σ** — conservative entry, patient exit for full mean-reversion.
6. **NEAR_EXPIRY exit** — auto-exit 3 days before expiry to avoid delivery risk.
7. **P&L in basis points** — not price points. A 50bps basis change on 1M PKR notional = 5,000 PKR.
8. **OI integration** — if OI data is available, high OI + extreme basis = higher conviction.
9. **No TA libraries** — all math in numpy/pandas.
10. **Scanner only shows |z-score| > 1.0** — captures approaching opportunities (watch) and active signals (trade).
11. **This is a PAIRS trade** — always long one leg, short the other. Market-neutral by design.
12. **PSX edge:** Settlement is PHYSICAL DELIVERY. This guarantees convergence — the structural edge that makes this strategy work even when statistical arb fails.
